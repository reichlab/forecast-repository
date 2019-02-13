import enum
import logging

from forecast_app.models import ScoreValue
from forecast_app.scores.state_machine import LineProcessingMachine, _calculate_bin_score_values


logger = logging.getLogger(__name__)


def _calculate_pit_score_values(score, forecast_model):
    """
    Implements the 'Probability Integral Transform (PIT)' score, defined for each predictive distribution (i.e., each
    new target in a forecast) as `(s1 + s2)/2` where s1 is the sum of all bin row values _up to_ the true bin, and s2
    is that same sum but also including the true bin.

    We use a state machine formalism to represent and implement this program. The diagram is located at
    pit-score-state-machine.png .

    Note that correctly calculating this score can depend on missing bin rows whose values are zero, and therefore are
    not in the database - see [Consider not storing bin rows with zero values #84](https://github.com/reichlab/forecast-repository/issues/84) .
    """
    line_processing_machine = PitLineProcessingMachine(score)
    _calculate_bin_score_values(forecast_model, line_processing_machine)


#
# PitLineProcessingMachine
#

@enum.unique
class PitMachineState(enum.Enum):
    distribution_start = 0
    pre_match_collecting = 1
    post_match_skipping_to_next_distribution = 2


class PitLineProcessingMachine(LineProcessingMachine):
    """
    A state machine that processes forecast bin lines and saves score values accordingly. Use: call set_input_tuple()
    and then advance() for each bin row. Assumes lines are ordered by:

            'forecast__id', 'location__id', 'target__id', 'bin_start_incl' - see query in _calculate_pit_score_values()

    Implementation: We start collecting values with the first bin row up to the true bin, then calculate the score as
    described in _calculate_pit_score_values(), and then finally skip the remaining post-truth bins until the next
    target/predictive distribution. We use one extended state variable:

    - values_pre_match: a list of values up to the true bin
    """


    def __init__(self, score):
        super().__init__(score)
        self.values_pre_match = []  # values (floats) seen up to a match
        self.transition_to_state(PitMachineState.distribution_start)  # initial state


    def advance(self):
        # logger.debug('advance(): input state={}'.format(self.state))

        # in distribution_start - 2 cases
        if (self.state == PitMachineState.distribution_start) \
                and not self.is_match():  # case a)
            self.transition_to_state(PitMachineState.pre_match_collecting)
        elif self.state == PitMachineState.distribution_start:  # self.is_match(). case b)
            self.save_score()  # transition action
            self.transition_to_state(PitMachineState.post_match_skipping_to_next_distribution)

        # in pre_match_collecting - 4 cases
        elif (self.state == PitMachineState.pre_match_collecting) \
                and not self.is_start_new_distribution() \
                and not self.is_match():  # case a)
            self.transition_to_state(PitMachineState.pre_match_collecting)
        elif (self.state == PitMachineState.pre_match_collecting) \
                and not self.is_start_new_distribution() \
                and self.is_match():  # case b)
            self.save_score()  # transition action
            self.transition_to_state(PitMachineState.post_match_skipping_to_next_distribution)
        elif (self.state == PitMachineState.pre_match_collecting) \
                and self.is_start_new_distribution() \
                and not self.is_match():  # case c)
            self.clear_state_vars()  # transition action
            self.transition_to_state(PitMachineState.pre_match_collecting)
        elif (self.state == PitMachineState.pre_match_collecting) \
                and self.is_start_new_distribution() \
                and self.is_match():  # case d)
            self.save_score()  # transition action - must come first!
            self.clear_state_vars()
            self.transition_to_state(PitMachineState.post_match_skipping_to_next_distribution)

        # in post_match_skipping_to_next_distribution - 3 cases
        elif (self.state == PitMachineState.post_match_skipping_to_next_distribution) \
                and not self.is_start_new_distribution():  # case a)
            self.transition_to_state(PitMachineState.post_match_skipping_to_next_distribution)
        elif (self.state == PitMachineState.post_match_skipping_to_next_distribution) \
                and self.is_start_new_distribution() \
                and not self.is_match():  # case b)
            self.clear_state_vars()  # transition action
            self.transition_to_state(PitMachineState.pre_match_collecting)
        elif (self.state == PitMachineState.post_match_skipping_to_next_distribution) \
                and self.is_start_new_distribution() \
                and self.is_match():  # case c)
            self.clear_state_vars()  # transition action - must come first!
            self.save_score()
            self.transition_to_state(PitMachineState.post_match_skipping_to_next_distribution)


    def transition_to_state(self, new_state):
        # logger.debug('transition_to_state(): {} -> {}'.format(self.state, new_state))

        if new_state == PitMachineState.distribution_start:
            self.clear_state_vars()
        elif new_state == PitMachineState.pre_match_collecting:
            self.add_value_to_pre()
        elif new_state == PitMachineState.post_match_skipping_to_next_distribution:
            pass  # no entry actions
        else:
            raise RuntimeError('bad new_state: {}'.format(new_state))

        # set the new state
        self.state = new_state


    def clear_state_vars(self):
        # logger.debug('clear_state_vars()')
        self.values_pre_match = []


    def add_value_to_pre(self):
        self.values_pre_match.append(self.input_tuple_current.predicted_value)
        # logger.debug('add_value_to_pre(): {} -> {}'.format(self.input_tuple_current.predicted_value, self.values_pre_match))


    def save_score(self):
        matching_input_tuple = self.input_tuple_current
        values_pre_match_sum = sum(self.values_pre_match)
        score_value = ((values_pre_match_sum * 2) + matching_input_tuple.predicted_value) / 2

        # logger.debug('save_score: {}'.format( [matching_input_tuple, self.values_pre_match, '.', values_pre_match_sum, score_value]))
        ScoreValue.objects.create(forecast_id=matching_input_tuple.forecast_pk,
                                  location_id=matching_input_tuple.location_pk,
                                  target_id=matching_input_tuple.target_pk,
                                  score=self.score, value=score_value)
