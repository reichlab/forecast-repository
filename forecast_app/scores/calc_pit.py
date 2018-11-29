import enum
import logging
from collections import defaultdict

from more_itertools import peekable

from forecast_app.models import ForecastData, ScoreValue
from forecast_app.scores.state_machine import LineProcessingMachine, InputTuple


logger = logging.getLogger(__name__)


def _calculate_pit_score_values(score, forecast_model):
    """
    Implements the 'Probability Integral Transform (PIT)' score, defined for each predictive distribution (i.e., each
    new target in a forecast) as `(s1 + s2)/2` where s1 is the sum of all bin rows _up to_ the true bin, and s2 is that
    same sum but also including the true bin.

    We use a state machine formalism to represent and implement this program. The diagram is located at
    pit-score-state-machine.png .

    Note that correctly calculating this score can depend on missing bin rows whose values are zero, and therefore are
    not in the database - see [Consider not storing bin rows with zero values #84](https://github.com/reichlab/forecast-repository/issues/84) .
    """
    from forecast_app.scores.definitions import _validate_score_targets_and_data, \
        _timezero_loc_target_pks_to_template_bin  # avoid circular imports


    targets = _validate_score_targets_and_data(forecast_model)  # raises RuntimeError if invalid

    # cache truth values
    tz_loc_targ_pks_to_templ_bin = _timezero_loc_target_pks_to_template_bin(forecast_model.project)

    # calculate scores for all combinations of location and target
    forecast_data_qs = ForecastData.objects \
        .filter(forecast__forecast_model=forecast_model,
                is_point_row=False,
                target__in=targets) \
        .order_by('forecast__id', 'location__id', 'target__id', 'bin_start_incl') \
        .values_list('forecast__id', 'forecast__time_zero__id', 'location__id', 'target__id',
                     'bin_start_incl', 'bin_end_notincl', 'value')

    # Calculate scores for all combinations of location and target. Note that b/c we need to account for missing bin
    # rows whose values are zero, we have some logic to test three cases: a) we passed the missing bin (we test
    # bin_start_incl), or b) the true bin comes after the last forecast bin (we test is_start_new_distribution).
    #
    # Re: iterator() memory usage: see comment in _calc_log_bin_score_values()
    forec_tz_loc_targ_pk_to_error_str = defaultdict(int)  # helps eliminate duplicate warnings
    line_processing_machine = PitLineProcessingMachine(score)
    input_tuple_prev = None  # for tracking distribution transitions (when target changes)
    is_seen_true_bin = False  # ""
    qs_iterator = peekable(forecast_data_qs.iterator())  # peekable -> can see if next bin row starts new distribution
    for forecast_pk, timezero_pk, location_pk, target_pk, bin_start_incl, bin_end_notincl, predicted_value \
            in qs_iterator:
        if predicted_value is None:
            # note: future validation might ensure no bin values are None
            continue  # skip this forecast's contribution to the score

        try:
            true_bin_start_incl, true_bin_end_notincl, true_value = \
                tz_loc_targ_pks_to_templ_bin[timezero_pk][location_pk][target_pk]
            if true_value is None:
                continue  # skip this forecast's contribution to the score
        except KeyError:
            error_key = (forecast_pk, timezero_pk, location_pk, target_pk)
            forec_tz_loc_targ_pk_to_error_str[error_key] += 1
            continue  # skip this forecast's contribution to the score

        input_tuple = InputTuple(forecast_pk, location_pk, target_pk,
                                 bin_start_incl, bin_end_notincl, predicted_value, true_value)
        # we know predicted_value is zero b/c only zero bins are missing from forecast data:
        input_tuple_true_bin = InputTuple(forecast_pk, location_pk, target_pk,
                                          true_bin_start_incl, true_bin_end_notincl, 0.0, true_value)
        is_start_new_distribution = (input_tuple_prev is None) or (target_pk != input_tuple_prev.target_pk)
        if is_start_new_distribution:
            is_seen_true_bin = False

        is_seen_true_bin = (bin_start_incl == true_bin_start_incl) or is_seen_true_bin

        # handle missing true bin by processing it before the current one. case a) - passed it:
        if (not is_seen_true_bin) and (bin_start_incl > true_bin_start_incl):
            line_processing_machine.set_input_tuple(input_tuple_true_bin)
            line_processing_machine.advance()

        # process the current bin as usual
        line_processing_machine.set_input_tuple(input_tuple)
        line_processing_machine.advance()
        input_tuple_prev = input_tuple

        # case b). NB: must be done *after* current bin row is processed. Also NB: assumes there is a true bin for me
        try:
            target_pk_next = qs_iterator.peek()[3]
            if target_pk_next and (target_pk_next != target_pk) and (not is_seen_true_bin):
                line_processing_machine.set_input_tuple(input_tuple_true_bin)
                line_processing_machine.advance()
        except StopIteration:
            pass

    # print errors
    for (forecast_pk, timezero_pk, location_pk, target_pk) in sorted(forec_tz_loc_targ_pk_to_error_str.keys()):
        count = forec_tz_loc_targ_pk_to_error_str[forecast_pk, timezero_pk, location_pk, target_pk]
        logger.warning("_calculate_pit_score_values(): missing {} truth value(s): "
                       "forecast_pk={}, timezero_pk={}, location_pk={}, target_pk={}"
                       .format(count, forecast_pk, timezero_pk, location_pk, target_pk))


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
