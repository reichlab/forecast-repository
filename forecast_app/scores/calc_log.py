import collections
import logging
import math
from collections import namedtuple

from transitions import Machine, State

from forecast_app.models import ForecastData, ScoreValue
from forecast_app.models.data import CDCData


logger = logging.getLogger(__name__)

logging.getLogger("transitions").setLevel(logging.WARNING)  # o/w too noisy


#
# Implementation notes:
#
# We use a sliding window of num_bins_one_side via two extended state variables:
#
# - values_pre_match_dq: collects at most the previous num_bins_one_side pre-match values seen (it is a
#   https://docs.python.org/3.6/library/collections.html#collections.deque )
# - values_post_match: collects the post-match values. note that it includes the matching value itself
#
# Once we fill values_post_match with num_bins_one_side + 1 items, we can save the score and then skip to the next
# target.
#
# We use a state machine formalism to represent and implement this program. The library we chose is
# https://github.com/pytransitions/transitions . The diagram is located at multi-bin-score-state-machine.JPG .
#
#
# Re: iterator() memory usage: django 1.1 uses django.db.models.sql.constants.GET_ITERATOR_CHUNK_SIZE, which is
# hard-coded to 100. django 2 allows passing in: def iterator(self, chunk_size=2000). note that iterator() takes
# care of making postgres use server-side cursors for iteration without our having to deal with the backend database.
#


def _calc_log_bin(score, forecast_model, num_bins_one_side):
    """
    Implements the 'log_single_bin' (AKA 'Log score (single bin)') and 'log_multi_bin' (AKA 'Log score (multi bin)')
    scores per:
        - https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin
    or
        - https://github.com/reichlab/flusight/wiki/Scoring#3-log-score-multi-bin
    as controlled by num_bins.

    :param num_bins_one_side: (AKA the 'window' per above link) is number of bins rows *on one side* of the matching bin
        row to sum when calculating the score. thus the total number of bins in the 'window' centered on the matching
        bin row is: (2 * num_bins) + 1 . pass zero to get single bin behavior.
    """
    from forecast_app.scores.definitions import _validate_score_targets_and_data, \
        _timezero_loc_target_pks_to_truth_values, _validate_truth, _validate_predicted_value  # avoid circular imports


    try:
        targets = _validate_score_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)

    # cache truth values: [location_name][target_name][timezero_date]:
    timezero_loc_target_pks_to_truth_values = _timezero_loc_target_pks_to_truth_values(forecast_model)

    # calculate scores for all combinations of location and target
    forecast_data_qs = ForecastData.objects \
        .filter(forecast__forecast_model=forecast_model,
                row_type=CDCData.BIN_ROW_TYPE,
                target__in=targets) \
        .order_by('forecast__id', 'location__id', 'target__id', 'bin_start_incl') \
        .values_list('forecast__id', 'forecast__time_zero__id', 'location__id', 'target__id',
                     'bin_start_incl', 'bin_end_notincl', 'value')

    line_processing_machine = LineProcessingMachine(score, num_bins_one_side)
    for forecast_pk, timezero_pk, location_pk, target_pk, bin_start_incl, bin_end_notincl, predicted_value \
            in forecast_data_qs.iterator():
        try:
            true_value = _validate_truth(timezero_loc_target_pks_to_truth_values, forecast_model, forecast_pk,
                                         timezero_pk, location_pk, target_pk)
            _validate_predicted_value(forecast_model, forecast_pk, timezero_pk, location_pk, target_pk,
                                      predicted_value)
            input_tuple = InputTuple(forecast_pk, location_pk, target_pk,
                                     bin_start_incl, bin_end_notincl,
                                     predicted_value, true_value)
            line_processing_machine.set_input_tuple(input_tuple)  # todo a way to do this via advance()?
            line_processing_machine.advance()

        except RuntimeError as rte:
            logger.warning(rte)
            continue  # skip this forecast's contribution to the score

    # handle the case where we fall off the end and haven't saved the score yet
    # todo is there a way to do this all in the machine without having to call it externally like this?
    line_processing_machine.handle_post_to_eof()


#
# LineProcessingMachine
#

# used to track previous and current inputs - passed to LineProcessingMachine.set_input_tuple()
InputTuple = namedtuple('InputTuple', ['forecast_pk', 'location_pk', 'target_pk',
                                       'bin_start_incl', 'bin_end_notincl',
                                       'predicted_value', 'true_value'])


class LineProcessingMachine(Machine):

    def __init__(self, score, num_bins_one_side):
        """
        :param score: the Score to save into
        :param num_bins_one_side: as documented in _calc_log_bin()
        """
        self.score = score
        self.num_bins_one_side = num_bins_one_side

        # extended state variables
        self.values_pre_match_dq = collections.deque([], self.num_bins_one_side)  # default is moving from right to left
        self.values_post_match = []

        # input (and past) variables passed to set_input_tuple() - InputTuple instances
        self.input_tuple_previous = None
        self.input_tuple_current = None

        # define the machine
        states = [
            State(name='distribution_start', on_enter='clear_state_vars'),
            State(name='pre_match_collecting', on_enter='add_value_to_pre'),
            State(name='post_match_collecting', on_enter='add_value_to_post'),
            State(name='post_match_end_of_file', on_enter='save_score'),  # NB: state transitioned to manually @ EOF
            State(name='skipping_to_next_distribution'),
        ]
        super().__init__(self, states=states, initial='distribution_start')

        # add 'distribution_start' transitions
        self.add_transition(trigger='advance',
                            source='distribution_start', dest='post_match_collecting',
                            conditions='is_match')
        self.add_transition(trigger='advance',
                            source='distribution_start', dest='pre_match_collecting',
                            unless='is_match')

        # add 'pre_match_collecting' transitions
        self.add_transition(trigger='advance',
                            source='pre_match_collecting', dest='post_match_collecting',
                            conditions='is_match')
        self.add_transition(trigger='advance',
                            source='pre_match_collecting', dest='pre_match_collecting',
                            unless='is_match')

        # add 'post_match_collecting' transitions
        self.add_transition(trigger='advance',
                            source='post_match_collecting', dest='skipping_to_next_distribution',
                            conditions='is_post_full',
                            after='save_score')
        self.add_transition(trigger='advance',
                            source='post_match_collecting', dest='post_match_collecting',
                            unless='is_post_full')
        self.add_transition(trigger='post_to_eof',
                            source='post_match_collecting', dest='post_match_end_of_file')

        # 'post_match_end_of_file': no transitions

        # add 'skipping_to_next_distribution' transitions
        self.add_transition(trigger='advance',
                            source='skipping_to_next_distribution', dest='distribution_start',
                            conditions='is_start_new_distribution')
        self.add_transition(trigger='advance',
                            source='skipping_to_next_distribution', dest='skipping_to_next_distribution',
                            unless='is_start_new_distribution')


    def set_input_tuple(self, input_tuple):
        """
        Called (externally) for each new input bin line before advancing the machine, sets my input variables
        accordingly.
        """
        self.input_tuple_previous = self.input_tuple_current
        self.input_tuple_current = input_tuple


    def clear_state_vars(self):
        self.values_pre_match_dq.clear()
        self.values_post_match = []

        self.input_tuple_previous = None
        self.input_tuple_current = None


    def is_start_new_distribution(self):
        """
        :return: True if starting a new target (i.e., a new predictive distribution). assumes lines are ordered by:
            'forecast__id', 'location__id', 'target__id', 'bin_start_incl' - see _calc_log_bin()
        """
        return self.input_tuple_current.target_pk != self.input_tuple_previous.target_pk


    def is_match(self):
        if self.input_tuple_current.true_value is None:
            is_truth_in_bin = (self.input_tuple_current.bin_start_incl is None) or \
                              (self.input_tuple_current.bin_end_notincl is None)
        else:
            is_truth_in_bin = self.input_tuple_current.bin_start_incl \
                              <= self.input_tuple_current.true_value \
                              < self.input_tuple_current.bin_end_notincl
        return is_truth_in_bin


    def is_post_full(self):
        is_post_full = len(self.values_post_match) == (self.num_bins_one_side + 1)
        return is_post_full


    def add_value_to_pre(self):
        self.values_pre_match_dq.append(self.input_tuple_current.predicted_value)


    def add_value_to_post(self):
        self.values_post_match.append(self.input_tuple_current.predicted_value)


    def save_score(self):
        from forecast_app.scores.definitions import LOG_SINGLE_BIN_NEGATIVE_INFINITY  # avoid circular imports


        try:
            if self.input_tuple_current.true_value is None:
                # this is the case where the true value is None. in this case, the score should degenerate to the
                # num_bins_one_side=0 'Log score (single bin)' calculation. b/c we append to this, the first value
                # in values_post_match is the predicted value for the matching bin
                value_sum = self.values_post_match[0]
            else:
                value_sum = sum(list(self.values_pre_match_dq) + self.values_post_match)
            score_value = math.log(value_sum)
        except ValueError:  # math.log(0) -> ValueError: math domain error
            # implements the logic: "clip Math.log(0) to -999 instead of its real value (-Infinity)"
            # from: https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin
            score_value = LOG_SINGLE_BIN_NEGATIVE_INFINITY

        ScoreValue.objects.create(forecast_id=self.input_tuple_current.forecast_pk,
                                  location_id=self.input_tuple_current.location_pk,
                                  target_id=self.input_tuple_current.target_pk,
                                  score=self.score, value=score_value)


    def handle_post_to_eof(self):
        """
        Handles the EOF case - see multi-bin-score-state-machine.JPG .
        """
        if self.state == 'post_match_collecting':
            self.post_to_eof()
