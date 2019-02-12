import collections
import enum
import logging
import math
from enum import Enum

from forecast_app.models import ForecastData, ScoreValue
from forecast_app.scores.state_machine import LineProcessingMachine, InputTuple


logger = logging.getLogger(__name__)


def _calc_log_bin_score_values(score, forecast_model, num_bins_one_side):
    """
    Implements the 'log_single_bin' (AKA 'Log score (single bin)') and 'log_multi_bin' (AKA 'Log score (multi bin)')
    scores per:
        - https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin
    or
        - https://github.com/reichlab/flusight/wiki/Scoring#3-log-score-multi-bin
    as controlled by num_bins.

    We use a state machine formalism to represent and implement this program. The diagram is located at
    multi-bin-score-state-machine.png .

    Note that correctly calculating this score can depend on missing bin rows whose values are zero, and therefore are
    not in the database - see [Consider not storing bin rows with zero values #84](https://github.com/reichlab/forecast-repository/issues/84) .

    :param num_bins_one_side: (AKA the 'window' per above link) is number of bins rows *on one side* of the matching bin
        row to sum when calculating the score. thus the total number of bins in the 'window' centered on the matching
        bin row is: (2 * num_bins) + 1 . pass zero to get single bin behavior.
    """
    from forecast_app.scores.definitions import _validate_score_targets_and_data, \
        _timezero_loc_target_pks_to_truth_values, _validate_truth  # avoid circular imports


    try:
        targets = _validate_score_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)
        return

    # cache truth values: [location_name][target_name][timezero_date]:
    tz_loc_targ_pks_to_truth_vals = _timezero_loc_target_pks_to_truth_values(forecast_model)

    # calculate scores for all combinations of location and target
    forecast_data_qs = ForecastData.objects \
        .filter(forecast__forecast_model=forecast_model,
                is_point_row=False,
                target__in=targets) \
        .order_by('forecast__id', 'location__id', 'target__id', 'bin_start_incl') \
        .values_list('forecast__id', 'forecast__time_zero__id', 'location__id', 'target__id',
                     'bin_start_incl', 'bin_end_notincl', 'value')

    # calculate scores for all combinations of location and target
    # Re: iterator() memory usage: django 1.1 uses django.db.models.sql.constants.GET_ITERATOR_CHUNK_SIZE, which is
    # hard-coded to 100. django 2 allows passing in: def iterator(self, chunk_size=2000). note that iterator() takes
    # care of making postgres use server-side cursors for iteration without our having to deal with the backend database.

    # collect errors so we don't log thousands of duplicate messages. dict format:
    #   {(forecast_pk, timezero_pk, location_pk, target_pk): error_string, ...}:
    forec_tz_loc_targ_pk_to_error_str = {}  # helps eliminate duplicate warnings
    line_processing_machine = LogLineProcessingMachine(score, num_bins_one_side)
    for forecast_pk, timezero_pk, location_pk, target_pk, bin_start_incl, bin_end_notincl, predicted_value \
            in forecast_data_qs.iterator():
        # NB: we do NOT check true_value is None here, unlike _calculate_error_score_values(), b/c that condition is
        # used by LogLineProcessingMachine.save_score() and LineProcessingMachine.is_match()
        if predicted_value is None:
            # note: future validation might ensure no bin values are None
            continue  # skip this forecast's contribution to the score

        true_value, error_string = _validate_truth(tz_loc_targ_pks_to_truth_vals,
                                                   timezero_pk, location_pk, target_pk)
        if error_string:
            error_key = (forecast_pk, timezero_pk, location_pk, target_pk)
            if error_key not in forec_tz_loc_targ_pk_to_error_str:
                forec_tz_loc_targ_pk_to_error_str[error_key] = error_string
            continue  # skip this forecast's contribution to the score

        input_tuple = InputTuple(forecast_pk, location_pk, target_pk, bin_start_incl, bin_end_notincl,
                                 predicted_value, true_value)
        line_processing_machine.set_input_tuple(input_tuple)
        line_processing_machine.advance()

    # handle the case where we fall off the end and haven't saved the score yet
    line_processing_machine.handle_post_to_eof()

    # print errors
    for (forecast_pk, timezero_pk, location_pk, target_pk), error_string in forec_tz_loc_targ_pk_to_error_str.items():
        logger.warning("_calc_log_bin_score_values(): truth validation error: {!r}: "
                       "score_pk={}, forecast_model_pk={}, forecast_pk={}, timezero_pk={}, location_pk={}, target_pk={}"
                       .format(error_string, score.pk, forecast_model.pk, forecast_pk, timezero_pk, location_pk,
                               target_pk))


#
# LogLineProcessingMachine
#

@enum.unique
class LogMachineState(Enum):
    distribution_start = 0
    pre_match_collecting = 1
    post_match_collecting = 2
    skipping_to_next_distribution = 3
    post_match_end_of_file = 4


class LogLineProcessingMachine(LineProcessingMachine):
    """
    A state machine that processes forecast bin lines and saves score values accordingly. Use: call set_input_tuple()
    and then advance() for each bin row. Assumes lines are ordered by:

            'forecast__id', 'location__id', 'target__id', 'bin_start_incl' - see query in _calc_log_bin_score_values()

    Implementation: We use a sliding window of num_bins_one_side via two extended state variables:

    - values_pre_match_dq: collects at most the previous num_bins_one_side pre-match values seen (it is a
      https://docs.python.org/3.6/library/collections.html#collections.deque )
    - values_tuples_post_match: collects the post-match values. note that it includes the matching value itself

    Once we fill values_tuples_post_match with num_bins_one_side + 1 items, we can save the score and then skip to the next
    target.
    """


    def __init__(self, score, num_bins_one_side):
        super().__init__(score)
        self.num_bins_one_side = num_bins_one_side
        self.values_pre_match_dq = collections.deque([], self.num_bins_one_side)  # values (floats) seen up to a match
        self.values_tuples_post_match = []  # /InputTuples/, not floats, so we can get the matching distribution's PKs
        self.transition_to_state(LogMachineState.distribution_start)  # initial state


    def advance(self):
        # logger.debug('advance(): input state={}'.format(self.state))

        # in distribution_start - 2 cases
        if (self.state == LogMachineState.distribution_start) \
                and not self.is_match():  # case a)
            self.transition_to_state(LogMachineState.pre_match_collecting)
        elif self.state == LogMachineState.distribution_start:  # self.is_match(). case b)
            self.transition_to_state(LogMachineState.post_match_collecting)

        # in pre_match_collecting - 4 cases
        elif (self.state == LogMachineState.pre_match_collecting) \
                and not self.is_start_new_distribution() \
                and not self.is_match():  # case a)
            self.transition_to_state(LogMachineState.pre_match_collecting)
        elif (self.state == LogMachineState.pre_match_collecting) \
                and self.is_match():  # cases b) and d)
            self.transition_to_state(LogMachineState.post_match_collecting)
        elif (self.state == LogMachineState.pre_match_collecting) \
                and self.is_start_new_distribution() \
                and not self.is_match():  # case c)
            self.transition_to_state(LogMachineState.pre_match_collecting)

        # in post_match_collecting - 4 cases
        elif (self.state == LogMachineState.post_match_collecting) \
                and not self.is_start_new_distribution() \
                and not self.is_post_full():  # case a)
            self.transition_to_state(LogMachineState.post_match_collecting)
        elif (self.state == LogMachineState.post_match_collecting) \
                and not self.is_start_new_distribution() \
                and self.is_post_full():  # case b)
            self.save_score()  # transition action
            self.transition_to_state(LogMachineState.skipping_to_next_distribution)
        elif (self.state == LogMachineState.post_match_collecting) \
                and self.is_start_new_distribution() \
                and not self.is_match():  # case c)
            self.save_score()  # transition action
            self.clear_state_vars()  # ""
            self.transition_to_state(LogMachineState.pre_match_collecting)
        elif (self.state == LogMachineState.post_match_collecting) \
                and self.is_start_new_distribution() \
                and self.is_match():  # case d)
            self.save_score()  # transition action
            self.clear_state_vars()  # ""
            self.transition_to_state(LogMachineState.post_match_collecting)

        # in skipping_to_next_distribution - 3 cases
        elif (self.state == LogMachineState.skipping_to_next_distribution) \
                and not self.is_start_new_distribution() \
                and not self.is_match():  # case a)
            self.transition_to_state(LogMachineState.skipping_to_next_distribution)
        elif (self.state == LogMachineState.skipping_to_next_distribution) \
                and self.is_start_new_distribution() \
                and not self.is_match():  # case b)
            self.clear_state_vars()  # transition action
            self.transition_to_state(LogMachineState.pre_match_collecting)
        elif (self.state == LogMachineState.skipping_to_next_distribution) \
                and self.is_start_new_distribution() \
                and self.is_match():  # case c)
            self.clear_state_vars()  # transition action
            self.transition_to_state(LogMachineState.post_match_collecting)

        else:
            raise RuntimeError('advance(): no matching transitions: {}'.format(self.state))


    def transition_to_state(self, new_state):
        # logger.debug('transition_to_state(): {} -> {}'.format(self.state, new_state))

        if new_state == LogMachineState.distribution_start:
            self.clear_state_vars()
        elif new_state == LogMachineState.pre_match_collecting:
            self.add_value_to_pre()
        elif new_state == LogMachineState.post_match_collecting:
            self.add_value_to_post()
        elif new_state == LogMachineState.skipping_to_next_distribution:
            pass  # no entry actions
        elif new_state == LogMachineState.post_match_end_of_file:
            self.save_score()
        else:
            raise RuntimeError('bad new_state: {}'.format(new_state))

        # set the new state
        self.state = new_state


    def clear_state_vars(self):
        # logger.debug('clear_state_vars()')
        self.values_pre_match_dq.clear()
        self.values_tuples_post_match = []


    def is_post_full(self):
        is_post_full = len(self.values_tuples_post_match) == (self.num_bins_one_side + 1)
        return is_post_full


    def add_value_to_pre(self):
        # logger.debug('add_value_to_pre(): {}'.format(self.input_tuple_current.predicted_value))
        self.values_pre_match_dq.append(self.input_tuple_current.predicted_value)


    def add_value_to_post(self):
        # logger.debug('add_value_to_post(): {}'.format(self.input_tuple_current.predicted_value))
        self.values_tuples_post_match.append(self.input_tuple_current)


    def save_score(self):
        from forecast_app.scores.definitions import LOG_SINGLE_BIN_NEGATIVE_INFINITY  # avoid circular imports


        matching_input_tuple = self.values_tuples_post_match[0]  # the matching one is first b/c we append
        try:
            if matching_input_tuple.true_value is None:
                # in this case, the score should degenerate to the num_bins_one_side=0 'Log score (single bin)'
                # calculation
                value_sum = matching_input_tuple.predicted_value
            else:
                value_sum = sum(list(self.values_pre_match_dq) +
                                [input_tuple.predicted_value for input_tuple in self.values_tuples_post_match])
            score_value = math.log(value_sum)
        except ValueError:  # math.log(0) -> ValueError: math domain error
            # implements the logic: "clip Math.log(0) to -999 instead of its real value (-Infinity)"
            # from: https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin
            score_value = LOG_SINGLE_BIN_NEGATIVE_INFINITY

        # logger.debug('save_score: {}'.format([self.values_pre_match_dq, self.values_tuples_post_match, '.', value_sum, score_value]))
        ScoreValue.objects.create(forecast_id=matching_input_tuple.forecast_pk,
                                  location_id=matching_input_tuple.location_pk,
                                  target_id=matching_input_tuple.target_pk,
                                  score=self.score, value=score_value)


    def handle_post_to_eof(self):
        # logger.debug('handle_post_to_eof(): self.state={}'.format(self.state))
        if self.state == LogMachineState.post_match_collecting:
            self.transition_to_state(LogMachineState.post_match_end_of_file)
