import logging
from abc import ABC, abstractmethod
from collections import namedtuple, defaultdict

from more_itertools import peekable

from forecast_app.models import ForecastData


logger = logging.getLogger(__name__)


#
# _calculate_bin_score_values()
#

def _calculate_bin_score_values(forecast_model, line_processing_machine):
    """
    Helper function that handles common logic for log and pit scores.

    :param line_processing_machine: either a LogLineProcessingMachine or PitLineProcessingMachine. this class determines
        important logic that is used
    """

    from forecast_app.scores.calc_log import LogLineProcessingMachine
    from forecast_app.scores.definitions import _validate_score_targets_and_data, \
        _timezero_loc_target_pks_to_template_bin  # avoid circular imports


    is_log_score = isinstance(line_processing_machine, LogLineProcessingMachine)  # pit o/w
    try:
        targets = _validate_score_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)
        return

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
    # rows whose values are zero, we have some logic to test two cases: a) we passed the missing bin (we test
    # bin_start_incl), or b) the true bin comes after the last forecast bin (we test is_start_new_distribution).
    # Re: iterator() memory usage: see comment in _calc_log_bin_score_values()

    # collect errors so we don't log thousands of duplicate messages. dict format:
    #   {(forecast_pk, timezero_pk, location_pk, target_pk): count, ...}:
    forec_tz_loc_targ_pk_to_error_str = defaultdict(int)  # helps eliminate duplicate warnings
    input_tuple_prev = None  # for tracking distribution transitions (when target changes)
    is_seen_true_bin = False  # ""
    qs_iterator = peekable(forecast_data_qs.iterator())  # peekable -> can see if next bin row starts new distribution
    for forecast_pk, timezero_pk, location_pk, target_pk, bin_start_incl, bin_end_notincl, predicted_value \
            in qs_iterator:
        if predicted_value is None:
            # note: future validation might ensure no bin values are None. only valid case: season onset point rows
            continue  # skip this forecast's contribution to the score

        try:
            true_bin_start_incl, true_bin_end_notincl, true_value = \
                tz_loc_targ_pks_to_templ_bin[timezero_pk][location_pk][target_pk]
        except KeyError:
            error_key = (forecast_pk, timezero_pk, location_pk, target_pk)
            forec_tz_loc_targ_pk_to_error_str[error_key] += 1
            continue  # skip this forecast's contribution to the score

        # NB: for log score we do NOT check true_value is None here, unlike _calculate_error_score_values(), b/c that
        # condition is used by LogLineProcessingMachine.save_score() and LineProcessingMachine.is_match():
        if (not is_log_score) and (true_value is None):
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

        # handle missing true bin by processing it before the current one. case a) - passed it.
        # if true_bin_start_incl == None then we don't advance b/c this case (passed it) doesn't apply
        if (true_bin_start_incl is not None) and (not is_seen_true_bin) and (bin_start_incl > true_bin_start_incl):
            is_seen_true_bin = True
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
                is_seen_true_bin = True
                line_processing_machine.set_input_tuple(input_tuple_true_bin)
                line_processing_machine.advance()
        except StopIteration:
            pass

    if is_log_score:  # handle the case where we fall off the end and haven't saved the score yet
        line_processing_machine.handle_post_to_eof()

    # print errors
    for (forecast_pk, timezero_pk, location_pk, target_pk) in sorted(forec_tz_loc_targ_pk_to_error_str.keys()):
        count = forec_tz_loc_targ_pk_to_error_str[forecast_pk, timezero_pk, location_pk, target_pk]
        logger.warning("_calculate_pit_score_values(): missing {} truth value(s): "
                       "forecast_pk={}, timezero_pk={}, location_pk={}, target_pk={}"
                       .format(count, forecast_pk, timezero_pk, location_pk, target_pk))


#
# InputTuple and LineProcessingMachine
#

# used to track previous and current inputs - passed to LineProcessingMachine.set_input_tuple()
InputTuple = namedtuple('InputTuple', ['forecast_pk', 'location_pk', 'target_pk',
                                       'bin_start_incl', 'bin_end_notincl',
                                       'predicted_value', 'true_value'])


class LineProcessingMachine(ABC):
    """
    An abstract state machine class that supports bin line processing scores like log and pit. It is used to process
    bin lines by calling set_input_tuple() and then advance() for each line. Lines must be sorted in this order:

        'forecast__id', 'location__id', 'target__id', 'bin_start_incl'

    """


    def __init__(self, score) -> None:
        super().__init__()
        self.score = score

        self.input_tuple_previous = None
        self.input_tuple_current = None

        self.state = None


    @abstractmethod
    def advance(self):
        """
        Decides the state to transition to, executing post-transition (but pre-state change) actions.
        """
        pass


    @abstractmethod
    def transition_to_state(self, new_state):
        """
        Saves the new state and handles running any entry actions.
        """
        pass


    @abstractmethod
    def save_score(self):
        """
        Creates a ScoreValue instance.
        """
        pass


    def set_input_tuple(self, input_tuple):
        # logger.debug('set_input_tuple(): {}'.format(input_tuple))
        self.input_tuple_previous = self.input_tuple_current
        self.input_tuple_current = input_tuple


    def is_match(self):
        if self.input_tuple_current.true_value is None:
            is_truth_in_bin = (self.input_tuple_current.bin_start_incl is None) or \
                              (self.input_tuple_current.bin_end_notincl is None)
        else:
            is_truth_in_bin = self.input_tuple_current.bin_start_incl \
                              <= self.input_tuple_current.true_value \
                              < self.input_tuple_current.bin_end_notincl
        return is_truth_in_bin


    def is_start_new_distribution(self):
        """
        :return: True if starting a new target (i.e., a new predictive distribution). assumes lines are ordered by:
            'forecast__id', 'location__id', 'target__id', 'bin_start_incl' - see _calc_log_bin_score_values()
        """
        return self.input_tuple_current.target_pk != self.input_tuple_previous.target_pk
