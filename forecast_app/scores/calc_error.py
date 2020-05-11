import logging
from collections import defaultdict
from itertools import groupby

from forecast_app.models import PointPrediction, ScoreValue, TimeZero


logger = logging.getLogger(__name__)


def _calculate_error_score_values(score, forecast_model, is_absolute_error):
    """
    Implements the 'error' and 'abs_error' scores. Creates ScoreValue instances for the passed args, saving them into
    the passed score. The score is simply `true_value - predicted_value` (optionally passed to abs() based on
    is_absolute_error) for each combination of Unit + Target in forecast_model's project. Runs in the calling thread
    and therefore blocks. Note that this implementation uses a naive approach to calculating scores, iterating over
    truth and forecast tables instead of caching.

    :param score: a Score
    :param forecast_model: a ForecastModel
    :param is_absolute_error: True if abs() should be called
    """
    try:
        from forecast_app.scores.definitions import _validate_score_targets_and_data  # avoid circular imports
        from forecast_app.scores.calc_log import LOG_SINGLE_BIN_NEGATIVE_INFINITY


        targets = _validate_score_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)
        return

    # cache truth values: [timezero_pk][unit_pk][target_id] -> truth_values (a list):
    tz_unit_targ_pks_to_truth_vals = _tz_unit_targ_pks_to_truth_values(forecast_model.project)

    # get predicted point values. NB: b/c PointPrediction has three value types (only one of which is non-None), we will
    # get an error below when predicted_value is text (Target.POINT_TEXT). NB: we retrieve and max() only the two
    # numeric value fields (value_i and value_f), excluding value_t (which has no meaningful numeric semantics)
    forecast_point_predictions_qs = PointPrediction.objects \
        .filter(target__in=targets,
                forecast__forecast_model=forecast_model) \
        .values_list('forecast__id', 'forecast__time_zero__id', 'unit__id', 'target__id',
                     'value_i', 'value_f', 'value_t', 'value_d', 'value_b')  # only one of value_* is non-None

    # calculate scores for all combinations of unit and target.
    # collect errors so we don't log tons of messages. format: {timezero_pk: count, ...}
    timezero_pk_to_error_count = defaultdict(int)
    for forecast_pk, timezero_pk, unit_pk, target_pk, pred_value_i, pred_value_f, pred_value_t, pred_value_d, \
        pred_value_b in forecast_point_predictions_qs:
        predicted_value = PointPrediction.first_non_none_value(pred_value_i, pred_value_f, pred_value_t, pred_value_d,
                                                               pred_value_b)
        true_value, error_string = _validate_truth(tz_unit_targ_pks_to_truth_vals, timezero_pk, unit_pk, target_pk)
        if error_string:
            timezero_pk_to_error_count[timezero_pk] += 1
            continue  # skip this forecast's contribution to the score

        if (true_value is None) or (predicted_value is None):
            # note: future validation might ensure no bin values are None. only valid case: season onset point rows
            continue  # skip this forecast's contribution to the score

        ScoreValue.objects.create(forecast_id=forecast_pk, unit_id=unit_pk,
                                  target_id=target_pk, score=score,
                                  value=abs(true_value - predicted_value)
                                  if is_absolute_error else true_value - predicted_value)

    # print errors
    for timezero_pk, error_count in sorted(timezero_pk_to_error_count.items()):
        time_zero = TimeZero.objects.get(id=timezero_pk)
        logger.warning(f"errors validating truth: {time_zero.timezero_date}: {error_count}")


def _tz_unit_targ_pks_to_truth_values(project):
    """
    Similar to Project.unit_target_name_tz_date_to_truth(), returns project's truth values as a nested dict
    that's organized for easy access using these keys: [timezero_pk][unit_pk][target_id] -> truth_values (a list).
    """
    truth_data_qs = project.truth_data_qs() \
        .order_by('time_zero__id', 'unit__id', 'target__id') \
        .values_list('time_zero__id', 'unit__id', 'target__id',
                     'value_i', 'value_f', 'value_t', 'value_d', 'value_b')

    tz_unit_targ_pks_to_truth_vals = {}  # {timezero_pk: {unit_pk: {target_id: truth_value}}}
    for time_zero_id, unit_target_val_grouper in groupby(truth_data_qs, key=lambda _: _[0]):
        unit_targ_pks_to_truth = {}  # {unit_pk: {target_id: truth_value}}
        tz_unit_targ_pks_to_truth_vals[time_zero_id] = unit_targ_pks_to_truth
        for unit_id, target_val_grouper in groupby(unit_target_val_grouper, key=lambda _: _[1]):
            target_pk_to_truth = defaultdict(list)  # {target_id: truth_value}
            unit_targ_pks_to_truth[unit_id] = target_pk_to_truth
            for _, _, target_id, value_i, value_f, value_t, value_d, value_b in target_val_grouper:
                value = PointPrediction.first_non_none_value(value_i, value_f, value_t, value_d, value_b)
                target_pk_to_truth[target_id].append(value)

    return tz_unit_targ_pks_to_truth_vals


def _validate_truth(timezero_loc_target_pks_to_truth_values, timezero_pk, unit_pk, target_pk):
    """
    :return: 2-tuple of the form: (truth_value, error_string) where error_string is non-None if the inputs were invalid.
        in that case, truth_value is None. o/w truth_value is valid
    """
    if timezero_pk not in timezero_loc_target_pks_to_truth_values:
        return None, 'timezero_pk not in truth'
    elif unit_pk not in timezero_loc_target_pks_to_truth_values[timezero_pk]:
        return None, 'unit_pk not in truth'
    elif target_pk not in timezero_loc_target_pks_to_truth_values[timezero_pk][unit_pk]:
        return None, 'target_pk not in truth'

    truth_values = timezero_loc_target_pks_to_truth_values[timezero_pk][unit_pk][target_pk]
    if len(truth_values) == 0:  # truth not available
        return None, 'truth value not found'
    elif len(truth_values) > 1:
        return None, '>1 truth values found'

    return truth_values[0], None
