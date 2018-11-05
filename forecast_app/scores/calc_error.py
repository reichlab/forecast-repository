import logging

from forecast_app.models import ForecastData
from forecast_app.models.data import CDCData


logger = logging.getLogger(__name__)


def calculate_error_score_values(score, forecast_model, is_absolute_error):
    """
    Implements the 'error' and 'abs_error' scores. Creates ScoreValue instances for the passed args, saving them into
    the passed score. The score is simply `true_value - predicted_value` (optionally passed to abs() based on
    is_absolute_error) for each combination of Location + Target in forecast_model's project. Runs in the calling thread
    and therefore blocks. Note that this implementation uses a naive approach to calculating scores, iterating over
    truth and forecast tables instead of caching.

    :param score: a Score
    :param forecast_model: a ForecastModel
    :param is_absolute_error: True if abs() should be called
    """
    from forecast_app.models import ScoreValue  # avoid circular imports


    try:
        from forecast_app.scores.definitions import _validate_score_targets_and_data, \
            _validate_truth, LOG_SINGLE_BIN_NEGATIVE_INFINITY  # avoid circular imports


        targets = _validate_score_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)

    # cache truth values: [location_name][target_name][timezero_date]:
    from forecast_app.scores.definitions import _timezero_loc_target_pks_to_truth_values  # avoid circular imports


    timezero_loc_target_pks_to_truth_values = _timezero_loc_target_pks_to_truth_values(forecast_model)

    # get predicted point values
    forecast_data_qs = ForecastData.objects \
        .filter(is_point_row=True,
                target__in=targets,
                forecast__forecast_model=forecast_model) \
        .values_list('forecast__id', 'forecast__time_zero__id', 'location__id', 'target__id', 'value')

    # calculate scores for all combinations of location and target. keep a list of errors so we don't log thousands of
    # duplicate messages. dict format: {(forecast_pk, timezero_pk, location_pk, target_pk): error_string, ...}:
    forec_tz_loc_targ_pk_to_error_str = {}
    for forecast_pk, timezero_pk, location_pk, target_pk, predicted_value in forecast_data_qs:
        true_value, error_string = _validate_truth(timezero_loc_target_pks_to_truth_values, forecast_pk,
                                                   timezero_pk, location_pk, target_pk)
        error_key = (forecast_pk, timezero_pk, location_pk, target_pk)
        if error_string and (error_key not in forec_tz_loc_targ_pk_to_error_str):
            forec_tz_loc_targ_pk_to_error_str[error_key] = error_string
            continue  # skip this forecast's contribution to the score

        if true_value is None or predicted_value is None:
            # note: future validation might ensure no bin values are None
            continue  # skip this forecast's contribution to the score

        ScoreValue.objects.create(forecast_id=forecast_pk, location_id=location_pk,
                                  target_id=target_pk, score=score,
                                  value=abs(true_value - predicted_value)
                                  if is_absolute_error else true_value - predicted_value)

    # print errors
    for (forecast_pk, timezero_pk, location_pk, target_pk), error_string in forec_tz_loc_targ_pk_to_error_str.items():
        logger.warning("calculate_error_score_values(): truth validation error: {!r}: "
                       "forecast_pk={}, timezero_pk={}, location_pk={}, target_pk={}"
                       .format(error_string, forecast_pk, timezero_pk, location_pk, target_pk))
