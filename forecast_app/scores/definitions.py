import logging
import math
from collections import defaultdict
from itertools import groupby

from django.db import transaction

from forecast_app.models import Forecast, ScoreValue, ForecastData
from forecast_app.models.data import CDCData


logger = logging.getLogger(__name__)

#
# ---- Score instance definitions ----
#

# provides information about all scores in the system. used by ensure_all_scores_exist() to create Score instances. maps
# each Score's abbreviation to a 2-tuple: (name, description). recall that the abbreviation is used to look up the
# corresponding function in the forecast_app.scores.functions module - see `calc_<abbreviation>` documentation in Score
SCORE_ABBREV_TO_NAME_AND_DESCR = {
    'error': ('Error', "The the truth value minus the model's point estimate."),
    'abs_error': ('Absolute Error', "The absolute value of the truth value minus the model's point estimate. "
                                    "Lower is better."),
    'const': ('Constant Value', "A debugging score that scores 1.0 only for first location and first target."),
    'log_single_bin': ('Log score (single bin)', "Natural log of probability assigned to the true bin. Higher is "
                                                 "better."),
}


#
# ---- 'Constant Value' calculation function ----
#

def calc_const(score, forecast_model):
    """
    A simple demo that calculates 'Constant Value' scores for the first location and first target in forecast_model's
    project.
    """
    first_location = forecast_model.project.locations.first()
    first_target = forecast_model.project.targets.first()
    if (not first_location) or (not first_target):
        logger.warning("calc_const(): no location or no target found. first_location={}, first_target={}"
                       .format(first_location, first_target))
        return

    for forecast in Forecast.objects.filter(forecast_model=forecast_model):
        ScoreValue.objects.create(score=score, forecast=forecast, location=first_location, target=first_target,
                                  value=1.0)


#
# ---- 'Error' and 'Absolute Error' calculation functions ----
#

def calc_log_single_bin(score, forecast_model):
    """
    Calculates 'Log score (single bin)' scores per
    https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin.
    """
    try:
        targets = _validate_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)

    # cache truth values: [location_name][target_name][timezero_date]:
    timezero_loc_target_pks_to_truth_values = _timezero_loc_target_pks_to_truth_values(forecast_model)

    # calculate scores for all combinations of location and target. NB: this naive approach is slow b/c we query ea time
    locations = forecast_model.project.locations.all()
    for forecast in forecast_model.forecasts.all():
        timezero_pk = forecast.time_zero.pk
        for location in locations:
            for target in targets:
                try:
                    true_value = _validate_truth(timezero_loc_target_pks_to_truth_values, forecast_model, forecast.pk,
                                                 timezero_pk, location.pk, target.pk)
                    predicted_value = _bin_predicted_value_containing_true_value(forecast, location, target, true_value)
                    _validate_predicted_value(forecast_model, forecast.pk, timezero_pk, location.pk, target.pk,
                                              predicted_value)
                    ScoreValue.objects.create(forecast=forecast, location=location, target=target, score=score,
                                              value=math.log(predicted_value))
                except RuntimeError as rte:
                    logger.warning(rte)
                    continue  # skip this forecast's contribution to the score


def _bin_predicted_value_containing_true_value(forecast, location, target, true_value):
    """
    Returns the bin in forecast's data for location target that contains true_value.

    :return: 3-tuple: (bin_start_incl, bin_end_notincl, value)
    """
    forecast_data_qs = ForecastData.objects \
        .filter(forecast=forecast,
                row_type=CDCData.BIN_ROW_TYPE,
                location=location,
                target=target,
                bin_start_incl__lte=true_value,
                bin_end_notincl__gt=true_value) \
        .values_list('value', flat=True)
    if forecast_data_qs.count() != 1:
        raise RuntimeError("_bin_predicted_value_containing_true_value(): got {} bin rows, not one. forecast={}, "
                           "location={}, target={}, true_value={}"
                           .format(forecast_data_qs.count(), forecast, location, target, true_value))

    return forecast_data_qs[0]


#
# ---- 'Error' and 'Absolute Error' calculation functions ----
#


def calc_error(score, forecast_model):
    """
    Calculates 'Error' scores.
    """
    calculate_error_score_values(score, forecast_model, is_absolute_error=False)


def calc_abs_error(score, forecast_model):
    """
    Calculates 'Absolute Error' scores.
    """
    calculate_error_score_values(score, forecast_model, is_absolute_error=True)


@transaction.atomic
def calculate_error_score_values(score, forecast_model, is_absolute_error):
    """
    Creates ScoreValue instances for the passed args, saving them into the passed score. The score is simply `true_value
    - predicted_value` (optionally passed to abs() based on is_absolute_error) for each combination of Location + Target
    in forecast_model's project. Runs in the calling thread and therefore blocks. Note that this implementation uses a
    naive approach to calculating scores, iterating over truth and forecast tables instead of caching.
    
    :param score: a Score
    :param forecast_model: a ForecastModel
    :param is_absolute_error: True if abs() should be called
    """
    from forecast_app.models import ScoreValue  # avoid circular imports


    try:
        targets = _validate_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)

    # cache truth values: [location_name][target_name][timezero_date]:
    timezero_loc_target_pks_to_truth_values = _timezero_loc_target_pks_to_truth_values(forecast_model)

    # get predicted point values
    forecast_data_qs = ForecastData.objects \
        .filter(row_type=CDCData.POINT_ROW_TYPE,
                target__in=targets,
                forecast__forecast_model=forecast_model) \
        .values_list('forecast__id', 'forecast__time_zero__id', 'location__id', 'target__id', 'value')

    # calculate scores for all combinations of location and target
    for forecast_pk, timezero_pk, location_pk, target_pk, predicted_value in forecast_data_qs:
        try:
            _validate_predicted_value(forecast_model, forecast_pk, timezero_pk, location_pk, target_pk, predicted_value)
            true_value = _validate_truth(timezero_loc_target_pks_to_truth_values, forecast_model, forecast_pk,
                                         timezero_pk, location_pk, target_pk)
            ScoreValue.objects.create(forecast_id=forecast_pk, location_id=location_pk,
                                      target_id=target_pk, score=score,
                                      value=abs(true_value - predicted_value)
                                      if is_absolute_error else true_value - predicted_value)
        except RuntimeError as rte:
            logger.warning(rte)
            continue  # skip this forecast's contribution to the score


#
# ---- utility functions ----
#

def _timezero_loc_target_pks_to_truth_values(forecast_model):
    """
    Similar to Project.location_target_name_tz_date_to_truth(), returns forecast_model's truth values as a nested dict
    that's organized for easy access using these keys: [timezero_pk][location_pk][target_id] -> truth_values (a list).
    """
    truth_data_qs = forecast_model.project.truth_data_qs() \
        .order_by('time_zero__id', 'location__id', 'target__id') \
        .values_list('time_zero__id', 'location__id', 'target__id', 'value')

    timezero_loc_target_pks_to_truth_values = {}  # {timezero_pk: {location_pk: {target_id: truth_value}}}
    for time_zero_id, loc_target_val_grouper in groupby(truth_data_qs, key=lambda _: _[0]):
        loc_targ_pks_to_truth = {}  # {location_pk: {target_id: truth_value}}
        timezero_loc_target_pks_to_truth_values[time_zero_id] = loc_targ_pks_to_truth
        for location_id, target_val_grouper in groupby(loc_target_val_grouper, key=lambda _: _[1]):
            target_pk_to_truth = defaultdict(list)  # {target_id: truth_value}
            loc_targ_pks_to_truth[location_id] = target_pk_to_truth
            for _, _, target_id, value in target_val_grouper:
                target_pk_to_truth[target_id].append(value)

    return timezero_loc_target_pks_to_truth_values


def _validate_targets_and_data(forecast_model):
    # validate targets
    targets = forecast_model.project.visualization_targets()
    if not targets:
        raise RuntimeError("calculate_error_score_values(): no visualization targets. project={}"
                           .format(forecast_model.project))

    # validate forecast data
    if not forecast_model.forecasts.exists():
        raise RuntimeError("calculate_error_score_values(): could not calculate absolute errors: model had "
                           "no data: {}".format(forecast_model))

    return targets


def _validate_predicted_value(forecast_model, forecast_pk, timezero_pk, location_pk, target_id, predicted_value):
    # validate predicted_value. todo is predicted_value ever None (e.g., 'NA')?
    if predicted_value is None:
        raise RuntimeError("calculate_error_score_values(): predicted_value is None."
                           "forecast_model={}, forecast_pk={}, timezero_pk={}, location_pk={}, target_id={}"
                           .format(forecast_model, forecast_pk, timezero_pk, location_pk, target_id))


def _validate_truth(timezero_loc_target_pks_to_truth_values, forecast_model, forecast_pk,
                    timezero_pk, location_pk, target_id):
    # todo: duplicate of iterate_forecast_errors()
    if timezero_pk not in timezero_loc_target_pks_to_truth_values:
        raise RuntimeError("calculate_error_score_values(): timezero_pk not in truth: "
                           "forecast_model={}, timezero_pk={}".format(forecast_model, timezero_pk))
    elif location_pk not in timezero_loc_target_pks_to_truth_values[timezero_pk]:
        raise RuntimeError("calculate_error_score_values(): location_pk not in truth: "
                           "forecast_model={}, location_pk={}".format(forecast_model, location_pk))
    elif target_id not in timezero_loc_target_pks_to_truth_values[timezero_pk][location_pk]:
        raise RuntimeError("calculate_error_score_values(): target_id not in truth: "
                           "forecast_model={}, target_id={}".format(forecast_model, target_id))

    truth_values = timezero_loc_target_pks_to_truth_values[timezero_pk][location_pk][target_id]
    if len(truth_values) == 0:  # truth not available
        raise RuntimeError("calculate_error_score_values(): truth value not found. "
                           "forecast_model={}, timezero_pk={}, location_pk={}, target_id={}"
                           .format(forecast_model, timezero_pk, location_pk, target_id))
    elif len(truth_values) > 1:
        raise RuntimeError("calculate_error_score_values(): >1 truth values found. "
                           "forecast_model={}, timezero_pk={}, location_pk={}, target_id={}"
                           .format(forecast_model, timezero_pk, location_pk, target_id))

    true_value = truth_values[0]
    if true_value is None:
        raise RuntimeError("calculate_error_score_values(): true_value is None. "
                           "forecast_model={}, forecast_pk={}, timezero_pk={}, location_pk={}, target_id={}"
                           .format(forecast_model, forecast_pk, timezero_pk, location_pk, target_id))

    return true_value
