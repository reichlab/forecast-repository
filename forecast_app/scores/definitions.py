import logging
from collections import defaultdict
from itertools import groupby

from django.db import connection

from forecast_app.models import Forecast, ScoreValue, TruthData
from forecast_app.models.data import ProjectTemplateData, ForecastData
from forecast_app.scores.calc_error import _calculate_error_score_values
from forecast_app.scores.calc_log import _calc_log_bin_score_values
from forecast_app.scores.calc_pit import _calculate_pit_score_values


logger = logging.getLogger(__name__)

#
# ---- Score instance definitions ----
#

# provides information about all scores in the system. used by ensure_all_scores_exist() to create Score instances. maps
# each Score's abbreviation to a 2-tuple: (name, description). recall that the abbreviation is used to look up the
# corresponding function in the forecast_app.scores.functions module - see `calc_<abbreviation>` documentation in Score.
# in that sense, these abbreviations are the official names to use when looking up a particular score
SCORE_ABBREV_TO_NAME_AND_DESCR = {
    # 'const': ('Constant Value', "A debugging score that scores 1.0 only for first location and first target."),
    'error': ('Error', "The the truth value minus the model's point estimate."),
    'abs_error': ('Absolute Error', "The absolute value of the truth value minus the model's point estimate. "
                                    "Lower is better."),
    'log_single_bin': ('Log score (single bin)', "Natural log of probability assigned to the true bin. Higher is "
                                                 "better."),
    'log_multi_bin': ('Log score (multi bin)', "This is calculated by finding the natural log of probability "
                                               "assigned to the true and a few neighbouring bins. Higher is better."),
    'pit': ('Probability Integral Transform (PIT)', "The probability integral transform (PIT) is a metric commonly "
                                                    "used to evaluate the calibration of probabilistic forecasts."),
}


#
# ---- 'Constant Value' calculation function ----
#

def calc_const(score, forecast_model):
    """
    A simple demo that calculates 'Constant Value' scores for the first location and first target in forecast_model's
    project. To activate it, add this entry to SCORE_ABBREV_TO_NAME_AND_DESCR:

        'const': ('Constant Value', "A debugging score that scores 1.0 only for first location and first target."),

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
# ---- 'error' and 'abs_error' calculation functions ----
#

def calc_error(score, forecast_model):
    """
    Calculates 'error' scores.
    """
    _calculate_error_score_values(score, forecast_model, False)


def calc_abs_error(score, forecast_model):
    """
    Calculates 'abs_error' scores.
    """
    _calculate_error_score_values(score, forecast_model, True)


#
# ---- 'log_single_bin' and 'log_multi_bin' calculation functions ----
#

LOG_SINGLE_BIN_NEGATIVE_INFINITY = -999  # see use below for docs


def calc_log_single_bin(score, forecast_model):
    """
    Calculates 'Log score (single bin)' scores per
    https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin .
    """
    _calc_log_bin_score_values(score, forecast_model, 0)


def calc_log_multi_bin(score, forecast_model):
    """
    Calculates 'Log score (multi bin)' scores per
    https://github.com/reichlab/flusight/wiki/Scoring#3-log-score-multi-bin .
    """
    _calc_log_bin_score_values(score, forecast_model, 5)


#
# ---- 'pit' calculation functions ----
#

def calc_pit(score, forecast_model):
    """
    Calculates 'pit' score.
    """
    _calculate_pit_score_values(score, forecast_model)


#
# ---- log and 'pit' helper functions ----
#

def _calc_bin_score(score, forecast_model, is_log_score, num_bins_one_side):
    """
    Function shared by log and pit scores. is_log_score controls which score calculation function is called - either
    log or pit. num_bins_one_side is used if is_log_score.
    """
    try:
        _validate_score_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)
        return

    # collect errors so we don't log thousands of duplicate messages. dict format:
    #   {(timezero_pk, location_pk, target_pk): count, ...}:
    tz_loc_targ_pks_to_error_count = defaultdict(int)  # helps eliminate duplicate warnings

    # cache the three necessary bins and values - template, truth, and forecasts
    # 1/3 template: [location_pk][target_pk] -> [(bin_start_incl_1, bin_end_notincl_1), ...]:
    ltpk_to_templ_st_ends = _ltpk_to_templ_st_ends(forecast_model.project)

    # 2/3 truth: [timezero_pk][location_pk][target_pk] -> (bin_start_incl, bin_end_notincl, true_value):
    tzltpk_to_truth_st_end_val = _tzltpk_to_truth_st_end_val(forecast_model.project)

    # 3/3 forecast: [timezero_pk][location_pk][target_pk] -> {(bin_start_incl_1, bin_end_notincl_1) -> predicted_value_1, ...}:
    tzltpk_to_forec_st_end_to_pred_val = _tzltpk_to_forec_st_end_to_pred_val(forecast_model)

    # it is convenient to iterate over truths to get all timezero/location/target combinations. this will omit forecasts
    # with no truth, but that's OK b/c without truth, a forecast makes no contribution to the score
    for truth_data in forecast_model.project.truth_data_qs():  # truth_data: time_zero, location, target, value
        # get template bins for this forecast
        templ_st_ends = ltpk_to_templ_st_ends[truth_data.location.pk][truth_data.target.pk]

        # get and validate truth for this forecast
        try:
            truth_st_end_val = tzltpk_to_truth_st_end_val[truth_data.time_zero.pk][truth_data.location.pk][
                truth_data.target.pk]
            true_bin_key = truth_st_end_val[0], truth_st_end_val[1]
            true_bin_idx = templ_st_ends.index(true_bin_key)  # NB: non-deterministic for (None, None) true bin keys!
        except KeyError:
            error_key = (truth_data.time_zero.pk, truth_data.location.pk, truth_data.target.pk)
            tz_loc_targ_pks_to_error_count[error_key] += 1
            continue  # skip this forecast's contribution to the score

        # get forecast bins and predicted values for this forecast
        forec_st_end_to_pred_val = tzltpk_to_forec_st_end_to_pred_val[truth_data.time_zero.pk][truth_data.location.pk][
            truth_data.target.pk]

        # dispatch to scoring function
        if is_log_score:
            from forecast_app.scores.calc_log import save_log_score  # avoid circular imports


            save_log_score(score, forecast_model, templ_st_ends, forec_st_end_to_pred_val,
                           true_bin_key, true_bin_idx, truth_data, num_bins_one_side)
        else:
            from forecast_app.scores.calc_pit import save_pit_score  # avoid circular imports


            save_pit_score(score, forecast_model, templ_st_ends, forec_st_end_to_pred_val,
                           true_bin_key, true_bin_idx, truth_data)

    # print errors
    for (timezero_pk, location_pk, target_pk) in sorted(tz_loc_targ_pks_to_error_count.keys()):
        count = tz_loc_targ_pks_to_error_count[timezero_pk, location_pk, target_pk]
        logger.warning("_calculate_pit_score_values(): missing {} truth value(s): "
                       "timezero_pk={}, location_pk={}, target_pk={}"
                       .format(count, timezero_pk, location_pk, target_pk))


#
# ---- predictive distribution (aka 'bin') lookup functions ----
#
# notes:
# - all of these return nested dicts mapping [timezero_pk][location_pk][target_pk] -> something related to the
#   distribution, either a tuple, list, or a dict
# - the naming convention is to start each function with the prefix '_tzltpk_to_', which reads as:
#   '_timezero_pk_location_pk_target_pk_to_'
# - we abbreviate 'start' and 'end' to 'st_end'
# - we sometimes refer to a (bin_start_incl, bin_end_notincl) 2-tuple as a 'bin key'
#


def _tzltpk_to_truth_st_end_val(project):
    """
    Returns project's truth data merged with the template as a single 3-tuple:
        [timezero_pk][location_pk][target_pk] -> (bin_start_incl, bin_end_notincl, true_value)

    We need the template to get bin_start_incl and bin_end_notincl for the truth.
    """
    sql = """
        SELECT truthd.time_zero_id, truthd.location_id, truthd.target_id,
               templd.bin_start_incl, templd.bin_end_notincl, truthd.value as true_value
        FROM {truthdata_table_name} as truthd
               LEFT JOIN {templatedata_table_name} as templd
                         ON truthd.location_id = templd.location_id
                           AND truthd.target_id = templd.target_id
        WHERE templd.project_id = %s
          AND NOT templd.is_point_row
          AND ((truthd.value >= templd.bin_start_incl) OR ((truthd.value IS NULL) AND (templd.bin_start_incl IS NULL)))
          AND ((truthd.value < templd.bin_end_notincl) OR ((truthd.value IS NULL) AND (templd.bin_end_notincl IS NULL)))
        ORDER BY truthd.time_zero_id, truthd.location_id, truthd.target_id, templd.bin_start_incl
    """.format(truthdata_table_name=TruthData._meta.db_table,
               templatedata_table_name=ProjectTemplateData._meta.db_table)
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        rows = cursor.fetchall()

    # build the dict
    tz_loc_targ_pks_to_templ_truth_vals = {}  # {timezero_pk: {location_pk: {target_id: (bin_start_incl, bin_end_notincl, true_value)}}}
    for time_zero_id, loc_target_val_grouper in groupby(rows, key=lambda _: _[0]):
        loc_targ_pks_to_templ_truth = {}  # {location_pk: {target_id: (bin_start_incl, bin_end_notincl, true_value)}}
        tz_loc_targ_pks_to_templ_truth_vals[time_zero_id] = loc_targ_pks_to_templ_truth
        for location_id, target_val_grouper in groupby(loc_target_val_grouper, key=lambda _: _[1]):
            target_pk_to_truth = {}  # {target_id: (bin_start_incl, bin_end_notincl, true_value)}
            loc_targ_pks_to_templ_truth[location_id] = target_pk_to_truth
            for _, _, target_id, bin_start_incl, bin_end_notincl, true_value in target_val_grouper:
                target_pk_to_truth[target_id] = [bin_start_incl, bin_end_notincl, true_value]

    return tz_loc_targ_pks_to_templ_truth_vals


def _ltpk_to_templ_st_ends(project):
    """
    Returns project's template data as a list of 2-tuples (bin_start_incl, bin_end_notincl):
        [location_pk][target_pk] -> [(bin_start_incl_1, bin_end_notincl_1), ...]

    The are ordered by bin_start_incl. Only returns rows whose targets match non_date_targets().
    """
    targets = project.non_date_targets()
    template_data_qs = project.cdcdata_set \
        .filter(is_point_row=False,
                target__in=targets) \
        .order_by('location__id', 'target__id', 'bin_start_incl') \
        .values_list('location__id', 'target__id', 'bin_start_incl', 'bin_end_notincl')

    # build the dict
    ltpk_to_templ_st_ends = {}  # {location_pk: {target_id: [(bin_start_incl_1, bin_end_notincl_1), ...]}}
    for location_id, target_val_grouper in groupby(template_data_qs, key=lambda _: _[0]):
        tpk_to_templ_st_ends = defaultdict(list)  # {target_id: [(bin_start_incl_1, bin_end_notincl_1), ...]}
        ltpk_to_templ_st_ends[location_id] = tpk_to_templ_st_ends
        for _, target_id, bin_start_incl, bin_end_notincl in target_val_grouper:
            tpk_to_templ_st_ends[target_id].append((bin_start_incl, bin_end_notincl))

    return ltpk_to_templ_st_ends


def _tzltpk_to_forec_st_end_to_pred_val(forecast_model):
    """
    Returns forecast's prediction data as a dict that maps 2-tuples (bin_start_incl, bin_end_notincl) to predicted
    values:
        [timezero_pk][location_pk][target_pk] -> {(bin_start_incl_1, bin_end_notincl_1) -> predicted_value_1, ...}

    Only returns rows whose targets match non_date_targets().
    """
    targets = forecast_model.project.non_date_targets()
    forecast_data_qs = ForecastData.objects \
        .filter(forecast__forecast_model=forecast_model,
                is_point_row=False,
                target__in=targets) \
        .order_by('forecast__time_zero__id', 'location__id', 'target__id') \
        .values_list('forecast__time_zero__id', 'location__id', 'target__id',
                     'bin_start_incl', 'bin_end_notincl', 'value')

    # build the dict
    tzltpk_to_forec_st_end_to_pred_val = {}  # {timezero_pk: {location_pk: {target_id: {(bin_start_incl_1, bin_end_notincl_1) -> predicted_value_1, ...}}}}
    for time_zero_id, loc_target_val_grouper in groupby(forecast_data_qs, key=lambda _: _[0]):
        ltpk_to_forec_st_end_to_pred_val = {}  # {location_pk: {target_id: {(bin_start_incl_1, bin_end_notincl_1) -> predicted_value_1, ...}}}
        tzltpk_to_forec_st_end_to_pred_val[time_zero_id] = ltpk_to_forec_st_end_to_pred_val
        for location_id, target_val_grouper in groupby(loc_target_val_grouper, key=lambda _: _[1]):
            tpk_to_forec_st_end_to_pred_val = defaultdict(
                dict)  # {target_id: {(bin_start_incl_1, bin_end_notincl_1) -> predicted_value_1, ...}}
            ltpk_to_forec_st_end_to_pred_val[location_id] = tpk_to_forec_st_end_to_pred_val
            for _, _, target_id, bin_start_incl, bin_end_notincl, pred_value in target_val_grouper:
                tpk_to_forec_st_end_to_pred_val[target_id][(bin_start_incl, bin_end_notincl)] = pred_value

    return tzltpk_to_forec_st_end_to_pred_val


def _timezero_loc_target_pks_to_truth_values(forecast_model):
    """
    Similar to Project.location_target_name_tz_date_to_truth(), returns forecast_model's truth values as a nested dict
    that's organized for easy access using these keys: [timezero_pk][location_pk][target_id] -> truth_values (a list).
    """
    truth_data_qs = forecast_model.project.truth_data_qs() \
        .order_by('time_zero__id', 'location__id', 'target__id') \
        .values_list('time_zero__id', 'location__id', 'target__id', 'value')

    tz_loc_targ_pks_to_truth_vals = {}  # {timezero_pk: {location_pk: {target_id: truth_value}}}
    for time_zero_id, loc_target_val_grouper in groupby(truth_data_qs, key=lambda _: _[0]):
        loc_targ_pks_to_truth = {}  # {location_pk: {target_id: truth_value}}
        tz_loc_targ_pks_to_truth_vals[time_zero_id] = loc_targ_pks_to_truth
        for location_id, target_val_grouper in groupby(loc_target_val_grouper, key=lambda _: _[1]):
            target_pk_to_truth = defaultdict(list)  # {target_id: truth_value}
            loc_targ_pks_to_truth[location_id] = target_pk_to_truth
            for _, _, target_id, value in target_val_grouper:
                target_pk_to_truth[target_id].append(value)

    return tz_loc_targ_pks_to_truth_vals


#
# validation functions
#

def _validate_score_targets_and_data(forecast_model):
    # validate targets
    targets = forecast_model.project.non_date_targets()
    if not targets:
        raise RuntimeError("_validate_score_targets_and_data(): no targets. project={}".format(forecast_model.project))

    # validate forecast data
    if not forecast_model.forecasts.exists():
        raise RuntimeError("_validate_score_targets_and_data(): could not calculate absolute errors: model had "
                           "no data: {}".format(forecast_model))

    return targets


def _validate_truth(timezero_loc_target_pks_to_truth_values, timezero_pk, location_pk, target_pk):
    """
    :return: 2-tuple of the form: (truth_value, error_string) where error_string is non-None if the inputs were invalid.
        in that case, truth_value is None. o/w truth_value_or_none is valid
    """
    if timezero_pk not in timezero_loc_target_pks_to_truth_values:
        return None, 'timezero_pk not in truth'
    elif location_pk not in timezero_loc_target_pks_to_truth_values[timezero_pk]:
        return None, 'location_pk not in truth'
    elif target_pk not in timezero_loc_target_pks_to_truth_values[timezero_pk][location_pk]:
        return None, 'target_pk not in truth'

    truth_values = timezero_loc_target_pks_to_truth_values[timezero_pk][location_pk][target_pk]
    if len(truth_values) == 0:  # truth not available
        return None, 'truth value not found'
    elif len(truth_values) > 1:
        return None, '>1 truth values found'

    return truth_values[0], None
