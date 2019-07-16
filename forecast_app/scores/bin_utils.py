from collections import defaultdict
from itertools import groupby

from django.db import connection

from forecast_app.models import TruthData, TimeZero, Forecast, ForecastModel
from forecast_app.scores.definitions import _validate_score_targets_and_data, logger


def _calc_bin_score(score, forecast_model, save_score_fcn, **kwargs):
    """
    Function shared by log and pit scores.

    :param: save_score_fcn: a function of xx args that creates and saves a ScoreValue. args:
        score, forecast_model, templ_st_ends, forec_st_end_to_pred_val, true_bin_key, true_bin_idx, truth_data, **kwargs
    :param: kwargs: passed to save_score_fcn
    """
    try:
        _validate_score_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)
        return

    # collect errors so we don't log thousands of duplicate messages. dict format:
    #   {(timezero_pk, location_pk, target_pk): count, ...}:
    # note that the granularity is poor - there are multiple possible errors related to a particular 3-tuple
    tz_loc_targ_pks_to_error_count = defaultdict(int)  # helps eliminate duplicate warnings

    # cache the three necessary bins and values - template, truth, and forecasts
    # 1/3 template: [location_pk][target_pk] -> [(bin_start_incl_1, bin_end_notincl_1), ...]:
    ltpk_to_templ_st_ends = _ltpk_to_templ_st_ends(forecast_model.project)

    # 2/3 truth: [timezero_pk][location_pk][target_pk] -> (bin_start_incl, bin_end_notincl, true_value):
    tzltpk_to_truth_st_end_val = _tzltpk_to_truth_st_end_val(forecast_model.project)

    # 3/3 forecast: [timezero_pk][location_pk][target_pk] -> {(bin_start_incl_1, bin_end_notincl_1) -> predicted_value_1, ...}:
    tzltpk_to_forec_st_end_to_pred_val = _tzltpk_to_forec_st_end_to_pred_val(forecast_model)

    # it is convenient to iterate over truths to get all timezero/location/target combinations. this will omit forecasts
    # with no truth, but that's OK b/c without truth, a forecast makes no contribution to the score. we use direct SQL
    # to work with PKs and avoid ORM object lookup overhead, mainly for TruthData -> TimeZero -> Forecast -> PK
    for time_zero_pk, forecast_pk, location_pk, target_pk, truth_value in \
            _truth_data_pks_for_forecast_model(forecast_model):
        # get template bins for this forecast
        try:
            templ_st_ends = ltpk_to_templ_st_ends[location_pk][target_pk]
        except KeyError:
            error_key = (time_zero_pk, location_pk, target_pk)
            tz_loc_targ_pks_to_error_count[error_key] += 1
            continue  # skip this forecast's contribution to the score

        # get and validate truth for this forecast
        try:
            truth_st_end_val = tzltpk_to_truth_st_end_val[time_zero_pk][location_pk][target_pk]
            true_bin_key = truth_st_end_val[0], truth_st_end_val[1]
            true_bin_idx = templ_st_ends.index(true_bin_key)  # NB: non-deterministic for (None, None) true bin keys!
        except (KeyError, ValueError):
            error_key = (time_zero_pk, location_pk, target_pk)
            tz_loc_targ_pks_to_error_count[error_key] += 1
            continue  # skip this forecast's contribution to the score

        # get forecast bins and predicted values for this forecast
        try:
            forec_st_end_to_pred_val = tzltpk_to_forec_st_end_to_pred_val[time_zero_pk][location_pk][target_pk]
        except KeyError:
            error_key = (time_zero_pk, location_pk, target_pk)
            tz_loc_targ_pks_to_error_count[error_key] += 1
            continue  # skip this forecast's contribution to the score

        # dispatch to scoring function
        save_score_fcn(score, time_zero_pk, forecast_pk, location_pk, target_pk, truth_value,
                       templ_st_ends, forec_st_end_to_pred_val,
                       true_bin_key, true_bin_idx, **kwargs)

    # print errors
    for (timezero_pk, location_pk, target_pk) in sorted(tz_loc_targ_pks_to_error_count.keys()):
        count = tz_loc_targ_pks_to_error_count[timezero_pk, location_pk, target_pk]
        logger.warning("_calculate_pit_score_values(): missing {} truth value(s): "
                       "timezero_pk={}, location_pk={}, target_pk={}"
                       .format(count, timezero_pk, location_pk, target_pk))


def _truth_data_pks_for_forecast_model(forecast_model):
    sql = """
        SELECT td.time_zero_id, f.id, td.location_id, td.target_id, td.value
        FROM {truth_data_table_name} AS td
               LEFT JOIN {timezero_table_name} AS tz ON td.time_zero_id = tz.id
               LEFT JOIN {forecast_table_name} AS f ON tz.id = f.time_zero_id
               LEFT JOIN {forecastmodel_table_name} AS fm ON f.forecast_model_id = fm.id
        WHERE fm.id = %s;
    """.format(truth_data_table_name=TruthData._meta.db_table,
               timezero_table_name=TimeZero._meta.db_table,
               forecast_table_name=Forecast._meta.db_table,
               forecastmodel_table_name=ForecastModel._meta.db_table)
    with connection.cursor() as cursor:
        cursor.execute(sql, (forecast_model.pk,))
        return cursor.fetchall()


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
