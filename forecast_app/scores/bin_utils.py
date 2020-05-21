import csv
import io
from collections import defaultdict
from itertools import groupby

from django.db import connection

from forecast_app.models import TruthData, TimeZero, Forecast, ForecastModel, ScoreValue, TargetLwr, Target, \
    BinDistribution, PointPrediction
from forecast_app.models.project import POSTGRES_NULL_VALUE
from forecast_app.scores.definitions import _validate_score_targets_and_data, logger


def _calc_bin_score(score, forecast_model, save_score_fcn, **kwargs):
    """
    Function shared by log and pit scores.

    :param save_score_fcn: a function that creates and saves a ScoreValue. args: see save_score_fcn() call below
    :param kwargs: passed through to save_score_fcn
    """
    try:
        _validate_score_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(rte)
        return

    # cache the three necessary bins and values - lwrs, truth, and forecasts 1/3 lwrs: [target_pk] -> [lwr_1, ...]:
    targ_pk_to_lwrs = _targ_pk_to_lwrs(forecast_model.project)

    # 2/3 truth: [timezero_pk][unit_pk][target_pk] -> true_lwr:
    tz_loc_targ_pk_to_true_lwr = _tz_loc_targ_pk_to_true_lwr(forecast_model.project)

    # 3/3 forecast: [timezero_pk][unit_pk][target_pk][cat_value] -> predicted_value:
    tz_loc_targ_pk_lwr_to_pred_val = _tz_loc_targ_pk_lwr_to_pred_val(forecast_model)

    # it is convenient to iterate over truths to get all timezero/unit/target combinations. this will omit forecasts
    # with no truth, but that's OK b/c without truth, a forecast makes no contribution to the score. we use direct SQL
    # to work with PKs and avoid ORM object lookup overhead, mainly for TruthData -> TimeZero -> Forecast -> PK.
    # we collect all ScoreValue rows and then bulk insert them as an optimization, rather than create separate ORM
    # instances:
    score_values = []  # list of 5-tuples: (score.pk, forecast.pk, unit.pk, target.pk, score_value)
    num_warnings = 0
    for time_zero_pk, forecast_pk, unit_pk, target_pk, truth_value in \
            _truth_data_pks_for_forecast_model(forecast_model):
        # get lwrs for this forecast
        try:
            lwrs = targ_pk_to_lwrs[target_pk]
        except KeyError:
            num_warnings += 1
            continue  # skip this forecast's contribution to the score

        # get and validate truth for this forecast
        try:
            true_lwr = tz_loc_targ_pk_to_true_lwr[time_zero_pk][unit_pk][target_pk]
            true_bin_idx = lwrs.index(true_lwr)  # NB: non-deterministic for (None, None) true bin keys!
        except (KeyError, ValueError):
            num_warnings += 1
            continue  # skip this forecast's contribution to the score

        # get forecast bins and predicted values for this forecast
        try:
            lwr_to_pred_val = tz_loc_targ_pk_lwr_to_pred_val[time_zero_pk][unit_pk][target_pk]
        except KeyError:
            num_warnings += 1
            continue  # skip this forecast's contribution to the score

        # dispatch to scoring function if we have any predicted values to work with
        if lwr_to_pred_val:
            score_value = save_score_fcn(score, forecast_pk, unit_pk, target_pk, truth_value, lwrs,
                                         lwr_to_pred_val, true_lwr, true_bin_idx, **kwargs)
            score_values.append((score.pk, forecast_pk, unit_pk, target_pk, score_value))

    # insert the ScoreValues!
    _insert_score_values(score_values)

    # print warning count
    logger.warning(f"_calc_bin_score(): num_warnings={num_warnings}")


def _truth_data_pks_for_forecast_model(forecast_model):
    """
    NB: Only compares TruthData.value_i and TruthData.value_f columns. todo xx should base this on Target.type?

    :param forecast_model: a ForecastModel
    :return: truth data in forecast_model as a list of 5-tuples where truth_value is the (first?) non-null truth value
        in TruthData.value_i and TruthData.value_f: (time_zero_pk, forecast_pk, unit_pk, target_pk, truth_value)

    """
    sql = f"""
        SELECT truthd.time_zero_id, f.id, truthd.unit_id, truthd.target_id, COALESCE(truthd.value_i, truthd.value_f)
        FROM {TruthData._meta.db_table} AS truthd
               LEFT JOIN {TimeZero._meta.db_table} AS tz ON truthd.time_zero_id = tz.id
               LEFT JOIN {Forecast._meta.db_table} AS f ON tz.id = f.time_zero_id
               LEFT JOIN {ForecastModel._meta.db_table} AS fm ON f.forecast_model_id = fm.id
        WHERE fm.id = %s;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (forecast_model.pk,))
        return cursor.fetchall()


def _insert_score_values(score_values):
    """
    Called by _calc_bin_score(), does a bulk insert of score_values - creates ScoreValue instances. See docs in
    _insert_prediction_rows() for postgres-specific rationale.

    :param score_values: a list of 5-tuples: (score.pk, forecast.pk, unit.pk, target.pk, score_value)
    """
    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    columns_names = [ScoreValue._meta.get_field('score').column,
                     ScoreValue._meta.get_field('forecast').column,
                     ScoreValue._meta.get_field('unit').column,
                     ScoreValue._meta.get_field('target').column,
                     ScoreValue._meta.get_field('value').column]
    scorevalue_table_name = ScoreValue._meta.db_table
    with connection.cursor() as cursor:
        if connection.vendor == 'postgresql':
            string_io = io.StringIO()
            csv_writer = csv.writer(string_io, delimiter=',')
            csv_writer.writerows(score_values)
            string_io.seek(0)
            cursor.copy_from(string_io, scorevalue_table_name, columns=columns_names, sep=',', null=POSTGRES_NULL_VALUE)
        else:
            column_names = (', '.join(columns_names))
            values_percent_s = ', '.join(['%s'] * len(columns_names))
            sql = f"""
                INSERT INTO {scorevalue_table_name} ({column_names})
                VALUES ({values_percent_s});
                """
            cursor.executemany(sql, score_values)


#
# ---- predictive distribution (aka 'bin') lookup functions ----
#

def _tz_loc_targ_pk_to_true_lwr(project):
    """
    Returns project's TruthData merged with the project's TargetLwrs:

        [timezero_pk][unit_pk][target_pk] -> true_lwr

    We need the TargetLwr to get lwr and upper for the truth.
    NB: Only compares TruthData.value_i and TruthData.value_f columns. todo xx should base this on Target.type?
    """
    sql = f"""
        SELECT truthd.time_zero_id, truthd.unit_id, truthd.target_id, targlwr.lwr
        FROM {TruthData._meta.db_table} as truthd
               LEFT JOIN {TargetLwr._meta.db_table} as targlwr
                    ON truthd.target_id = targlwr.target_id
               LEFT JOIN {Target._meta.db_table} as target
                    ON targlwr.target_id = target.id
        WHERE target.project_id = %s
          AND ((COALESCE(truthd.value_i, truthd.value_f) >= targlwr.lwr) OR
               ((COALESCE(truthd.value_i, truthd.value_f) IS NULL) AND (targlwr.lwr IS NULL)))
          AND ((COALESCE(truthd.value_i, truthd.value_f) < targlwr.upper) OR
               ((COALESCE(truthd.value_i, truthd.value_f) IS NULL) AND (targlwr.upper IS NULL)))
        ORDER BY truthd.time_zero_id, truthd.unit_id, truthd.target_id, targlwr.lwr
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        rows = cursor.fetchall()

    # build the dict
    tz_loc_targ_pks_to_true_lwr = {}  # {timezero_pk: {unit_pk: {target_id: true_lwr}}}
    for time_zero_id, loc_target_val_grouper in groupby(rows, key=lambda _: _[0]):
        loc_targ_pks_to_truth_bin_start = {}  # {unit_pk: {target_id: true_lwr}}
        tz_loc_targ_pks_to_true_lwr[time_zero_id] = loc_targ_pks_to_truth_bin_start
        for unit_id, target_val_grouper in groupby(loc_target_val_grouper, key=lambda _: _[1]):
            target_pk_to_truth = {}  # {target_id: true_lwr}
            loc_targ_pks_to_truth_bin_start[unit_id] = target_pk_to_truth
            for _, _, target_id, true_lwr in target_val_grouper:
                target_pk_to_truth[target_id] = true_lwr

    return tz_loc_targ_pks_to_true_lwr


def _targ_pk_to_lwrs(project):
    """
    Returns project's lwr data as a dict: [target_pk] -> [lwr_1, ...]. Each list is sorted by lwr.
    Only returns rows whose targets match numeric_targets().
    """
    targets = project.numeric_targets()
    target_lwr_qs = TargetLwr.objects \
        .filter(target__in=targets) \
        .order_by('target__id', 'lwr') \
        .values_list('target__id', 'lwr')

    # build the dict
    target_pk_to_lwrs = {}  # {target_id: [lwr_1, ...]}
    for target_id, lwr_grouper in groupby(target_lwr_qs, key=lambda _: _[0]):
        target_pk_to_lwrs[target_id] = [lwr for _, lwr in lwr_grouper]

    return target_pk_to_lwrs


def _tz_loc_targ_pk_lwr_to_pred_val(forecast_model):
    """
    Returns prediction data for all forecasts in forecast_model as a dict:

        [timezero_pk][unit_pk][target_pk][cat_value] -> predicted_value

    Only returns rows whose targets match numeric_targets().
    """
    targets = forecast_model.project.numeric_targets()
    bin_dist_qs = BinDistribution.objects \
        .filter(forecast__forecast_model=forecast_model,
                target__in=targets) \
        .order_by('forecast__time_zero__id', 'unit__id', 'target__id') \
        .values_list('forecast__time_zero__id', 'unit__id', 'target__id', 'prob',
                     'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')  # only one of cat_* is non-None

    # build the dict: {timezero_pk: {unit_pk: {target_id: {lwr_1: predicted_value_1, ...}}}}:
    tzltpk_to_forec_st_to_pred_val = {}
    for time_zero_id, unit_target_val_grouper in groupby(bin_dist_qs, key=lambda _: _[0]):
        ltpk_to_forec_start_to_pred_val = {}  # {unit_pk: {target_id: {lwr_1: predicted_value_1, ...}}}
        tzltpk_to_forec_st_to_pred_val[time_zero_id] = ltpk_to_forec_start_to_pred_val
        for unit_id, target_val_grouper in groupby(unit_target_val_grouper, key=lambda _: _[1]):
            # {target_id: {lwr_1: predicted_value_1, ...}}:
            tpk_to_forec_start_to_pred_val = defaultdict(dict)
            ltpk_to_forec_start_to_pred_val[unit_id] = tpk_to_forec_start_to_pred_val
            for _, _, target_id, pred_value, cat_i, cat_f, cat_t, cat_d, cat_b in target_val_grouper:
                cat_value = PointPrediction.first_non_none_value(cat_i, cat_f, cat_t, cat_d, cat_b)
                tpk_to_forec_start_to_pred_val[target_id][cat_value] = pred_value

    return tzltpk_to_forec_st_to_pred_val
