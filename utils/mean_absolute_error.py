import logging
from itertools import groupby

from django.db import connection

from forecast_app.models import Forecast, Score, ScoreValue, ForecastModel, TimeZero


logger = logging.getLogger(__name__)


def unit_to_mean_abs_error_rows_for_project(project, season_name):
    """
    Called by the project_scores() view function, returns a dict containing a table of mean absolute errors for all
    models and all units in project for season_name, or for all timezeros if season_name=None. The dict maps:

        {unit_name: (mean_abs_error_rows, target_to_min_mae), ...}

    where the 2-tuples are detailed next. Returns {} if no truth data or no appropriate target_names in project.

    The first tuple - `mean_abs_error_rows` - is a table in the form of a list of score_value_rows where each row
    corresponds to a model, and each column corresponds to a target, i.e., X=target vs. Y=Model. The format:

        [[model1_pk, target1_mae, target2_mae, ...], ...]

    The first row is the header. For example, recall the Mean Absolute Error table from http://reichlab.io/flusight/ ,
    such as for these settings: US National > 2016-2017 > 1 wk, 2 wk, 3 wk, 4 wk ->

        +----------+------+------+------+------+
        | Model    | 1 wk | 2 wk | 3 wk | 4 wk |
        +----------+------+------+------+------+
        | kcde     | 0.29 | 0.45 | 0.61 | 0.69 |
        | kde      | 0.58 | 0.59 | 0.6  | 0.6  |
        | sarima   | 0.23 | 0.35 | 0.49 | 0.56 |
        | ensemble | 0.3  | 0.4  | 0.53 | 0.54 |
        +----------+------+------+------+------+

    The second tuple - `target_to_min_mae` - is a dict that maps: {target: minimum_mae). It is ([], {}) if the project
    does not have appropriate target_names defined in its configuration. NB: assumes all of project's models have the
    same target_names
    """
    targets = project.numeric_targets()  # order_by('name')
    if not targets:
        return {}

    for forecast_model in project.models.all():
        if not forecast_model.forecasts.exists():
            logger.warning("unit_to_mean_abs_error_rows_for_project(): could not calculate absolute errors: model "
                           "had no data: {}".format(forecast_model))
            continue

    # first build loc_to_model_to_target_to_mae
    score_value_rows = _score_value_rows_for_season(project, season_name)
    unit_id_to_obj = {unit.pk: unit for unit in project.units.all()}
    forecast_model_id_to_obj = {forecast_model.pk: forecast_model for forecast_model in project.models.all()}
    target_id_to_obj = {target.pk: target for target in project.targets.all()}
    loc_to_model_to_target_to_mae = {}
    for unit_pk, model_target_avg_grouper in groupby(score_value_rows, key=lambda _: _[0]):
        model_to_target_to_mae = {}
        for forecast_model_pk, target_avg_grouper in groupby(model_target_avg_grouper, key=lambda _: _[1]):
            target_to_mae = {}
            for _, _, target_pk, mae in list(target_avg_grouper):
                target_to_mae[target_id_to_obj[target_pk]] = mae
            model_to_target_to_mae[forecast_model_id_to_obj[forecast_model_pk]] = target_to_mae
        loc_to_model_to_target_to_mae[unit_id_to_obj[unit_pk]] = model_to_target_to_mae

    # done
    return {unit.name: (_mean_abs_error_rows_for_loc(loc_to_model_to_target_to_mae, unit, targets),
                            _target_to_min_mae_for_loc(loc_to_model_to_target_to_mae, unit, targets))
            for unit in loc_to_model_to_target_to_mae}


def _mean_abs_error_rows_for_loc(loc_to_model_to_target_to_mae, unit, targets):
    """
    :return: the first tuple as described above
    """
    rows = [['Model', *[target.name for target in targets]]]  # header
    # for forecast_model, target_to_mae in loc_to_model_to_target_to_mae[unit].items():
    model_to_target_to_mae = loc_to_model_to_target_to_mae[unit]
    for forecast_model in sorted(model_to_target_to_mae.keys(), key=lambda model: model.name):
        target_to_mae = model_to_target_to_mae[forecast_model]
        model_row = [forecast_model.pk]
        for target in targets:
            # todo xx will this break the template javascript?:
            model_row.append(target_to_mae[target] if target in target_to_mae else None)
        rows.append(model_row)
    return rows


def _target_to_min_mae_for_loc(loc_to_model_to_target_to_mae, unit, targets):
    """
    :return: the second tuple as described above
    """
    target_to_min_mae = {target.name: None for target in targets}  # tracks min MAE for bolding in table. filled next
    for forecast_model, target_to_mae in loc_to_model_to_target_to_mae[unit].items():
        for target, mae_val in target_to_mae.items():
            target_to_min_mae[target.name] = min(mae_val, target_to_min_mae[target.name]) \
                if target_to_min_mae[target.name] else mae_val
    return target_to_min_mae


def _score_value_rows_for_season(project, season_name):
    score = Score.objects.filter(abbreviation='abs_error').first()  # hard-coded official abbrev
    if not score:
        raise RuntimeError('could not find score')

    if not score.num_score_values_for_project(project):
        raise RuntimeError('no score values for project')

    # rows are ordered so we can groupby()
    # todo xx use meta for column names
    sql_select = """
        SELECT sv.unit_id, model.id, sv.target_id, avg(sv.value)
        FROM {scorevalue_table_name} as sv
               LEFT JOIN {forecast_table_name} f on sv.forecast_id = f.id
               LEFT JOIN {forecastmodel_table_name} model on f.forecast_model_id = model.id
               LEFT JOIN {timezero_table_name} tz on f.time_zero_id = tz.id
    """.format(scorevalue_table_name=ScoreValue._meta.db_table,
               forecast_table_name=Forecast._meta.db_table,
               forecastmodel_table_name=ForecastModel._meta.db_table,
               timezero_table_name=TimeZero._meta.db_table)
    sql_where = """
        WHERE sv.score_id = %s
          AND model.project_id = %s
    """
    if season_name:
        sql_where = sql_where + """
          AND tz.timezero_date >= %s
          AND tz.timezero_date <= %s
        """

    sql_group_order_by = """
        GROUP BY sv.unit_id, model.id, sv.target_id
        ORDER BY sv.unit_id, model.id, sv.target_id;
    """

    sql = sql_select + sql_where + sql_group_order_by
    with connection.cursor() as cursor:
        if season_name:
            season_start_date, season_end_date = project.start_end_dates_for_season(season_name)
            cursor.execute(sql, (score.pk, project.pk, season_start_date, season_end_date))
        else:
            cursor.execute(sql, (score.pk, project.pk))
        return cursor.fetchall()
