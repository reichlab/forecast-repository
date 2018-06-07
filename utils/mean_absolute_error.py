import logging
from itertools import groupby

from django.db import connection

from forecast_app.models import ForecastData, Forecast, ForecastModel, TimeZero
from forecast_app.models.data import CDCData


logger = logging.getLogger(__name__)


def location_to_mean_abs_error_rows_for_project(project, season_name):
    """
    Called by the project_visualizations() view function, returns a dict containing a table of mean absolute errors for
    all models and all locations in project for season_name. The dict maps:
    {location: (mean_abs_error_rows, target_to_min_mae)}, where rows is a table in the form of a list of rows where each
    row corresponds to a model, and each column corresponds to a target, i.e., X=target vs. Y=Model.

    See _mean_abs_error_rows_for_project() for the format of mean_abs_error_rows.

    Returns {} if no truth data or no appropriate targets in project.
    """
    if not project.is_truth_data_loaded():  # no reason to do all the work
        return {}

    targets = project.visualization_targets()
    if not targets:
        return {}

    targets = sorted(targets)

    # cache all the data we need for all models
    logger.debug("location_to_mean_abs_error_rows_for_project(): calling: _model_id_to_point_values_dict(). "
                 "project={}, season_name={}, targets={}".format(project, season_name, targets))
    model_id_to_point_values_dict = _model_id_to_point_values_dict(project, season_name, targets)
    logger.debug("location_to_mean_abs_error_rows_for_project(): calling: _model_id_to_forecast_id_tz_date_csv_fname()")
    model_id_to_forecast_id_tz_date_csv_fname = _model_id_to_forecast_id_tz_date_csv_fname(
        project, project.models.all(), season_name)
    logger.debug("location_to_mean_abs_error_rows_for_project(): calling: _mean_abs_error_rows_for_project(), multiple")
    loc_target_tz_date_to_truth = project.location_target_timezero_date_to_truth(season_name)
    location_to_mean_abs_error_rows = {
        location: _mean_abs_error_rows_for_project(project, targets, location, model_id_to_point_values_dict,
                                                   model_id_to_forecast_id_tz_date_csv_fname,
                                                   loc_target_tz_date_to_truth)
        for location in project.get_locations()}
    logger.debug("location_to_mean_abs_error_rows_for_project(): done")
    return location_to_mean_abs_error_rows


def _mean_abs_error_rows_for_project(project, targets, location, model_id_to_point_values_dict,
                                     model_id_to_forecast_id_tz_date_csv_fname, loc_target_tz_date_to_truth):
    """
    Returns a 2-list of the form: (rows, target_to_min_mae), where rows is a table in the form of a list of rows where
    each row corresponds to a model, and each column corresponds to a target, i.e., X=target vs. Y=Model. The format:

        [[model1_pk, target1_mae, target2_mae, ...], ...]

    The first row is the header.

    Recall the Mean Absolute Error table from http://reichlab.io/flusight/ , such as for these settings:

        US National > 2016-2017 > 1 wk, 2 wk, 3 wk, 4 wk ->

        +----------+------+------+------+------+
        | Model    | 1 wk | 2 wk | 3 wk | 4 wk |
        +----------+------+------+------+------+
        | kcde     | 0.29 | 0.45 | 0.61 | 0.69 |
        | kde      | 0.58 | 0.59 | 0.6  | 0.6  |
        | sarima   | 0.23 | 0.35 | 0.49 | 0.56 |
        | ensemble | 0.3  | 0.4  | 0.53 | 0.54 |
        +----------+------+------+------+------+

    The second return arg - target_to_min_mae - is a dict that maps: {target_ minimum_mae). Returns ([], {}) if the
    project does not have appropriate targets defined in its configuration. NB: assumes all of project's models have the
    same targets - something is validated by ForecastModel.load_forecast()
    """
    logger.debug("_mean_abs_error_rows_for_project(): entered. project={}, targets={}, location={}"
                 .format(project, targets, location))
    target_to_min_mae = {target: None for target in targets}  # tracks min MAE for bolding in table. filled next
    rows = [['Model', *targets]]  # header
    for forecast_model in sorted(project.models.all(), key=lambda fm: fm.name):
        row = [forecast_model.pk]
        for target in targets:
            forecast_to_point_dict = model_id_to_point_values_dict[forecast_model.pk] \
                if forecast_model.pk in model_id_to_point_values_dict \
                else {}
            forecast_id_tz_date_csv_fname = model_id_to_forecast_id_tz_date_csv_fname[forecast_model.pk] \
                if forecast_model.pk in model_id_to_forecast_id_tz_date_csv_fname \
                else {}
            mae_val = mean_absolute_error(forecast_model, location, target,
                                          forecast_to_point_dict, forecast_id_tz_date_csv_fname,
                                          loc_target_tz_date_to_truth)
            if not mae_val:
                return [rows, {}]  # just header

            target_to_min_mae[target] = min(mae_val, target_to_min_mae[target]) \
                if target_to_min_mae[target] else mae_val
            row.append(mae_val)
        rows.append(row)

    logger.debug("_mean_abs_error_rows_for_project(): done")
    return [rows, target_to_min_mae]


def mean_absolute_error(forecast_model, location, target, forecast_to_point_dict, forecast_id_tz_date_csv_fname,
                        loc_target_tz_date_to_truth):
    """
    Calculates the mean absolute error for the passed model and parameters. Note: Uses cached values
    (forecast_to_point_dict and forecast_id_tz_date_csv_fname) instead of hitting the database directly, for
    speed.

    :param: forecast_model: ForecastModel whose forecasts are used for the calculation
    :param: location: a location in the model
    :param: target: "" target ""
    :param: forecast_to_point_dict: cached points for forecast_model as returned by _model_id_to_point_values_dict()
    :param: forecast_id_tz_date_csv_fname: cached rows for forecast_model as returned by
        _model_id_to_forecast_id_tz_date_csv_fname()
    :return: mean absolute error (scalar) for my predictions for a location and target. returns None if can't be
        calculated
    """
    forecasts = forecast_model.forecasts.all()
    if not forecasts:
        raise RuntimeError("Could not calculate absolute errors: no data. forecast_model={}".format(forecast_model))

    cdc_file_name_to_abs_error = {}
    for forecast_id, forecast_timezero_date, forecast_csv_filename in forecast_id_tz_date_csv_fname:
        try:
            truth_values = loc_target_tz_date_to_truth[location][target][forecast_timezero_date]
        except KeyError as ke:
            logger.warning("mean_absolute_error(): loc_target_tz_date_to_truth was missing a key: {}. location={}, "
                           "target={}, forecast_timezero_date={}. loc_target_tz_date_to_truth={}"
                           .format(ke.args, location, target, forecast_timezero_date, loc_target_tz_date_to_truth))
            continue  # skip this forecast's contribution to the score

        if len(truth_values) == 0:  # truth not available
            logger.warning("mean_absolute_error(): truth value not found. forecast_model={}, location={!r}, "
                           "target={!r}, forecast_id={}, forecast_timezero_date={}"
                           .format(forecast_model, location, target, forecast_id, forecast_timezero_date))
            continue  # skip this forecast's contribution to the score
        elif len(truth_values) > 1:
            logger.warning("mean_absolute_error(): >1 truth values found. forecast_model={}, location={!r}, "
                           "target={!r}, forecast_id={}, forecast_timezero_date={}, truth_values={}"
                           .format(forecast_model, location, target, forecast_id, forecast_timezero_date, truth_values))
            continue  # skip this forecast's contribution to the score

        true_value = truth_values[0]
        if true_value is None:
            logger.warning("mean_absolute_error(): truth value was NA. forecast_id={}, location={!r}, target={!r}, "
                           "forecast_timezero_date={}".format(forecast_id, location, target, forecast_timezero_date))
            continue  # skip this forecast's contribution to the score

        predicted_value = forecast_to_point_dict[forecast_id][location][target]
        abs_error = abs(predicted_value - true_value)
        cdc_file_name_to_abs_error[forecast_csv_filename] = abs_error

    return (sum(cdc_file_name_to_abs_error.values()) / len(cdc_file_name_to_abs_error)) if cdc_file_name_to_abs_error \
        else None


def _model_id_to_forecast_id_tz_date_csv_fname(project, forecast_models, season_name):
    """
    Returns a dict for forecast_models and season_name that maps: ForecastModel.pk -> 3-tuple of the form:
    (forecast_id, forecast_timezero_date, forecast_csv_filename). This is an optimization that avoids some ORM overhead
    when simply iterating like so: `for forecast in forecast_model.forecasts.all(): ...`
    """
    # get the rows, ordered so we can groupby()
    sql = """
        SELECT fm.id, f.id, tz.timezero_date, f.csv_filename
        FROM {forecastmodel_table_name} fm
          JOIN {forecast_table_name} f on fm.id = f.forecast_model_id
          JOIN {timezero_table_name} tz ON f.time_zero_id = tz.id
        WHERE fm.id IN ({model_ids_query_string})
              AND %s <= tz.timezero_date
              AND tz.timezero_date <= %s
        ORDER BY fm.id;
    """.format(forecastmodel_table_name=ForecastModel._meta.db_table,
               forecast_table_name=Forecast._meta.db_table,
               timezero_table_name=TimeZero._meta.db_table,
               model_ids_query_string=', '.join(['%s'] * len(forecast_models)))
    with connection.cursor() as cursor:
        season_start_date, season_end_date = project.start_end_dates_for_season(season_name)
        forecast_model_ids = [forecast_model.pk for forecast_model in forecast_models]
        logger.debug("_model_id_to_forecast_id_tz_date_csv_fname(): calling: execute(): {}, {}".format(
            sql,
            [*forecast_model_ids, season_start_date, season_end_date]))
        cursor.execute(sql, [*forecast_model_ids, season_start_date, season_end_date])
        rows = cursor.fetchall()

    # build the dict
    logger.debug("_model_id_to_forecast_id_tz_date_csv_fname(): building model_id_to_forecast_id_tz_date_csv_fname")
    model_id_to_forecast_id_tz_date_csv_fname = {}  # return value. filled next
    for model_pk, forecast_row_grouper in groupby(rows, key=lambda _: _[0]):
        model_id_to_forecast_id_tz_date_csv_fname[model_pk] = [row[1:] for row in forecast_row_grouper]

    logger.debug("_model_id_to_forecast_id_tz_date_csv_fname(): done")
    return model_id_to_forecast_id_tz_date_csv_fname


def _model_id_to_point_values_dict(project, season_name, targets):
    """
    :return: a dict that provides predicted point values for all of project's models, season_name, and targets. The dict
        drills down as such:

    - model_to_point_dicts: {forecast_model_id -> forecast_to_point_dicts}
    - forecast_to_point_dicts: {forecast_id -> location_to_point_dicts}
    - location_to_point_dicts: {location -> target_to_points_dicts}
    - target_to_points_dicts: {target -> point_value}
    """
    forecast_models = project.models.all()

    # get the rows, ordered so we can groupby()
    sql = """
        SELECT fm.id, f.id, fd.location, fd.target, fd.value
        FROM {forecast_data_table_name} fd
          JOIN {forecast_table_name} f ON fd.forecast_id = f.id
          JOIN {timezero_table_name} tz ON f.time_zero_id = tz.id
          JOIN {forecastmodel_table_name} fm ON f.forecast_model_id = fm.id
        WHERE fm.id IN ({model_ids_query_string})
              AND fd.row_type = %s
              AND fd.target IN ({target_query_string})
              AND %s <= tz.timezero_date
              AND tz.timezero_date <= %s
        ORDER BY fm.id, f.id, fd.location, fd.target;
    """.format(forecast_data_table_name=ForecastData._meta.db_table,
               forecast_table_name=Forecast._meta.db_table,
               timezero_table_name=TimeZero._meta.db_table,
               forecastmodel_table_name=ForecastModel._meta.db_table,
               model_ids_query_string=', '.join(['%s'] * len(forecast_models)),
               target_query_string=', '.join(['%s'] * len(targets)))
    with connection.cursor() as cursor:
        season_start_date, season_end_date = project.start_end_dates_for_season(season_name)
        forecast_model_ids = [forecast_model.pk for forecast_model in forecast_models]
        logger.debug("_model_id_to_point_values_dict(): calling: execute(): {}, {}".format(
            sql,
            [*forecast_model_ids, CDCData.POINT_ROW_TYPE, *targets, season_start_date, season_end_date]))
        cursor.execute(sql, [*forecast_model_ids, CDCData.POINT_ROW_TYPE, *targets, season_start_date, season_end_date])
        rows = cursor.fetchall()

    # build the dict
    logger.debug("_model_id_to_point_values_dict(): building models_to_point_values_dicts")
    models_to_point_values_dicts = {}  # return value. filled next
    for model_pk, forecast_loc_target_val_grouper in groupby(rows, key=lambda _: _[0]):
        forecast_to_point_dicts = {}
        for forecast_pk, loc_target_val_grouper in groupby(forecast_loc_target_val_grouper, key=lambda _: _[1]):
            location_to_point_dicts = {}
            for location, target_val_grouper in groupby(loc_target_val_grouper, key=lambda _: _[2]):
                grouper_rows = list(target_val_grouper)
                location_to_point_dicts[location] = {grouper_row[3]: grouper_row[4] for grouper_row in grouper_rows}
            forecast_to_point_dicts[forecast_pk] = location_to_point_dicts
        models_to_point_values_dicts[model_pk] = forecast_to_point_dicts

    logger.debug("_model_id_to_point_values_dict(): done")
    return models_to_point_values_dicts
