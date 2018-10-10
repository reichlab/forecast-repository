import logging
from itertools import groupby

from forecast_app.models import ForecastData, Forecast
from forecast_app.models.data import CDCData


logger = logging.getLogger(__name__)


def location_to_mean_abs_error_rows_for_project(project, season_name):
    """
    Called by the project_scores() view function, returns a dict containing a table of mean absolute errors for
    all models and all locations in project for season_name. The dict maps:
    {location.name: (mean_abs_error_rows, target_to_min_mae)}, where rows is a table in the form of a list of rows where
    each row corresponds to a model, and each column corresponds to a target, i.e., X=target vs. Y=Model.

    See _mean_abs_error_rows_for_project() for the format of mean_abs_error_rows.

    Returns {} if no truth data or no appropriate target_names in project.
    """
    if not project.is_truth_data_loaded():  # no reason to do all the work
        return {}

    target_names = [target.name for target in project.visualization_targets()]
    if not target_names:
        return {}

    # cache all the data we need for all models
    model_id_to_point_values_dict = _model_id_to_point_values_dict(project, target_names, season_name)
    model_id_to_forecast_id_tz_dates = _model_id_to_forecast_id_tz_dates(project, season_name)
    loc_target_tz_date_to_truth = project.location_target_name_tz_date_to_truth(season_name)  # target__id
    forecast_models = project.models.order_by('name')

    for forecast_model in forecast_models:
        if not forecast_model.forecasts.exists():
            logger.warning("location_to_mean_abs_error_rows_for_project(): could not calculate absolute errors: model "
                           "had no data: {}".format(forecast_model))
            continue

    location_to_mean_abs_error_rows = {
        location_name: _mean_abs_error_rows_for_project(forecast_models, target_names, location_name,
                                                        model_id_to_point_values_dict, model_id_to_forecast_id_tz_dates,
                                                        loc_target_tz_date_to_truth)
        for location_name in project.locations.all().values_list('name', flat=True)}
    return location_to_mean_abs_error_rows


def _mean_abs_error_rows_for_project(forecast_models, target_names, location_name,
                                     model_id_to_point_values_dict, model_id_to_forecast_id_tz_dates,
                                     loc_target_tz_date_to_truth):
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
    project does not have appropriate target_names defined in its configuration. NB: assumes all of project's models have the
    same target_names - something is validated by ForecastModel.load_forecast()
    """
    target_to_min_mae = {target: None for target in target_names}  # tracks min MAE for bolding in table. filled next
    rows = [['Model', *target_names]]  # header
    for forecast_model in forecast_models:
        row = [forecast_model.pk]
        for target_name in target_names:
            forecast_to_point_dict = model_id_to_point_values_dict[forecast_model.pk] \
                if forecast_model.pk in model_id_to_point_values_dict \
                else {}
            forecast_id_tz_dates = model_id_to_forecast_id_tz_dates[forecast_model.pk] \
                if forecast_model.pk in model_id_to_forecast_id_tz_dates \
                else {}
            mae_val = mean_absolute_error(forecast_model, location_name, target_name,
                                          forecast_to_point_dict, forecast_id_tz_dates, loc_target_tz_date_to_truth)
            if not mae_val:
                logger.warning("_mean_abs_error_rows_for_project(): no mae_val. forecast_model={}, location_name={!r}, "
                               "target_name={!r}".format(forecast_model, location_name, target_name))
                row.append(None)  # NB: has to be handled correctly when displaying; o/w might show as NaN, etc.
                continue

            target_to_min_mae[target_name] = min(mae_val, target_to_min_mae[target_name]) \
                if target_to_min_mae[target_name] else mae_val
            row.append(mae_val)
        rows.append(row)

    return [rows, target_to_min_mae]


def mean_absolute_error(forecast_model, location_name, target_name,
                        forecast_to_point_dict, forecast_id_tz_dates, loc_target_tz_date_to_truth):
    """
    Calculates the mean absolute error for the passed model and parameters. Note: Uses cached values
    (forecast_to_point_dict and forecast_id_tz_dates) instead of hitting the database directly, for
    speed.

    :param: forecast_model: ForecastModel whose forecasts are used for the calculation
    :param: location_name: a location_name in the model
    :param: target_name: "" target_name ""
    :param: forecast_to_point_dict: cached points for forecast_model as returned by _model_id_to_point_values_dict()
    :param: forecast_id_tz_dates: cached rows for forecast_model as returned by _model_id_to_forecast_id_tz_dates()
    :return: mean absolute error (scalar) for my predictions for a location_name and target_name. returns None if can't
        be calculated
    """
    forecast_id_to_abs_error = {}


    def mae_fcn(forecast_id, forecast_timezero_date, predicted_value, true_value):
        abs_error = abs(predicted_value - true_value)
        forecast_id_to_abs_error[forecast_id] = abs_error


    iterate_forecast_errors(forecast_model, location_name, target_name,
                            forecast_to_point_dict, forecast_id_tz_dates, loc_target_tz_date_to_truth,
                            mae_fcn)

    return (sum(forecast_id_to_abs_error.values()) / len(forecast_id_to_abs_error)) if forecast_id_to_abs_error \
        else None


#
# caching functions to speed up prediction and truth calculations
#

def _model_id_to_forecast_id_tz_dates(project, season_name=None):
    """
    Returns a dict for forecast_models and season_name that maps: ForecastModel.pk -> list of 2-tuples of the form:
    (forecast_id, forecast_timezero_date). This is an optimization that avoids some ORM overhead when simply iterating
    like so: `for forecast in forecast_model.forecasts.all(): ...`
    """
    logger.debug("_model_id_to_forecast_id_tz_dates(): entered. project={}, season_name={}"
                 .format(project, season_name))
    # get the rows, ordered so we can groupby()
    forecast_data_qs = Forecast.objects.filter(forecast_model__project=project)
    if season_name:
        season_start_date, season_end_date = project.start_end_dates_for_season(season_name)
        forecast_data_qs = forecast_data_qs \
            .filter(time_zero__timezero_date__gte=season_start_date,
                    time_zero__timezero_date__lte=season_end_date)
    forecast_data_qs = forecast_data_qs \
        .order_by('forecast_model__id') \
        .values_list('forecast_model__id', 'id', 'time_zero__timezero_date')

    # build the dict
    model_id_to_forecast_id_tz_date = {}  # return value. filled next
    for model_pk, forecast_row_grouper in groupby(forecast_data_qs, key=lambda _: _[0]):
        model_id_to_forecast_id_tz_date[model_pk] = [row[1:] for row in forecast_row_grouper]

    logger.debug("_model_id_to_forecast_id_tz_dates(): done ({})".format(len(model_id_to_forecast_id_tz_date)))
    return model_id_to_forecast_id_tz_date


def _model_id_to_point_values_dict(project, target_names, season_name=None):
    """
    :return: a dict that provides predicted point values for all of project's models, season_name, and target_names.
        Use is like: the_dict[forecast_model_id][forecast_id][location_name][target_name]
    """
    logger.debug("_model_id_to_point_values_dict(): entered. project={}, target_names={}, season_name={}"
                 .format(project, target_names, season_name))
    # get the rows, ordered so we can groupby()
    forecast_data_qs = ForecastData.objects \
        .filter(row_type=CDCData.POINT_ROW_TYPE,
                target__name__in=target_names,
                forecast__forecast_model__project=project)
    if season_name:
        season_start_date, season_end_date = project.start_end_dates_for_season(season_name)
        forecast_data_qs = forecast_data_qs \
            .filter(forecast__time_zero__timezero_date__gte=season_start_date,
                    forecast__time_zero__timezero_date__lte=season_end_date)
    forecast_data_qs = forecast_data_qs \
        .order_by('forecast__forecast_model__id', 'forecast__id', 'location__id') \
        .values_list('forecast__forecast_model__id', 'forecast__id', 'location__name', 'target__name', 'value')

    # build the dict
    models_to_point_values_dicts = {}  # return value. filled next
    for model_pk, forecast_loc_target_val_grouper in groupby(forecast_data_qs, key=lambda _: _[0]):
        forecast_to_point_dicts = {}
        for forecast_pk, loc_target_val_grouper in groupby(forecast_loc_target_val_grouper, key=lambda _: _[1]):
            location_to_point_dicts = {}
            for location, target_val_grouper in groupby(loc_target_val_grouper, key=lambda _: _[2]):
                grouper_rows = list(target_val_grouper)
                location_to_point_dicts[location] = {grouper_row[3]: grouper_row[4] for grouper_row in grouper_rows}
            forecast_to_point_dicts[forecast_pk] = location_to_point_dicts
        models_to_point_values_dicts[model_pk] = forecast_to_point_dicts

    logger.debug("_model_id_to_point_values_dict(): done ({})".format(len(models_to_point_values_dicts)))
    return models_to_point_values_dicts


#
# iterate_forecast_errors()
#

def iterate_forecast_errors(forecast_model, location_name, target_name,
                            forecast_to_point_dict, forecast_id_tz_dates, loc_target_tz_date_to_truth,
                            error_fcn):
    """
    A helper function that takes a function of four args and calls it for each forecast id and timezero_date in
    forecast_id_tz_dates. This function handles various errors related to truth, skipping those forecasts that caused
    errors. error_fcn's args:

        forecast_id, forecast_timezero_date, predicted_value, true_value

    error_fcn is purely used for its side-effects - this function returns None.
    """
    for forecast_id, forecast_timezero_date in forecast_id_tz_dates:
        try:
            truth_values = loc_target_tz_date_to_truth[location_name][target_name][forecast_timezero_date]
        except KeyError as ke:
            logger.warning("calculate_absolute_error(): loc_target_tz_date_to_truth was missing a key: {}. "
                           "location_name={}, target_name={}, forecast_timezero_date={}. loc_target_tz_date_to_truth={}"
                           .format(ke.args, location_name, target_name, forecast_timezero_date,
                                   loc_target_tz_date_to_truth))
            continue  # skip this forecast's contribution to the score

        if len(truth_values) == 0:  # truth not available
            logger.warning("calculate_absolute_error(): truth value not found. forecast_model={}, location_name={!r}, "
                           "target_name={!r}, forecast_id={}, forecast_timezero_date={}"
                           .format(forecast_model, location_name, target_name, forecast_id, forecast_timezero_date))
            continue  # skip this forecast's contribution to the score
        elif len(truth_values) > 1:
            logger.warning("calculate_absolute_error(): >1 truth values found. forecast_model={}, location_name={!r}, "
                           "target_name={!r}, forecast_id={}, forecast_timezero_date={}, truth_values={}"
                           .format(forecast_model, location_name, target_name, forecast_id, forecast_timezero_date,
                                   truth_values))
            continue  # skip this forecast's contribution to the score

        true_value = truth_values[0]
        if true_value is None:
            logger.warning("calculate_absolute_error(): truth value was None. forecast_id={}, location_name={!r}, "
                           "target_name={!r}, forecast_timezero_date={}"
                           .format(forecast_id, location_name, target_name, forecast_timezero_date))
            continue  # skip this forecast's contribution to the score

        predicted_value = forecast_to_point_dict[forecast_id][location_name][target_name]
        error_fcn(forecast_id, forecast_timezero_date, predicted_value, true_value)
