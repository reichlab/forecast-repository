import pymmwr

from utils.delphi import delphi_wili_for_mmwr_year_week
from utils.utilities import is_date_in_season


def mean_absolute_error(forecast_model, season_start_year, location, target, wili_for_epi_week_fcn):
    """
    :param: forecast_model: ForecastModel whose forecasts are used for the calculation
    :param: location: a location in the model
    :param: target: "" target ""
    :param: season_start_year: year of the season, e.g., 2016 for the season 2016-2017
    :param: wili_for_epi_week_fcn: a function of three args (year, week, location_name) that returns the true/actual
        wili value for an epi week. (2017-09-06: from Abhinav: We use wili for all our work. *w* in wili is for
        weighted. If I recall correctly, wili is the ili which is normalized according to population. So two wilis from
        two different regions can be compared fairly but not two ilis.)
    :return: mean absolute error (scalar) for my predictions for a location and target. returns None if can't be
        calculated
    """
    forecasts = forecast_model.forecasts.all()
    if not forecasts:
        raise RuntimeError("could not calculate absolute errors: no data. forecast_model={}".format(forecast_model))

    week_increment = forecast_model.project.get_week_increment_for_target_name(target)
    if not week_increment:
        return None

    cdc_file_name_to_abs_error = {}
    for forecast in forecasts:
        if not is_date_in_season(forecast.time_zero.timezero_date, season_start_year):
            continue

        tz_ywd_mmwr_dict = pymmwr.date_to_mmwr_week(forecast.time_zero.timezero_date)
        future_yw_mmwr_dict = pymmwr.mmwr_week_with_delta(tz_ywd_mmwr_dict['year'],
                                                          tz_ywd_mmwr_dict['week'],
                                                          week_increment)
        true_value = wili_for_epi_week_fcn(forecast_model.project,
                                           future_yw_mmwr_dict['year'],
                                           future_yw_mmwr_dict['week'],
                                           location)
        predicted_value = forecast.get_target_point_value(location, target)  # slow!
        abs_error = abs(predicted_value - true_value)
        cdc_file_name_to_abs_error[forecast.csv_filename] = abs_error

    return (sum(cdc_file_name_to_abs_error.values()) / len(cdc_file_name_to_abs_error)) if cdc_file_name_to_abs_error \
        else None


def mean_abs_error_rows_for_project(project, season_start_year, location):
    """
    Called by the project_visualizations() view function, returns a table in the form of a list of rows where each row
    corresponds to a model, and each column corresponds to a target, i.e., X=target vs. Y=Model. The format:

        [[model_name1, target1_mae, target2_mae, ...], ...]

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

    Returns [] if the project does not have a config_dict.
    """
    # NB: assumes all of project's models have the same targets - something is validated by
    # ForecastModel.load_forecast()

    # todo return indication of best model for each target -> bold in project_visualizations.html
    targets = project.get_targets_for_mean_absolute_error()
    if not targets:
        return []

    targets = sorted(targets)
    rows = [['Model', *targets]]  # header
    for forecast_model in project.models.all():
        row = [forecast_model.name]
        for target in targets:
            mae_val = mean_absolute_error(forecast_model, season_start_year, location, target,
                                          delphi_wili_for_mmwr_year_week)
            if not mae_val:
                return []

            row.append("{:0.2f}".format(mae_val))
        rows.append(row)
    return rows
