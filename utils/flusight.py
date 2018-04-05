#
# This file defines functions related to the Flusight D3 component at https://github.com/reichlab/d3-foresight
#
from itertools import groupby

import pymmwr
from django.db import connection

from forecast_app.models import ForecastData, Forecast, TimeZero, ForecastModel
from forecast_app.models.data import CDCData


def data_dict_for_models(forecast_models, location):
    """
    Returns a dict containing forecast_model's point forecasts structured according to
    https://github.com/reichlab/d3-foresight , i.e.,

    let data = {
    timePoints,
    models: [
      {
        id: 'mod',
        meta: {
          name: 'Name',
          description: 'Model description here',
          url: 'http://github.com'
        },
        predictions
      }
    ]
    }

    where timePoints is a list of objects like: { "week": 1, "year": 2016 },

    and predictions is a list of 'series' objects containing 'point' objects, e.g.,

     "predictions": [
       {
         "series": [
           { "point": 0.7090864619172196 },
           { "point": 1.4934249007589637 }
         ]
       },
       {
         "series": [
           { "point": 0.912445314619254 },
           { "point": 0.4646919757087566 }
         ]
       }
       ...
     ]

    Notes:
    - The length of predictions must match that of timePoints, using null for missing points.
    - All models must belong to the same Project.
    - Returns None if the project has no get_targets_for_mean_absolute_error().
    """
    if not forecast_models:
        return None

    projects = [forecast_model.project for forecast_model in forecast_models]
    if not projects.count(projects[0]) == len(projects):
        raise RuntimeError("Not all models are in the same Project")

    targets = projects[0].get_targets_for_mean_absolute_error()
    if not targets:
        return None

    targets = sorted(targets)
    model_dicts = [_flusight_model_dict_for_model(forecast_model, location, targets) for forecast_model in
                   forecast_models]
    return {'timePoints': (_time_points_for_project(projects[0])),
            'models': sorted(model_dicts, key=lambda _: _['meta']['name'])}


def _time_points_for_project(project):
    time_points = []
    # order_by -> matches ORDER BY in _point_values_for_model():
    for time_zero in project.timezeros.order_by('timezero_date'):
        tz_ywd_mmwr_dict = pymmwr.date_to_mmwr_week(time_zero.timezero_date)
        time_points.append({'week': tz_ywd_mmwr_dict['week'],
                            'year': tz_ywd_mmwr_dict['year']})
    return time_points


def _flusight_model_dict_for_model(forecast_model, location, targets):
    prediction_dicts = _prediction_dicts_for_model(forecast_model, location, targets)
    return {
        # todo xx model_abbr from metadata.txt? here we include id to ensure truncated model names are unique
        'id': forecast_model.name[:10] + '(' + str(forecast_model.id) + ')',
        'meta': {
            'name': forecast_model.name,
            'description': forecast_model.description,
            'url': forecast_model.home_url},
        'predictions': prediction_dicts
    }


def _prediction_dicts_for_model(forecast_model, location, targets):
    """
    :return: a list of series dicts, one per project TimeZero, each containing a list of point predictions, one per
        each passed 'n step ahead' target
    """
    # first set timezeros_to_target_points, which maps all Project TimeZeros initially to None. updated next for
    # existing forecasts. basically a LEFT OUTER JOIN
    timezeros_to_target_points = {time_zero.timezero_date: None
                                  for time_zero in forecast_model.project.timezeros.all()}
    point_value_rows = _point_values_for_model(forecast_model, location, targets)
    for timezero_date, point_value_grouper in groupby(point_value_rows, key=lambda _: _[0]):
        point_values = [_[1] for _ in list(point_value_grouper)]  # tz.timezero_date, fd.value
        timezeros_to_target_points[timezero_date] = point_values  # replace None

    # now build predictions dicts from timezeros_to_target_points
    prediction_dicts = []
    for timezero_date in sorted(timezeros_to_target_points.keys()):
        if timezeros_to_target_points[timezero_date]:
            points_list = [{'point': point} for point in timezeros_to_target_points[timezero_date]]
            prediction_dicts.append({'series': points_list})
        else:  # no forecasts for this TimeZero
            prediction_dicts.append(None)
    return prediction_dicts


def _point_values_for_model(forecast_model, location, targets):
    # query notes:
    # - ORDER BY ensures groupby() will work
    # - we don't need to select the timezero dates or targets b/c forecast ids have 1:1 correspondence to TimeZeros
    # - "" b/c targets are needed only for ordering
    sql = """
        SELECT tz.timezero_date, fd.value
        FROM {forecast_data_table_name} fd
          JOIN {forecast_table_name} f ON fd.forecast_id = f.id
          JOIN {timezero_table_name} tz ON f.time_zero_id = tz.id
          JOIN {forecastmodel_table_name} fm ON f.forecast_model_id = fm.id
        WHERE fd.location = %s
              AND fd.target IN ({target_query_string})
              AND fd.row_type = %s
              AND fm.id = %s
        ORDER BY fd.forecast_id, fd.target;
    """.format(forecast_data_table_name=ForecastData._meta.db_table,
               forecast_table_name=Forecast._meta.db_table,
               timezero_table_name=TimeZero._meta.db_table,
               forecastmodel_table_name=ForecastModel._meta.db_table,
               target_query_string=', '.join(['%s'] * len(targets)))
    with connection.cursor() as cursor:
        cursor.execute(sql, [location, *targets, CDCData.POINT_ROW_TYPE, forecast_model.pk])
        return cursor.fetchall()
