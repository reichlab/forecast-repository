from itertools import groupby

from forecast_app.models import ForecastData, ForecastModel
from forecast_app.models.data import CDCData
#
# This file defines functions related to the Flusight D3 component at https://github.com/reichlab/d3-foresight
#
from utils.utilities import YYYYMMDD_DATE_FORMAT


def flusight_location_to_data_dict(forecast_models, season_name, request=None):
    """
    Returns a dict containing forecast_model's point forecasts for all locations in season_name, structured
    according to https://github.com/reichlab/d3-foresight . Keys are the locations, and values are the individual data
    dicts for each as expected by the component. Passing all locations this way allows only a single page load for a
    particular season, with subsequent user selection of locations doing only a data replot in the component.

    Recall the format of the data dicts:

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

    where timePoints is a list of dates in "YYYYMMDD" format, and predictions is a list of 'series' objects containing
    'point' objects, e.g.,

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
    - Returns None if the project has no visualization_targets().
    - If request is passed then it is used to calculate each model's absolute URL (used in the flusight component's info
      box). o/w the model's home_url is used
    """
    if not forecast_models:
        return None

    projects = [forecast_model.project for forecast_model in forecast_models]
    if not projects.count(projects[0]) == len(projects):
        raise RuntimeError("Not all models are in the same Project")

    project = projects[0]
    targets = project.visualization_targets()
    if not targets:
        return None
    else:
        targets = sorted(targets)

    # set time_points. order_by -> matches ORDER BY in _flusight_point_value_rows_for_models():
    project_timezeros = project.timezeros_in_season(season_name)
    model_to_location_timezero_points = _model_to_location_timezero_points(project, forecast_models, season_name,
                                                                           targets)

    # now that we have model_to_location_timezero_points, we can build the return value, extracting each
    # location from all of the models
    locations_to_flusight_data_dicts = {}  # return value. filled next
    for location in project.get_locations():
        model_dicts = _model_dicts_for_location_to_timezero_points(project_timezeros, location,
                                                                   model_to_location_timezero_points, request)
        data_dict = {'timePoints': [timezero.timezero_date.strftime(YYYYMMDD_DATE_FORMAT)
                                    for timezero in project_timezeros],
                     'models': sorted(model_dicts, key=lambda _: _['meta']['name'])}
        locations_to_flusight_data_dicts[location] = data_dict
    return locations_to_flusight_data_dicts


def _model_dicts_for_location_to_timezero_points(project_timezeros, location,
                                                 model_to_location_timezero_points, request):
    model_dicts = []
    for forecast_model, location_to_timezero_points in model_to_location_timezero_points.items():
        timezero_to_points = location_to_timezero_points[location] if location in location_to_timezero_points \
            else {}  # NB: ordered by timezero_date
        model_dict = {
            'id': forecast_model.name[:10] + '(' + str(forecast_model.id) + ')',
            'meta': {
                'name': forecast_model.name,
                'description': forecast_model.description,
                # 'url': forecast_model.home_url,
                'url': request.build_absolute_uri(
                    forecast_model.get_absolute_url()) if request else forecast_model.home_url,
            },
            'predictions': _prediction_dicts_for_timezero_points(project_timezeros, timezero_to_points)
        }
        model_dicts.append(model_dict)
    return model_dicts


def _prediction_dicts_for_timezero_points(project_timezeros, timezero_to_points):
    prediction_dicts = []
    for timezero in project_timezeros:
        if timezero.timezero_date in timezero_to_points:
            point_dicts = [{'point': point} for point in timezero_to_points[timezero.timezero_date]]
            prediction_dicts.append({'series': point_dicts})
        else:  # no forecasts for this TimeZero
            prediction_dicts.append(None)
    return prediction_dicts


def _model_to_location_timezero_points(project, forecast_models, season_name, targets):
    """
    :return: a dict that maps: forecast_model -> location_dict. each location_dict maps: location ->
        timezero_points_dict, which maps timezero_datetime -> point values. note that some project TimeZeros have no
        predictions
    """
    # get the rows, ordered so we can groupby()
    # note that some project timezeros might not be returned by _flusight_point_value_rows_for_models():
    # query notes:
    # - ORDER BY ensures groupby() will work
    # - we don't need to select targets b/c forecast ids have 1:1 correspondence to TimeZeros
    # - "" b/c targets are needed only for ordering
    season_start_date, season_end_date = project.start_end_dates_for_season(season_name)
    rows = ForecastData.objects \
        .filter(forecast__forecast_model__in=forecast_models) \
        .filter(row_type=CDCData.POINT_ROW_TYPE) \
        .filter(target__in=targets) \
        .filter(forecast__time_zero__timezero_date__gte=season_start_date) \
        .filter(forecast__time_zero__timezero_date__lte=season_end_date) \
        .order_by('forecast__forecast_model__id', 'location', 'forecast__time_zero__timezero_date', 'target') \
        .values_list('forecast__forecast_model__id', 'location', 'forecast__time_zero__timezero_date', 'value')

    # build the dict
    model_to_location_timezero_points = {}  # return value. filled next
    for model_pk, loc_tz_val_grouper in groupby(rows, key=lambda _: _[0]):
        location_to_timezero_points_dict = {}
        for location, timezero_values_grouper in groupby(loc_tz_val_grouper, key=lambda _: _[1]):
            timezero_to_points_dict = {}
            for timezero_date, values_grouper in groupby(timezero_values_grouper, key=lambda _: _[2]):
                point_values = [_[3] for _ in list(values_grouper)]
                timezero_to_points_dict[timezero_date] = point_values
            location_to_timezero_points_dict[location] = timezero_to_points_dict
        forecast_model = ForecastModel.objects.get(pk=model_pk)
        model_to_location_timezero_points[forecast_model] = location_to_timezero_points_dict

    # b/c _flusight_point_value_rows_for_models() does not return any rows for models that don't have data for
    # season_name and targets, we need to add empty model entries for callers
    for forecast_model in forecast_models:
        if forecast_model not in model_to_location_timezero_points:
            model_to_location_timezero_points[forecast_model] = {}

    return model_to_location_timezero_points
