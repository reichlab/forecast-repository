from itertools import groupby

from forecast_app.models import ForecastModel, PointPrediction


#
# This file defines functions related to the Flusight D3 component at https://github.com/reichlab/d3-foresight
#

YYYYMMDD_DATE_FORMAT = '%Y%m%d'  # e.g., '20170117'


def flusight_unit_to_data_dict(project, season_name, request=None):
    """
    Returns a dict containing project's forecast_model's point forecasts for all units in season_name, structured
    according to https://github.com/reichlab/d3-foresight . Keys are the unit names, and values are the individual
    data dicts for each as expected by the component. Passing all units this way allows only a single page load for
    a particular season, with subsequent user selection of units doing only a data replot in the component.

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
    - Returns None if the project has no step_ahead_targets().
    - If request is passed then it is used to calculate each model's absolute URL (used in the flusight component's info
      box). o/w the model's home_url is used
    """
    if not project.models.count():
        return None

    step_ahead_targets = project.step_ahead_targets()
    if not step_ahead_targets:
        return None

    # set time_points. order_by -> matches ORDER BY in _flusight_point_value_rows_for_models():
    project_timezeros = project.timezeros_in_season(season_name)
    model_to_unit_timezero_points = _model_id_to_unit_timezero_points(project, season_name, step_ahead_targets)

    # now that we have model_to_unit_timezero_points, we can build the return value, extracting each
    # unit from all of the models
    units_to_flusight_data_dicts = {}  # return value. filled next
    for unit_name in project.units.all().values_list('name', flat=True):
        model_dicts = _model_dicts_for_unit_to_timezero_points(project_timezeros, unit_name,
                                                                   model_to_unit_timezero_points, request)
        data_dict = {'timePoints': [timezero.timezero_date.strftime(YYYYMMDD_DATE_FORMAT)
                                    for timezero in project_timezeros],
                     'models': sorted(model_dicts, key=lambda _: _['meta']['name'])}
        units_to_flusight_data_dicts[unit_name] = data_dict
    return units_to_flusight_data_dicts


def _model_dicts_for_unit_to_timezero_points(project_timezeros, unit_name,
                                                 model_to_unit_timezero_points, request):
    model_dicts = []
    for forecast_model, unit_to_timezero_points in model_to_unit_timezero_points.items():
        timezero_to_points = unit_to_timezero_points[unit_name] \
            if unit_name in unit_to_timezero_points else {}  # NB: ordered by timezero_date
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
            prediction_dict = {'series': point_dicts}
            if timezero.data_version_date:
                prediction_dict['dataVersionTime'] = timezero.data_version_date.strftime(YYYYMMDD_DATE_FORMAT)
            prediction_dicts.append(prediction_dict)
        else:  # no forecasts for this TimeZero
            prediction_dicts.append(None)
    return prediction_dicts


def _model_id_to_unit_timezero_points(project, season_name, step_ahead_targets):
    """
    Returns forecast_model's truth values as a nested dict that's organized for easy access using these keys:

        [forecast_model][unit][timezero_date] -> point_values (a list)

    Note that some project TimeZeros have no predictions.
    """
    # get the rows, ordered so we can groupby()
    # note that some project timezeros might not be returned by _flusight_point_value_rows_for_models():
    # query notes:
    # - ORDER BY ensures groupby() will work
    # - we don't need to select targets b/c forecast ids have 1:1 correspondence to TimeZeros
    # - "" b/c targets are needed only for ordering
    # - ORDER BY target__step_ahead_increment ensures values are sorted by target deterministically
    season_start_date, season_end_date = project.start_end_dates_for_season(season_name)
    forecast_point_predictions_qs = PointPrediction.objects \
        .filter(forecast__forecast_model__project=project,
                target__in=step_ahead_targets,
                forecast__time_zero__timezero_date__gte=season_start_date,
                forecast__time_zero__timezero_date__lte=season_end_date) \
        .order_by('forecast__forecast_model__id', 'unit__id', 'forecast__time_zero__timezero_date',
                  'target__step_ahead_increment') \
        .values_list('forecast__forecast_model__id', 'unit__name', 'forecast__time_zero__timezero_date',
                     'value_i', 'value_f', 'value_t', 'value_d', 'value_b')  # only one of value_* is non-None

    # build the dict
    model_to_unit_timezero_points = {}  # return value. filled next
    for model_pk, loc_tz_val_grouper in groupby(forecast_point_predictions_qs, key=lambda _: _[0]):
        unit_to_timezero_points_dict = {}
        for unit, timezero_values_grouper in groupby(loc_tz_val_grouper, key=lambda _: _[1]):
            timezero_to_points_dict = {}
            for timezero_date, values_grouper in groupby(timezero_values_grouper, key=lambda _: _[2]):
                point_values = [PointPrediction.first_non_none_value(_[3], _[4], _[5], _[6], _[7])
                                for _ in list(values_grouper)]
                timezero_to_points_dict[timezero_date] = point_values
            unit_to_timezero_points_dict[unit] = timezero_to_points_dict
        forecast_model = ForecastModel.objects.get(pk=model_pk)
        model_to_unit_timezero_points[forecast_model] = unit_to_timezero_points_dict

    # b/c _flusight_point_value_rows_for_models() does not return any rows for models that don't have data for
    # season_name and step_ahead_targets, we need to add empty model entries for callers
    for forecast_model in project.models.all():
        if forecast_model not in model_to_unit_timezero_points:
            model_to_unit_timezero_points[forecast_model] = {}

    return model_to_unit_timezero_points
