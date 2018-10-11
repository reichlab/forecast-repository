import logging

from django.db import transaction

from forecast_app.models import Forecast, ScoreValue
from utils.mean_absolute_error import _model_id_to_point_values_dict, _model_id_to_forecast_id_tz_dates, \
    iterate_forecast_errors


logger = logging.getLogger(__name__)


#
# ---- Score instance definitions ----
#

# provides information about all scores in the system. used by xx() to create Score instances. maps each Score's
# abbreviation to a 2-tuple: (name, description). recall that the abbreviation is used to look up the corresponding
# function in the forecast_app.scores.functions module - see `calc_<abbreviation>` documentation in Score
SCORE_ABBREV_TO_NAME_AND_DESCR = {
    'error': ('Error', "The the truth value minus the model's point estimate."),
    'abs_error': ('Absolute Error', "The absolute value of the truth value minus the model's point estimate. "
                                    "Lower is better."),
    'const': ('Constant Value', "A debugging score that scores 1.0 only for first location and first target."),
}


#
# ---- 'Constant Value' calculation function ----
#

def calc_const(self, the_project):
    """
    A simple demo that calculates 'Constant Value' scores.
    """
    first_location = the_project.locations.first()
    first_target = the_project.targets.first()
    if (not first_location) or (not first_target):
        logger.warning("calc_const(): no location or no target found. first_location={}, first_target={}"
                       .format(first_location, first_target))
        return

    for forecast in Forecast.objects.filter(forecast_model__project=the_project):
        ScoreValue.objects.create(score=self, forecast=forecast, location=first_location, target=first_target,
                                  value=1.0)


#
# ---- 'Error' and 'Absolute Error' calculation functions ----
#


def calc_error(score, project):
    """
    Calculates 'Error' scores.
    """
    calculate_error_score_values(score, project, is_absolute_error=False)


def calc_abs_error(score, project):
    """
    Calculates 'Absolute Error' scores.
    """
    calculate_error_score_values(score, project, is_absolute_error=True)


@transaction.atomic
def calculate_error_score_values(score, project, is_absolute_error):
    """
    Creates ScoreValue instances for the passed args, saving them into the passed score. The score is simply `true_value
    - predicted_value` (optionally passed to abs() based on is_absolute_error) for each combination of ForecastModel +
    Location + Target in project. Calculates scores for all (!) ForecastModels in project, so should probably be
    enqueued in RQ rather than run in the calling thread.
    
    :param score: a Score
    :param project: a Project
    :param is_absolute_error: True if abs() should be called
    """
    from forecast_app.models import ScoreValue  # avoid circular imports


    # validate targets
    targets = project.visualization_targets()
    if not targets:
        logger.warning("calculate_error_score_values(): no visualization targets. project={}".format(project))
        return

    # cache all the data we need for all models
    locations = project.locations.all()
    model_id_to_point_values_dict = _model_id_to_point_values_dict(project, [target.name for target in targets])
    model_id_to_forecast_id_tz_dates = _model_id_to_forecast_id_tz_dates(project)
    loc_target_tz_date_to_truth = project.location_target_name_tz_date_to_truth()

    # calculate for all combinations of model, location, and target
    for forecast_model in project.models.order_by('name'):
        if not forecast_model.forecasts.exists():
            logger.warning("calculate_error_score_values(): could not calculate absolute errors: model had "
                           "no data: {}".format(forecast_model))
            continue

        for location in locations:
            for target in targets:
                forecast_to_point_dict = model_id_to_point_values_dict[forecast_model.pk] \
                    if forecast_model.pk in model_id_to_point_values_dict \
                    else {}
                forecast_id_tz_dates = model_id_to_forecast_id_tz_dates[forecast_model.pk] \
                    if forecast_model.pk in model_id_to_forecast_id_tz_dates \
                    else {}
                iterate_forecast_errors(forecast_model, location.name, target.name,
                                        forecast_to_point_dict, forecast_id_tz_dates, loc_target_tz_date_to_truth,
                                        lambda forecast_id, forecast_timezero_date, predicted_value, true_value:
                                        ScoreValue.objects.create(forecast_id=forecast_id, location=location,
                                                                  target=target, score=score,
                                                                  value=abs(true_value - predicted_value)
                                                                  if is_absolute_error else true_value - predicted_value))
