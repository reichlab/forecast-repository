import logging

from forecast_app.models import Score, ScoreValue
from utils.mean_absolute_error import _model_id_to_point_values_dict, _model_id_to_forecast_id_tz_dates


logger = logging.getLogger(__name__)

#
# ---- Absolute Error score functions ----
#

# todo xx this is a place-holder implementation. the design needs fleshing out so that this can be a method on a Score subcass, for example

ABSOLUTE_ERROR_SCORE_NAME = 'Absolute Error'  # official name
ABSOLUTE_ERROR_SCORE_DESCRIPTION = "The absolute of error between the model's point estimate and the truth value. " \
                                   "Lower is better."  # official description


# todo xx almost all code duplicated from location_to_mean_abs_error_rows_for_project():
def calculate_absolute_error_score_values(project):
    """
    The top-level function that creates ScoreValue instances for the passed args, saving them into the passed score.
    Calculates scores for all (!) ForecastModels in project. Uses ABSOLUTE_ERROR_SCORE_NAME to look up the Score
    instance to store the score values into.

    :param project: a Project
    """
    # clear or create the Score
    abs_err_score = Score.objects.filter(name=ABSOLUTE_ERROR_SCORE_NAME).first()
    if abs_err_score:
        abs_err_score.values.all().delete()
    else:
        abs_err_score = Score.objects.create(name=ABSOLUTE_ERROR_SCORE_NAME,
                                             description=ABSOLUTE_ERROR_SCORE_DESCRIPTION)

    # validate targets
    targets = project.visualization_targets()
    if not targets:
        logger.warning("No visualization targets. project={}".format(project))
        return

    # cache all the data we need for all models
    locations = project.locations.all()
    model_id_to_point_values_dict = _model_id_to_point_values_dict(project, [target.name for target in targets])
    model_id_to_forecast_id_tz_dates = _model_id_to_forecast_id_tz_dates(project)
    loc_target_tz_date_to_truth = project.location_target_name_tz_date_to_truth()

    # calculate for all combinations of model, location, and target
    for forecast_model in project.models.order_by('name'):
        if not forecast_model.forecasts.exists():
            # todo this should probably simply skip the model's contribution
            raise RuntimeError("Could not calculate absolute errors: model had no data: {}".format(forecast_model))

        for location in locations:
            for target in targets:
                forecast_to_point_dict = model_id_to_point_values_dict[forecast_model.pk] \
                    if forecast_model.pk in model_id_to_point_values_dict \
                    else {}
                forecast_id_tz_dates = model_id_to_forecast_id_tz_dates[forecast_model.pk] \
                    if forecast_model.pk in model_id_to_forecast_id_tz_dates \
                    else {}
                calculate_absolute_error(abs_err_score, forecast_model, location, target,
                                         forecast_to_point_dict, forecast_id_tz_dates, loc_target_tz_date_to_truth)


# todo xx almost all code duplicated from mean_absolute_error():
def calculate_absolute_error(abs_err_score, forecast_model, location, target,
                             forecast_to_point_dict, forecast_id_tz_dates, loc_target_tz_date_to_truth):
    for forecast_id, forecast_timezero_date in forecast_id_tz_dates:
        try:
            truth_values = loc_target_tz_date_to_truth[location.name][target.name][forecast_timezero_date]
        except KeyError as ke:
            logger.warning("loc_target_tz_date_to_truth was missing a key: {}. location.name={}, target.name={}, " \
                           "forecast_timezero_date={}. loc_target_tz_date_to_truth={}"
                           .format(ke.args, location.name, target.name, forecast_timezero_date,
                                   loc_target_tz_date_to_truth))
            continue  # skip this forecast's contribution to the score

        if len(truth_values) == 0:  # truth not available
            logger.warning("truth value not found. forecast_model={}, location.name={!r}, target.name={!r}, "
                           "forecast_id={}, forecast_timezero_date={}"
                           .format(forecast_model, location.name, target.name, forecast_id, forecast_timezero_date))
            continue  # skip this forecast's contribution to the score
        elif len(truth_values) > 1:
            logger.warning(">1 truth values found. forecast_model={}, location.name={!r}, target.name={!r}, " \
                           "forecast_id={}, forecast_timezero_date={}, truth_values={}"
                           .format(forecast_model, location.name, target.name, forecast_id, forecast_timezero_date,
                                   truth_values))
            continue  # skip this forecast's contribution to the score

        true_value = truth_values[0]
        if true_value is None:
            logger.warning("truth value was None. forecast_id={}, location.name={!r}, target.name={!r}, "
                           "forecast_timezero_date={}"
                           .format(forecast_id, location.name, target.name, forecast_timezero_date))
            continue  # skip this forecast's contribution to the score

        predicted_value = forecast_to_point_dict[forecast_id][location.name][target.name]
        ScoreValue.objects.create(score=abs_err_score, forecast_model=forecast_model,
                                  location=location, target=target,
                                  value=(abs(true_value - predicted_value)))
