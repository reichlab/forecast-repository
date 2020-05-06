import logging

from forecast_app.models import Forecast, ScoreValue
from forecast_app.scores.calc_error import _calculate_error_score_values
from forecast_app.scores.calc_interval import _calculate_interval_score_values
from forecast_app.scores.calc_log import _calc_log_bin_score_values
from forecast_app.scores.calc_pit import _calculate_pit_score_values


logger = logging.getLogger(__name__)

#
# ---- Score instance definitions ----
#

# provides information about all scores in the system. used by ensure_all_scores_exist() to create Score instances. maps
# each Score's abbreviation to a 2-tuple: (name, description). recall that the abbreviation is used to look up the
# corresponding function in the `forecast_app.scores.functions` (this) module - see `calc_<abbreviation>` documentation
# in Score. in that sense, these abbreviations are the official names to use when looking up a particular score
SCORE_ABBREV_TO_NAME_AND_DESCR = {
    # 'const': ('Constant Value', "A debugging score that's 1.0 - only for first unit and first target."),
    'error': ('Error', "The the truth value minus the model's point estimate."),
    'abs_error': ('Absolute Error', "The absolute value of the truth value minus the model's point estimate. "
                                    "Lower is better."),
    'log_single_bin': ('Log score (single bin)', "Natural log of probability assigned to the true bin. Higher is "
                                                 "better."),
    'log_multi_bin': ('Log score (multi bin)', "This is calculated by finding the natural log of probability "
                                               "assigned to the true and a few neighbouring bins. Higher is better."),
    # from nick re: pit lower/higher is better: "one individual score is not meaningful/interpretable in this way":
    'pit': ('Probability Integral Transform (PIT)', "The probability integral transform (PIT) is a metric commonly "
                                                    "used to evaluate the calibration of probabilistic forecasts."),
    'interval_02': ('Interval score (alpha=0.2)', "The interval score is a proper score used to assess calibration and "
                                                  "sharpness of quantile forecasts. Lower is better."),
}


#
# ---- 'Constant Value' calculation function ----
#

def calc_const(score, forecast_model):
    """
    A simple demo that calculates 'Constant Value' scores for the first unit and first target in forecast_model's
    project. To activate it, add this entry to SCORE_ABBREV_TO_NAME_AND_DESCR:

        'const': ('Constant Value', "A debugging score that scores 1.0 only for first unit and first target."),

    """
    first_unit = forecast_model.project.units.first()
    first_target = forecast_model.project.targets.first()
    if (not first_unit) or (not first_target):
        logger.warning("calc_const(): no unit or no target found. first_unit={}, first_target={}"
                       .format(first_unit, first_target))
        return

    for forecast in Forecast.objects.filter(forecast_model=forecast_model):
        ScoreValue.objects.create(score=score, forecast=forecast, unit=first_unit, target=first_target,
                                  value=1.0)


#
# ---- 'error' and 'abs_error' calculation functions ----
#

def calc_error(score, forecast_model):
    """
    Calculates 'error' scores.
    """
    _calculate_error_score_values(score, forecast_model, False)


def calc_abs_error(score, forecast_model):
    """
    Calculates 'abs_error' scores.
    """
    _calculate_error_score_values(score, forecast_model, True)


#
# ---- 'log_single_bin' and 'log_multi_bin' calculation functions ----
#

def calc_log_single_bin(score, forecast_model):
    """
    Calculates 'Log score (single bin)' scores per
    https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin .
    """
    _calc_log_bin_score_values(score, forecast_model, 0)


def calc_log_multi_bin(score, forecast_model):
    """
    Calculates 'Log score (multi bin)' scores per
    https://github.com/reichlab/flusight/wiki/Scoring#3-log-score-multi-bin .
    """
    _calc_log_bin_score_values(score, forecast_model, 5)


#
# ---- 'pit' calculation functions ----
#

def calc_pit(score, forecast_model):
    """
    Calculates 'pit' score.
    """
    _calculate_pit_score_values(score, forecast_model)


#
# ---- 'pit' calculation functions ----
#

def calc_interval_02(score, forecast_model):
    """
    Calculates 'interval_02' score.
    """
    _calculate_interval_score_values(score, forecast_model, 0.2)


#
# validation functions
#

def _validate_score_targets_and_data(forecast_model):
    # validate targets
    targets = forecast_model.project.numeric_targets()
    if not targets:
        raise RuntimeError("_validate_score_targets_and_data(): no targets. project={}".format(forecast_model.project))

    # validate forecast data
    if not forecast_model.forecasts.exists():
        raise RuntimeError("_validate_score_targets_and_data(): could not calculate absolute errors: model had "
                           "no data: {}".format(forecast_model))

    return targets
