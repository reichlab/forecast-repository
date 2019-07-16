import logging

from forecast_app.models import Forecast, ScoreValue
from forecast_app.scores.calc_error import _calculate_error_score_values
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
    # 'const': ('Constant Value', "A debugging score that scores 1.0 only for first location and first target."),
    'error': ('Error', "The the truth value minus the model's point estimate."),
    'abs_error': ('Absolute Error', "The absolute value of the truth value minus the model's point estimate. "
                                    "Lower is better."),
    'log_single_bin': ('Log score (single bin)', "Natural log of probability assigned to the true bin. Higher is "
                                                 "better."),
    'log_multi_bin': ('Log score (multi bin)', "This is calculated by finding the natural log of probability "
                                               "assigned to the true and a few neighbouring bins. Higher is better."),
    'pit': ('Probability Integral Transform (PIT)', "The probability integral transform (PIT) is a metric commonly "
                                                    "used to evaluate the calibration of probabilistic forecasts."),
}


#
# ---- 'Constant Value' calculation function ----
#

def calc_const(score, forecast_model):
    """
    A simple demo that calculates 'Constant Value' scores for the first location and first target in forecast_model's
    project. To activate it, add this entry to SCORE_ABBREV_TO_NAME_AND_DESCR:

        'const': ('Constant Value', "A debugging score that scores 1.0 only for first location and first target."),

    """
    first_location = forecast_model.project.locations.first()
    first_target = forecast_model.project.targets.first()
    if (not first_location) or (not first_target):
        logger.warning("calc_const(): no location or no target found. first_location={}, first_target={}"
                       .format(first_location, first_target))
        return

    for forecast in Forecast.objects.filter(forecast_model=forecast_model):
        ScoreValue.objects.create(score=score, forecast=forecast, location=first_location, target=first_target,
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

LOG_SINGLE_BIN_NEGATIVE_INFINITY = -999  # see use below for docs


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
# validation functions
#

def _validate_score_targets_and_data(forecast_model):
    # validate targets
    targets = forecast_model.project.non_date_targets()
    if not targets:
        raise RuntimeError("_validate_score_targets_and_data(): no targets. project={}".format(forecast_model.project))

    # validate forecast data
    if not forecast_model.forecasts.exists():
        raise RuntimeError("_validate_score_targets_and_data(): could not calculate absolute errors: model had "
                           "no data: {}".format(forecast_model))

    return targets


def _validate_truth(timezero_loc_target_pks_to_truth_values, timezero_pk, location_pk, target_pk):
    """
    :return: 2-tuple of the form: (truth_value, error_string) where error_string is non-None if the inputs were invalid.
        in that case, truth_value is None. o/w truth_value_or_none is valid
    """
    if timezero_pk not in timezero_loc_target_pks_to_truth_values:
        return None, 'timezero_pk not in truth'
    elif location_pk not in timezero_loc_target_pks_to_truth_values[timezero_pk]:
        return None, 'location_pk not in truth'
    elif target_pk not in timezero_loc_target_pks_to_truth_values[timezero_pk][location_pk]:
        return None, 'target_pk not in truth'

    truth_values = timezero_loc_target_pks_to_truth_values[timezero_pk][location_pk][target_pk]
    if len(truth_values) == 0:  # truth not available
        return None, 'truth value not found'
    elif len(truth_values) > 1:
        return None, '>1 truth values found'

    return truth_values[0], None
