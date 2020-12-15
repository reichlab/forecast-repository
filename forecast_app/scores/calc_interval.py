import logging
from collections import defaultdict
from itertools import groupby

from django.db.models import Q

from forecast_app.models import QuantileDistribution, PointPrediction
from utils.project_truth import truth_data_qs


logger = logging.getLogger(__name__)


def _calculate_interval_score_values(score, forecast_model, alpha):
    """
    Implements an interval score as inspired by "Strictly Proper Scoring Rules, Prediction, and Estimation" by
    Tilmann Gneiting & Adrian E Raftery. Only calculates ScoreValues for QuantileDistribution data in forecast_model.
    """
    from forecast_app.scores.definitions import _validate_score_targets_and_data  # avoid circular imports
    from forecast_app.scores.bin_utils import _insert_score_values


    try:
        targets = _validate_score_targets_and_data(forecast_model)
    except RuntimeError as rte:
        logger.warning(f"_calculate_interval_score_values(): _validate_score_targets_and_data() failed. "
                       f"rte={rte!r}, score={score}, forecast_model={forecast_model}")
        return

    lower_interval_quantile = alpha / 2
    upper_interval_quantile = 1 - (alpha / 2)

    # step 1/2: build dict tz_unit_targ_pk_to_l_u_vals:
    #   [timezero_id][unit_id][target_id] -> (lower_interval_value, upper_interval_value):
    tz_unit_targ_pk_to_l_u_vals = {}
    quantile_predictions_qs = QuantileDistribution.objects \
        .filter(Q(forecast__forecast_model=forecast_model),  # AND
                Q(target__in=targets),  # AND
                (Q(quantile=lower_interval_quantile) | Q(quantile=upper_interval_quantile))) \
        .order_by('forecast__time_zero__id', 'unit__id', 'target__id', 'quantile') \
        .values_list('forecast__time_zero__id', 'unit__id', 'target__id', 'quantile',
                     'value_i', 'value_f', 'value_d')  # only one of value_* is non-None
    for timezero_id, unit_target_val_grouper in groupby(quantile_predictions_qs, key=lambda _: _[0]):
        tz_unit_targ_pk_to_l_u_vals[timezero_id] = {}
        for unit_id, target_val_grouper in groupby(unit_target_val_grouper, key=lambda _: _[1]):
            tz_unit_targ_pk_to_l_u_vals[timezero_id][unit_id] = defaultdict(list)
            for _, _, target_id, quantile, value_i, value_f, value_d in target_val_grouper:
                value = PointPrediction.first_non_none_value(value_i, value_f, None, value_d, None)
                tz_unit_targ_pk_to_l_u_vals[timezero_id][unit_id][target_id].append(value)

    # step 2/2: iterate over truths, calculating scores. it is convenient to iterate over truths to get all
    # timezero/unit/target combinations. this will omit forecasts with no truth, but that's OK b/c without truth, a
    # forecast makes no contribution to the score. note that we collect all ScoreValue rows and then bulk insert them as
    # an optimization, rather than create separate ORM instances
    score_values = []  # list of 5-tuples: (score.pk, forecast.pk, unit.pk, target.pk, score_value)
    timezero_id_to_forecast_id = {forecast.time_zero.pk: forecast.pk for forecast in forecast_model.forecasts.all()}
    the_truth_data_qs = truth_data_qs(forecast_model.project) \
        .filter(target__in=targets) \
        .values_list('forecast__time_zero__id', 'unit__id', 'target__id', 'value_i', 'value_f', 'value_t', 'value_d',
                     'value_b')  # only one of value_* is non-None
    num_warnings = 0
    for timezero_id, unit_id, target_id, value_i, value_f, value_t, value_d, value_b in the_truth_data_qs:
        truth_value = PointPrediction.first_non_none_value(value_i, value_f, value_t, value_d, value_b)
        try:
            lower_upper_interval_values = tz_unit_targ_pk_to_l_u_vals[timezero_id][unit_id][target_id]
            if not lower_upper_interval_values:
                # defaultdict(list) -> [] result if match [timezero_id][unit_id] but not target_id
                num_warnings += 1
                continue  # skip this forecast's contribution to the score
            elif len(lower_upper_interval_values) == 1:  # median quantile (alpha = 1.0) has same lower and upper
                lower_interval_value = upper_interval_value = lower_upper_interval_values[0]
            elif len(lower_upper_interval_values) == 2:  # median quantile (alpha = 1.0) has same lower and upper
                lower_interval_value, upper_interval_value = lower_upper_interval_values
            else:
                # should never happen (?) given `_validate_quantile_predictions()` catches "quantile`s must be unique"
                raise RuntimeError(f">2 lower_upper_interval_values: {lower_upper_interval_values}. "
                                   f"timezero_id={timezero_id}, unit_id={unit_id}, target_id={target_id}")

            interval_width = upper_interval_value - lower_interval_value
            penalty_l = (2 / alpha) * max(lower_interval_value - truth_value, 0)
            penalty_u = (2 / alpha) * max(truth_value - upper_interval_value, 0)
            score_value = interval_width + penalty_l + penalty_u
            score_values.append((score.pk, timezero_id_to_forecast_id[timezero_id], unit_id, target_id, score_value))
        except KeyError:  # no lower/upper values for one of timezero_id, unit_id, target_id
            num_warnings += 1
            continue  # skip this forecast's contribution to the score

    # insert the ScoreValues!
    _insert_score_values(score_values)

    # print warning count
    logger.warning(f"_calculate_interval_score_values(): done. score={score}, forecast_model={forecast_model}, "
                   f"num_warnings={num_warnings}")
