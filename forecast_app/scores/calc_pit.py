import logging

from forecast_app.models import ScoreValue


logger = logging.getLogger(__name__)


def _calculate_pit_score_values(score, forecast_model):
    """
    Implements the 'Probability Integral Transform (PIT)' score, defined for each predictive distribution (i.e., each
    new target in a forecast) as `(s1 + s2)/2` where s1 is the sum of all bin row values _up to_ the true bin, and s2
    is that same sum but also including the true bin.

    Note that correctly calculating this score can depend on missing bin rows whose values are zero, and therefore are
    not in the database - see [Consider not storing bin rows with zero values #84](https://github.com/reichlab/forecast-repository/issues/84) .
    """
    from forecast_app.scores.definitions import _calc_bin_score  # avoid circular imports


    _calc_bin_score(score, forecast_model, False, None)


def save_pit_score(score, forecast_model, templ_st_ends, forec_st_end_to_pred_val,
                   true_bin_key, true_bin_idx, truth_data):
    template_bin_keys_pre_truth = templ_st_ends[:true_bin_idx]  # excluding true bin
    if truth_data.value is None:  # score degenerates to using only the predicted true value
        pred_vals_pre_truth = []
    else:
        pred_vals_pre_truth = [forec_st_end_to_pred_val[key] if key in forec_st_end_to_pred_val else 0
                               for key in template_bin_keys_pre_truth]  # 0 b/c unforecasted bins are 0 value ones
    pred_vals_pre_truth_sum = sum(pred_vals_pre_truth)
    true_bin_pred_val = forec_st_end_to_pred_val[true_bin_key] if true_bin_key in forec_st_end_to_pred_val else 0
    pit_score_value = ((pred_vals_pre_truth_sum * 2) + true_bin_pred_val) / 2  # 0 b/c ""
    forecast = forecast_model.forecast_for_time_zero(truth_data.time_zero)  # todo xx slow!
    # logger.debug('save_pit_score: {}'.format([score, forecast.pk, truth_data.location.pk, truth_data.target.pk, truth_data.target.pk, pit_score_value]))
    ScoreValue.objects.create(forecast_id=forecast.pk,
                              location_id=truth_data.location.pk,
                              target_id=truth_data.target.pk,
                              score=score, value=pit_score_value)
