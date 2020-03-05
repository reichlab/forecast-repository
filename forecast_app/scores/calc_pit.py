import logging


logger = logging.getLogger(__name__)


def _calculate_pit_score_values(score, forecast_model):
    """
    Implements the 'Probability Integral Transform (PIT)' score, defined for each predictive distribution (i.e., each
    new target in a forecast) as `(s1 + s2)/2` where s1 is the sum of all bin row values _up to_ the true bin, and s2
    is that same sum but also including the true bin.

    Note that correctly calculating this score does NOT depend on missing bin rows whose values are zero, and therefore
    are not in the database - see [Consider not storing bin rows with zero values #84](https://github.com/reichlab/forecast-repository/issues/84) .
    """
    from forecast_app.scores.bin_utils import _calc_bin_score


    _calc_bin_score(score, forecast_model, save_pit_score)


def save_pit_score(score, forecast_pk, unit_pk, target_pk, truth_value, lwrs, lwr_to_pred_val,
                   true_lwr, true_bin_idx):
    lwrs_pre_truth = lwrs[:true_bin_idx]  # excluding true bin
    if truth_value is None:  # score degenerates to using only the predicted true value
        pred_vals_pre_truth = []
    else:
        # use 0 b/c unforecasted bins are 0 value ones:
        pred_vals_pre_truth = [lwr_to_pred_val[lwr] if lwr in lwr_to_pred_val else 0 for lwr in lwrs_pre_truth]
    pred_vals_pre_truth_sum = sum(pred_vals_pre_truth)
    true_bin_pred_val = lwr_to_pred_val[true_lwr] if true_lwr in lwr_to_pred_val else 0
    pit_score_value = ((pred_vals_pre_truth_sum * 2) + true_bin_pred_val) / 2  # 0 b/c ""
    return pit_score_value
