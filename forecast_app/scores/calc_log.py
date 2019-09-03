import logging
import math

from forecast_app.models import ScoreValue


logger = logging.getLogger(__name__)

LOG_SINGLE_BIN_NEGATIVE_INFINITY = -999  # see use below for docs


def _calc_log_bin_score_values(score, forecast_model, num_bins_one_side):
    """
    Implements the 'log_single_bin' (AKA 'Log score (single bin)') and 'log_multi_bin' (AKA 'Log score (multi bin)')
    scores per:
        - https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin
    or
        - https://github.com/reichlab/flusight/wiki/Scoring#3-log-score-multi-bin
    as controlled by num_bins.

    Note that correctly calculating this score can depend on missing bin rows whose values are zero, and therefore are
    not in the database - see [Consider not storing bin rows with zero values #84](https://github.com/reichlab/forecast-repository/issues/84) .

    :param num_bins_one_side: (AKA the 'window' per above link) is number of bins rows *on one side* of the matching bin
        row to sum when calculating the score. thus the total number of bins in the 'window' centered on the matching
        bin row is: (2 * num_bins) + 1 . pass zero to get single bin behavior.
    """
    from forecast_app.scores.bin_utils import _calc_bin_score


    _calc_bin_score(score, forecast_model, save_log_score, num_bins_one_side=num_bins_one_side)


def save_log_score(score, forecast_pk, location_pk, target_pk, truth_value, bin_lwrs, bin_lwr_to_pred_val,
                   true_bin_lwr, true_bin_idx, num_bins_one_side):
    if truth_value is None:  # score degenerates to the num_bins_one_side=0 'Log score (single bin)' calculation
        num_bins_one_side = 0

    start_idx = max(0, true_bin_idx - num_bins_one_side)  # max() in case window is before first bin
    end_idx = true_bin_idx + num_bins_one_side + 1  # don't care if it's after the last bin - slice ignores
    bin_lwrs_pre_post_truth = bin_lwrs[start_idx:end_idx]
    # use 0 b/c unforecasted bins are 0 value ones:
    pred_vals_both_windows = [bin_lwr_to_pred_val[bin_lwr] if bin_lwr in bin_lwr_to_pred_val else 0
                              for bin_lwr in bin_lwrs_pre_post_truth]
    pred_vals_both_windows_sum = sum(pred_vals_both_windows)

    try:
        log_multi_bin_score_value = math.log(pred_vals_both_windows_sum)
    except ValueError:  # math.log(0) -> ValueError: math domain error
        # implements the logic: "clip Math.log(0) to -999 instead of its real value (-Infinity)"
        # from: https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin
        log_multi_bin_score_value = LOG_SINGLE_BIN_NEGATIVE_INFINITY

    return log_multi_bin_score_value
