import logging
import math

from forecast_app.models import ScoreValue


logger = logging.getLogger(__name__)


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
    from forecast_app.scores.definitions import _calc_bin_score  # avoid circular imports


    _calc_bin_score(score, forecast_model, True, num_bins_one_side)


def save_log_score(score, forecast_model, templ_st_ends, forec_st_end_to_pred_val,
                   true_bin_key, true_bin_idx, truth_data, num_bins_one_side):
    from forecast_app.scores.definitions import LOG_SINGLE_BIN_NEGATIVE_INFINITY


    if truth_data.value is None:  # score degenerates to the num_bins_one_side=0 'Log score (single bin)' calculation
        num_bins_one_side = 0

    start_idx = max(0, true_bin_idx - num_bins_one_side)  # max() in case window is before first bin
    end_idx = true_bin_idx + num_bins_one_side + 1  # don't care if it's after the last bin - slice ignores
    templ_bin_keys_pre_post_truth = templ_st_ends[start_idx:end_idx]
    pred_vals_both_windows = [forec_st_end_to_pred_val[key] if key in forec_st_end_to_pred_val else 0
                              for key in templ_bin_keys_pre_post_truth]  # 0 b/c unforecasted bins are 0 value ones
    pred_vals_both_windows_sum = sum(pred_vals_both_windows)

    try:
        log_multi_bin_score_value = math.log(pred_vals_both_windows_sum)
    except ValueError:  # math.log(0) -> ValueError: math domain error
        # implements the logic: "clip Math.log(0) to -999 instead of its real value (-Infinity)"
        # from: https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin
        log_multi_bin_score_value = LOG_SINGLE_BIN_NEGATIVE_INFINITY

    forecast = forecast_model.forecast_for_time_zero(truth_data.time_zero)  # todo xx slow!
    # logger.debug('save_pit_score: {}'.format([score, forecast.pk, truth_data.location.pk, truth_data.target.pk, truth_data.target.pk, pit_score_value]))
    ScoreValue.objects.create(forecast_id=forecast.pk,
                              location_id=truth_data.location.pk,
                              target_id=truth_data.target.pk,
                              score=score, value=log_multi_bin_score_value)

# def save_pit_score(self):
#     from forecast_app.scores.definitions import LOG_SINGLE_BIN_NEGATIVE_INFINITY  # avoid circular imports
#
#
#     matching_input_tuple = self.values_tuples_post_match[0]  # the matching one is first b/c we append
#     try:
#         if matching_input_tuple.true_value is None:
#             # in this case, the score should degenerate to the num_bins_one_side=0 'Log score (single bin)'
#             # calculation
#             value_sum = matching_input_tuple.predicted_value
#         else:
#             value_sum = sum(list(self.values_pre_match_dq) +
#                             [input_tuple.predicted_value for input_tuple in self.values_tuples_post_match])
#         score_value = math.log(value_sum)
#     except ValueError:  # math.log(0) -> ValueError: math domain error
#         # implements the logic: "clip Math.log(0) to -999 instead of its real value (-Infinity)"
#         # from: https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin
#         score_value = LOG_SINGLE_BIN_NEGATIVE_INFINITY
#
#     # logger.debug('save_pit_score: {}'.format([self.values_pre_match_dq, self.values_tuples_post_match, '.', value_sum, score_value]))
#     ScoreValue.objects.create(forecast_id=matching_input_tuple.forecast_pk,
#                               location_id=matching_input_tuple.location_pk,
#                               target_id=matching_input_tuple.target_pk,
#                               score=self.score, value=score_value)
#
