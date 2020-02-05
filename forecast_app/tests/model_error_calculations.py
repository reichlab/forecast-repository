from forecast_app.tests.test_model_error_score_tables import mock_wili_for_epi_week_fcn

EPI_YR_WK_TO_ACTUAL_WILI = {
    (2016, 51): 2.74084,
    (2016, 52): 3.36496,
    (2017, 1): 3.0963,
    (2017, 2): 3.08492,
    (2017, 3): 3.51496,
    (2017, 4): 3.8035,
    (2017, 5): 4.45059,
    (2017, 6): 5.07947,
}


# 'mini' season for testing. US National,1 wk ahead,Point,percent,NA,NA,<value>. for 1/2/3/4 wk ahead:
model_to_1234_wk_predictions = {
    'ensemble': {
        # EW1-KoTstable-2017-01-17.csv:
        (2017, 1): [3.00101461253164, 2.72809349594878, 2.5332588357381, 2.42985946508278],
        # EW2-KoTstable-2017-01-23.csv:
        (2017, 2): [3.10195298710819, 2.91921994766766, 2.62890032240603, 2.51167850869295],
        # EW51-KoTstable-2017-01-03.csv:
        (2016, 51): [3.09726111422542, 3.16574167604131, 3.27725563891349, 3.26876032541822],
        # EW52-KoTstable-2017-01-09.csv:
        (2016, 52): [3.19530812711723, 3.1770797037445, 2.9971630233303, 2.71017892766803],
    },
    'kde': {
        # EW1-KOTkde-2016-11-12.csv:
        (2017, 1): [2.9, 3.1, 3.2, 3.3],
        # EW2-KOTkde-2016-11-12.csv:
        (2017, 2): [3.1, 3.2, 3.2, 3.3],
        # EW51-KOTkde-2016-11-12.csv:
        (2016, 51): [2.6, 2.7, 2.9, 3.1],
        # EW52-KOTkde-2016-11-12.csv:
        (2016, 52): [2.7, 2.9, 3.1, 3.2, ]
    }
}


def increment_epi_yr_wk(epi_yr_wk, delta_weeks):
    # adds delta_weeks to epi_yr_wk modulo the year (wraps from EW 52 in one year to 01 in next year). Args:
    # epi_yr_wk: 2-tuple as in EPI_YR_WK_TO_ACTUAL_WILI.keys()
    # delta_weeks: integer >= 0
    epi_yr_wks_sorted = sorted(EPI_YR_WK_TO_ACTUAL_WILI.keys())
    epi_yr_wk_idx = epi_yr_wks_sorted.index(epi_yr_wk)
    return epi_yr_wks_sorted[epi_yr_wk_idx + delta_weeks]


for model, predictions in model_to_1234_wk_predictions.items():
    print(model)
    for epi_yr_wk, pred_vals_targets_1_thru_4 in predictions.items():
        act_vals_targets_1_thru_4 = [mock_wili_for_epi_week_fcn(*increment_epi_yr_wk(epi_yr_wk, index), None)
                                     for index in [1, 2, 3, 4]]
        abs_errors = [abs(pred - act) for pred, act in zip(pred_vals_targets_1_thru_4, act_vals_targets_1_thru_4)]
        print('  ', epi_yr_wk, '.', list(zip(pred_vals_targets_1_thru_4, act_vals_targets_1_thru_4)))
