import os
from pathlib import Path

import django
from django.test import TestCase

from utils.cdc_format_utils import mean_absolute_error_for_model_dir

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
django.setup()

#
# mock for cdc_format_utils.true_value_for_epi_week
#

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


def mock_wili_for_epi_week_fcn(year, week, location_name):  # location_name is ignored
    return EPI_YR_WK_TO_ACTUAL_WILI[(year, week)]


class ModelErrorScoreTablesTestCase(TestCase):
    """
    Recreates flusight's "Mean Absolute Error" and "Mean Log Score" tables.
    """

    def test_mean_absolute_error(self):
        # EWs: EW43 ... EW52 (2016), EW1 ... EW18 (2017)

        # Mean Absolute Error table: X=target vs. Y=Model
        # ex: http://reichlab.io/flusight/ : US National > 2016-2017 > 1 wk, 2 wk, 3 wk, 4 wk:
        # +----------+------+------+------+------+
        # | Model    | 1 wk | 2 wk | 3 wk | 4 wk |
        # +----------+------+------+------+------+
        # | kcde     | 0.29 | 0.45 | 0.61 | 0.69 |
        # | kde      | 0.58 | 0.59 | 0.6  | 0.6  |
        # | sarima   | 0.23 | 0.35 | 0.49 | 0.56 |
        # | ensemble | 0.3  | 0.4  | 0.53 | 0.54 |
        # +----------+------+------+------+------+

        # 'mini' season for testing. from:
        #   model_error_calculations.txt -> model_error_calculations.py -> model_error_calculations.xlsx:
        model_target_to_exp_mae = {('kde', '1 wk ahead'): 0.440285,
                                   ('kde', '2 wk ahead'): 0.39992,
                                   ('kde', '3 wk ahead'): 0.6134925,
                                   ('kde', '4 wk ahead'): 0.98713,
                                   ('ensemble', '1 wk ahead'): 0.215904853,
                                   ('ensemble', '2 wk ahead'): 0.458186984,
                                   ('ensemble', '3 wk ahead'): 0.950515864,
                                   ('ensemble', '4 wk ahead'): 1.482010693}
        data_root = Path('model_error')
        for (model_name, target_name), exp_mae in model_target_to_exp_mae.items():
            act_mae = mean_absolute_error_for_model_dir(Path(data_root, model_name), 2016, 'US National', target_name,
                                                        wili_for_epi_week_fcn=mock_wili_for_epi_week_fcn)
            self.assertAlmostEqual(exp_mae, act_mae)
