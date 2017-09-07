import os
import unittest
from pathlib import Path

import django
from django.test import TestCase

from utils.cdc_format_utils import mean_absolute_error_for_model_dir

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
django.setup()


# mock for cdc_format_utils.true_value_for_epi_week
def test_true_value_for_epi_week_fcn(season_start_year, ew_week_number, location_name):
    return 0  # todo look up once dynamically and then hardcode here xx


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

        # from model_error_calculations.txt -> model_error_calculations.py -> model_error_calculations.xlsx:
        target_model_to_exp_mae = {('1 wk ahead', 'kde'): 0.440285,
                                   ('2 wk ahead', 'kde'): 0.39992,
                                   ('3 wk ahead', 'kde'): 0.6134925,
                                   ('4 wk ahead', 'kde'): 0.98713,
                                   ('1 wk ahead', 'ensemble'): 0.215904853,
                                   ('2 wk ahead', 'ensemble'): 0.458186984,
                                   ('3 wk ahead', 'ensemble'): 0.950515864,
                                   ('4 wk ahead', 'ensemble'): 1.482010693}

        # data_root = Path('~/IdeaProjects/split_kot_models_from_submissions/').expanduser()
        data_root = Path('model_error').expanduser()
        location_name = 'US National'
        for (target_name, model_name), exp_mae in target_model_to_exp_mae.items():
            model_csv_path = Path(data_root, model_name)
            act_mae = mean_absolute_error_for_model_dir(model_csv_path, 2016, location_name, target_name,
                                                        true_value_for_epi_week_fcn=test_true_value_for_epi_week_fcn)
            self.assertEqual(exp_mae, act_mae)

    @unittest.skip  # todo
    def test_dir_constraints(self):
        # Constraints:
        # - all files much match across dirs - recall one had an extra file
        # - all files must have same locations and targets
        # - todo others
        self.fail()
