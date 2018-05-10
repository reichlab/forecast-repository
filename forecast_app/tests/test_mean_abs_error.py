from pathlib import Path

import pymmwr
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.tests.test_project import TEST_CONFIG_DICT
from forecast_app.tests.test_utils import mock_wili_for_epi_week_fcn
from utils.mean_absolute_error import mean_absolute_error, _model_ids_to_point_values_dicts, \
    _model_ids_to_forecast_rows, location_to_mean_abs_error_rows_for_project


class MAETestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=TEST_CONFIG_DICT)
        cls.project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)

        # EW1-KoTstable-2017-01-17.csv -> EW1 in 2017:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 1)))
        cls.forecast1 = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), time_zero)

        # EW2-KoTstable-2017-01-23.csv -> EW2 in 2017:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 2)))
        cls.forecast2 = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW2-KoTstable-2017-01-23.csv'), time_zero)

        # EW51-KoTstable-2017-01-03.csv -> EW51 in 2016:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 51)))
        cls.forecast3 = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW51-KoTstable-2017-01-03.csv'), time_zero)

        # EW52-KoTstable-2017-01-09.csv -> EW52 in 2016:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 52)))
        cls.forecast4 = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW52-KoTstable-2017-01-09.csv'), time_zero)


    def test_mean_absolute_error(self):
        # 'mini' season for testing. from:
        #   model_error_calculations.txt -> model_error_calculations.py -> model_error_calculations.xlsx:
        exp_target_to_mae = {'1 wk ahead': 0.215904853,
                             '2 wk ahead': 0.458186984,
                             '3 wk ahead': 0.950515864,
                             '4 wk ahead': 1.482010693}

        for target, exp_mae in exp_target_to_mae.items():
            model_ids_to_point_values_dicts = _model_ids_to_point_values_dicts(self.project, None, [target])
            model_ids_to_forecast_rows = _model_ids_to_forecast_rows(self.project, [self.forecast_model], None)
            act_mae = mean_absolute_error(self.forecast_model, 'US National', target, mock_wili_for_epi_week_fcn,
                                          model_ids_to_point_values_dicts[self.forecast_model.pk],
                                          model_ids_to_forecast_rows[self.forecast_model.pk])
            self.assertAlmostEqual(exp_mae, act_mae)


    def test_location_to_mean_abs_error_rows_for_project(self):
        exp_locations = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                         'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9',
                         'US National']
        exp_target_to_mae = {'1 wk ahead': 0.215904853,
                             '2 wk ahead': 0.458186984,
                             '3 wk ahead': 0.950515864,
                             '4 wk ahead': 1.482010693}

        # sanity-check keys
        act_dict = location_to_mean_abs_error_rows_for_project(self.project, None, mock_wili_for_epi_week_fcn)
        self.assertEqual(set(exp_locations), set(act_dict.keys()))

        # spot-check one location
        act_mean_abs_error_rows, act_target_to_min_mae = act_dict['US National']

        self.assertEqual(['Model', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead'],
                         act_mean_abs_error_rows[0])  # row0

        self.assertEqual(act_mean_abs_error_rows[1][0], self.forecast_model.pk)  # row1, col0

        # row1, col1+. values happen to be sorted
        for exp_mae, act_mae in zip(list(sorted(exp_target_to_mae.values())), act_mean_abs_error_rows[1][1:]):
            self.assertAlmostEqual(exp_mae, act_mae)

        # act_target_to_min_mae
        for target, exp_mae in exp_target_to_mae.items():
            self.assertAlmostEqual(exp_mae, act_target_to_min_mae[target])
