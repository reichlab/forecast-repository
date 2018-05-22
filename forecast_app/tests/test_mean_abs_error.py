import datetime
from collections import defaultdict
from pathlib import Path

import pymmwr
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import CDC_CONFIG_DICT
from utils.mean_absolute_error import mean_absolute_error, _model_id_to_point_values_dict, \
    _model_id_to_forecast_id_tz_date_csv_fname, location_to_mean_abs_error_rows_for_project, \
    _loc_target_tz_date_to_truth


class MAETestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
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

        # 'mini' season for testing. from:
        #   model_error_calculations.txt -> model_error_calculations.py -> model_error_calculations.xlsx:
        cls.exp_target_to_mae = {'1 wk ahead': 0.215904853,
                                 '2 wk ahead': 0.458186984,
                                 '3 wk ahead': 0.950515864,
                                 '4 wk ahead': 1.482010693}
        cls.project.load_truth_data(Path('forecast_app/tests/truth_data/mean-abs-error-truths.csv'))


    def test_mean_absolute_error(self):
        for target, exp_mae in self.exp_target_to_mae.items():
            model_id_to_point_values_dict = _model_id_to_point_values_dict(self.project, None, [target])
            model_id_to_forecast_id_tz_date_csv_fname = _model_id_to_forecast_id_tz_date_csv_fname(
                self.project, [self.forecast_model], None)
            loc_target_tz_date_to_truth = _loc_target_tz_date_to_truth(self.project)
            act_mae = mean_absolute_error(self.forecast_model, 'US National', target,
                                          model_id_to_point_values_dict[self.forecast_model.pk],
                                          model_id_to_forecast_id_tz_date_csv_fname[self.forecast_model.pk],
                                          loc_target_tz_date_to_truth)
            self.assertIsNotNone(act_mae)
            self.assertAlmostEqual(exp_mae, act_mae)


    def test_location_to_mean_abs_error_rows_for_project(self):
        exp_locations = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                         'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9',
                         'US National']

        # sanity-check keys
        act_dict = location_to_mean_abs_error_rows_for_project(self.project, None)
        self.assertEqual(set(exp_locations), set(act_dict.keys()))

        # spot-check one location
        self.assertTrue(act_dict['US National'])  # must have some values
        act_mean_abs_error_rows, act_target_to_min_mae = act_dict['US National']

        self.assertEqual(['Model', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead'],
                         act_mean_abs_error_rows[0])  # row0

        self.assertEqual(act_mean_abs_error_rows[1][0], self.forecast_model.pk)  # row1, col0

        # row1, col1+. values happen to be sorted
        for exp_mae, act_mae in zip(list(sorted(self.exp_target_to_mae.values())), act_mean_abs_error_rows[1][1:]):
            self.assertAlmostEqual(exp_mae, act_mae)

        # act_target_to_min_mae
        for target, exp_mae in self.exp_target_to_mae.items():
            self.assertAlmostEqual(exp_mae, act_target_to_min_mae[target])


    def test_loc_target_tz_date_to_truth(self):
        self.project.delete_truth_data()
        self.project.load_truth_data(Path('forecast_app/tests/truth_data/mean-abs-error-truths-dups.csv'))
        exp_loc_target_tz_date_to_truth = _exp_loc_target_tz_date_to_truth()
        act_loc_target_tz_date_to_truth = _loc_target_tz_date_to_truth(self.project)
        self.assertEqual(exp_loc_target_tz_date_to_truth, act_loc_target_tz_date_to_truth)


def _exp_loc_target_tz_date_to_truth():
    exp_loc_target_tz_date_to_truth = {
        'HHS Region 1': {
            '1 wk ahead': {
                datetime.date(2017, 1, 1): [1.52411],
                datetime.date(2017, 1, 8): [1.73987],
                datetime.date(2016, 12, 18): [1.41861],
                datetime.date(2016, 12, 25): [1.57644],
            },
            '2 wk ahead': {
                datetime.date(2017, 1, 1): [1.73987],
                datetime.date(2017, 1, 8): [2.06524],
                datetime.date(2016, 12, 18): [1.57644],
                datetime.date(2016, 12, 25): [1.52411],
            },
            '3 wk ahead': {
                datetime.date(2017, 1, 1): [2.06524],
                datetime.date(2017, 1, 8): [2.51375],
                datetime.date(2016, 12, 18): [1.52411],
                datetime.date(2016, 12, 25): [1.73987],
            },
            '4 wk ahead': {
                datetime.date(2017, 1, 1): [2.51375],
                datetime.date(2017, 1, 8): [3.19221],
                datetime.date(2016, 12, 18): [1.73987],
                datetime.date(2016, 12, 25): [2.06524],
            }},
        'US National': {
            '1 wk ahead': {
                datetime.date(2017, 1, 1): [3.08492],
                datetime.date(2017, 1, 8): [3.51496],
                datetime.date(2016, 12, 18): [3.36496, 9.0],  # NB two
                datetime.date(2016, 12, 25): [3.0963],
            },
            '2 wk ahead': {
                datetime.date(2017, 1, 1): [3.51496],
                datetime.date(2017, 1, 8): [3.8035],
                datetime.date(2016, 12, 18): [3.0963],
                datetime.date(2016, 12, 25): [3.08492],
            },
            '3 wk ahead': {
                datetime.date(2017, 1, 1): [3.8035],
                datetime.date(2017, 1, 8): [4.45059],
                datetime.date(2016, 12, 18): [3.08492],
                datetime.date(2016, 12, 25): [3.51496],
            },
            '4 wk ahead': {
                datetime.date(2017, 1, 1): [4.45059],
                datetime.date(2017, 1, 8): [5.07947],
                datetime.date(2016, 12, 18): [3.51496],
                datetime.date(2016, 12, 25): [3.8035],
            }
        }
    }
    # convert innermost dicts to defaultdicts, which are what _loc_target_tz_date_to_truth() returns
    for location, target_tz_dict in exp_loc_target_tz_date_to_truth.items():
        for target, tz_date_truth in target_tz_dict.items():
            exp_loc_target_tz_date_to_truth[location][target] = defaultdict(list, tz_date_truth)
    return exp_loc_target_tz_date_to_truth
