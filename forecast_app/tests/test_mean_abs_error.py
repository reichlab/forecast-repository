from pathlib import Path

import pymmwr
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, CDC_CONFIG_DICT
from utils.mean_absolute_error import mean_absolute_error, _model_id_to_point_values_dict, \
    _model_id_to_forecast_id_tz_dates, location_to_mean_abs_error_rows_for_project


class MAETestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        make_cdc_locations_and_targets(cls.project)
        # Target.objects.create(project=cls.project, name="1 wk ahead", description="d",
        #                       is_step_ahead=True, step_ahead_increment=1)
        # Target.objects.create(project=cls.project, name="2 wk ahead", description="d",
        #                       is_step_ahead=True, step_ahead_increment=3)
        # Target.objects.create(project=cls.project, name="3 wk ahead", description="d",
        #                       is_step_ahead=True, step_ahead_increment=3)
        # Target.objects.create(project=cls.project, name="4 wk ahead", description="d",
        #                       is_step_ahead=True, step_ahead_increment=4)
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
            model_id_to_point_values_dict = _model_id_to_point_values_dict(self.project, [target], None)
            model_id_to_forecast_id_tz_dates = _model_id_to_forecast_id_tz_dates(self.project, None)
            loc_target_tz_date_to_truth = self.project.location_target_name_tz_date_to_truth()  # target__id
            act_mae = mean_absolute_error(self.forecast_model, 'US National', target,
                                          model_id_to_point_values_dict[self.forecast_model.pk],
                                          model_id_to_forecast_id_tz_dates[self.forecast_model.pk],
                                          loc_target_tz_date_to_truth)
            self.assertIsNotNone(act_mae)
            self.assertAlmostEqual(exp_mae, act_mae)


    def test_model_id_to_forecast_id_tz_dates_bug(self):
        # expose a bug in _model_id_to_forecast_id_tz_dates() when season_name != None:
        # django.core.exceptions.FieldError: Cannot resolve keyword 'forecast' into field. Choices are: cdcdata_set, csv_filename, forecast_model, forecast_model_id, id, scorevalue, time_zero, time_zero_id
        TimeZero.objects.create(project=self.project, timezero_date='2016-02-01',
                                is_season_start=True, season_name='season1')
        _model_id_to_forecast_id_tz_dates(self.project, 'season1')


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
