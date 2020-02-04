import datetime
import logging
from pathlib import Path

import pymmwr
from django.test import TestCase

from forecast_app.models import Project, TimeZero, Score
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import load_cdc_csv_forecast_file, make_cdc_locations_and_targets
from utils.make_thai_moph_project import load_cdc_csv_forecasts_from_dir
from utils.mean_absolute_error import location_to_mean_abs_error_rows_for_project, _score_value_rows_for_season
from utils.project import load_truth_data


logging.getLogger().setLevel(logging.ERROR)


class MAETestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()
        make_cdc_locations_and_targets(cls.project)
        cls.forecast_model = ForecastModel.objects.create(project=cls.project)

        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 1)))
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
        cls.forecast1 = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, time_zero)

        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 2)))
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW2-KoTstable-2017-01-23.csv')  # EW02 2017
        cls.forecast2 = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, time_zero)

        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 51)))
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW51-KoTstable-2017-01-03.csv')  # EW51 2016
        cls.forecast3 = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, time_zero)

        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 52)))
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW52-KoTstable-2017-01-09.csv')  # EW52 2016
        cls.forecast4 = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, time_zero)

        # 'mini' season for testing. from:
        #   model_error_calculations.txt -> model_error_calculations.py -> model_error_calculations.xlsx:
        cls.exp_target_to_mae = {'1 wk ahead': 0.215904853,
                                 '2 wk ahead': 0.458186984,
                                 '3 wk ahead': 0.950515864,
                                 '4 wk ahead': 1.482010693}
        load_truth_data(cls.project, Path('forecast_app/tests/truth_data/mean-abs-error-truths.csv'))

        # score needed for MAE calculation
        Score.ensure_all_scores_exist()
        cls.score = Score.objects.filter(abbreviation='abs_error').first()  # hard-coded official name
        cls.score.update_score_for_model(cls.forecast_model)


    def test_mae(self):
        project2 = Project.objects.create()
        make_cdc_locations_and_targets(project2)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 23), is_season_start=True, season_name='s1')
        TimeZero.objects.create(project=project2, timezero_date=datetime.date(2016, 10, 30))
        TimeZero.objects.create(project=project2, timezero_date=datetime.date(2016, 11, 6))
        forecast_model2 = ForecastModel.objects.create(project=project2)
        load_cdc_csv_forecasts_from_dir(forecast_model2, Path('forecast_app/tests/load_forecasts'), 2016)
        load_truth_data(project2, Path('utils/ensemble-truth-table-script/truths-2016-2017-reichlab.csv'))

        Score.ensure_all_scores_exist()
        score = Score.objects.filter(abbreviation='abs_error').first()  # hard-coded official name
        score.update_score_for_model(forecast_model2)

        score_value_rows_for_season = _score_value_rows_for_season(project2, 's1')
        self.assertEqual(5 * 11, len(score_value_rows_for_season))  # 5 targets * 11 locations

        # spot-check a location
        exp_maes = [0.1830079332082548, 0.127335480231265, 0.040631614561185525, 0.09119562794624952,
                    0.15125133156909953]
        hhs1_loc = project2.locations.filter(name='HHS Region 1').first()
        hhs1_loc_rows = filter(lambda row: row[0] == hhs1_loc.id, score_value_rows_for_season)
        act_maes = [row[-1] for row in hhs1_loc_rows]
        for exp_mae, act_mae in zip(exp_maes, act_maes):
            self.assertAlmostEqual(exp_mae, act_mae)

        # test location_to_mean_abs_error_rows_for_project(), since we have a nice fixture
        loc_to_mae_rows_no_season = location_to_mean_abs_error_rows_for_project(project2, None)
        self.assertEqual(loc_to_mae_rows_no_season,
                         location_to_mean_abs_error_rows_for_project(project2, 's1'))  # season_name shouldn't matter
        self.assertEqual(set(project2.locations.values_list('name', flat=True)), set(loc_to_mae_rows_no_season))

        exp_rows = [['Model', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead', 'Season peak percentage'],
                    [forecast_model2.pk, 0.127335480231265, 0.040631614561185525, 0.09119562794624952,
                     0.15125133156909953, 0.1830079332082548]]
        act_rows = loc_to_mae_rows_no_season[hhs1_loc.name][0]
        self.assertEqual(exp_rows[0], act_rows[0])  # header
        self.assertEqual(exp_rows[1][0], act_rows[1][0])  # model
        self.assertAlmostEqual(exp_rows[1][1], act_rows[1][1])  # 1 wk ahead
        self.assertAlmostEqual(exp_rows[1][2], act_rows[1][2])
        self.assertAlmostEqual(exp_rows[1][3], act_rows[1][3])
        self.assertAlmostEqual(exp_rows[1][4], act_rows[1][4])
        self.assertAlmostEqual(exp_rows[1][5], act_rows[1][5])

        target_spp = project2.targets.filter(name='Season peak percentage').first()
        target_1wk = project2.targets.filter(name='1 wk ahead').first()
        target_2wk = project2.targets.filter(name='2 wk ahead').first()
        target_3wk = project2.targets.filter(name='3 wk ahead').first()
        target_4wk = project2.targets.filter(name='4 wk ahead').first()
        exp_loc_to_min = {
            target_spp: 0.1830079332082548,
            target_1wk: 0.127335480231265,
            target_2wk: 0.040631614561185525,
            target_3wk: 0.09119562794624952,
            target_4wk: 0.15125133156909953
        }
        act_loc_to_min = loc_to_mae_rows_no_season[hhs1_loc.name][1]
        self.assertAlmostEqual(exp_loc_to_min[target_spp], act_loc_to_min[target_spp.name])
        self.assertAlmostEqual(exp_loc_to_min[target_1wk], act_loc_to_min[target_1wk.name])
        self.assertAlmostEqual(exp_loc_to_min[target_2wk], act_loc_to_min[target_2wk.name])
        self.assertAlmostEqual(exp_loc_to_min[target_3wk], act_loc_to_min[target_3wk.name])
        self.assertAlmostEqual(exp_loc_to_min[target_4wk], act_loc_to_min[target_4wk.name])
