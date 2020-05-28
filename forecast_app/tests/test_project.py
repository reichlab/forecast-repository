import datetime
import json
import logging
from pathlib import Path
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.test import TestCase

from forecast_app.api_views import csv_response_for_project_truth_data
from forecast_app.models import Project, TimeZero, Target, Score, Job, BinDistribution, NamedDistribution, \
    PointPrediction, SampleDistribution, QuantileDistribution, Forecast
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.views import ProjectDetailView, _unit_to_actual_points, _unit_to_actual_max_val, \
    _upload_truth_worker
from utils.cdc import load_cdc_csv_forecast_file, make_cdc_units_and_targets
from utils.forecast import PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS, load_predictions_from_json_io_dict
from utils.make_minimal_projects import _make_docs_project
from utils.make_thai_moph_project import create_thai_units_and_targets
from utils.project import create_project_from_json, load_truth_data, query_forecasts_for_project, \
    validate_forecasts_query, CSV_HEADER
from utils.utilities import get_or_create_super_po_mo_users, YYYY_MM_DD_DATE_FORMAT


logging.getLogger().setLevel(logging.ERROR)


class ProjectTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date=datetime.date(2017, 1, 1))
        make_cdc_units_and_targets(cls.project)

        cls.forecast_model = ForecastModel.objects.create(project=cls.project, name='fm1', abbreviation='abbrev')
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
        cls.forecast = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, cls.time_zero)


    def test_load_truth_data(self):
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-ok.csv'), is_convert_na_none=True)
        self.assertEqual(7, self.project.truth_data_qs().count())
        self.assertTrue(self.project.is_truth_data_loaded())
        self.assertEqual('truths-ok.csv', self.project.truth_csv_filename)
        self.assertIsInstance(self.project.truth_updated_at, datetime.datetime)

        self.project.delete_truth_data()
        self.assertFalse(self.project.is_truth_data_loaded())
        self.assertFalse(self.project.truth_csv_filename)
        self.assertIsNone(self.project.truth_updated_at)

        # csv references non-existent TimeZero in Project: should not raise error
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-bad-timezero.csv'),
                        'truths-bad-timezero.csv', is_convert_na_none=True)

        # csv references non-existent unit in Project: should not raise error
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-bad-location.csv'),
                        'truths-bad-location.csv', is_convert_na_none=True)

        # csv references non-existent target in Project: should not raise error
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-bad-target.csv'),
                        'truths-bad-target.csv', is_convert_na_none=True)

        project2 = Project.objects.create()
        make_cdc_units_and_targets(project2)
        self.assertEqual(0, project2.truth_data_qs().count())
        self.assertFalse(project2.is_truth_data_loaded())

        TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 1, 1))
        load_truth_data(project2, Path('forecast_app/tests/truth_data/truths-ok.csv'), is_convert_na_none=True)
        self.assertEqual(7, project2.truth_data_qs().count())

        # test get_truth_data_preview()
        exp_truth_preview = [
            [datetime.date(2017, 1, 1), 'US National', '1 wk ahead', 0.73102],
            [datetime.date(2017, 1, 1), 'US National', '2 wk ahead', 0.688338],
            [datetime.date(2017, 1, 1), 'US National', '3 wk ahead', 0.732049],
            [datetime.date(2017, 1, 1), 'US National', '4 wk ahead', 0.911641],
            [datetime.date(2017, 1, 1), 'US National', 'Season peak percentage', None],
            [datetime.date(2017, 1, 1), 'US National', 'Season peak week', None],
            [datetime.date(2017, 1, 1), 'US National', 'Season onset', '2017-11-20']]
        self.assertEqual(exp_truth_preview, project2.get_truth_data_preview())


    def test_load_truth_data_other_files(self):
        # test truth files that used to be in yyyymmdd or yyyyww (EW) formats
        # truths-ok.csv (2017-01-17-truths.csv would basically test the same)
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-ok.csv'), is_convert_na_none=True)
        exp_rows = [(datetime.date(2017, 1, 1), 'US National', '1 wk ahead', None, 0.73102, None, None, None),
                    (datetime.date(2017, 1, 1), 'US National', '2 wk ahead', None, 0.688338, None, None, None),
                    (datetime.date(2017, 1, 1), 'US National', '3 wk ahead', None, 0.732049, None, None, None),
                    (datetime.date(2017, 1, 1), 'US National', '4 wk ahead', None, 0.911641, None, None, None),
                    (datetime.date(2017, 1, 1), 'US National', 'Season onset', None, None, '2017-11-20', None, None),
                    (datetime.date(2017, 1, 1), 'US National', 'Season peak percentage', None, None, None, None, None),
                    (datetime.date(2017, 1, 1), 'US National', 'Season peak week', None, None, None, None, None)]
        act_rows = self.project.truth_data_qs() \
            .order_by('unit__name', 'target__name') \
            .values_list('time_zero__timezero_date', 'unit__name', 'target__name',
                         'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
        self.assertEqual(exp_rows, list(act_rows))

        # truths-2016-2017-reichlab-small.csv
        project2 = Project.objects.create()
        TimeZero.objects.create(project=project2, timezero_date=datetime.date(2016, 10, 30))
        make_cdc_units_and_targets(project2)
        load_truth_data(project2, Path('forecast_app/tests/scores/truths-2016-2017-reichlab-small.csv'),
                        is_convert_na_none=True)
        exp_rows = [(datetime.date(2016, 10, 30), 'US National', '1 wk ahead', None, 1.55838, None, None, None),
                    (datetime.date(2016, 10, 30), 'US National', '2 wk ahead', None, 1.64639, None, None, None),
                    (datetime.date(2016, 10, 30), 'US National', '3 wk ahead', None, 1.91196, None, None, None),
                    (datetime.date(2016, 10, 30), 'US National', '4 wk ahead', None, 1.81129, None, None, None),
                    (datetime.date(2016, 10, 30), 'US National', 'Season onset', None, None, '2016-12-11', None, None),
                    (datetime.date(2016, 10, 30), 'US National', 'Season peak percentage',
                     None, 5.06094, None, None, None),
                    (datetime.date(2016, 10, 30), 'US National', 'Season peak week',
                     None, None, None, datetime.date(2017, 2, 5), None)]

        act_rows = project2.truth_data_qs() \
            .order_by('unit__name', 'target__name') \
            .values_list('time_zero__timezero_date', 'unit__name', 'target__name',
                         'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
        self.assertEqual(exp_rows, list(act_rows))


    def test_export_truth_data(self):
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-ok.csv'), is_convert_na_none=True)
        response = csv_response_for_project_truth_data(self.project)
        exp_content = ['timezero,unit,target,value',
                       '2017-01-01,US National,1 wk ahead,0.73102',
                       '2017-01-01,US National,2 wk ahead,0.688338',
                       '2017-01-01,US National,3 wk ahead,0.732049',
                       '2017-01-01,US National,4 wk ahead,0.911641',
                       '2017-01-01,US National,Season peak percentage,',
                       '2017-01-01,US National,Season peak week,',
                       '2017-01-01,US National,Season onset,2017-11-20',
                       '']
        act_content = response.content.decode("utf-8").split('\r\n')
        self.assertEqual(exp_content, act_content)


    def test_timezeros_unique(self):
        project = Project.objects.create()
        with self.assertRaises(ValidationError) as context:
            timezeros = [TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1)),
                         TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))]
            project.timezeros.add(*timezeros)
            project.save()
        self.assertIn("found duplicate TimeZero.timezero_date", str(context.exception))


    def test_get_num_rows(self):
        time_zero2 = TimeZero.objects.create(project=self.project, timezero_date=datetime.date(2017, 1, 2))
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
        load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, time_zero2)
        self.assertEqual(self.project.get_num_forecast_rows_all_models(), 8019 * 2)
        self.assertEqual(self.project.get_num_forecast_rows_all_models_estimated(),
                         8019 * 2)  # exact b/c uniform forecasts


    def test_score_csv_file_cache(self):
        # verify post_save worked
        self.assertIsNotNone(self.project.score_csv_file_cache)

        # test CSV file gets created
        load_truth_data(self.project, Path('utils/ensemble-truth-table-script/truths-2016-2017-reichlab.csv'),
                        is_convert_na_none=True)
        Score.ensure_all_scores_exist()
        score = Score.objects.filter(abbreviation='abs_error').first()
        score.update_score_for_model(self.forecast_model)

        # NB: this test assumes delete was called before upload
        with patch('utils.cloud_file.delete_file') as delete_file_mock, \
                patch('utils.cloud_file.upload_file') as upload_file_mock:
            self.project.score_csv_file_cache.update_score_csv_file_cache()

            args = delete_file_mock.call_args[0]  # delete_file(the_object)
            delete_file_mock.assert_called_once()
            self.assertEqual(self.project.score_csv_file_cache, args[0])

            args = upload_file_mock.call_args[0]  # upload_file(the_object, data_file)
            upload_file_mock.assert_called_once()
            self.assertEqual(self.project.score_csv_file_cache, args[0])
            self.assertGreater(len(args[1]), 0)


    def test_row_count_cache(self):
        self.assertIsNotNone(self.project.row_count_cache)  # verify post_save worked
        # assume last_update default works
        self.assertIsNone(self.project.row_count_cache.row_count)

        self.project.row_count_cache.update_row_count_cache()
        # NB: we assume last_update default works
        self.assertEqual(self.project.get_num_forecast_rows_all_models(), self.project.row_count_cache.row_count)


    def test_summary_counts(self):
        self.assertEqual((1, 1, 8019), self.project.get_summary_counts())  # num_models, num_forecasts, num_rows


    def test_timezero_seasons(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project2 = create_project_from_json(Path('forecast_app/tests/projects/cdc-project.json'), po_user)

        # 2015-01-01 <no season>  time_zero1    not within
        # 2015-02-01 <no season>  time_zero2    not within
        # 2016-02-01 season1      time_zero3  start
        # 2017-01-01   ""         time_zero4    within
        # 2017-02-01 season2      time_zero5  start
        # 2018-01-01 season3      time_zero6  start
        time_zero1 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2015, 1, 1),
                                             is_season_start=False)  # no season for this TZ. explicit arg
        time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2015, 2, 1),
                                             is_season_start=False)  # ""
        time_zero3 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2016, 2, 1),
                                             is_season_start=True, season_name='season1')  # start season1. 2 TZs
        time_zero4 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 1, 1)
                                             )  # in season1. default args
        time_zero5 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 2, 1),
                                             is_season_start=True, season_name='season2')  # start season2. 1 TZ
        time_zero6 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2018, 1, 1),
                                             is_season_start=True, season_name='season3')  # start season3. 1 TZ

        # test Project.timezeros_num_forecasts() b/c it's convenient here
        self.assertEqual(
            [(time_zero1, 0), (time_zero2, 0), (time_zero3, 0), (time_zero4, 0), (time_zero5, 0), (time_zero6, 0)],
            ProjectDetailView.timezeros_num_forecasts(project2))

        # above create() calls test valid TimeZero season values

        # test invalid TimeZero season values
        with self.assertRaises(ValidationError) as context:
            TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 1, 1),
                                    is_season_start=True, season_name=None)  # season start, no season name (passed)
        self.assertIn('passed is_season_start with no season_name', str(context.exception))

        with self.assertRaises(ValidationError) as context:
            TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 1, 1),
                                    is_season_start=True)  # season start, no season name (default)
        self.assertIn('passed is_season_start with no season_name', str(context.exception))

        with self.assertRaises(ValidationError) as context:
            TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 1, 1),
                                    is_season_start=False, season_name='season4')  # no season start, season name
        self.assertIn('passed season_name but not is_season_start', str(context.exception))

        # test seasons()
        self.assertEqual(['season1', 'season2', 'season3'], sorted(project2.seasons()))

        # test start_end_dates_for_season()
        self.assertEqual((time_zero3.timezero_date, time_zero4.timezero_date),
                         project2.start_end_dates_for_season('season1'))  # two TZs
        self.assertEqual((time_zero5.timezero_date, time_zero5.timezero_date),
                         project2.start_end_dates_for_season('season2'))  # only one TZ -> start == end
        self.assertEqual((time_zero6.timezero_date, time_zero6.timezero_date),
                         project2.start_end_dates_for_season('season3'))  # ""

        # test timezeros_in_season()
        with self.assertRaises(RuntimeError) as context:
            project2.timezeros_in_season('not a valid season')
        self.assertIn('invalid season_name', str(context.exception))

        self.assertEqual([time_zero3, time_zero4], project2.timezeros_in_season('season1'))
        self.assertEqual([time_zero5], project2.timezeros_in_season('season2'))
        self.assertEqual([time_zero6], project2.timezeros_in_season('season3'))

        # test timezeros_in_season() w/no season, but followed by some seasons
        self.assertEqual([time_zero1, time_zero2], project2.timezeros_in_season(None))

        # test timezeros_in_season() w/no season, followed by no seasons, i.e., no seasons at all in the project
        project3 = Project.objects.create()
        time_zero7 = TimeZero.objects.create(project=project3, timezero_date=datetime.date(2015, 1, 1))
        self.assertEqual([time_zero7], project3.timezeros_in_season(None))

        # test start_end_dates_for_season()
        self.assertEqual((time_zero7.timezero_date, time_zero7.timezero_date),
                         project3.start_end_dates_for_season(None))

        # test unit_to_max_val()
        forecast_model = ForecastModel.objects.create(project=project2, name='name', abbreviation='abbrev')
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
        load_cdc_csv_forecast_file(2016, forecast_model, csv_file_path, time_zero3)
        exp_unit_to_max_val = {'HHS Region 1': 2.06145600601835, 'HHS Region 10': 2.89940153907353,
                               'HHS Region 2': 4.99776594895244, 'HHS Region 3': 2.99944727598047,
                               'HHS Region 4': 2.62168214634388, 'HHS Region 5': 2.19233072084465,
                               'HHS Region 6': 4.41926018901693, 'HHS Region 7': 2.79371802884364,
                               'HHS Region 8': 1.69920709944699, 'HHS Region 9': 3.10232205135854,
                               'US National': 3.00101461253164}
        act_unit_to_max_val = project2.unit_to_max_val('season1', project2.step_ahead_targets())
        self.assertEqual(exp_unit_to_max_val, act_unit_to_max_val)

        # test timezero_to_season_name()
        exp_timezero_to_season_name = {
            time_zero1: None,
            time_zero2: None,
            time_zero3: 'season1',
            time_zero4: 'season1',
            time_zero5: 'season2',
            time_zero6: 'season3',
        }
        self.assertEqual(exp_timezero_to_season_name, project2.timezero_to_season_name())

        # test season_name_containing_timezero(). test both cases: first timezero starts a season or not
        timezero_to_exp_season_name = {time_zero1: None,
                                       time_zero2: None,
                                       time_zero3: 'season1',
                                       time_zero4: 'season1',
                                       time_zero5: 'season2',
                                       time_zero6: 'season3'}
        for timezero, exp_season_name in timezero_to_exp_season_name.items():
            self.assertEqual(exp_season_name, project2.season_name_containing_timezero(timezero))

        del (timezero_to_exp_season_name[time_zero1])
        del (timezero_to_exp_season_name[time_zero2])
        time_zero1.delete()
        time_zero2.delete()
        for timezero, exp_season_name in timezero_to_exp_season_name.items():
            self.assertEqual(exp_season_name, project2.season_name_containing_timezero(timezero))


    def test_visualization_targets(self):
        self.assertEqual(['1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead'],
                         [target.name for target in self.project.step_ahead_targets()])


    def test_reference_target_for_actual_values(self):
        self.assertEqual(Target.objects.filter(project=self.project, name='1 wk ahead').first(),
                         self.project.reference_target_for_actual_values())

        project = Project.objects.create()
        make_cdc_units_and_targets(project)
        Target.objects.filter(project=project, name='1 wk ahead').delete()
        self.assertEqual(Target.objects.filter(project=project, name='2 wk ahead').first(),
                         project.reference_target_for_actual_values())

        project = Project.objects.create()
        create_thai_units_and_targets(project)
        self.assertEqual(Target.objects.filter(project=project, name='1_biweek_ahead').first(),
                         project.reference_target_for_actual_values())

        project = Project.objects.create()  # no Targets
        self.assertIsNone(project.reference_target_for_actual_values())


    def test_actual_values(self):
        project = Project.objects.create()
        make_cdc_units_and_targets(project)

        # create TimeZeros only for the first few in truths-2017-2018-reichlab.csv (other truth will be skipped)
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 23))
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 30))
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 8, 6))

        load_truth_data(project, Path('utils/ensemble-truth-table-script/truths-2017-2018-reichlab.csv'),
                        is_convert_na_none=True)
        exp_loc_tz_date_to_actual_vals = {
            'HHS Region 1': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.303222],
                datetime.date(2017, 8, 6): [0.286054]},
            'HHS Region 10': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.364459],
                datetime.date(2017, 8, 6): [0.240377]},
            'HHS Region 2': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [1.32634],
                datetime.date(2017, 8, 6): [1.34713]},
            'HHS Region 3': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.797999],
                datetime.date(2017, 8, 6): [0.586092]},
            'HHS Region 4': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.476357],
                datetime.date(2017, 8, 6): [0.483647]},
            'HHS Region 5': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.602327],
                datetime.date(2017, 8, 6): [0.612967]},
            'HHS Region 6': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [1.15229],
                datetime.date(2017, 8, 6): [0.96867]},
            'HHS Region 7': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.174172],
                datetime.date(2017, 8, 6): [0.115888]},
            'HHS Region 8': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.33984],
                datetime.date(2017, 8, 6): [0.359646]},
            'HHS Region 9': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.892872],
                datetime.date(2017, 8, 6): [0.912778]},
            'US National': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.73102],
                datetime.date(2017, 8, 6): [0.688338]},
        }
        self.assertEqual(exp_loc_tz_date_to_actual_vals, project.unit_timezero_date_to_actual_vals(None))

        # test _unit_to_actual_points()
        exp_unit_to_actual_points = {'HHS Region 1': [None, 0.303222, 0.286054],
                                     'HHS Region 10': [None, 0.364459, 0.240377],
                                     'HHS Region 2': [None, 1.32634, 1.34713],
                                     'HHS Region 3': [None, 0.797999, 0.586092],
                                     'HHS Region 4': [None, 0.476357, 0.483647],
                                     'HHS Region 5': [None, 0.602327, 0.612967],
                                     'HHS Region 6': [None, 1.15229, 0.96867],
                                     'HHS Region 7': [None, 0.174172, 0.115888],
                                     'HHS Region 8': [None, 0.33984, 0.359646],
                                     'HHS Region 9': [None, 0.892872, 0.912778],
                                     'US National': [None, 0.73102, 0.688338]}
        self.assertEqual(exp_unit_to_actual_points, _unit_to_actual_points(exp_loc_tz_date_to_actual_vals))

        # test _unit_to_actual_max_val()
        exp_unit_to_actual_max_val = {'HHS Region 1': 0.303222, 'HHS Region 10': 0.364459, 'HHS Region 2': 1.34713,
                                      'HHS Region 3': 0.797999, 'HHS Region 4': 0.483647, 'HHS Region 5': 0.612967,
                                      'HHS Region 6': 1.15229, 'HHS Region 7': 0.174172, 'HHS Region 8': 0.359646,
                                      'HHS Region 9': 0.912778, 'US National': 0.73102}
        self.assertEqual(exp_unit_to_actual_max_val, _unit_to_actual_max_val(exp_loc_tz_date_to_actual_vals))

        del exp_loc_tz_date_to_actual_vals['HHS Region 1'][datetime.date(2017, 7, 30)]  # leave only None
        del exp_loc_tz_date_to_actual_vals['HHS Region 1'][datetime.date(2017, 8, 6)]  # ""
        exp_unit_to_actual_max_val = {'HHS Region 1': None, 'HHS Region 10': 0.364459, 'HHS Region 2': 1.34713,
                                      'HHS Region 3': 0.797999, 'HHS Region 4': 0.483647, 'HHS Region 5': 0.612967,
                                      'HHS Region 6': 1.15229, 'HHS Region 7': 0.174172, 'HHS Region 8': 0.359646,
                                      'HHS Region 9': 0.912778, 'US National': 0.73102}
        self.assertEqual(exp_unit_to_actual_max_val, _unit_to_actual_max_val(exp_loc_tz_date_to_actual_vals))

        # test 2 step ahead target first one not available
        project.targets.get(name='1 wk ahead').delete()  # recall: TruthData.target: on_delete=models.CASCADE
        exp_loc_tz_date_to_actual_vals = {
            'HHS Region 1': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.286054]},
            'HHS Region 10': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.240377]},
            'HHS Region 2': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [1.34713]},
            'HHS Region 3': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.586092]},
            'HHS Region 4': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.483647]},
            'HHS Region 5': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.612967]},
            'HHS Region 6': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.96867]},
            'HHS Region 7': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.115888]},
            'HHS Region 8': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.359646]},
            'HHS Region 9': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.912778]},
            'US National': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.688338]}
        }
        self.assertEqual(exp_loc_tz_date_to_actual_vals, project.unit_timezero_date_to_actual_vals(None))

        # test no step ahead targets available
        project.targets.all().delete()
        self.assertEqual({}, project.unit_timezero_date_to_actual_vals(None))


    def test_unit_target_name_tz_date_to_truth(self):
        # at this point self.project.timezeros.all() = <QuerySet [(1, datetime.date(2017, 1, 1), None, False, None)]>,
        # so add remaining TimeZeros so that truths are not skipped when loading mean-abs-error-truths-dups.csv
        TimeZero.objects.create(project=self.project, timezero_date=datetime.date(2016, 12, 18))
        TimeZero.objects.create(project=self.project, timezero_date=datetime.date(2016, 12, 25))
        # we omit 20170108

        self.project.delete_truth_data()
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/mean-abs-error-truths-dups.csv'),
                        is_convert_na_none=True)

        exp_loc_target_tz_date_to_truth = {
            'HHS Region 1': {
                '1 wk ahead': {
                    datetime.date(2017, 1, 1): [1.52411],
                    datetime.date(2016, 12, 18): [1.41861],
                    datetime.date(2016, 12, 25): [1.57644],
                },
                '2 wk ahead': {
                    datetime.date(2017, 1, 1): [1.73987],
                    datetime.date(2016, 12, 18): [1.57644],
                    datetime.date(2016, 12, 25): [1.52411],
                },
                '3 wk ahead': {
                    datetime.date(2017, 1, 1): [2.06524],
                    datetime.date(2016, 12, 18): [1.52411],
                    datetime.date(2016, 12, 25): [1.73987],
                },
                '4 wk ahead': {
                    datetime.date(2017, 1, 1): [2.51375],
                    datetime.date(2016, 12, 18): [1.73987],
                    datetime.date(2016, 12, 25): [2.06524],
                }},
            'US National': {
                '1 wk ahead': {
                    datetime.date(2017, 1, 1): [3.08492],
                    datetime.date(2016, 12, 18): [3.36496, 9.0],  # NB two!
                    datetime.date(2016, 12, 25): [3.0963],
                },
                '2 wk ahead': {
                    datetime.date(2017, 1, 1): [3.51496],
                    datetime.date(2016, 12, 18): [3.0963],
                    datetime.date(2016, 12, 25): [3.08492],
                },
                '3 wk ahead': {
                    datetime.date(2017, 1, 1): [3.8035],
                    datetime.date(2016, 12, 18): [3.08492],
                    datetime.date(2016, 12, 25): [3.51496],
                },
                '4 wk ahead': {
                    datetime.date(2017, 1, 1): [4.45059],
                    datetime.date(2016, 12, 18): [3.51496],
                    datetime.date(2016, 12, 25): [3.8035],
                }
            }
        }
        act_loc_target_tz_date_to_truth = self.project.unit_target_name_tz_date_to_truth()
        self.assertEqual(exp_loc_target_tz_date_to_truth, act_loc_target_tz_date_to_truth)


    def test_unit_target_name_tz_date_to_truth_multi_season(self):
        # test multiple seasons
        project = Project.objects.create()
        make_cdc_units_and_targets(project)

        # create TimeZeros only for the first few in truths-2017-2018-reichlab.csv (other truth will be skipped),
        # separated into two small seasons
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 23),
                                is_season_start=True, season_name='season1')
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 30))
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 8, 6),
                                is_season_start=True, season_name='season2')
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 8, 13))
        load_truth_data(project, Path('utils/ensemble-truth-table-script/truths-2017-2018-reichlab.csv'),
                        is_convert_na_none=True)  # 4004 rows

        # test unit_target_name_tz_date_to_truth() with above multiple seasons - done in this method b/c we've
        # set up some seasons :-)
        act_loc_target_tz_date_to_truth = project.unit_target_name_tz_date_to_truth('season1')
        self.assertEqual(_exp_loc_tz_date_to_actual_vals_season_1a(), act_loc_target_tz_date_to_truth)

        # test unit_timezero_date_to_actual_vals() with above multiple seasons
        self.assertEqual(_exp_loc_tz_date_to_actual_vals_season_1b(),
                         project.unit_timezero_date_to_actual_vals('season1'))
        self.assertEqual(_exp_loc_tz_date_to_actual_vals_season_2b(),
                         project.unit_timezero_date_to_actual_vals('season2'))


    def test_0_step_target(self):
        project = Project.objects.create()
        make_cdc_units_and_targets(project)

        # create TimeZeros only for the first few in truths-2017-2018-reichlab.csv (other truth will be skipped)
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 23))
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 30))
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 8, 6))
        load_truth_data(project, Path('utils/ensemble-truth-table-script/truths-2017-2018-reichlab.csv'),
                        is_convert_na_none=True)

        # change '1 wk ahead' to '0 wk ahead' in Target and truth data. also tests that target names are not used
        # (ids or step_ahead_increment should be used)
        target = project.targets.get(name='1 wk ahead')
        target.name = '0 wk ahead'
        target.step_ahead_increment = 0
        target.save()

        exp_loc_tz_date_to_actual_vals = {
            'HHS Region 1': {datetime.date(2017, 7, 23): [0.303222],
                             datetime.date(2017, 7, 30): [0.286054],
                             datetime.date(2017, 8, 6): [0.341359]},
            'HHS Region 10': {datetime.date(2017, 7, 23): [0.364459],
                              datetime.date(2017, 7, 30): [0.240377],
                              datetime.date(2017, 8, 6): [0.126923]},
            'HHS Region 2': {datetime.date(2017, 7, 23): [1.32634],
                             datetime.date(2017, 7, 30): [1.34713],
                             datetime.date(2017, 8, 6): [1.15738]},
            'HHS Region 3': {datetime.date(2017, 7, 23): [0.797999],
                             datetime.date(2017, 7, 30): [0.586092],
                             datetime.date(2017, 8, 6): [0.611163]},
            'HHS Region 4': {datetime.date(2017, 7, 23): [0.476357],
                             datetime.date(2017, 7, 30): [0.483647],
                             datetime.date(2017, 8, 6): [0.674289]},
            'HHS Region 5': {datetime.date(2017, 7, 23): [0.602327],
                             datetime.date(2017, 7, 30): [0.612967],
                             datetime.date(2017, 8, 6): [0.637141]},
            'HHS Region 6': {datetime.date(2017, 7, 23): [1.15229],
                             datetime.date(2017, 7, 30): [0.96867],
                             datetime.date(2017, 8, 6): [1.02289]},
            'HHS Region 7': {datetime.date(2017, 7, 23): [0.174172],
                             datetime.date(2017, 7, 30): [0.115888],
                             datetime.date(2017, 8, 6): [0.112074]},
            'HHS Region 8': {datetime.date(2017, 7, 23): [0.33984],
                             datetime.date(2017, 7, 30): [0.359646],
                             datetime.date(2017, 8, 6): [0.326402]},
            'HHS Region 9': {datetime.date(2017, 7, 23): [0.892872],
                             datetime.date(2017, 7, 30): [0.912778],
                             datetime.date(2017, 8, 6): [1.012]},
            'US National': {datetime.date(2017, 7, 23): [0.73102],
                            datetime.date(2017, 7, 30): [0.688338],
                            datetime.date(2017, 8, 6): [0.732049]}
        }
        self.assertEqual(exp_loc_tz_date_to_actual_vals, project.unit_timezero_date_to_actual_vals(None))


    def test_timezeros_num_forecasts(self):
        self.assertEqual([(self.time_zero, 1)], ProjectDetailView.timezeros_num_forecasts(self.project))


    def test_process_upload_truth_job(self):
        # test `_upload_truth_worker()` error conditions. this test is complicated by that function's use of
        # the `job_cloud_file` context manager. solution is per https://stackoverflow.com/questions/60198229/python-patch-context-manager-to-return-object
        with patch('forecast_app.models.job.job_cloud_file') as mock:  # returns 2-tuple: (job, cloud_file_fp)
            job = Job.objects.create()
            job.input_json = {}  # no 'project_pk'
            job.save()
            mock.return_value.__enter__.return_value = (job, None)
            with self.assertRaises(RuntimeError) as context:
                _upload_truth_worker(job.pk)
            self.assertIn("missing 'project_pk' in job", str(context.exception))

            # test no 'filename'
            job.input_json = {'project_pk': None}  # no 'filename'
            job.save()
            with self.assertRaises(RuntimeError) as context:
                _upload_truth_worker(job.pk)
            self.assertIn("missing 'filename' in job", str(context.exception))

            # test bad 'project_pk'
            job.input_json = {'project_pk': -1, 'filename': None}
            job.save()
            with self.assertRaises(RuntimeError) as context:
                _upload_truth_worker(job.pk)
            self.assertIn("no Project found for project_pk", str(context.exception))


    def test_query_forecasts_for_project(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        model = forecast_model.abbreviation
        tz = time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        timezero_to_season_name = project.timezero_to_season_name()
        seas = timezero_to_season_name[time_zero]

        # ---- case: all BinDistributions in project. check cat and prob columns ----
        rows = query_forecasts_for_project(project,
                                           {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution]]})
        self.assertEqual(CSV_HEADER, rows.pop(0))

        exp_rows_bin = [(model, tz, seas, 'location1', 'Season peak week', 'bin', '2019-12-15', 0.01),
                        (model, tz, seas, 'location1', 'Season peak week', 'bin', '2019-12-22', 0.1),
                        (model, tz, seas, 'location1', 'Season peak week', 'bin', '2019-12-29', 0.89),
                        (model, tz, seas, 'location1', 'season severity', 'bin', 'moderate', 0.1),
                        (model, tz, seas, 'location1', 'season severity', 'bin', 'severe', 0.9),
                        (model, tz, seas, 'location2', 'Season peak week', 'bin', '2019-12-15', 0.01),
                        (model, tz, seas, 'location2', 'Season peak week', 'bin', '2019-12-22', 0.05),
                        (model, tz, seas, 'location2', 'Season peak week', 'bin', '2019-12-29', 0.05),
                        (model, tz, seas, 'location2', 'Season peak week', 'bin', '2020-01-05', 0.89),
                        (model, tz, seas, 'location2', 'above baseline', 'bin', False, 0.1),
                        (model, tz, seas, 'location2', 'above baseline', 'bin', True, 0.9),
                        (model, tz, seas, 'location2', 'pct next week', 'bin', 1.1, 0.3),
                        (model, tz, seas, 'location2', 'pct next week', 'bin', 2.2, 0.2),
                        (model, tz, seas, 'location2', 'pct next week', 'bin', 3.3, 0.5),
                        (model, tz, seas, 'location3', 'cases next week', 'bin', 2, 0.1),
                        (model, tz, seas, 'location3', 'cases next week', 'bin', 50, 0.9)]  # sorted
        # model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
        act_rows = [(row[0], row[1], row[2], row[3], row[4], row[5], row[7], row[8]) for row in rows]
        self.assertEqual(exp_rows_bin, sorted(act_rows))

        # ----  case: all NamedDistributions in project. check family, and param1, 2, and 3 columns ----
        rows = query_forecasts_for_project(project,
                                           {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[NamedDistribution]]})
        self.assertEqual(CSV_HEADER, rows.pop(0))

        exp_rows_named = [(model, tz, seas, 'location1', 'cases next week', 'named',
                           NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION[NamedDistribution.POIS_DIST], 1.1, None,
                           None),
                          (model, tz, seas, 'location1', 'pct next week', 'named',
                           NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION[NamedDistribution.NORM_DIST], 1.1, 2.2, None)
                          ]  # sorted
        # model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
        act_rows = [(row[0], row[1], row[2], row[3], row[4], row[5], row[11], row[12], row[13], row[14])
                    for row in rows]
        self.assertEqual(exp_rows_named, sorted(act_rows))

        # ---- case: all PointPredictions in project. check value column ----
        rows = query_forecasts_for_project(project,
                                           {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction]]})
        self.assertEqual(CSV_HEADER, rows.pop(0))

        exp_rows_point = [
            (model, tz, seas, 'location1', 'Season peak week', 'point', '2019-12-22'),
            (model, tz, seas, 'location1', 'above baseline', 'point', True),
            (model, tz, seas, 'location1', 'pct next week', 'point', 2.1),
            (model, tz, seas, 'location1', 'season severity', 'point', 'mild'),
            (model, tz, seas, 'location2', 'Season peak week', 'point', '2020-01-05'),
            (model, tz, seas, 'location2', 'cases next week', 'point', 5),
            (model, tz, seas, 'location2', 'pct next week', 'point', 2.0),
            (model, tz, seas, 'location2', 'season severity', 'point', 'moderate'),
            (model, tz, seas, 'location3', 'Season peak week', 'point', '2019-12-29'),
            (model, tz, seas, 'location3', 'cases next week', 'point', 10),
            (model, tz, seas, 'location3', 'pct next week', 'point', 3.567)]  # sorted
        # model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
        act_rows = [(row[0], row[1], row[2], row[3], row[4], row[5], row[6]) for row in rows]
        self.assertEqual(exp_rows_point, sorted(act_rows))

        # ---- case: all SampleDistributions in project. check sample column ----
        rows = query_forecasts_for_project(project,
                                           {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleDistribution]]})
        self.assertEqual(CSV_HEADER, rows.pop(0))

        exp_rows_sample = [(model, tz, seas, 'location1', 'Season peak week', 'sample', '2019-12-15'),
                           (model, tz, seas, 'location1', 'Season peak week', 'sample', '2020-01-05'),
                           (model, tz, seas, 'location2', 'above baseline', 'sample', False),
                           (model, tz, seas, 'location2', 'above baseline', 'sample', True),
                           (model, tz, seas, 'location2', 'above baseline', 'sample', True),
                           (model, tz, seas, 'location2', 'cases next week', 'sample', 0),
                           (model, tz, seas, 'location2', 'cases next week', 'sample', 2),
                           (model, tz, seas, 'location2', 'cases next week', 'sample', 5),
                           (model, tz, seas, 'location2', 'season severity', 'sample', 'high'),
                           (model, tz, seas, 'location2', 'season severity', 'sample', 'mild'),
                           (model, tz, seas, 'location2', 'season severity', 'sample', 'moderate'),
                           (model, tz, seas, 'location2', 'season severity', 'sample', 'moderate'),
                           (model, tz, seas, 'location2', 'season severity', 'sample', 'severe'),
                           (model, tz, seas, 'location3', 'Season peak week', 'sample', '2019-12-16'),
                           (model, tz, seas, 'location3', 'Season peak week', 'sample', '2020-01-06'),
                           (model, tz, seas, 'location3', 'above baseline', 'sample', False),
                           (model, tz, seas, 'location3', 'above baseline', 'sample', True),
                           (model, tz, seas, 'location3', 'above baseline', 'sample', True),
                           (model, tz, seas, 'location3', 'pct next week', 'sample', 0.0),
                           (model, tz, seas, 'location3', 'pct next week', 'sample', 0.0001),
                           (model, tz, seas, 'location3', 'pct next week', 'sample', 2.3),
                           (model, tz, seas, 'location3', 'pct next week', 'sample', 6.5),
                           (model, tz, seas, 'location3', 'pct next week', 'sample', 10.0234)]  # sorted
        # model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
        act_rows = [(row[0], row[1], row[2], row[3], row[4], row[5], row[9]) for row in rows]
        self.assertEqual(exp_rows_sample, sorted(act_rows))

        # ---- case: all QuantileDistributions in project. check quantile and value columns ----
        rows = query_forecasts_for_project(project,
                                           {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[QuantileDistribution]]})
        self.assertEqual(CSV_HEADER, rows.pop(0))

        exp_rows_quantile = [(model, tz, seas, 'location2', 'Season peak week', 'quantile', 0.5, '2019-12-22'),
                             (model, tz, seas, 'location2', 'Season peak week', 'quantile', 0.75, '2019-12-29'),
                             (model, tz, seas, 'location2', 'Season peak week', 'quantile', 0.975, '2020-01-05'),
                             (model, tz, seas, 'location2', 'pct next week', 'quantile', 0.025, 1.0),
                             (model, tz, seas, 'location2', 'pct next week', 'quantile', 0.25, 2.2),
                             (model, tz, seas, 'location2', 'pct next week', 'quantile', 0.5, 2.2),
                             (model, tz, seas, 'location2', 'pct next week', 'quantile', 0.75, 5.0),
                             (model, tz, seas, 'location2', 'pct next week', 'quantile', 0.975, 50.0),
                             (model, tz, seas, 'location3', 'cases next week', 'quantile', 0.25, 0),
                             (model, tz, seas, 'location3', 'cases next week', 'quantile', 0.75, 50)]  # sorted
        # model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
        act_rows = [(row[0], row[1], row[2], row[3], row[4], row[5], row[10], row[6]) for row in rows]
        self.assertEqual(exp_rows_quantile, sorted(act_rows))

        # ---- case: empty query -> all forecasts in project ----
        rows = query_forecasts_for_project(project, {})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin),
                         len(rows))

        # ---- case: only one unit ----
        rows = query_forecasts_for_project(project, {'units': [project.units.filter(name='location3').first().pk]})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(17, len(rows))

        # ---- case: only one target ----
        rows = query_forecasts_for_project(project,
                                           {'targets': [project.targets.filter(name='above baseline').first().pk]})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(9, len(rows))

        # following two tests require a second model, timezero, and forecast
        forecast_model2 = ForecastModel.objects.create(project=project, name=model, abbreviation='abbrev')
        time_zero2 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2011, 10, 22))
        forecast2 = Forecast.objects.create(forecast_model=forecast_model2, source='docs-predictions.json',
                                            time_zero=time_zero2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, False)

        # ---- case: empty query -> all forecasts in project. s/be twice as many now ----
        rows = query_forecasts_for_project(project, {})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin) * 2,
                         len(rows))

        # ---- case: only one timezero ----
        rows = query_forecasts_for_project(project, {'timezeros': [time_zero2.pk]})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin),
                         len(rows))

        # ---- case: only one model ----
        rows = query_forecasts_for_project(project, {'models': [forecast_model2.pk]})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin),
                         len(rows))


    def test_query_forecasts_for_project_max_num_rows(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, _, _, _ = _make_docs_project(po_user)
        rows = query_forecasts_for_project(project, {})
        self.assertEqual(63, len(rows))  # all forecasts in project, plus header

        try:
            query_forecasts_for_project(project, {}, max_num_rows=62)
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        with self.assertRaises(RuntimeError) as context:
            query_forecasts_for_project(project, {}, max_num_rows=10)
        self.assertIn("number of rows exceeded maximum", str(context.exception))


    def test__validate_forecasts_query(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)

        # case: query not a dict
        validate_forecasts_query(project, -1)

        # case: query contains invalid keys
        error_messages, _ = validate_forecasts_query(project, {'foo': -1})
        self.assertEqual(1, len(error_messages))
        self.assertIn("one or more query keys was invalid", error_messages[0])

        # case: bad object id
        for key_name in ['models', 'units', 'targets', 'timezeros']:
            error_messages, _ = validate_forecasts_query(project, {key_name: [-1]})
            self.assertEqual(1, len(error_messages))
            self.assertIn("contained ID(s) of objects that don't exist", error_messages[0])

        # case: bad type
        error_messages, _ = validate_forecasts_query(project, {'types': ['bad type']})
        self.assertEqual(1, len(error_messages))
        self.assertIn("one or more types were invalid prediction types", error_messages[0])

        # case: ids from other project (!)
        project2, time_zero2, forecast_model2, forecast2 = _make_docs_project(po_user)
        for query_dict in [{'models': list(project2.models.all().values_list('id', flat=True))},
                           {'units': list(project2.units.all().values_list('id', flat=True))},
                           {'targets': list(project2.targets.all().values_list('id', flat=True))},
                           {'timezeros': list(project2.timezeros.all().values_list('id', flat=True))}]:
            query_key = list(query_dict.keys())[0]
            error_messages, _ = validate_forecasts_query(project, query_dict)
            self.assertEqual(1, len(error_messages))
            self.assertIn(f"`{query_key}` contained ID(s) of objects that don't exist in project", error_messages[0])

        # case: blue sky
        query = {'models': list(project.models.all().values_list('id', flat=True)),
                 'units': list(project.units.all().values_list('id', flat=True)),
                 'targets': list(project.targets.all().values_list('id', flat=True)),
                 'timezeros': list(project.timezeros.all().values_list('id', flat=True)),
                 'types': list(PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS.values())}
        error_messages, _ = validate_forecasts_query(project, query)
        self.assertEqual(0, len(error_messages))


def _exp_loc_tz_date_to_actual_vals_season_1a():
    return {
        'HHS Region 1': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.303222],
                                        datetime.date(2017, 7, 30): [0.286054]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.286054],
                                        datetime.date(2017, 7, 30): [0.341359]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.341359],
                                        datetime.date(2017, 7, 30): [0.325429]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.325429],
                                        datetime.date(2017, 7, 30): [0.339203]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-11-19'],
                                          datetime.date(2017, 7, 30): ['2017-11-19']},
                         'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                    datetime.date(2017, 7, 30): [None]},
                         'Season peak week': {datetime.date(2017, 7, 23): [None],
                                              datetime.date(2017, 7, 30): [None]}},
        'HHS Region 10': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.364459],
                                         datetime.date(2017, 7, 30): [0.240377]},
                          '2 wk ahead': {datetime.date(2017, 7, 23): [0.240377],
                                         datetime.date(2017, 7, 30): [0.126923]},
                          '3 wk ahead': {datetime.date(2017, 7, 23): [0.126923],
                                         datetime.date(2017, 7, 30): [0.241729]},
                          '4 wk ahead': {datetime.date(2017, 7, 23): [0.241729],
                                         datetime.date(2017, 7, 30): [0.293072]},
                          'Season onset': {datetime.date(2017, 7, 23): ['2017-12-17'],
                                           datetime.date(2017, 7, 30): ['2017-12-17']},
                          'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                     datetime.date(2017, 7, 30): [None]},
                          'Season peak week': {datetime.date(2017, 7, 23): [None],
                                               datetime.date(2017, 7, 30): [None]}},
        'HHS Region 2': {'1 wk ahead': {datetime.date(2017, 7, 23): [1.32634],
                                        datetime.date(2017, 7, 30): [1.34713]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [1.34713],
                                        datetime.date(2017, 7, 30): [1.15738]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [1.15738],
                                        datetime.date(2017, 7, 30): [1.41483]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [1.41483],
                                        datetime.date(2017, 7, 30): [1.32425]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-03'],
                                          datetime.date(2017, 7, 30): ['2017-12-03']},
                         'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                    datetime.date(2017, 7, 30): [None]},
                         'Season peak week': {datetime.date(2017, 7, 23): [None],
                                              datetime.date(2017, 7, 30): [None]}},
        'HHS Region 3': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.797999],
                                        datetime.date(2017, 7, 30): [0.586092]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.586092],
                                        datetime.date(2017, 7, 30): [0.611163]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.611163],
                                        datetime.date(2017, 7, 30): [0.623141]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.623141],
                                        datetime.date(2017, 7, 30): [0.781271]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-17'],
                                          datetime.date(2017, 7, 30): ['2017-12-17']},
                         'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                    datetime.date(2017, 7, 30): [None]},
                         'Season peak week': {datetime.date(2017, 7, 23): [None],
                                              datetime.date(2017, 7, 30): [None]}},
        'HHS Region 4': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.476357],
                                        datetime.date(2017, 7, 30): [0.483647]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.483647],
                                        datetime.date(2017, 7, 30): [0.674289]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.674289],
                                        datetime.date(2017, 7, 30): [0.782429]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.782429],
                                        datetime.date(2017, 7, 30): [1.11294]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-11-05'],
                                          datetime.date(2017, 7, 30): ['2017-11-05']},
                         'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                    datetime.date(2017, 7, 30): [None]},
                         'Season peak week': {datetime.date(2017, 7, 23): [None],
                                              datetime.date(2017, 7, 30): [None]}},
        'HHS Region 5': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.602327],
                                        datetime.date(2017, 7, 30): [0.612967]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.612967],
                                        datetime.date(2017, 7, 30): [0.637141]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.637141],
                                        datetime.date(2017, 7, 30): [0.627954]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.627954],
                                        datetime.date(2017, 7, 30): [0.724628]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-03'],
                                          datetime.date(2017, 7, 30): ['2017-12-03']},
                         'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                    datetime.date(2017, 7, 30): [None]},
                         'Season peak week': {datetime.date(2017, 7, 23): [None],
                                              datetime.date(2017, 7, 30): [None]}},
        'HHS Region 6': {'1 wk ahead': {datetime.date(2017, 7, 23): [1.15229],
                                        datetime.date(2017, 7, 30): [0.96867]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.96867],
                                        datetime.date(2017, 7, 30): [1.02289]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [1.02289],
                                        datetime.date(2017, 7, 30): [1.66769]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [1.66769],
                                        datetime.date(2017, 7, 30): [1.74834]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-11-26'],
                                          datetime.date(2017, 7, 30): ['2017-11-26']},
                         'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                    datetime.date(2017, 7, 30): [None]},
                         'Season peak week': {datetime.date(2017, 7, 23): [None],
                                              datetime.date(2017, 7, 30): [None]}},
        'HHS Region 7': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.174172],
                                        datetime.date(2017, 7, 30): [0.115888]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.115888],
                                        datetime.date(2017, 7, 30): [0.112074]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.112074],
                                        datetime.date(2017, 7, 30): [0.233776]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.233776],
                                        datetime.date(2017, 7, 30): [0.142496]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-03'],
                                          datetime.date(2017, 7, 30): ['2017-12-03']},
                         'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                    datetime.date(2017, 7, 30): [None]},
                         'Season peak week': {datetime.date(2017, 7, 23): [None],
                                              datetime.date(2017, 7, 30): [None]}},
        'HHS Region 8': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.33984],
                                        datetime.date(2017, 7, 30): [0.359646]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.359646],
                                        datetime.date(2017, 7, 30): [0.326402]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.326402],
                                        datetime.date(2017, 7, 30): [0.419146]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.419146],
                                        datetime.date(2017, 7, 30): [0.714684]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-10'],
                                          datetime.date(2017, 7, 30): ['2017-12-10']},
                         'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                    datetime.date(2017, 7, 30): [None]},
                         'Season peak week': {datetime.date(2017, 7, 23): [None],
                                              datetime.date(2017, 7, 30): [None]}},
        'HHS Region 9': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.892872],
                                        datetime.date(2017, 7, 30): [0.912778]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.912778],
                                        datetime.date(2017, 7, 30): [1.012]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [1.012],
                                        datetime.date(2017, 7, 30): [1.26206]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [1.26206],
                                        datetime.date(2017, 7, 30): [1.28077]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-03'],
                                          datetime.date(2017, 7, 30): ['2017-12-03']},
                         'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                    datetime.date(2017, 7, 30): [None]},
                         'Season peak week': {datetime.date(2017, 7, 23): [None],
                                              datetime.date(2017, 7, 30): [None]}},
        'US National': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.73102],
                                       datetime.date(2017, 7, 30): [0.688338]},
                        '2 wk ahead': {datetime.date(2017, 7, 23): [0.688338],
                                       datetime.date(2017, 7, 30): [0.732049]},
                        '3 wk ahead': {datetime.date(2017, 7, 23): [0.732049],
                                       datetime.date(2017, 7, 30): [0.911641]},
                        '4 wk ahead': {datetime.date(2017, 7, 23): [0.911641],
                                       datetime.date(2017, 7, 30): [1.02105]},
                        'Season onset': {datetime.date(2017, 7, 23): ['2017-11-19'],
                                         datetime.date(2017, 7, 30): ['2017-11-19']},
                        'Season peak percentage': {datetime.date(2017, 7, 23): [None],
                                                   datetime.date(2017, 7, 30): [None]},
                        'Season peak week': {datetime.date(2017, 7, 23): [None],
                                             datetime.date(2017, 7, 30): [None]}}
    }


def _exp_loc_tz_date_to_actual_vals_season_1b():
    return {
        'HHS Region 1': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [0.303222],
        },
        'HHS Region 10': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [0.364459],
        },
        'HHS Region 2': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [1.32634],
        },
        'HHS Region 3': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [0.797999],
        },
        'HHS Region 4': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [0.476357],
        },
        'HHS Region 5': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [0.602327],
        },
        'HHS Region 6': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [1.15229],
        },
        'HHS Region 7': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [0.174172],
        },
        'HHS Region 8': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [0.33984],
        },
        'HHS Region 9': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [0.892872],
        },
        'US National': {
            datetime.date(2017, 7, 23): None,
            datetime.date(2017, 7, 30): [0.73102],
        },
    }


def _exp_loc_tz_date_to_actual_vals_season_2b():
    return {
        'HHS Region 1': {
            datetime.date(2017, 8, 6): [0.286054],
            datetime.date(2017, 8, 13): [0.341359],
        },
        'HHS Region 10': {
            datetime.date(2017, 8, 6): [0.240377],
            datetime.date(2017, 8, 13): [0.126923],
        },
        'HHS Region 2': {
            datetime.date(2017, 8, 6): [1.34713],
            datetime.date(2017, 8, 13): [1.15738],
        },
        'HHS Region 3': {
            datetime.date(2017, 8, 6): [0.586092],
            datetime.date(2017, 8, 13): [0.611163],
        },
        'HHS Region 4': {
            datetime.date(2017, 8, 6): [0.483647],
            datetime.date(2017, 8, 13): [0.674289],
        },
        'HHS Region 5': {
            datetime.date(2017, 8, 6): [0.612967],
            datetime.date(2017, 8, 13): [0.637141],
        },
        'HHS Region 6': {
            datetime.date(2017, 8, 6): [0.96867],
            datetime.date(2017, 8, 13): [1.02289],
        },
        'HHS Region 7': {
            datetime.date(2017, 8, 6): [0.115888],
            datetime.date(2017, 8, 13): [0.112074],
        },
        'HHS Region 8': {
            datetime.date(2017, 8, 6): [0.359646],
            datetime.date(2017, 8, 13): [0.326402],
        },
        'HHS Region 9': {
            datetime.date(2017, 8, 6): [0.912778],
            datetime.date(2017, 8, 13): [1.012],
        },
        'US National': {
            datetime.date(2017, 8, 6): [0.688338],
            datetime.date(2017, 8, 13): [0.732049],
        },
    }
