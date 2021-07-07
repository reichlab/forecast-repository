import datetime
import json
import logging
from pathlib import Path
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.db import transaction
from django.test import TestCase

from forecast_app.models import Project, TimeZero, Job, Forecast, PredictionData
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.views import ProjectDetailView, _upload_truth_worker
from utils.cdc_io import load_cdc_csv_forecast_file, make_cdc_units_and_targets
from utils.forecast import load_predictions_from_json_io_dict
from utils.make_minimal_projects import _make_docs_project
from utils.project import create_project_from_json
from utils.project_truth import load_truth_data, is_truth_data_loaded, get_truth_data_preview, truth_data_qs, \
    oracle_model_for_project, truth_batches, truth_batch_forecasts, truth_delete_batch, truth_batch_summary_table
from utils.utilities import get_or_create_super_po_mo_users


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
        self.assertEqual(5, truth_data_qs(self.project).count())
        self.assertTrue(is_truth_data_loaded(self.project))

        # csv references non-existent TimeZero in Project: the bad timezero 2017-01-02 is skipped by
        # _read_truth_data_rows(), but the remaining data that's loaded (the three 2017-01-01 rows) is therefore a
        # subset. this raised 'new data is a subset of previous' prior to this issue:
        # [support truth "diff" uploads #319](https://github.com/reichlab/forecast-repository/issues/319), but now
        # subsets are allowed.
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-bad-timezero.csv'),
                        'truths-bad-timezero.csv', is_convert_na_none=True)

        # csv references non-existent unit in Project: the bad unit is skipped, again resulting in a subset. again,
        # subsets are now allowed
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-bad-location.csv'),
                        'truths-bad-location.csv', is_convert_na_none=True)

        # csv references non-existent target in Project: the bad target is skipped. subset is allowed
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-bad-target.csv'),
                        'truths-bad-target.csv', is_convert_na_none=True)

        project2 = Project.objects.create()
        make_cdc_units_and_targets(project2)
        self.assertEqual(0, truth_data_qs(project2).count())
        self.assertFalse(is_truth_data_loaded(project2))

        TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 1, 1))
        load_truth_data(project2, Path('forecast_app/tests/truth_data/truths-ok.csv'), is_convert_na_none=True)
        self.assertEqual(5, truth_data_qs(project2).count())

        # test get_truth_data_preview()
        exp_truth_preview = [
            (datetime.date(2017, 1, 1), 'US National', '1 wk ahead', 0.73102),
            (datetime.date(2017, 1, 1), 'US National', '2 wk ahead', 0.688338),
            (datetime.date(2017, 1, 1), 'US National', '3 wk ahead', 0.732049),
            (datetime.date(2017, 1, 1), 'US National', '4 wk ahead', 0.911641),
            (datetime.date(2017, 1, 1), 'US National', 'Season onset', '2017-11-20')]
        self.assertEqual(sorted(exp_truth_preview), sorted(get_truth_data_preview(project2)))


    def test_load_truth_data_versions(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)  # loads docs-ground-truth.csv

        oracle_model = oracle_model_for_project(project)
        self.assertEqual(3, oracle_model.forecasts.count())  # for 3 timezeros: 2011-10-02, 2011-10-09, 2011-10-16
        self.assertEqual(14, truth_data_qs(project).count())
        self.assertTrue(is_truth_data_loaded(project))

        with self.assertRaisesRegex(RuntimeError, 'cannot load 100% duplicate data'):
            load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth.csv'),
                            file_name='docs-ground-truth.csv')

        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-non-dup.csv'),
                        file_name='docs-ground-truth-non-dup.csv')
        self.assertEqual(3 * 2, oracle_model.forecasts.count())
        self.assertEqual(14 * 2, truth_data_qs(project).count())


    def test_load_truth_data_diff(self):
        """
        Tests the relaxing of this forecast version rule when loading truth (issue
        [support truth "diff" uploads #319](https://github.com/reichlab/forecast-repository/issues/319) ):
            3. New forecast versions cannot imply any retracted prediction elements in existing versions, i.e., you
            cannot load data that's a subset of the previous forecast's data.
        """
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)  # loads docs-ground-truth.csv

        oracle_model = oracle_model_for_project(project)
        self.assertEqual(3, oracle_model.forecasts.count())  # for 3 timezeros: 2011-10-02, 2011-10-09, 2011-10-16
        self.assertEqual(14, truth_data_qs(project).count())

        # updates only the five location2 rows:
        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-diff.csv'),
                        file_name='docs-ground-truth-diff.csv')
        self.assertEqual(3 + 1, oracle_model.forecasts.count())
        self.assertEqual(14 + 5, truth_data_qs(project).count())


    def test_load_truth_data_other_files(self):
        # test truth files that used to be in yyyymmdd or yyyyww (EW) formats
        # truths-ok.csv (2017-01-17-truths.csv would basically test the same)
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-ok.csv'), is_convert_na_none=True)
        exp_rows = [(datetime.date(2017, 1, 1), 'US National', '1 wk ahead', 0.73102),
                    (datetime.date(2017, 1, 1), 'US National', '2 wk ahead', 0.688338),
                    (datetime.date(2017, 1, 1), 'US National', '3 wk ahead', 0.732049),
                    (datetime.date(2017, 1, 1), 'US National', '4 wk ahead', 0.911641),
                    (datetime.date(2017, 1, 1), 'US National', 'Season onset', '2017-11-20')]

        # note: https://code.djangoproject.com/ticket/32483 sqlite3 json query bug -> we manually access field instead
        # of using 'data__value'
        pred_data_qs = PredictionData.objects \
            .filter(pred_ele__forecast__forecast_model=oracle_model_for_project(self.project)) \
            .values_list('pred_ele__forecast__time_zero__timezero_date', 'pred_ele__unit__name',
                         'pred_ele__target__name', 'data')
        act_rows = [(tz_date, unit__name, target__name, data['value'])
                    for tz_date, unit__name, target__name, data in pred_data_qs]
        self.assertEqual(sorted(exp_rows), sorted(list(act_rows)))

        # truths-2016-2017-reichlab-small.csv
        project2 = Project.objects.create()
        TimeZero.objects.create(project=project2, timezero_date=datetime.date(2016, 10, 30))
        make_cdc_units_and_targets(project2)
        load_truth_data(project2, Path('forecast_app/tests/truth_data/truths-2016-2017-reichlab-small.csv'),
                        is_convert_na_none=True)
        exp_rows = [(datetime.date(2016, 10, 30), 'US National', '1 wk ahead', 1.55838),
                    (datetime.date(2016, 10, 30), 'US National', '2 wk ahead', 1.64639),
                    (datetime.date(2016, 10, 30), 'US National', '3 wk ahead', 1.91196),
                    (datetime.date(2016, 10, 30), 'US National', '4 wk ahead', 1.81129),
                    (datetime.date(2016, 10, 30), 'US National', 'Season onset', '2016-12-11'),
                    (datetime.date(2016, 10, 30), 'US National', 'Season peak percentage', 5.06094),
                    (datetime.date(2016, 10, 30), 'US National', 'Season peak week', '2017-02-05')]
        # note: https://code.djangoproject.com/ticket/32483 sqlite3 json query bug -> we manually access field instead
        # of using 'data__value'
        pred_data_qs = PredictionData.objects \
            .filter(pred_ele__forecast__forecast_model=oracle_model_for_project(project2)) \
            .values_list('pred_ele__forecast__time_zero__timezero_date', 'pred_ele__unit__name',
                         'pred_ele__target__name', 'data')
        act_rows = [(tz_date, unit__name, target__name, data['value'])
                    for tz_date, unit__name, target__name, data in pred_data_qs]
        self.assertEqual(sorted(exp_rows), sorted(list(act_rows)))


    def test_load_truth_data_partial_dup(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)  # loads batch: docs-ground-truth.csv

        try:
            load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-partial-dup.csv'),
                            file_name='docs-ground-truth-partial-dup.csv')
            batches = truth_batches(project)
            self.assertEqual(2, len(batches))
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")


    def test_truth_batches(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)  # loads batch: docs-ground-truth.csv

        # add a second batch
        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-non-dup.csv'),
                        file_name='docs-ground-truth-non-dup.csv')
        oracle_model = oracle_model_for_project(project)
        first_forecast = oracle_model.forecasts.first()
        last_forecast = oracle_model.forecasts.last()

        # test truth_batches() and truth_batch_forecasts() for each batch
        batches = truth_batches(project)
        self.assertEqual(2, len(batches))
        self.assertEqual(first_forecast.source, batches[0][0])
        self.assertEqual(first_forecast.issued_at, batches[0][1])
        self.assertEqual(last_forecast.source, batches[1][0])
        self.assertEqual(last_forecast.issued_at, batches[1][1])

        for source, issued_at in batches:
            forecasts = truth_batch_forecasts(project, source, issued_at)
            self.assertEqual(3, len(forecasts))
            for forecast in forecasts:
                self.assertEqual(source, forecast.source)
                self.assertEqual(issued_at, forecast.issued_at)

        # test truth_batch_summary_table(). NB: utctimetuple() makes sqlite comparisons work
        exp_table = [(source, issued_at.utctimetuple(), len(truth_batch_forecasts(project, source, issued_at)))
                     for source, issued_at in batches]
        act_table = [(source, issued_at.utctimetuple(), num_forecasts)
                     for source, issued_at, num_forecasts in truth_batch_summary_table(project)]
        self.assertEqual(exp_table, act_table)

        # finally, test deleting a batch. try deleting the first, which should fail due to version rules.
        # transaction.atomic() somehow avoids the second `truth_delete_batch()` call getting the error:
        # django.db.transaction.TransactionManagementError: An error occurred in the current transaction. You can't execute queries until the end of the 'atomic' block.
        with transaction.atomic():
            with self.assertRaisesRegex(RuntimeError, 'you cannot delete a forecast that has any newer versions'):
                truth_delete_batch(project, batches[0][0], batches[0][1])

        # delete second batch - should not fail
        truth_delete_batch(project, batches[1][0], batches[1][1])
        batches = truth_batches(project)
        self.assertEqual(1, len(batches))
        self.assertEqual(first_forecast.source, batches[0][0])
        self.assertEqual(first_forecast.issued_at, batches[0][1])


    def test_timezeros_unique(self):
        project = Project.objects.create()
        with self.assertRaises(ValidationError) as context:
            timezeros = [TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1)),
                         TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))]
            project.timezeros.add(*timezeros)
            project.save()
        self.assertIn("found duplicate TimeZero.timezero_date", str(context.exception))


    def test_num_pred_ele_rows_all_models(self):
        # 154 initial (11 * 7 * 2 = locations * targets * points/bins)
        self.assertEqual(11 * 7 * 2, self.project.num_pred_ele_rows_all_models(is_oracle=False))

        time_zero2 = TimeZero.objects.create(project=self.project, timezero_date=datetime.date(2017, 1, 2))

        # EW01 2017. 165 rows, 6 zero bins. same number of unique prediction elements, though
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')
        load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, time_zero2)
        self.assertEqual((11 * 7 * 2) * 2, self.project.num_pred_ele_rows_all_models(is_oracle=False))


    def test_summary_counts(self):
        # num_models, num_forecasts
        self.assertEqual((1, 1), self.project.num_models_forecasts())


    def test_timezero_seasons(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
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


    def test_timezeros_num_forecasts(self):
        self.assertEqual([(self.time_zero, 1)], ProjectDetailView.timezeros_num_forecasts(self.project))


    def test__upload_truth_worker_bad_inputs(self):
        # test `_upload_truth_worker()` error conditions. this test is complicated by that function's use of
        # the `job_cloud_file` context manager. solution is per https://stackoverflow.com/questions/60198229/python-patch-context-manager-to-return-object
        with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                patch('utils.project_truth.load_truth_data') as load_truth_mock:
            job = Job.objects.create()
            job.input_json = {}  # no 'project_pk'
            job.save()
            job_cloud_file_mock.return_value.__enter__.return_value = (job, None)  # 2-tuple: (job, cloud_file_fp)
            _upload_truth_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            job.refresh_from_db()
            load_truth_mock.assert_not_called()
            self.assertEqual(Job.FAILED, job.status)

            # test no 'filename'
            job.input_json = {'project_pk': None}  # no 'filename'
            job.save()
            _upload_truth_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            job.refresh_from_db()
            load_truth_mock.assert_not_called()
            self.assertEqual(Job.FAILED, job.status)

            # test bad 'project_pk'
            job.input_json = {'project_pk': -1, 'filename': None}
            job.save()
            _upload_truth_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            job.refresh_from_db()
            load_truth_mock.assert_not_called()
            self.assertEqual(Job.FAILED, job.status)


    def test__upload_truth_worker_blue_sky(self):
        with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                patch('utils.project_truth.load_truth_data') as load_truth_mock:
            job = Job.objects.create()
            job.input_json = {'project_pk': self.project.pk, 'filename': 'a name!'}
            job.save()
            job_cloud_file_mock.return_value.__enter__.return_value = (job, None)  # 2-tuple: (job, cloud_file_fp)
            _upload_truth_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            job.refresh_from_db()
            load_truth_mock.assert_called_once()
            self.assertEqual(Job.SUCCESS, job.status)


    def test_last_update(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)

        # one truth and one forecast (yes truth, yes forecasts)
        self.assertEqual(forecast.created_at, project.last_update())

        # add a second forecast for a newer timezero (yes truth, yes forecasts)
        time_zero2 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2011, 10, 3))
        forecast2 = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions-non-dup.json',
                                            time_zero=time_zero2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions-non-dup.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, is_validate_cats=False)
        self.assertEqual(forecast2.created_at, project.last_update())


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
                                          datetime.date(2017, 7, 30): ['2017-11-19']}},
        'HHS Region 10': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.364459],
                                         datetime.date(2017, 7, 30): [0.240377]},
                          '2 wk ahead': {datetime.date(2017, 7, 23): [0.240377],
                                         datetime.date(2017, 7, 30): [0.126923]},
                          '3 wk ahead': {datetime.date(2017, 7, 23): [0.126923],
                                         datetime.date(2017, 7, 30): [0.241729]},
                          '4 wk ahead': {datetime.date(2017, 7, 23): [0.241729],
                                         datetime.date(2017, 7, 30): [0.293072]},
                          'Season onset': {datetime.date(2017, 7, 23): ['2017-12-17'],
                                           datetime.date(2017, 7, 30): ['2017-12-17']}},
        'HHS Region 2': {'1 wk ahead': {datetime.date(2017, 7, 23): [1.32634],
                                        datetime.date(2017, 7, 30): [1.34713]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [1.34713],
                                        datetime.date(2017, 7, 30): [1.15738]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [1.15738],
                                        datetime.date(2017, 7, 30): [1.41483]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [1.41483],
                                        datetime.date(2017, 7, 30): [1.32425]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-03'],
                                          datetime.date(2017, 7, 30): ['2017-12-03']}},
        'HHS Region 3': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.797999],
                                        datetime.date(2017, 7, 30): [0.586092]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.586092],
                                        datetime.date(2017, 7, 30): [0.611163]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.611163],
                                        datetime.date(2017, 7, 30): [0.623141]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.623141],
                                        datetime.date(2017, 7, 30): [0.781271]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-17'],
                                          datetime.date(2017, 7, 30): ['2017-12-17']}},
        'HHS Region 4': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.476357],
                                        datetime.date(2017, 7, 30): [0.483647]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.483647],
                                        datetime.date(2017, 7, 30): [0.674289]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.674289],
                                        datetime.date(2017, 7, 30): [0.782429]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.782429],
                                        datetime.date(2017, 7, 30): [1.11294]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-11-05'],
                                          datetime.date(2017, 7, 30): ['2017-11-05']}},
        'HHS Region 5': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.602327],
                                        datetime.date(2017, 7, 30): [0.612967]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.612967],
                                        datetime.date(2017, 7, 30): [0.637141]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.637141],
                                        datetime.date(2017, 7, 30): [0.627954]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.627954],
                                        datetime.date(2017, 7, 30): [0.724628]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-03'],
                                          datetime.date(2017, 7, 30): ['2017-12-03']}},
        'HHS Region 6': {'1 wk ahead': {datetime.date(2017, 7, 23): [1.15229],
                                        datetime.date(2017, 7, 30): [0.96867]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.96867],
                                        datetime.date(2017, 7, 30): [1.02289]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [1.02289],
                                        datetime.date(2017, 7, 30): [1.66769]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [1.66769],
                                        datetime.date(2017, 7, 30): [1.74834]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-11-26'],
                                          datetime.date(2017, 7, 30): ['2017-11-26']}},
        'HHS Region 7': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.174172],
                                        datetime.date(2017, 7, 30): [0.115888]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.115888],
                                        datetime.date(2017, 7, 30): [0.112074]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.112074],
                                        datetime.date(2017, 7, 30): [0.233776]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.233776],
                                        datetime.date(2017, 7, 30): [0.142496]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-03'],
                                          datetime.date(2017, 7, 30): ['2017-12-03']}},
        'HHS Region 8': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.33984],
                                        datetime.date(2017, 7, 30): [0.359646]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.359646],
                                        datetime.date(2017, 7, 30): [0.326402]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [0.326402],
                                        datetime.date(2017, 7, 30): [0.419146]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [0.419146],
                                        datetime.date(2017, 7, 30): [0.714684]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-10'],
                                          datetime.date(2017, 7, 30): ['2017-12-10']}},
        'HHS Region 9': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.892872],
                                        datetime.date(2017, 7, 30): [0.912778]},
                         '2 wk ahead': {datetime.date(2017, 7, 23): [0.912778],
                                        datetime.date(2017, 7, 30): [1.012]},
                         '3 wk ahead': {datetime.date(2017, 7, 23): [1.012],
                                        datetime.date(2017, 7, 30): [1.26206]},
                         '4 wk ahead': {datetime.date(2017, 7, 23): [1.26206],
                                        datetime.date(2017, 7, 30): [1.28077]},
                         'Season onset': {datetime.date(2017, 7, 23): ['2017-12-03'],
                                          datetime.date(2017, 7, 30): ['2017-12-03']}},
        'US National': {'1 wk ahead': {datetime.date(2017, 7, 23): [0.73102],
                                       datetime.date(2017, 7, 30): [0.688338]},
                        '2 wk ahead': {datetime.date(2017, 7, 23): [0.688338],
                                       datetime.date(2017, 7, 30): [0.732049]},
                        '3 wk ahead': {datetime.date(2017, 7, 23): [0.732049],
                                       datetime.date(2017, 7, 30): [0.911641]},
                        '4 wk ahead': {datetime.date(2017, 7, 23): [0.911641],
                                       datetime.date(2017, 7, 30): [1.02105]},
                        'Season onset': {datetime.date(2017, 7, 23): ['2017-11-19'],
                                         datetime.date(2017, 7, 30): ['2017-11-19']}}
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
