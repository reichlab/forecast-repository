import csv
import datetime
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from django.test import TestCase
from rest_framework.test import APIRequestFactory
from rq.timeouts import JobTimeoutException

from forecast_app.models import Project, TimeZero, Job, PredictionElement
from forecast_app.models.forecast import Forecast
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.views import _upload_forecast_worker
from utils.cdc_io import load_cdc_csv_forecast_file, make_cdc_units_and_targets
from utils.forecast import json_io_dict_from_forecast, load_predictions_from_json_io_dict
from utils.make_minimal_projects import _make_docs_project
from utils.make_thai_moph_project import load_cdc_csv_forecasts_from_dir
from utils.project import create_project_from_json
from utils.utilities import get_or_create_super_po_mo_users


class ForecastTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()
        make_cdc_units_and_targets(cls.project)
        cls.forecast_model = ForecastModel.objects.create(project=cls.project, name='name', abbreviation='abbrev')
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date=datetime.date(2017, 1, 1))
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
        cls.forecast = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, cls.time_zero)
        cls.forecast.issued_at -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        cls.forecast.save()


    def test_load_forecast_created_at_field(self):
        project2 = Project.objects.create()
        make_cdc_units_and_targets(project2)
        time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date.today())
        forecast_model2 = ForecastModel.objects.create(project=project2, name='name', abbreviation='abbrev')
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        forecast2 = load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, time_zero2)
        self.assertIsNotNone(forecast2.created_at)


    def test_load_forecast(self):
        self.assertEqual(1, len(self.forecast_model.forecasts.all()))
        self.assertIsInstance(self.forecast, Forecast)
        self.assertEqual('EW1-KoTstable-2017-01-17.csv', self.forecast.source)
        self.assertEqual(11 * 7 * 2, self.forecast.pred_eles.count())  # locations * targets * points/bins

        # check 'US National' targets: spot-check a few point rows
        act_points_qs = self.forecast.pred_eles.filter(unit__name='nat', pred_class=PredictionElement.POINT_CLASS)
        self.assertEqual(7, act_points_qs.count())

        # test empty file
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bad_file_no_header-2017-01-17.csv')  # EW01 2017?
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn('empty file', str(context.exception))

        # test a bad data file header
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bad_file_header-2017-01-17.csv')  # EW01 2017?
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn('invalid header', str(context.exception))

        # test load_forecast() with timezero not in the project
        project2 = Project.objects.create()  # no TimeZeros
        make_cdc_units_and_targets(project2)

        forecast_model2 = ForecastModel.objects.create(project=project2, name='name', abbreviation='abbrev')
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, self.time_zero)
        self.assertIn("time_zero was not in project", str(context.exception))


    def test_load_forecasts_from_dir(self):
        project2 = Project.objects.create()
        make_cdc_units_and_targets(project2)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 23),  # 20161023-KoTstable-20161109.cdc.csv
                                data_version_date=None)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 30),  # 20161030-KoTstable-20161114.cdc.csv
                                data_version_date=None)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 11, 6),  # 20161106-KoTstable-20161121.cdc.csv
                                data_version_date=None)
        forecast_model2 = ForecastModel.objects.create(project=project2, name='name', abbreviation='abbrev')

        # copy the two files from 'forecast_app/tests/load_forecasts' to a temp dir, run the loader, and then copy a
        # third file over to test that it skips already-loaded ones
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            test_file_dir = Path('forecast_app/tests/load_forecasts')
            shutil.copy(str(test_file_dir / '20161023-KoTstable-20161109.cdc.csv'), str(temp_dir))
            shutil.copy(str(test_file_dir / '20161030-KoTstable-20161114.cdc.csv'), str(temp_dir))

            forecasts = load_cdc_csv_forecasts_from_dir(forecast_model2, temp_dir, 2016)
            self.assertEqual(2, len(forecasts))
            self.assertEqual(2, len(forecast_model2.forecasts.all()))

            # copy third file and test only new loaded
            shutil.copy(str(test_file_dir / 'third-file/20161106-KoTstable-20161121.cdc.csv'), str(temp_dir))
            forecasts = load_cdc_csv_forecasts_from_dir(forecast_model2, temp_dir, 2016)
            self.assertEqual(1, len(forecasts))


    def test_cdc_forecast_data_validation(self):
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bad-point-na-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn("None point values are only valid for 'Season onset' targets", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bin-doesnt-sum-to-one-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn("Entries in the database rows in the `prob` column must be numbers in [0, 1]",
                      str(context.exception))

        try:
            # date-based Point row w/NA value is OK:
            csv_file_path = Path('forecast_app/tests/EW1-ok-point-na-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")


    @unittest.skip("todo")
    def test_forecast_data_validation_additional(self):
        # test points lie within the range of point values in the template. see Nick's comment
        # ( https://github.com/reichlab/forecast-repository/issues/18#issuecomment-335654340 ):
        # The thought was that for each target we could look at all of the values in the point rows for that target and
        # would end up with a vector of numbers. The minimum and maximum of those numbers would define the acceptable
        # range for the point values in the files themselves. E.g., if the predictions for target K should never be
        # negative, then whoever made the template file would explicitly place a zero in at least one of the target K
        # point rows. And none of those rows would have negative values. Is this too "cute" of a way to set the max/min
        # ranges for testing? Alternatively, we could hard-code them as part of the project.
        self.fail()  # todo

        # see @josh's comment ( https://reichlab.slack.com/archives/C57HNDFN0/p1507744847000350 ):
        # how important is the left handedness of the bins? by that i mean that bin_start_incl and bin_end_notincl
        # versus bin_start_notincl and bin_end_incl
        self.fail()  # todo


    def test_delete_forecast(self):
        # add a second forecast version, check its rows were added, delete it, and test that the data was deleted (via
        # CASCADE)
        self.assertEqual(1, self.forecast_model.forecasts.count())  # from setUpTestData()
        self.assertEqual(11 * 7 * 2, self.forecast.pred_eles.count())  # locations * targets * points/bins

        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')  # EW01 2017
        forecast2 = load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertEqual(2, self.forecast_model.forecasts.count())  # now two
        self.assertEqual(11 * 7 * 2, self.forecast.pred_eles.count())

        # turns out there are the 17 duplicates that are not loaded. MUS: from an sqlite3 run:
        #   f  cls  u    t   is_r   hash
        # [(2,  2,   1,  1,   0,    'af49e5f6850e59638eb322a2293bd6c4'),  # {'value': '2016-12-26'}  'HHS Region 1', 'Season onset'
        #  (2,  2,   1,  2,   0,    'edb5fbaf20fbba5d1d1455d864f2bc95'),  # {'value': '2017-02-06'}  'HHS Region 1', 'Season peak week'
        #  (2,  2,  10,  1,   0,    '73c3608a9829c5fe4103d1fd2b26c369'),  # {'value': '2016-12-12'}  'HHS Region 10', 'Season onset'
        #  (2,  2,  10,  2,   0,    '5959bec40e3eb79763a6cd78e150e145'),  # {'value': '2017-01-09'}  'HHS Region 10', 'Season peak week'
        #  (2,  2,   2,  1,   0,    'f0cc965a8f901a09412f4a8e87c5aa3e'),  # {'value': '2016-11-21'}
        #  (2,  2,   3,  1,   0,    '0f6791070c029b74186f03f98e487b59'),  # {'value': '2016-12-19'}
        #  (2,  2,   4,  1,   0,    '3b2bdabc810ce299f0e35a55b107c374'),  # {'value': '2016-11-14'}
        #  (2,  2,   4,  2,   0,    '177183a5a9c19cedca454389aa796aee'),  # {'value': '2017-02-13'}
        #  (2,  2,   5,  1,   0,    'af49e5f6850e59638eb322a2293bd6c4'),  # {'value': '2016-12-26'}
        #  (2,  2,   6,  1,   0,    'af49e5f6850e59638eb322a2293bd6c4'),  # {'value': '2016-12-26'}  ...
        #  (2,  2,   6,  2,   0,    '177183a5a9c19cedca454389aa796aee'),  # {'value': '2017-02-13'}
        #  (2,  2,   7,  1,   0,    'af49e5f6850e59638eb322a2293bd6c4'),  # {'value': '2016-12-26'}
        #  (2,  2,   8,  1,   0,    '0f6791070c029b74186f03f98e487b59'),  # {'value': '2016-12-19'}
        #  (2,  2,   9,  1,   0,    '0f6791070c029b74186f03f98e487b59'),  # {'value': '2016-12-19'}
        #  (2,  2,   9,  2,   0,    'edb5fbaf20fbba5d1d1455d864f2bc95'),  # {'value': '2017-02-06'}
        #  (2,  2,  11,  1,   0,    '73c3608a9829c5fe4103d1fd2b26c369'),  # {'value': '2016-12-12'}  'US National', 'Season onset'
        #  (2,  2,  11,  2,   0,    '86bf4bd34d86b349754bcc7412857faa')   # {'value': '2017-01-30'}  'US National', 'Season peak week'
        # ]
        self.assertEqual((11 * 7 * 2 - 17), forecast2.pred_eles.count())  # 154 - 17 duplicates = 137

        forecast2.delete()
        self.assertEqual(1, self.forecast_model.forecasts.count())  # back to one


    def test_forecast_for_time_zero(self):
        time_zero = TimeZero.objects.create(project=self.project,
                                            timezero_date=datetime.date.today(),
                                            data_version_date=None)
        self.assertEqual(None, self.forecast_model.forecast_for_time_zero(time_zero))

        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')  # EW01 2017
        forecast2 = load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, time_zero)
        self.assertEqual(forecast2, self.forecast_model.forecast_for_time_zero(time_zero))

        forecast2.delete()


    def test_json_io_dict_from_forecast(self):
        # tests that the json_io_dict_from_forecast()'s output order for SampleDistributions is preserved
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict_in, is_validate_cats=False)
            json_io_dict_out = json_io_dict_from_forecast(forecast, APIRequestFactory().request())

        # test round trip. ignore meta, but spot-check it first
        out_meta = json_io_dict_out['meta']
        self.assertEqual({'targets', 'forecast', 'units'}, set(out_meta.keys()))
        self.assertEqual({'cats', 'outcome_variable', 'name', 'is_step_ahead', 'type', 'description', 'id', 'url'},
                         set(out_meta['targets'][0].keys()))
        self.assertEqual({'time_zero', 'forecast_model', 'created_at', 'issued_at', 'notes', 'forecast_data', 'source',
                          'id', 'url'},
                         set(out_meta['forecast'].keys()))
        self.assertEqual({'id', 'url', 'name', 'abbreviation'}, set(out_meta['units'][0].keys()))
        self.assertIsInstance(out_meta['forecast']['time_zero'], dict)  # test that time_zero is expanded, not URL

        del (json_io_dict_in['meta'])
        del (json_io_dict_out['meta'])

        json_io_dict_in['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
        json_io_dict_out['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))

        self.assertEqual(json_io_dict_in, json_io_dict_out)

        # spot-check some sample predictions
        sample_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                            if (pred_dict['unit'] == 'loc3')
                            and (pred_dict['target'] == 'pct next week')
                            and (pred_dict['class'] == 'sample')][0]
        self.assertEqual([2.3, 6.5, 0.0, 10.0234, 0.0001], sample_pred_dict['prediction']['sample'])

        sample_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                            if (pred_dict['unit'] == 'loc2')
                            and (pred_dict['target'] == 'season severity')
                            and (pred_dict['class'] == 'sample')][0]
        self.assertEqual(['moderate', 'severe', 'high', 'moderate', 'mild'], sample_pred_dict['prediction']['sample'])

        sample_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                            if (pred_dict['unit'] == 'loc1')
                            and (pred_dict['target'] == 'Season peak week')
                            and (pred_dict['class'] == 'sample')][0]
        self.assertEqual(['2020-01-05', '2019-12-15'], sample_pred_dict['prediction']['sample'])

        # spot-check some quantile predictions
        quantile_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                              if (pred_dict['unit'] == 'loc2')
                              and (pred_dict['target'] == 'pct next week')
                              and (pred_dict['class'] == 'quantile')][0]
        self.assertEqual([0.025, 0.25, 0.5, 0.75, 0.975], quantile_pred_dict['prediction']['quantile'])
        self.assertEqual([1.0, 2.2, 2.2, 5.0, 50.0], quantile_pred_dict['prediction']['value'])

        quantile_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                              if (pred_dict['unit'] == 'loc2')
                              and (pred_dict['target'] == 'Season peak week')
                              and (pred_dict['class'] == 'quantile')][0]
        self.assertEqual([0.5, 0.75, 0.975], quantile_pred_dict['prediction']['quantile'])
        self.assertEqual(["2019-12-22", "2019-12-29", "2020-01-05"], quantile_pred_dict['prediction']['value'])


    def test__upload_forecast_worker_bad_inputs(self):
        # test `_upload_forecast_worker()` error conditions. this test is complicated by that function's use of
        # the `job_cloud_file` context manager. solution is per https://stackoverflow.com/questions/60198229/python-patch-context-manager-to-return-object
        with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                patch('utils.forecast.load_predictions_from_json_io_dict') as load_preds_mock, \
                patch('utils.forecast.cache_forecast_metadata') as cache_metatdata_mock:
            job = Job.objects.create()
            job.input_json = {}  # no 'forecast_pk'
            job.save()
            job_cloud_file_mock.return_value.__enter__.return_value = (job, None)  # 2-tuple: (job, cloud_file_fp)
            _upload_forecast_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            load_preds_mock.assert_not_called()

            # test no 'filename'
            job.input_json = {'forecast_pk': None}  # no 'filename'
            job.save()
            _upload_forecast_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            job.refresh_from_db()
            load_preds_mock.assert_not_called()
            self.assertEqual(Job.FAILED, job.status)

            # test bad 'forecast_pk'
            job.input_json = {'forecast_pk': -1, 'filename': None}
            job.save()
            _upload_forecast_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            job.refresh_from_db()
            load_preds_mock.assert_not_called()
            self.assertEqual(Job.FAILED, job.status)


    def test__upload_forecast_worker_deletes_forecast(self):
        # verifies that _upload_forecast_worker() deletes the (presumably empty) Forecast that's passed to it by
        # upload functions if the file is invalid. here we mock load_predictions_from_json_io_dict() to throw the two
        # exceptions that cause deletes: JobTimeoutException and Exception
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        forecast.issued_at -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        forecast.save()

        for exception, exp_job_status in [(Exception('load_preds_mock Exception'), Job.FAILED),
                                          (JobTimeoutException('load_preds_mock JobTimeoutException'), Job.TIMEOUT)]:
            with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                    patch('utils.forecast.load_predictions_from_json_io_dict') as load_preds_mock, \
                    patch('utils.forecast.cache_forecast_metadata') as cache_metatdata_mock, \
                    open('forecast_app/tests/predictions/docs-predictions.json') as cloud_file_fp:
                load_preds_mock.side_effect = exception
                forecast2 = Forecast.objects.create(forecast_model=forecast_model, time_zero=time_zero)
                job = Job.objects.create()
                job.input_json = {'forecast_pk': forecast2.pk, 'filename': 'a name!', 'format': 'json'}
                job.save()

                job_cloud_file_mock.return_value.__enter__.return_value = (job, cloud_file_fp)
                try:
                    _upload_forecast_worker(job.pk)
                except JobTimeoutException as jte:
                    pass  # expected re-raise of this exception
                job.refresh_from_db()
                self.assertEqual(exp_job_status, job.status)
                self.assertIsNone(Forecast.objects.filter(id=forecast2.id).first())  # deleted


    def test__upload_forecast_worker_atomic(self):
        # test `_upload_forecast_worker()` does not create a Forecast if subsequent calls to
        # `load_predictions_from_json_io_dict()` or `cache_forecast_metadata()` fail. this test is complicated by that
        # function's use of the `job_cloud_file` context manager. solution is per https://stackoverflow.com/questions/60198229/python-patch-context-manager-to-return-object
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        forecast.issued_at -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        forecast.save()

        with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                patch('utils.forecast.load_predictions_from_json_io_dict') as load_preds_mock, \
                patch('utils.forecast.cache_forecast_metadata') as cache_metatdata_mock:
            forecast2 = Forecast.objects.create(forecast_model=forecast_model, time_zero=time_zero)
            job = Job.objects.create()
            job.input_json = {'forecast_pk': forecast2.pk, 'filename': 'a name!', 'format': 'csv'}  # arbitrary format
            job.save()

            job_cloud_file_mock.return_value.__enter__.return_value = (job, None)  # 2-tuple: (job, cloud_file_fp)

            # test that no Forecast is created when load_predictions_from_json_io_dict() fails
            load_preds_mock.side_effect = Exception('load_preds_mock Exception')
            num_forecasts_before = forecast_model.forecasts.count()
            _upload_forecast_worker(job.pk)
            job.refresh_from_db()
            self.assertEqual(num_forecasts_before - 1, forecast_model.forecasts.count())  # -1 b/c forecast2 deleted
            self.assertEqual(Job.FAILED, job.status)

            # test when cache_forecast_metadata() fails
            load_preds_mock.reset_mock(side_effect=True)
            cache_metatdata_mock.side_effect = Exception('cache_metatdata_mock Exception')
            num_forecasts_before = forecast_model.forecasts.count()
            _upload_forecast_worker(job.pk)
            job.refresh_from_db()
            self.assertEqual(num_forecasts_before, forecast_model.forecasts.count())
            self.assertEqual(Job.FAILED, job.status)


    def test__upload_forecast_worker_blue_sky(self):
        # blue sky to verify load_predictions_from_json_io_dict() and cache_forecast_metadata() are called. also tests
        # that _upload_forecast_worker() correctly sets job.output_json. this test is complicated by that function's use
        # of the `job_cloud_file` context manager. solution is per https://stackoverflow.com/questions/60198229/python-patch-context-manager-to-return-object
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        forecast.issued_at -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        forecast.save()

        with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                patch('utils.forecast.load_predictions_from_json_io_dict') as load_preds_mock, \
                patch('utils.forecast.cache_forecast_metadata') as cache_metatdata_mock, \
                open('forecast_app/tests/predictions/docs-predictions.json') as cloud_file_fp:
            job = Job.objects.create()
            job.input_json = {'forecast_pk': forecast.pk, 'filename': 'a name!', 'format': 'json'}
            job.save()
            job_cloud_file_mock.return_value.__enter__.return_value = (job, cloud_file_fp)
            _upload_forecast_worker(job.pk)
            job.refresh_from_db()
            load_preds_mock.assert_called_once()
            cache_metatdata_mock.assert_called_once()
            self.assertEqual(Job.SUCCESS, job.status)
            self.assertEqual(job.input_json['forecast_pk'], job.output_json['forecast_pk'])


    def test__upload_forecast_worker_csv_file_format(self):
        # tests that `_upload_forecast_worker()` calls `json_io_dict_from_csv_rows()` when 'format' == 'csv', and that
        # it passes the output to `load_predictions_from_json_io_dict()`
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        forecast.issued_at -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        forecast.save()

        with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                patch('utils.forecast.load_predictions_from_json_io_dict') as load_preds_mock, \
                patch('utils.forecast.cache_forecast_metadata') as cache_metatdata_mock, \
                patch('utils.csv_io.json_io_dict_from_csv_rows') as dict_from_csv_mock, \
                open('forecast_app/tests/predictions/docs-predictions.csv') as cloud_file_fp:
            job = Job.objects.create()
            job.input_json = {'forecast_pk': forecast.pk, 'filename': 'a name!', 'format': 'csv'}
            job.save()
            job_cloud_file_mock.return_value.__enter__.return_value = (job, cloud_file_fp)
            _upload_forecast_worker(job.pk)
            job.refresh_from_db()

            # check dict_from_csv_mock
            cloud_file_fp.seek(0)
            csv_rows = list(csv.reader(cloud_file_fp))
            dict_from_csv_mock.assert_called_once_with(csv_rows)

            load_preds_mock.assert_called_once()
            cache_metatdata_mock.assert_called_once()
            self.assertEqual(Job.SUCCESS, job.status)
            self.assertEqual(job.input_json['forecast_pk'], job.output_json['forecast_pk'])
