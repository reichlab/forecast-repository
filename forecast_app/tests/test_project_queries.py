import datetime
import datetime
import json
import logging
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import BotoCoreError
from django.test import TestCase

from forecast_app.models import TimeZero, BinDistribution, NamedDistribution, \
    PointPrediction, SampleDistribution, QuantileDistribution, Forecast, Job, Score, Unit, Target, Project
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.scores.definitions import SCORE_ABBREV_TO_NAME_AND_DESCR
from forecast_app.tests.test_scores import _update_scores_for_all_projects
from utils.cdc_io import make_cdc_units_and_targets, load_cdc_csv_forecast_file
from utils.forecast import PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS, load_predictions_from_json_io_dict
from utils.make_minimal_projects import _make_docs_project
from utils.project import load_truth_data
from utils.project_queries import CSV_HEADER, query_forecasts_for_project, _forecasts_query_worker, \
    validate_scores_query, _scores_query_worker, _tz_unit_targ_pks_to_truth_values, query_scores_for_project, \
    SCORE_CSV_HEADER_PREFIX
from utils.project_queries import validate_forecasts_query
from utils.utilities import get_or_create_super_po_mo_users, YYYY_MM_DD_DATE_FORMAT


logging.getLogger().setLevel(logging.ERROR)


class ProjectQueriesTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        _, _, cls.po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        cls.project, cls.time_zero, cls.forecast_model, cls.forecast = _make_docs_project(cls.po_user)


    #
    # test forecast queries
    #

    def test_validate_forecasts_query(self):
        # case: query not a dict
        error_messages, _ = validate_forecasts_query(self.project, -1)
        self.assertEqual(1, len(error_messages))
        self.assertIn("query was not a dict", error_messages[0])

        # case: query contains invalid keys
        error_messages, _ = validate_forecasts_query(self.project, {'foo': -1})
        self.assertEqual(1, len(error_messages))
        self.assertIn("one or more query keys were invalid", error_messages[0])

        # case: query keys are not correct type (lists)
        for key_name in ['models', 'units', 'targets', 'timezeros']:
            error_messages, _ = validate_forecasts_query(self.project, {key_name: -1})
            self.assertEqual(1, len(error_messages))
            self.assertIn(f"'{key_name}' was not a list", error_messages[0])

        # case: bad object id
        for key_name in ['models', 'units', 'targets', 'timezeros']:
            error_messages, _ = validate_forecasts_query(self.project, {key_name: [-1]})
            self.assertEqual(1, len(error_messages))
            self.assertIn("contained ID(s) of objects that don't exist", error_messages[0])

        # case: bad type
        error_messages, _ = validate_forecasts_query(self.project, {'types': ['bad type']})
        self.assertEqual(1, len(error_messages))
        self.assertIn("one or more types were invalid prediction types", error_messages[0])

        # case: ids from other project (!)
        project2, time_zero2, forecast_model2, forecast2 = _make_docs_project(self.po_user)
        for query_dict in [{'models': list(project2.models.all().values_list('id', flat=True))},
                           {'units': list(project2.units.all().values_list('id', flat=True))},
                           {'targets': list(project2.targets.all().values_list('id', flat=True))},
                           {'timezeros': list(project2.timezeros.all().values_list('id', flat=True))}]:
            query_key = list(query_dict.keys())[0]
            error_messages, _ = validate_forecasts_query(self.project, query_dict)
            self.assertEqual(1, len(error_messages))
            self.assertIn(f"`{query_key}` contained ID(s) of objects that don't exist in project", error_messages[0])

        # case: blue sky
        query = {'models': list(self.project.models.all().values_list('id', flat=True)),
                 'units': list(self.project.units.all().values_list('id', flat=True)),
                 'targets': list(self.project.targets.all().values_list('id', flat=True)),
                 'timezeros': list(self.project.timezeros.all().values_list('id', flat=True)),
                 'types': list(PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS.values())}
        error_messages, _ = validate_forecasts_query(self.project, query)
        self.assertEqual(0, len(error_messages))


    def test_query_forecasts_for_project(self):
        model = self.forecast_model.abbreviation
        tz = self.time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        timezero_to_season_name = self.project.timezero_to_season_name()
        seas = timezero_to_season_name[self.time_zero]

        # ---- case: all BinDistributions in project. check cat and prob columns ----
        rows = query_forecasts_for_project(self.project,
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
        rows = query_forecasts_for_project(self.project,
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
        rows = query_forecasts_for_project(self.project,
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
        rows = query_forecasts_for_project(self.project,
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
        rows = query_forecasts_for_project(self.project,
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
        rows = query_forecasts_for_project(self.project, {})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin),
                         len(rows))

        # ---- case: only one unit ----
        rows = query_forecasts_for_project(self.project,
                                           {'units': [self.project.units.filter(name='location3').first().pk]})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(17, len(rows))

        # ---- case: only one target ----
        rows = query_forecasts_for_project(self.project,
                                           {'targets': [self.project.targets.filter(name='above baseline').first().pk]})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(9, len(rows))

        # following two tests require a second model, timezero, and forecast
        forecast_model2 = ForecastModel.objects.create(project=self.project, name=model, abbreviation='abbrev')
        time_zero2 = TimeZero.objects.create(project=self.project, timezero_date=datetime.date(2011, 10, 22))
        forecast2 = Forecast.objects.create(forecast_model=forecast_model2, source='docs-predictions.json',
                                            time_zero=time_zero2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, False)

        # ---- case: empty query -> all forecasts in project. s/be twice as many now ----
        rows = query_forecasts_for_project(self.project, {})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin) * 2,
                         len(rows))

        # ---- case: only one timezero ----
        rows = query_forecasts_for_project(self.project, {'timezeros': [time_zero2.pk]})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin),
                         len(rows))

        # ---- case: only one model ----
        rows = query_forecasts_for_project(self.project, {'models': [forecast_model2.pk]})
        self.assertEqual(CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin),
                         len(rows))


    def test_query_forecasts_for_project_max_num_rows(self):
        try:
            query_forecasts_for_project(self.project, {}, max_num_rows=62)  # actual number of rows = 62
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        with self.assertRaises(RuntimeError) as context:
            query_forecasts_for_project(self.project, {}, max_num_rows=61)
        self.assertIn("number of rows exceeded maximum", str(context.exception))


    def test__forecasts_query_worker(self):
        # tests the worker directly. above test verifies that it's called from `query_forecasts_endpoint()`

        # ensure query_forecasts_for_project() is called
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.project.pk, 'query': {}})
        with patch('utils.project_queries.query_forecasts_for_project') as query_mock, \
                patch('utils.cloud_file.upload_file'):
            _forecasts_query_worker(job.pk)
            query_mock.assert_called_once_with(self.project, {})

        # case: upload_file() does not error
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.project.pk, 'query': {}})
        with patch('utils.cloud_file.upload_file') as upload_mock:
            _forecasts_query_worker(job.pk)
            upload_mock.assert_called_once()

            job.refresh_from_db()
            self.assertEqual(Job.SUCCESS, job.status)

        # case: upload_file() errors. BotoCoreError: alt: Boto3Error, ClientError, ConnectionClosedError:
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.project.pk, 'query': {}})
        with patch('utils.cloud_file.upload_file', side_effect=BotoCoreError()) as upload_mock, \
                patch('forecast_app.notifications.send_notification_email'):
            _forecasts_query_worker(job.pk)
            upload_mock.assert_called_once()

            job.refresh_from_db()
            self.assertEqual(Job.FAILED, job.status)
            self.assertIn("_query_worker(): AWS error", job.failure_message)

        # case: allow actual utils.cloud_file.upload_file(), which calls Bucket.put_object(). we don't actually do this
        # in this test b/c we don't want to hit S3, but it's commented here for debugging:
        # _forecasts_query_worker(job.pk)
        # job.refresh_from_db()
        # self.assertEqual(Job.SUCCESS, job.status)


    #
    # test score queries
    #

    def test_validate_scores_query(self):
        """
        Nearly identical to test_validate_forecasts_query().
        """
        # case: query not a dict
        error_messages, _ = validate_scores_query(self.project, -1)
        self.assertEqual(1, len(error_messages))
        self.assertIn("query was not a dict", error_messages[0])

        # case: query contains invalid keys
        error_messages, _ = validate_scores_query(self.project, {'foo': -1})
        self.assertEqual(1, len(error_messages))
        self.assertIn("one or more query keys were invalid", error_messages[0])

        # case: query keys are not correct type (lists)
        for key_name in ['models', 'units', 'targets', 'timezeros']:
            error_messages, _ = validate_scores_query(self.project, {key_name: -1})
            self.assertEqual(1, len(error_messages))
            self.assertIn(f"'{key_name}' was not a list", error_messages[0])

        # case: bad object id
        for key_name in ['models', 'units', 'targets', 'timezeros']:
            error_messages, _ = validate_scores_query(self.project, {key_name: [-1]})
            self.assertEqual(1, len(error_messages))
            self.assertIn("contained ID(s) of objects that don't exist", error_messages[0])

        # case: bad score
        error_messages, _ = validate_scores_query(self.project, {'scores': ['bad score']})
        self.assertEqual(1, len(error_messages))
        self.assertIn("one or more scores were invalid abbreviations", error_messages[0])

        # case: ids from other project (!)
        project2, time_zero2, forecast_model2, forecast2 = _make_docs_project(self.po_user)
        for query_dict in [{'models': list(project2.models.all().values_list('id', flat=True))},
                           {'units': list(project2.units.all().values_list('id', flat=True))},
                           {'targets': list(project2.targets.all().values_list('id', flat=True))},
                           {'timezeros': list(project2.timezeros.all().values_list('id', flat=True))}]:
            query_key = list(query_dict.keys())[0]
            error_messages, _ = validate_scores_query(self.project, query_dict)
            self.assertEqual(1, len(error_messages))
            self.assertIn(f"`{query_key}` contained ID(s) of objects that don't exist in project", error_messages[0])

        # case: blue sky
        query = {'models': list(self.project.models.all().values_list('id', flat=True)),
                 'units': list(self.project.units.all().values_list('id', flat=True)),
                 'targets': list(self.project.targets.all().values_list('id', flat=True)),
                 'timezeros': list(self.project.timezeros.all().values_list('id', flat=True)),
                 'scores': list(SCORE_ABBREV_TO_NAME_AND_DESCR.keys())}
        error_messages, _ = validate_scores_query(self.project, query)
        self.assertEqual(0, len(error_messages))


    def _assert_list_of_lists_almost_equal(self, exp_rows, act_rows):
        """
        Utility that iterates over the two lists' elements, calling assertAlmostEqual on each
        """
        for exp_row, act_row in zip(exp_rows, act_rows):
            self.assertEqual(len(exp_row), len(act_row))
            for exp_row_val, act_row_val in zip(exp_row, act_row):
                self.assertAlmostEqual(exp_row_val, act_row_val)  # handles non-floats via '==''


    def test_query_scores_for_project(self):
        # add some more predictions to get more to work with
        time_zero_2 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
        forecast_2 = Forecast.objects.create(forecast_model=self.forecast_model, source='docs-predictions.json 2',
                                             time_zero=time_zero_2, notes="f2")
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast_2, json_io_dict_in, False)

        forecast_model_2 = ForecastModel.objects.create(project=self.project, name='docs forecast model 2',
                                                        abbreviation='docs_mod_2')
        time_zero_3 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 16)).first()
        forecast_3 = Forecast.objects.create(forecast_model=forecast_model_2, source='docs-predictions.json 3',
                                             time_zero=time_zero_3, notes="f3")
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast_3, json_io_dict_in, False)

        Score.ensure_all_scores_exist()
        _update_scores_for_all_projects()

        # ---- case: empty query -> all scores in project ----
        # note: following floating point values are as returned by postgres. sqlite3 rounds differently, so we use
        # assertAlmostEqual() to compare. columns: model, timezero, season, unit, target, truth, error, abs_error,
        # log_single_bin, log_multi_bin, pit, interval_2, interval_5, interval_10, interval_20, interval_30,
        # interval_40, interval_50, interval_60, interval_70, interval_80, interval_90, interval_100:
        exp_rows = [
            SCORE_CSV_HEADER_PREFIX + [score.abbreviation for score in Score.objects.all()],
            ['docs_mod', '2011-10-02', '2011-2012', 'location1', 'pct next week', 4.5432,
             2.4432, 2.4432, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
            ['docs_mod', '2011-10-09', '2011-2012', 'location2', 'pct next week', 99.9,
             97.9, 97.9, -999.0, -0.356674943938732, 1.0, None, 2045.0, None, None, None, None, 382.4, None, None, None,
             None, 195.4],
            ['docs_mod', '2011-10-09', '2011-2012', 'location2', 'cases next week', 3,
             -2.0, 2.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
            ['docs_mod_2', '2011-10-16', '2011-2012', 'location1', 'pct next week', 0.0,
             -2.1, 2.1, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]
        ]
        act_rows = list(query_scores_for_project(self.project, {}))  # list for generator
        self._assert_list_of_lists_almost_equal(exp_rows, act_rows)

        # ---- case: only one model ----
        act_rows = list(query_scores_for_project(self.project, {'models': [forecast_model_2.id]}))
        self._assert_list_of_lists_almost_equal([exp_rows[0], exp_rows[4]], act_rows)

        # ---- case: only one unit ----
        unit = self.project.units.filter(name='location1').first()
        act_rows = list(query_scores_for_project(self.project, {'units': [unit.id]}))
        self._assert_list_of_lists_almost_equal([exp_rows[0], exp_rows[1], exp_rows[4]], act_rows)

        # ---- case: only one target ----
        target = self.project.targets.filter(name='cases next week').first()
        act_rows = list(query_scores_for_project(self.project, {'targets': [target.id]}))
        self._assert_list_of_lists_almost_equal([exp_rows[0], exp_rows[3]], act_rows)

        # ---- case: only one timezero ----
        time_zero = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 2)).first()
        act_rows = list(query_scores_for_project(self.project, {'timezeros': [time_zero.id]}))
        self._assert_list_of_lists_almost_equal([exp_rows[0], exp_rows[1]], act_rows)

        # ---- case: only one score: some score values exist. 10 = pit ----
        exp_rows_pit = [[row[0], row[1], row[2], row[3], row[4], row[5], row[10]]
                        for row in [exp_rows[0]] + [exp_rows[2]]]
        act_rows = list(query_scores_for_project(self.project, {'scores': ['pit']}))  # hard-coded abbrev
        self._assert_list_of_lists_almost_equal(exp_rows_pit, act_rows)

        # ---- case: only one score: no score values exist. 11 = interval_2 ----
        exp_rows_interval_2 = [[row[0], row[1], row[2], row[3], row[4], row[5], row[11]]
                               for row in [exp_rows[0]]]  # just header
        act_rows = list(query_scores_for_project(self.project, {'scores': ['interval_2']}))  # hard-coded abbrev
        self._assert_list_of_lists_almost_equal(exp_rows_interval_2, act_rows)


    def test_query_scores_for_project_max_num_rows(self):
        # add some more predictions to get more to work with
        time_zero_2 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
        forecast_2 = Forecast.objects.create(forecast_model=self.forecast_model, source='docs-predictions.json 2',
                                             time_zero=time_zero_2, notes="f2")
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast_2, json_io_dict_in, False)

        forecast_model_2 = ForecastModel.objects.create(project=self.project, name='docs forecast model 2',
                                                        abbreviation='docs_mod_2')
        time_zero_3 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 16)).first()
        forecast_3 = Forecast.objects.create(forecast_model=forecast_model_2, source='docs-predictions.json 3',
                                             time_zero=time_zero_3, notes="f3")
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast_3, json_io_dict_in, False)

        Score.ensure_all_scores_exist()
        _update_scores_for_all_projects()

        try:
            list(query_scores_for_project(self.project, {}, max_num_rows=14))  # actual number of rows = 14
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        with self.assertRaises(RuntimeError) as context:
            list(query_scores_for_project(self.project, {}, max_num_rows=13))
        self.assertIn("number of rows exceeded maximum", str(context.exception))


    def test__scores_query_worker(self):
        """
        Nearly identical to test__forecasts_query_worker().
        """
        # tests the worker directly. above test verifies that it's called from `query_forecasts_endpoint()`

        # ensure query_forecasts_for_project() is called
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.project.pk, 'query': {}})
        with patch('utils.project_queries.query_scores_for_project') as query_mock, \
                patch('utils.cloud_file.upload_file'):
            _scores_query_worker(job.pk)
            query_mock.assert_called_once_with(self.project, {})

        # case: upload_file() does not error
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.project.pk, 'query': {}})
        with patch('utils.cloud_file.upload_file') as upload_mock:
            _scores_query_worker(job.pk)
            upload_mock.assert_called_once()

            job.refresh_from_db()
            self.assertEqual(Job.SUCCESS, job.status)

        # case: upload_file() errors. BotoCoreError: alt: Boto3Error, ClientError, ConnectionClosedError:
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.project.pk, 'query': {}})
        with patch('utils.cloud_file.upload_file', side_effect=BotoCoreError()) as upload_mock, \
                patch('forecast_app.notifications.send_notification_email'):
            _scores_query_worker(job.pk)
            upload_mock.assert_called_once()

            job.refresh_from_db()
            self.assertEqual(Job.FAILED, job.status)
            self.assertIn("_query_worker(): AWS error", job.failure_message)

        # case: allow actual utils.cloud_file.upload_file(), which calls Bucket.put_object(). we don't actually do this
        # in this test b/c we don't want to hit S3, but it's commented here for debugging:
        # _scores_query_worker(job.pk)
        # job.refresh_from_db()
        # self.assertEqual(Job.SUCCESS, job.status)


    def test__tz_unit_targ_pks_to_truth_values(self):
        # setup
        project = Project.objects.create()
        make_cdc_units_and_targets(project)

        # load truth only for the TimeZero in truths-2016-2017-reichlab.csv we're testing against
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1),
                                            is_season_start=True, season_name='season1')
        load_truth_data(project, Path('utils/ensemble-truth-table-script/truths-2016-2017-reichlab.csv'))

        forecast_model = ForecastModel.objects.create(project=project, name='test model', abbreviation='abbrev')
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        load_cdc_csv_forecast_file(2016, forecast_model, csv_file_path, time_zero)

        # test
        tz_pk = time_zero.pk
        loc1_pk = Unit.objects.filter(project=project, name='HHS Region 1').first().pk
        loc2_pk = Unit.objects.filter(project=project, name='HHS Region 2').first().pk
        loc3_pk = Unit.objects.filter(project=project, name='HHS Region 3').first().pk
        loc4_pk = Unit.objects.filter(project=project, name='HHS Region 4').first().pk
        loc5_pk = Unit.objects.filter(project=project, name='HHS Region 5').first().pk
        loc6_pk = Unit.objects.filter(project=project, name='HHS Region 6').first().pk
        loc7_pk = Unit.objects.filter(project=project, name='HHS Region 7').first().pk
        loc8_pk = Unit.objects.filter(project=project, name='HHS Region 8').first().pk
        loc9_pk = Unit.objects.filter(project=project, name='HHS Region 9').first().pk
        loc10_pk = Unit.objects.filter(project=project, name='HHS Region 10').first().pk
        loc11_pk = Unit.objects.filter(project=project, name='US National').first().pk
        target1_pk = Target.objects.filter(project=project, name='Season onset').first().pk
        target2_pk = Target.objects.filter(project=project, name='Season peak week').first().pk
        target3_pk = Target.objects.filter(project=project, name='Season peak percentage').first().pk
        target4_pk = Target.objects.filter(project=project, name='1 wk ahead').first().pk
        target5_pk = Target.objects.filter(project=project, name='2 wk ahead').first().pk
        target6_pk = Target.objects.filter(project=project, name='3 wk ahead').first().pk
        target7_pk = Target.objects.filter(project=project, name='4 wk ahead').first().pk
        exp_dict = {  # {timezero_pk: {unit_pk: {target_id: truth_value}}}
            tz_pk: {
                loc1_pk: {target1_pk: ['2016-12-25'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [3.19221],
                          target4_pk: [1.52411], target5_pk: [1.73987], target6_pk: [2.06524], target7_pk: [2.51375]},
                loc2_pk: {target1_pk: ['2016-11-20'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [6.93759],
                          target4_pk: [5.07086], target5_pk: [5.68166], target6_pk: [6.01053], target7_pk: [6.49829]},
                loc3_pk: {target1_pk: ['2016-12-18'], target2_pk: [datetime.date(2017, 2, 12)], target3_pk: [5.20003],
                          target4_pk: [2.81366], target5_pk: [3.09968], target6_pk: [3.45232], target7_pk: [3.73339]},
                loc4_pk: {target1_pk: ['2016-11-13'], target2_pk: [datetime.date(2017, 2, 12)], target3_pk: [5.5107],
                          target4_pk: [2.89395], target5_pk: [3.68564], target6_pk: [3.69188], target7_pk: [4.53169]},
                loc5_pk: {target1_pk: ['2016-12-25'], target2_pk: [datetime.date(2017, 2, 12)], target3_pk: [4.31787],
                          target4_pk: [2.11757], target5_pk: [2.4432], target6_pk: [2.76295], target7_pk: [3.182]},
                loc6_pk: {target1_pk: ['2017-01-08'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [9.87589],
                          target4_pk: [4.80185], target5_pk: [5.26955], target6_pk: [6.10427], target7_pk: [8.13221]},
                loc7_pk: {target1_pk: ['2016-12-25'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [6.35948],
                          target4_pk: [2.75581], target5_pk: [3.46528], target6_pk: [4.56991], target7_pk: [5.52653]},
                loc8_pk: {target1_pk: ['2016-12-18'], target2_pk: [datetime.date(2017, 2, 12)], target3_pk: [2.72703],
                          target4_pk: [1.90851], target5_pk: [2.2668], target6_pk: [2.07104], target7_pk: [2.27632]},
                loc9_pk: {target1_pk: ['2016-12-18'], target2_pk: [datetime.date(2016, 12, 25)], target3_pk: [3.30484],
                          target4_pk: [2.83778], target5_pk: [2.68071], target6_pk: [2.9577], target7_pk: [3.03987]},
                loc10_pk: {target1_pk: ['2016-12-11'], target2_pk: [datetime.date(2016, 12, 25)], target3_pk: [3.67061],
                           target4_pk: [2.15197], target5_pk: [3.25108], target6_pk: [2.51434], target7_pk: [2.28634]},
                loc11_pk: {target1_pk: ['2016-12-11'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [5.06094],
                           target4_pk: [3.07623], target5_pk: [3.50708], target6_pk: [3.79872], target7_pk: [4.43601]}}}
        act_dict = _tz_unit_targ_pks_to_truth_values(forecast_model.project)
        self.assertEqual(exp_dict, act_dict)
