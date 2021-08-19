import datetime
import json
import logging
import statistics
from numbers import Number
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import BotoCoreError
from django.test import TestCase

from forecast_app.models import TimeZero, Forecast, Job, Unit, Target
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.prediction_element import PRED_CLASS_INT_TO_NAME
from utils.forecast import load_predictions_from_json_io_dict, NamedData
from utils.make_minimal_projects import _make_docs_project
from utils.project import create_project_from_json
from utils.project_queries import FORECAST_CSV_HEADER, query_forecasts_for_project, _forecasts_query_worker, \
    validate_truth_query, _truth_query_worker, query_truth_for_project
from utils.project_queries import validate_forecasts_query
from utils.project_truth import TRUTH_CSV_HEADER, oracle_model_for_project, load_truth_data
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

        # case: as_of is not a string, is not a datetime, or does not have timezone info
        error_messages, _ = validate_forecasts_query(self.project, {'as_of': -1})
        self.assertEqual(1, len(error_messages))
        self.assertIn(f"'as_of' was not a string", error_messages[0])

        error_messages, _ = validate_forecasts_query(self.project, {'as_of': '202010119'})
        self.assertEqual(1, len(error_messages))
        self.assertIn(f"'as_of' was not a recognizable datetime format", error_messages[0])

        error_messages, _ = validate_forecasts_query(self.project, {'as_of': '2020-10-11'})
        self.assertEqual(1, len(error_messages))
        self.assertIn(f"'as_of' did not contain timezone info", error_messages[0])

        # case: bad object reference
        for key_name, exp_error_msg in [('models', 'model with abbreviation not found'),
                                        ('units', 'unit with name not found'),
                                        ('targets', 'target with name not found'),
                                        ('timezeros', 'timezero with date not found')]:
            error_messages, _ = validate_forecasts_query(self.project, {key_name: [-1]})
            self.assertEqual(1, len(error_messages))
            self.assertIn(exp_error_msg, error_messages[0])

        # case: bad type
        error_messages, _ = validate_forecasts_query(self.project, {'types': ['bad type']})
        self.assertEqual(1, len(error_messages))
        self.assertIn("one or more types were invalid prediction types", error_messages[0])

        # case: object references from other project (!)
        project2, time_zero2, forecast_model2, forecast2 = _make_docs_project(self.po_user)
        for query_dict, exp_error_msg in [
            ({'models': [project2.models.first().abbreviation]}, 'model with abbreviation not found'),
            ({'units': [project2.units.first().name]}, 'unit with name not found'),
            ({'targets': [project2.targets.first().name]}, 'target with name not found'),
            ({'timezeros': [project2.timezeros.first().timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)]},
             'timezero with date not found')]:
            error_messages, _ = validate_forecasts_query(self.project, query_dict)
            self.assertEqual(1, len(error_messages))
            self.assertIn(exp_error_msg, error_messages[0])

        # case: blue sky
        query = {'models': list(self.project.models.all().values_list('id', flat=True)),
                 'units': list(self.project.units.all().values_list('id', flat=True)),
                 'targets': list(self.project.targets.all().values_list('id', flat=True)),
                 'timezeros': list(self.project.timezeros.all().values_list('id', flat=True)),
                 'types': list(PRED_CLASS_INT_TO_NAME.values())}
        error_messages, _ = validate_forecasts_query(self.project, query)
        self.assertEqual(0, len(error_messages))


    def test_query_forecasts_for_project_no_versions(self):
        model = self.forecast_model.abbreviation
        tz = self.time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        timezero_to_season_name = self.project.timezero_to_season_name()
        seas = timezero_to_season_name[self.time_zero]

        # ---- case: all BinData in project. check cat and prob columns ----
        rows = list(query_forecasts_for_project(self.project, {'types': ['bin']}))  # list for generator
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))

        exp_rows_bin = [(model, tz, seas, 'location1', 'Season peak week', 'bin', '2019-12-15', 0.01),
                        (model, tz, seas, 'location1', 'Season peak week', 'bin', '2019-12-22', 0.1),
                        (model, tz, seas, 'location1', 'Season peak week', 'bin', '2019-12-29', 0.89),
                        (model, tz, seas, 'location1', 'season severity', 'bin', 'mild', 0.0),
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
                        (model, tz, seas, 'location3', 'cases next week', 'bin', 0, 0.0),
                        (model, tz, seas, 'location3', 'cases next week', 'bin', 2, 0.1),
                        (model, tz, seas, 'location3', 'cases next week', 'bin', 50, 0.9)]  # sorted
        # model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
        act_rows = [(row[0], row[1], row[2], row[3], row[4], row[5], row[7], row[8]) for row in rows]
        self.assertEqual(exp_rows_bin, sorted(act_rows))

        # ----  case: all named data in project. check family, and param1, 2, and 3 columns ----
        rows = list(query_forecasts_for_project(self.project, {'types': ['named']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))

        exp_rows_named = [(model, tz, seas, 'location1', 'cases next week', 'named', NamedData.POIS_DIST, 1.1, '', ''),
                          (model, tz, seas, 'location1', 'pct next week', 'named', NamedData.NORM_DIST, 1.1, 2.2, '')
                          ]  # sorted
        # model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
        act_rows = [(row[0], row[1], row[2], row[3], row[4], row[5], row[11], row[12], row[13], row[14])
                    for row in rows]
        self.assertEqual(exp_rows_named, sorted(act_rows))

        # ---- case: all PointData in project. check value column ----
        rows = list(query_forecasts_for_project(
            self.project, {'types': ['point']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))

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

        # ---- case: all SampleData in project. check sample column ----
        rows = list(query_forecasts_for_project(self.project, {'types': ['sample']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))

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

        # ---- case: all QuantileData in project. check quantile and value columns ----
        rows = list(query_forecasts_for_project(self.project, {'types': ['quantile']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))

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
        rows = list(query_forecasts_for_project(self.project, {}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin),
                         len(rows))

        # ---- case: only one unit ----
        rows = list(query_forecasts_for_project(self.project, {'units': ['location3']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
        self.assertEqual(18, len(rows))

        # ---- case: only one target ----
        rows = list(query_forecasts_for_project(self.project, {'targets': ['above baseline']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
        self.assertEqual(9, len(rows))

        # following two tests require a second model, timezero, and forecast
        forecast_model2 = ForecastModel.objects.create(project=self.project, name=model, abbreviation='abbrev')
        time_zero2 = TimeZero.objects.create(project=self.project, timezero_date=datetime.date(2011, 10, 22))
        forecast2 = Forecast.objects.create(forecast_model=forecast_model2, source='docs-predictions-non-dup.json',
                                            time_zero=time_zero2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions-non-dup.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, is_validate_cats=False)

        # ---- case: empty query -> all forecasts in project. s/be twice as many now, modulo non-dup having 4 fewer ----
        # "location2", "cases next week", "sample": non-dup has 2 not 3
        # "location3", "cases next week", "bin": 2 not 3
        # "location2", "season severity", "sample": 4 not 5
        # "location2", "Season peak week", "quantile": 2 not 3
        rows = list(query_forecasts_for_project(self.project, {}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
        self.assertEqual((len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin) * 2)
                         - 4, len(rows))

        # ---- case: only one timezero ----
        rows = list(query_forecasts_for_project(self.project, {'timezeros': ['2011-10-22']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin) - 4,
                         len(rows))

        # ---- case: only one model ----
        rows = list(query_forecasts_for_project(self.project, {'models': ['abbrev']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin) - 4,
                         len(rows))


    def test_query_forecasts_for_project_max_num_rows(self):
        try:
            list(query_forecasts_for_project(self.project, {}, max_num_rows=29))  # actual number of rows = 29
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        with self.assertRaises(RuntimeError) as context:
            list(query_forecasts_for_project(self.project, {}, max_num_rows=28))
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
            self.assertIn("_query_worker(): error", job.failure_message)

        # case: allow actual utils.cloud_file.upload_file(), which calls Bucket.put_object(). we don't actually do this
        # in this test b/c we don't want to hit S3, but it's commented here for debugging:
        # _forecasts_query_worker(job.pk)
        # job.refresh_from_db()
        # self.assertEqual(Job.SUCCESS, job.status)


    #
    # test forecast queries with auto-convert
    #

    def test_validate_forecasts_query_options(self):
        """
        test the 'options' query key for the auto-conversion of prediction types feature
        """
        # test valid options
        for valid_options in [
            {},
            {'convert.bin': True, 'convert.point': 'mean', 'convert.sample': 21, 'convert.quantile': [0.025, 0.975]},
            {'convert.bin': False, 'convert.point': 'median', 'convert.sample': 10, 'convert.quantile': [0.025, 0.975]},
        ]:
            error_messages, _ = validate_forecasts_query(self.project, {'types': ['point'], 'options': valid_options})
            self.assertEqual(0, len(error_messages))

        # test invalid options
        for invalid_options, exp_error_msg in [
            (-1, "options was not a dict"),
            ({'convert.bin': -1}, "bin option value was not a boolean"),
            ({'convert.named': True}, "one or more invalid options keys"),
            ({'convert.point': -1}, "point option value was not one of 'mean' or 'median'"),
            ({'convert.point': 'hmm'}, "point option value was not one of 'mean' or 'median'"),
            ({'convert.quantile': -1}, "quantile option value was not a list of unique numbers in [0, 1]"),
            ({'convert.quantile': []}, "quantile option value was not a list of unique numbers in [0, 1]"),
            ({'convert.quantile': [-1]}, "quantile option value was not a list of unique numbers in [0, 1]"),
            ({'convert.quantile': [0.025, 0.025]}, "quantile option value was not a list of unique numbers in [0, 1]"),
            ({'convert.sample': 'nope'}, "sample option value was not an int >0"),
            ({'convert.sample': 0}, "sample option value was not an int >0"),
        ]:
            error_messages, _ = validate_forecasts_query(self.project, {'types': ['point'], 'options': invalid_options})
            self.assertEqual(1, len(error_messages))
            self.assertIn(exp_error_msg, error_messages[0])

        # test options passed with no types
        error_messages, _ = validate_forecasts_query(self.project, {'options': valid_options})
        self.assertEqual(0, len(error_messages))

        # test options passed with no types
        error_messages, _ = validate_forecasts_query(self.project, {'types': [], 'options': valid_options})
        self.assertEqual(0, len(error_messages))


    def test_query_forecasts_for_project_convert_S_to_P(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='convert model', abbreviation='convs_model')
        tz1 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 2)).first()
        u1 = Unit.objects.filter(name='location1').first()
        t1 = Target.objects.filter(name='cases next week').first()
        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        samples = [0, 2, 2, 5]
        predictions = [{"unit": u1.name, "target": t1.name, "class": "sample", "prediction": {"sample": samples}}]
        load_predictions_from_json_io_dict(f1, {'predictions': predictions}, is_validate_cats=False)

        # case: S->P: mean
        exp_rows = [
            ['model', 'timezero', 'season', 'unit', 'target', 'class', 'value', 'cat', 'prob', 'sample', 'quantile',
             'family', 'param1', 'param2', 'param3'],
            ['convs_model', '2011-10-02', '2011-2012', 'location1', 'cases next week', 'point',
             statistics.mean(samples), '', '', '', '', '', '', '', '']]
        act_rows = list(query_forecasts_for_project(project,
                                                    {'types': ['point'], 'options': {'convert.point': 'mean'}}))
        self.assertEqual(exp_rows, act_rows)

        # case: S->P: median
        exp_rows = [
            ['model', 'timezero', 'season', 'unit', 'target', 'class', 'value', 'cat', 'prob', 'sample', 'quantile',
             'family', 'param1', 'param2', 'param3'],
            ['convs_model', '2011-10-02', '2011-2012', 'location1', 'cases next week', 'point',
             statistics.median(samples), '', '', '', '', '', '', '', '']]
        act_rows = list(query_forecasts_for_project(project,
                                                    {'types': ['point'], 'options': {'convert.point': 'median'}}))
        self.assertEqual(exp_rows, act_rows)

        # case: P->P (no conversion necessary). note that we pass the 'convert.point' option, which operates as both
        # a flag indicating conversion and a conversion option, even though in this case it won't apply b/c we already
        # have the desired point prediction
        f1.delete()
        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        predictions += [{"unit": u1.name, "target": t1.name, "class": "point", "prediction": {"value": 666}}]
        load_predictions_from_json_io_dict(f1, {'predictions': predictions}, is_validate_cats=False)
        exp_rows = [
            ['model', 'timezero', 'season', 'unit', 'target', 'class', 'value', 'cat', 'prob', 'sample', 'quantile',
             'family', 'param1', 'param2', 'param3'],
            ['convs_model', '2011-10-02', '2011-2012', 'location1', 'cases next week', 'point', 666, '', '', '', '', '',
             '', '', '']]
        act_rows = list(query_forecasts_for_project(project,
                                                    {'types': ['point'], 'options': {'convert.point': 'median'}}))
        self.assertEqual(exp_rows, act_rows)


    def test_query_forecasts_for_project_convert_unsupported_target_types(self):
        self.fail()  # todo xx


    def test_query_forecasts_for_project_convert_unsupported_conversions(self):
        self.fail()  # todo xx


    #
    # test truth queries
    #

    def _assert_list_of_lists_almost_equal(self, exp_rows, act_rows):
        """
        Utility that iterates over the two lists' elements, calling assertAlmostEqual on each
        """
        self.assertEqual(len(exp_rows), len(act_rows))
        for exp_row, act_row in zip(exp_rows, act_rows):
            self.assertEqual(len(exp_row), len(act_row))
            for exp_row_val, act_row_val in zip(exp_row, act_row):
                if isinstance(exp_row_val, Number) and isinstance(act_row_val, Number):
                    self.assertAlmostEqual(exp_row_val, act_row_val)  # handles non-floats via '==''
                else:
                    self.assertEqual(exp_row_val, act_row_val)


    def test_validate_truth_query(self):
        """
        Nearly identical to test_validate_forecasts_query().
        """
        # case: query not a dict
        error_messages, _ = validate_truth_query(self.project, -1)
        self.assertEqual(1, len(error_messages))
        self.assertIn("query was not a dict", error_messages[0])

        # case: query contains invalid keys
        error_messages, _ = validate_truth_query(self.project, {'foo': -1})
        self.assertEqual(1, len(error_messages))
        self.assertIn("one or more query keys were invalid", error_messages[0])

        # case: query keys are not correct type (lists)
        for key_name in ['units', 'targets', 'timezeros']:
            error_messages, _ = validate_truth_query(self.project, {key_name: -1})
            self.assertEqual(1, len(error_messages))
            self.assertIn(f"'{key_name}' was not a list", error_messages[0])

        # case: as_of is not a string, is not a datetime, or does not have timezone info
        error_messages, _ = validate_truth_query(self.project, {'as_of': -1})
        self.assertEqual(1, len(error_messages))
        self.assertIn(f"'as_of' was not a string", error_messages[0])

        error_messages, _ = validate_truth_query(self.project, {'as_of': '202010119'})
        self.assertEqual(1, len(error_messages))
        self.assertIn(f"'as_of' was not a recognizable datetime format", error_messages[0])

        error_messages, _ = validate_truth_query(self.project, {'as_of': '2020-10-11'})
        self.assertEqual(1, len(error_messages))
        self.assertIn(f"'as_of' did not contain timezone info", error_messages[0])

        # case: bad object reference
        for key_name, exp_error_msg in [('units', 'unit with name not found'),
                                        ('targets', 'target with name not found'),
                                        ('timezeros', 'timezero with date not found')]:
            error_messages, _ = validate_truth_query(self.project, {key_name: [-1]})
            self.assertEqual(1, len(error_messages))
            self.assertIn(exp_error_msg, error_messages[0])

        # case: object references from other project (!)
        project2, time_zero2, forecast_model2, forecast2 = _make_docs_project(self.po_user)
        for query_dict, exp_error_msg in [
            ({'units': [project2.units.first().name]}, 'unit with name not found'),
            ({'targets': [project2.targets.first().name]}, 'target with name not found'),
            ({'timezeros': [project2.timezeros.first().timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)]},
             'timezero with date not found')]:
            error_messages, _ = validate_truth_query(self.project, query_dict)
            self.assertEqual(1, len(error_messages))
            self.assertIn(exp_error_msg, error_messages[0])

        # case: blue sky
        query = {'units': list(self.project.units.all().values_list('id', flat=True)),
                 'targets': list(self.project.targets.all().values_list('id', flat=True)),
                 'timezeros': list(self.project.timezeros.all().values_list('id', flat=True))}
        error_messages, _ = validate_truth_query(self.project, query)
        self.assertEqual(0, len(error_messages))


    def test_query_truth_for_project(self):
        # note: _make_docs_project() loads: tests/truth_data/docs-ground-truth.csv
        # case: empty query -> all truth in project
        exp_rows = [['2011-10-02', 'location1', 'Season peak week', '2019-12-15'],
                    ['2011-10-02', 'location1', 'above baseline', True],
                    ['2011-10-02', 'location1', 'cases next week', 10],
                    ['2011-10-02', 'location1', 'pct next week', 4.5432],
                    ['2011-10-02', 'location1', 'season severity', 'moderate'],
                    ['2011-10-09', 'location2', 'Season peak week', '2019-12-29'],
                    ['2011-10-09', 'location2', 'above baseline', True],
                    ['2011-10-09', 'location2', 'cases next week', 3],
                    ['2011-10-09', 'location2', 'pct next week', 99.9],
                    ['2011-10-09', 'location2', 'season severity', 'severe'],
                    ['2011-10-16', 'location1', 'Season peak week', '2019-12-22'],
                    ['2011-10-16', 'location1', 'above baseline', False],
                    ['2011-10-16', 'location1', 'cases next week', 0],
                    ['2011-10-16', 'location1', 'pct next week', 0.0]]  # sorted
        act_rows = list(query_truth_for_project(self.project, {}))
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))

        # case: only one unit
        exp_rows = [['2011-10-02', 'location1', 'Season peak week', '2019-12-15'],
                    ['2011-10-02', 'location1', 'above baseline', True],
                    ['2011-10-02', 'location1', 'cases next week', 10],
                    ['2011-10-02', 'location1', 'pct next week', 4.5432],
                    ['2011-10-02', 'location1', 'season severity', 'moderate'],
                    ['2011-10-16', 'location1', 'Season peak week', '2019-12-22'],
                    ['2011-10-16', 'location1', 'above baseline', False],
                    ['2011-10-16', 'location1', 'cases next week', 0],
                    ['2011-10-16', 'location1', 'pct next week', 0.0]]  # sorted
        act_rows = list(query_truth_for_project(self.project, {'units': ['location1']}))
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))

        # case: only one target
        exp_rows = [['2011-10-02', 'location1', 'cases next week', 10],
                    ['2011-10-09', 'location2', 'cases next week', 3],
                    ['2011-10-16', 'location1', 'cases next week', 0]]  # sorted
        act_rows = list(query_truth_for_project(self.project, {'targets': ['cases next week']}))
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))

        # case: only one timezero
        exp_rows = [['2011-10-02', 'location1', 'Season peak week', '2019-12-15'],
                    ['2011-10-02', 'location1', 'above baseline', True],
                    ['2011-10-02', 'location1', 'cases next week', 10],
                    ['2011-10-02', 'location1', 'pct next week', 4.5432],
                    ['2011-10-02', 'location1', 'season severity', 'moderate']]  # sorted
        act_rows = list(query_truth_for_project(self.project, {'timezeros': ['2011-10-02']}))
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))


    def test_query_truth_for_project_max_num_rows(self):
        try:
            list(query_truth_for_project(self.project, {}, max_num_rows=14))  # actual number of rows = 14
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        with self.assertRaises(RuntimeError) as context:
            list(query_truth_for_project(self.project, {}, max_num_rows=13))
        self.assertIn("number of rows exceeded maximum", str(context.exception))


    def test_query_truth_for_project_as_of(self):
        # self.project has already loaded docs-ground-truth.csv
        last_forecast = Forecast.objects.filter(forecast_model=oracle_model_for_project(self.project)).last()

        # case a: no as_of -> latest version (only one version so far)
        exp_rows = [['2011-10-02', 'location1', 'Season peak week', '2019-12-15'],
                    ['2011-10-02', 'location1', 'above baseline', True],
                    ['2011-10-02', 'location1', 'cases next week', 10],
                    ['2011-10-02', 'location1', 'pct next week', 4.5432],
                    ['2011-10-02', 'location1', 'season severity', 'moderate'],
                    ['2011-10-09', 'location2', 'Season peak week', '2019-12-29'],
                    ['2011-10-09', 'location2', 'above baseline', True],
                    ['2011-10-09', 'location2', 'cases next week', 3],
                    ['2011-10-09', 'location2', 'pct next week', 99.9],
                    ['2011-10-09', 'location2', 'season severity', 'severe'],
                    ['2011-10-16', 'location1', 'Season peak week', '2019-12-22'],
                    ['2011-10-16', 'location1', 'above baseline', False],
                    ['2011-10-16', 'location1', 'cases next week', 0],
                    ['2011-10-16', 'location1', 'pct next week', 0.0]]  # sorted
        act_rows = list(query_truth_for_project(self.project, {}))  # list for generator
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))

        # case b: as_of < latest version's issue_date -> no rows. we use the newest oracle forecast, which is
        # ok to do b/c all forecasts that were uploaded from a single file are a "batch" that all has the same
        # source and issued_at, as set by `_load_truth_data()`
        as_of = (last_forecast.issued_at - datetime.timedelta(days=1)).isoformat()
        act_rows = list(query_truth_for_project(self.project, {'as_of': as_of}))
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self.assertEqual([], act_rows)

        # case c: as_of = latest version's issue_date -> latest version (same as case a)
        as_of = last_forecast.issued_at.isoformat()
        act_rows = list(query_truth_for_project(self.project, {'as_of': as_of}))
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))

        # load a second batch of forecasts (new versions)
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-non-dup.csv'),
                        file_name='docs-ground-truth-non-dup.csv')
        last_forecast = Forecast.objects.filter(forecast_model=oracle_model_for_project(self.project)).last()

        # case d: no as_of -> latest version (second version)
        exp_rows = [['2011-10-02', 'location1', 'Season peak week', '2019-12-29'],
                    ['2011-10-02', 'location1', 'above baseline', False],
                    ['2011-10-02', 'location1', 'cases next week', 11],
                    ['2011-10-02', 'location1', 'pct next week', 5.5432],
                    ['2011-10-02', 'location1', 'season severity', 'mild'],
                    ['2011-10-09', 'location2', 'Season peak week', '2019-12-22'],
                    ['2011-10-09', 'location2', 'above baseline', False],
                    ['2011-10-09', 'location2', 'cases next week', 4],
                    ['2011-10-09', 'location2', 'pct next week', 99.8],
                    ['2011-10-09', 'location2', 'season severity', 'moderate'],
                    ['2011-10-16', 'location1', 'Season peak week', '2019-12-15'],
                    ['2011-10-16', 'location1', 'above baseline', True],
                    ['2011-10-16', 'location1', 'cases next week', 1],
                    ['2011-10-16', 'location1', 'pct next week', 1.0]]  # sorted
        act_rows = list(query_truth_for_project(self.project, {}))  # list for generator
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))

        # case e: as_of < latest version's issue_date -> previous version (same as case a/c)
        as_of = (last_forecast.issued_at - datetime.timedelta(days=1)).isoformat()
        act_rows = list(query_truth_for_project(self.project, {'as_of': as_of}))
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self.assertEqual([], act_rows)

        # case f: as_of = latest version's issue_date -> latest version (same as case d)
        as_of = last_forecast.issued_at.isoformat()
        act_rows = list(query_truth_for_project(self.project, {'as_of': as_of}))
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))


    def test__truth_query_worker(self):
        """
        Nearly identical to test__forecasts_query_worker().
        """
        # tests the worker directly. above test verifies that it's called from `query_truth_endpoint()`

        # ensure query_truth_for_project() is called
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.project.pk, 'query': {}})
        with patch('utils.project_queries.query_truth_for_project') as query_mock, \
                patch('utils.cloud_file.upload_file'):
            _truth_query_worker(job.pk)
            query_mock.assert_called_once_with(self.project, {})

        # case: upload_file() does not error
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.project.pk, 'query': {}})
        with patch('utils.cloud_file.upload_file') as upload_mock:
            _truth_query_worker(job.pk)
            upload_mock.assert_called_once()

            job.refresh_from_db()
            self.assertEqual(Job.SUCCESS, job.status)

        # case: upload_file() errors. BotoCoreError: alt: Boto3Error, ClientError, ConnectionClosedError:
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.project.pk, 'query': {}})
        with patch('utils.cloud_file.upload_file', side_effect=BotoCoreError()) as upload_mock, \
                patch('forecast_app.notifications.send_notification_email'):
            _truth_query_worker(job.pk)
            upload_mock.assert_called_once()

            job.refresh_from_db()
            self.assertEqual(Job.FAILED, job.status)
            self.assertIn("_query_worker(): error", job.failure_message)

        # case: allow actual utils.cloud_file.upload_file(), which calls Bucket.put_object(). we don't actually do this
        # in this test b/c we don't want to hit S3, but it's commented here for debugging:
        # _truth_query_worker(job.pk)
        # job.refresh_from_db()
        # self.assertEqual(Job.SUCCESS, job.status)


    #
    # test as_of queries
    #

    def test_as_of_versions_issue_273(self):
        """
        tests the case in [Add forecast versioning](https://github.com/reichlab/forecast-repository/issues/273):

        Here's an example database with versions (header is timezeros, rows are forecast `issued_at`s). Each forecast
        only has one point prediction:

        +-----+-----+-----+
        |10/2 |10/9 |10/16|
        |tz1  |tz2  |tz3  |
        +=====+=====+=====+
        |10/2 |     |     |
        |f1   | -   | -   |  2.1
        +-----+-----+-----+
        |     |     |10/17|
        |-    | -   |f2   |  2.0
        +-----+-----+-----+
        |10/20|10/20|     |
        |f3   | f4  | -   |  3.567 | 10
        +-----+-----+-----+

        Here are some `as_of` examples (which forecast version would be used as of that date):

        +-----+----+----+----+
        |as_of|tz1 |tz2 |tz3 |
        +-----+----+----+----+
        |10/1 | -  | -  | -  |
        |10/3 | f1 | -  | -  |
        |10/18| f1 | -  | f2 |
        |10/20| f3 | f4 | f2 |
        |10/21| f3 | f4 | f2 |
        +-----+----+----+----+
        """
        # set up database
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='docs forecast model',
                                                      abbreviation='docs_mod')
        tz1 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 2)).first()
        tz2 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
        tz3 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 16)).first()

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        json_io_dict = {"predictions": [{"unit": "location1",
                                         "target": "pct next week",
                                         "class": "point",
                                         "prediction": {"value": 2.1}}]}
        load_predictions_from_json_io_dict(f1, json_io_dict, is_validate_cats=False)
        f1.issued_at = datetime.datetime.combine(tz1.timezero_date, datetime.time(), tzinfo=datetime.timezone.utc)
        f1.save()

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz3)
        json_io_dict = {"predictions": [{"unit": "location2",
                                         "target": "pct next week",
                                         "class": "point",
                                         "prediction": {"value": 2.0}}]}
        load_predictions_from_json_io_dict(f2, json_io_dict, is_validate_cats=False)
        f2.issued_at = datetime.datetime.combine(tz3.timezero_date, datetime.time(),
                                                 tzinfo=datetime.timezone.utc) + datetime.timedelta(days=1)
        f2.save()

        f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3', time_zero=tz1)
        json_io_dict = {"predictions": [
            {"unit": "location1", "target": "pct next week", "class": "point", "prediction": {"value": 2.1}},  # dup
            {"unit": "location3", "target": "pct next week", "class": "point", "prediction": {"value": 3.567}}]  # new
        }
        load_predictions_from_json_io_dict(f3, json_io_dict, is_validate_cats=False)
        f3.issued_at = datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                 tzinfo=datetime.timezone.utc) + datetime.timedelta(days=18)
        f3.save()

        f4 = Forecast.objects.create(forecast_model=forecast_model, source='f4', time_zero=tz2)
        json_io_dict = {"predictions": [{"unit": "location3",
                                         "target": "cases next week",
                                         "class": "point",
                                         "prediction": {"value": 10}}]}
        load_predictions_from_json_io_dict(f4, json_io_dict, is_validate_cats=False)
        f4.issued_at = f3.issued_at
        f4.save()

        # case: default (no `as_of`): all rows (no values are "shadowed")
        exp_rows = [['2011-10-02', 'location1', 'pct next week', 'point', 2.1],
                    ['2011-10-02', 'location3', 'pct next week', 'point', 3.567],
                    ['2011-10-09', 'location3', 'cases next week', 'point', 10],
                    ['2011-10-16', 'location2', 'pct next week', 'point', 2.0]]  # sorted
        act_rows = list(query_forecasts_for_project(project, {}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(exp_rows, sorted(act_rows))

        # case: 10/20: same as default
        as_of = datetime.datetime.combine(datetime.date(2011, 10, 20), datetime.time(), tzinfo=datetime.timezone.utc)
        act_rows = list(query_forecasts_for_project(project, {'as_of': as_of.isoformat()}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(sorted(exp_rows), sorted(act_rows))

        # case: 10/21: same as default
        as_of = datetime.datetime.combine(datetime.date(2011, 10, 21), datetime.time(), tzinfo=datetime.timezone.utc)
        act_rows = list(query_forecasts_for_project(project, {'as_of': as_of.isoformat()}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(sorted(exp_rows), sorted(act_rows))

        # case: 10/1: none
        exp_rows = []
        as_of = datetime.datetime.combine(datetime.date(2011, 10, 1), datetime.time(), tzinfo=datetime.timezone.utc)
        act_rows = list(query_forecasts_for_project(project, {'as_of': as_of.isoformat()}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(sorted(exp_rows), sorted(act_rows))

        # case: 10/3: just f1
        exp_rows = [['2011-10-02', 'location1', 'pct next week', 'point', 2.1]]
        as_of = datetime.datetime.combine(datetime.date(2011, 10, 2), datetime.time(), tzinfo=datetime.timezone.utc)
        act_rows = list(query_forecasts_for_project(project, {'as_of': as_of.isoformat()}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(sorted(exp_rows), sorted(act_rows))

        # case: 10/18: f1 and f2
        exp_rows = [['2011-10-02', 'location1', 'pct next week', 'point', 2.1],
                    ['2011-10-16', 'location2', 'pct next week', 'point', 2.0]]
        as_of = datetime.datetime.combine(datetime.date(2011, 10, 18), datetime.time(), tzinfo=datetime.timezone.utc)
        act_rows = list(query_forecasts_for_project(project, {'as_of': as_of.isoformat()}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    def test_as_of_versions_setting_0(self):
        """
        tests the case where users have updated only parts of a former forecast, which breaks `as_of` functionality as
        initially written (it was operating at the forecast/timezero/issued_at level, not factoring in the
        unit/target level). this example is from [Zoltar as_of query examples](https://docs.google.com/spreadsheets/d/1lT-WhgUG5vgonqjO_AvUDfXpNMC-alC7VHUzP4EJz7E/edit?ts=5fce8828#gid=0).
        NB: for convenience we adapt this example to use docs-project.json timezeros, units, and targets.

        forecasts:
        +-------------+----------+------------+------------+------+--------+-------+
        |    key      |           forecast table           |    prediction table   |
        | forecast_id | model_id | issued_at | timezero   | unit | target | value |
        +-------------+----------+------------+------------+------+--------+-------+
        | f1          | modelA   | tz1.tzd    | tz1        | u1   | t1     | 4     |  'tzd' = TimeZero.timezero_date
        | f1          | modelA   | tz1.tzd    | tz1        | u2   | t1     | 6     |
        |xf2xxxxxxxxxx|xmodelAxxx|xtz2.tzdxxxx|xtz1xxxxxxxx|xu1xxx|xt1xxxxx|x4xxxxx| <- row not present (strikeout): current practice is that teams submit duplicates of old forecasts
        | f2          | modelA   | tz2.tzd    | tz1        | u2   | t1     | 7     |
        +-------------+----------+------------+------------+------+--------+-------+

        desired as_of query {all units, all targets, all timezeroes, all models, as_of = tz2.tzd} returns:
        +-------------+----------+------------+------------+------+--------+-------+
        | forecast_id | model_id | issued_at | timezero   | unit | target | value |
        +-------------+----------+------------+------------+------+--------+-------+
        | f1          | modelA   | tz1.tzd    | tz1        | u1   | t1     | 4     |
        | f2          | modelA   | tz2.tzd    | tz1        | u2   | t1     | 7     |
        +-------------+----------+------------+------------+------+--------+-------+
        """
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='modelA', abbreviation='modelA')
        tz1 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 2)).first()
        tz2 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        u1, u2 = 'location1', 'location2'
        t1 = 'cases next week'
        json_io_dict = {"predictions": [{"unit": u1, "target": t1, "class": "point", "prediction": {"value": 4}},
                                        {"unit": u2, "target": t1, "class": "point", "prediction": {"value": 6}}]}
        load_predictions_from_json_io_dict(f1, json_io_dict, is_validate_cats=False)
        f1.issued_at = datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                 tzinfo=datetime.timezone.utc) + datetime.timedelta(days=1)
        f1.save()

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        json_io_dict = {"predictions": [
            {"unit": u1, "target": t1, "class": "point", "prediction": {"value": 4}},  # dup
            {"unit": u2, "target": t1, "class": "point", "prediction": {"value": 7}}]}  # new
        load_predictions_from_json_io_dict(f2, json_io_dict, is_validate_cats=False)
        f2.issued_at = datetime.datetime.combine(tz2.timezero_date, datetime.time(),
                                                 tzinfo=datetime.timezone.utc) + datetime.timedelta(days=1)
        f2.save()

        # case: {all units, all targets, all timezeroes, all models, as_of = tz2.tzd}
        exp_rows = [[tz1.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT), u1, t1, 'point', 4],
                    [tz1.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT), u2, t1, 'point', 7]]
        act_rows = list(query_forecasts_for_project(project,
                                                    {'as_of': f2.issued_at.isoformat()}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(sorted(exp_rows), sorted(act_rows))

        # case: same except as_of = tz1.tzd
        exp_rows = [[tz1.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT), u1, t1, 'point', 4],
                    [tz1.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT), u2, t1, 'point', 6]]
        act_rows = list(query_forecasts_for_project(project,
                                                    {'as_of': f1.issued_at.isoformat()}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    @staticmethod
    def _set_up_as_of_case():
        # test_as_of_case_*() helper
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='case model', abbreviation='case_model')

        tz1 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 2)).first()
        tz2 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
        u1 = Unit.objects.filter(name='location1').first()
        u2 = Unit.objects.filter(name='location2').first()
        u3 = Unit.objects.filter(name='location3').first()
        t1 = Target.objects.filter(name='cases next week').first()

        # load f1 (all "cases next week" dicts from docs-predictions.json)
        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        predictions = [
            {"unit": u1.name, "target": t1.name, "class": "named", "prediction": {"family": "pois", "param1": 1.1}},
            {"unit": u2.name, "target": t1.name, "class": "point", "prediction": {"value": 5}},
            {"unit": u2.name, "target": t1.name, "class": "sample", "prediction": {"sample": [0, 2, 5]}},
            {"unit": u3.name, "target": t1.name, "class": "bin",
             "prediction": {"cat": [0, 2, 50], "prob": [0.0, 0.1, 0.9]}},
            {"unit": u3.name, "target": t1.name, "class": "quantile",
             "prediction": {"quantile": [0.25, 0.75], "value": [0, 50]}}
        ]
        load_predictions_from_json_io_dict(f1, {'predictions': predictions}, is_validate_cats=False)
        f1.issued_at = datetime.datetime.combine(tz1.timezero_date, datetime.time(), tzinfo=datetime.timezone.utc)
        f1.save()
        return project, forecast_model, f1, tz1, tz2, u1, u2, u3, t1


    def test_as_of_case_a(self):
        """
        Tests inspired by [Zoltar as_of query examples](https://docs.google.com/spreadsheets/d/1lT-WhgUG5vgonqjO_AvUDfXpNMC-alC7VHUzP4EJz7E/edit?ts=5fce8828#gid=0)
        However, we include all five prediction types ("cases next week" target) instead of just points so we are
        confident that multi-row prediction types are correct. This test documents four cases - a through d - and then
        tests a. The other three tests are in separate methods, but refer to documentation in this one.

        Forecast data: f1 = all "cases next week" dicts from docs-predictions.json
        - all forecasts are for the same model
        - joined tables are shown
        - only param1 is shown
        - "*" = changed or retracted (via NULL)
        - only "*_i" columns are shown ("cases next week" is a discrete target)
        - 'tzd' = TimeZero.timezero_date

        In all cases we have two tests with two forecasts: f1: top (baseline) table, f2: specific case's table.

        baseline table:
        +--+----------+--------+----+------+--------+----------------------------------------------+
        |       forecast       |     pred ele       |                 pred data                    |
        |id|issued_at|timezero|unit|target| class  |                   data                       |
        +--+----------+--------+----+------+--------+----------------------------------------------+
        |f1|tz1.tzd   |tz1     |u1  |t1    |named   | {"family": "pois", "param1": 1.1}            |
        |f1|tz1.tzd   |tz1     |u2  |t1    |point   | {"value": 5}                                 |
        |f1|tz1.tzd   |tz1     |u2  |t1    |sample  | {"sample": [0, 2, 5]}                        |
        |f1|tz1.tzd   |tz1     |u3  |t1    |bin     | {"cat": [0, 2, 50], "prob": [0.0, 0.1, 0.9]} |
        |f1|tz1.tzd   |tz1     |u3  |t1    |quantile| {"quantile": [0.25, 0.75], "value": [0, 50]} |
        +--+----------+--------+----+------+--------+----------------------------------------------+

        case a)
        +--+----------+--------+----+------+--------+----------------------------------------------+
        |       forecast       |     pred ele       |                 pred data                    |
        |id|issued_at|timezero|unit|target| class  |                   data                       |
        +--+----------+--------+----+------+--------+----------------------------------------------+
        |f1|tz1.tzd   |tz1     |u1  |t1    |named   | None (retracted)                             |
        |f1|tz1.tzd   |tz1     |u2  |t1    |point   | None ""                                      |
        |f1|tz1.tzd   |tz1     |u2  |t1    |sample  | None ""                                      |
        |f1|tz1.tzd   |tz1     |u3  |t1    |bin     | None ""                                      |
        |f1|tz1.tzd   |tz1     |u3  |t1    |quantile| None ""                                      |
        +--+----------+--------+----+------+--------+----------------------------------------------+
        as_of = tz1 -> all rows from baseline table
        as_of = tz2 -> no rows (all are retracted)
        """
        project, forecast_model, f1, tz1, tz2, u1, u2, u3, t1 = ProjectQueriesTestCase._set_up_as_of_case()
        tz1str = tz1.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        model = forecast_model.abbreviation
        season = project.timezero_to_season_name()[tz1]

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        predictions = [
            {"unit": u1.name, "target": t1.name, "class": "named", "prediction": None},
            {"unit": u2.name, "target": t1.name, "class": "point", "prediction": None},
            {"unit": u2.name, "target": t1.name, "class": "sample", "prediction": None},
            {"unit": u3.name, "target": t1.name, "class": "bin", "prediction": None},
            {"unit": u3.name, "target": t1.name, "class": "quantile", "prediction": None}
        ]
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, is_validate_cats=False)
        f2.issued_at = datetime.datetime.combine(tz2.timezero_date, datetime.time(), tzinfo=datetime.timezone.utc)
        f2.save()

        # model,timezero,season,unit,target,class,value,cat,prob,sample,quantile,family,param1,param2,param3
        exp_rows = [  # all rows from top (baseline) table
            [model, tz1str, season, u1.name, t1.name, 'named', '', '', '', '', '', 'pois', 1.1, '', ''],
            [model, tz1str, season, u2.name, t1.name, 'point', 5, '', '', '', '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 0, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 2, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 5, '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 0, 0.0, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 2, 0.1, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 50, 0.9, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 0, '', '', '', 0.25, '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 50, '', '', '', 0.75, '', '', '', ''],
        ]
        act_rows = list(query_forecasts_for_project(project, {'as_of': f1.issued_at.isoformat()}))[1:]  # skip header
        self.assertEqual(sorted(exp_rows), sorted(act_rows))

        exp_rows = []  # no rows (all are retracted)
        act_rows = list(query_forecasts_for_project(project, {'as_of': f2.issued_at.isoformat()}))[1:]  # skip header
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    def test_as_of_case_b(self):
        """
        case b)
        +--+----------+--------+----+------+--------+------------------------------------------------+
        |f2|tz1.tzd   |tz1     |u1  |t1    |named   | {"family": "pois", "param1": 1.2*}             |  updated (*)
        |f2|tz1.tzd   |tz1     |u2  |t1    |point   | {"value": 6*}                                  |  updated
        |f2|tz1.tzd   |tz1     |u2  |t1    |sample  | {"sample": [0, 3*, 6*]}                        |  some updated, i.e., some dup rows
        |f2|tz1.tzd   |tz1     |u3  |t1    |bin     | {"cat": [0, 2, 50], "prob": [0.1*, 0.0*, 0.9]} |  some updated
        |f2|tz1.tzd   |tz1     |u3  |t1    |quantile| {"quantile": [0.25, 0.75], "value": [2*, 50]}  |  some updated
        +--+----------+--------+----+------+--------+------------------------------------------------+
        as_of = tz1 -> all rows from baseline
        as_of = tz2 -> all rows from case table (all are updated)
        """
        project, forecast_model, f1, tz1, tz2, u1, u2, u3, t1 = ProjectQueriesTestCase._set_up_as_of_case()
        tz1str = tz1.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        model = forecast_model.abbreviation
        season = project.timezero_to_season_name()[tz1]

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        predictions = [
            {"unit": u1.name, "target": t1.name, "class": "named", "prediction": {"family": "pois", "param1": 1.2}},
            {"unit": u2.name, "target": t1.name, "class": "point", "prediction": {"value": 6}},
            {"unit": u2.name, "target": t1.name, "class": "sample", "prediction": {"sample": [0, 3, 6]}},
            {"unit": u3.name, "target": t1.name, "class": "bin",
             "prediction": {"cat": [0, 2, 50], "prob": [0.1, 0.0, 0.9]}},
            {"unit": u3.name, "target": t1.name, "class": "quantile",
             "prediction": {"quantile": [0.25, 0.75], "value": [2, 50]}}
        ]
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, is_validate_cats=False)
        f2.issued_at = datetime.datetime.combine(tz2.timezero_date, datetime.time(), tzinfo=datetime.timezone.utc)
        f2.save()

        # model,timezero,season,unit,target,class,value,cat,prob,sample,quantile,family,param1,param2,param3
        exp_rows = [  # all rows from top (baseline) table
            [model, tz1str, season, u1.name, t1.name, 'named', '', '', '', '', '', 'pois', 1.1, '', ''],
            [model, tz1str, season, u2.name, t1.name, 'point', 5, '', '', '', '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 0, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 2, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 5, '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 0, 0.0, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 2, 0.1, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 50, 0.9, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 0, '', '', '', 0.25, '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 50, '', '', '', 0.75, '', '', '', ''],
        ]
        act_rows = list(query_forecasts_for_project(project,
                                                    {'as_of': f1.issued_at.isoformat()}))[1:]  # skip header
        self.assertEqual(sorted(exp_rows), sorted(act_rows))

        exp_rows = [  # all rows from case table
            [model, tz1str, season, u1.name, t1.name, 'named', '', '', '', '', '', 'pois', 1.2, '', ''],
            [model, tz1str, season, u2.name, t1.name, 'point', 6, '', '', '', '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 0, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 3, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 6, '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 0, 0.1, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 2, 0.0, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 50, 0.9, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 2, '', '', '', 0.25, '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 50, '', '', '', 0.75, '', '', '', ''],
        ]
        act_rows = list(query_forecasts_for_project(project,
                                                    {'as_of': f2.issued_at.isoformat()}))[1:]  # skip header
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    def test_as_of_case_c(self):
        """
        case c)
        +--+----------+--------+----+------+--------+-------------------------------------------------+
        |f2|tz1.tzd   |tz1     |u1  |t1    |named   | {"family": "pois", "param1": 1.1}               |
      . |f2|tz1.tzd   |tz1     |u2  |t1    |point   | {"value": 7*}                                   |  updated (*)
        |f2|tz1.tzd   |tz1     |u2  |t1    |sample  | {"sample": [0, 2, 5]}                           |
      . |f2|tz1.tzd   |tz1     |u3  |t1    |bin     | {"cat": [0, 2, 50], "prob": [0.5*, 0.3*, 0.2*]} |  all updated, i.e., no dup rows
        |f2|tz1.tzd   |tz1     |u3  |t1    |quantile| {"quantile": [0.25, 0.75], "value": [0, 50]}    |
        +--+----------+--------+----+------+--------+-------------------------------------------------+
        as_of = tz1 -> all rows from baseline table
        as_of = tz2 -> dotted rows
        +--+----------+--------+----+------+--------+----------------------------------------------+
      . |f1|tz1.tzd   |tz1     |u1  |t1    |named   | {"family": "pois", "param1": 1.1}            |
        |f1|tz1.tzd   |tz1     |u2  |t1    |point   | {"value": 5}                                 |
      . |f1|tz1.tzd   |tz1     |u2  |t1    |sample  | {"sample": [0, 2, 5]}                        |
        |f1|tz1.tzd   |tz1     |u3  |t1    |bin     | {"cat": [0, 2, 50], "prob": [0.0, 0.1, 0.9]} |
      . |f1|tz1.tzd   |tz1     |u3  |t1    |quantile| {"quantile": [0.25, 0.75], "value": [0, 50]} |
        +--+----------+--------+----+------+--------+----------------------------------------------+
        """
        project, forecast_model, f1, tz1, tz2, u1, u2, u3, t1 = ProjectQueriesTestCase._set_up_as_of_case()
        tz1str = tz1.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        tz2str = tz2.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        model = forecast_model.abbreviation
        season = project.timezero_to_season_name()[tz1]

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        predictions = [
            {"unit": u1.name, "target": t1.name, "class": "named", "prediction": {"family": "pois", "param1": 1.1}},
            {"unit": u2.name, "target": t1.name, "class": "point", "prediction": {"value": 7}},  # updated
            {"unit": u2.name, "target": t1.name, "class": "sample", "prediction": {"sample": [0, 2, 5]}},
            {"unit": u3.name, "target": t1.name, "class": "bin",  # updated
             "prediction": {"cat": [0, 2, 50], "prob": [0.5, 0.3, 0.2]}},
            {"unit": u3.name, "target": t1.name, "class": "quantile",
             "prediction": {"quantile": [0.25, 0.75], "value": [0, 50]}}
        ]
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, is_validate_cats=False)
        f2.issued_at = datetime.datetime.combine(tz2.timezero_date, datetime.time(), tzinfo=datetime.timezone.utc)
        f2.save()

        # model,timezero,season,unit,target,class,value,cat,prob,sample,quantile,family,param1,param2,param3
        exp_rows = [  # all rows from top (baseline) table
            [model, tz1str, season, u1.name, t1.name, 'named', '', '', '', '', '', 'pois', 1.1, '', ''],
            [model, tz1str, season, u2.name, t1.name, 'point', 5, '', '', '', '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 0, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 2, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 5, '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 0, 0.0, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 2, 0.1, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 50, 0.9, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 0, '', '', '', 0.25, '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 50, '', '', '', 0.75, '', '', '', ''],
        ]
        act_rows = list(query_forecasts_for_project(project, {'as_of': f1.issued_at.isoformat()}))[1:]  # skip header
        self.assertEqual(sorted(exp_rows), sorted(act_rows))

        exp_rows = [  # dotted rows
            [model, tz1str, season, u1.name, t1.name, 'named', '', '', '', '', '', 'pois', 1.1, '', ''],
            [model, tz1str, season, u2.name, t1.name, 'point', 7, '', '', '', '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 0, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 2, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 5, '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 0, 0.5, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 2, 0.3, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 50, 0.2, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 0, '', '', '', 0.25, '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 50, '', '', '', 0.75, '', '', '', ''],
        ]
        act_rows = list(query_forecasts_for_project(project, {'as_of': f2.issued_at.isoformat()}))[1:]  # skip header
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    def test_as_of_case_d(self):
        """
        case d)
        +--+----------+--------+----+------+--------+-------------------------------------------------+
      x |f1|tz1.tzd   |tz1     |u1  |t1    |named   | {"family": "pois", "param1": 1.1}               |  not updated, i.e., duplicate row
      x |f1|tz1.tzd   |tz1     |u2  |t1    |point   | {"value": 5}                                    |  ""
      . |f1|tz1.tzd   |tz1     |u2  |t1    |sample  | None (retracted)                                |
      . |f1|tz1.tzd   |tz1     |u3  |t1    |bin     | {"cat": [0, 2, 50], "prob": [0.5*, 0.3*, 0.2*]} |  all updated
      x |f1|tz1.tzd   |tz1     |u3  |t1    |quantile| {"quantile": [0.25, 0.75], "value": [0, 50]}    |  not updated, i.e., duplicate row
        +--+----------+--------+----+------+--------+-------------------------------------------------+
        as_of = tz1 -> all rows from baseline table
        as_of = tz2 -> dotted rows ('x' marks rows skipped by loader; retracted sample rows are not returned):
        +--+----------+--------+----+------+--------+----------------------------------------------+
      . |f1|tz1.tzd   |tz1     |u1  |t1    |named   | {"family": "pois", "param1": 1.1}            |
      . |f1|tz1.tzd   |tz1     |u2  |t1    |point   | {"value": 5}                                 |
        |f1|tz1.tzd   |tz1     |u2  |t1    |sample  | {"sample": [0, 2, 5]}                        |
        |f1|tz1.tzd   |tz1     |u3  |t1    |bin     | {"cat": [0, 2, 50], "prob": [0.0, 0.1, 0.9]} |
      . |f1|tz1.tzd   |tz1     |u3  |t1    |quantile| {"quantile": [0.25, 0.75], "value": [0, 50]} |
        +--+----------+--------+----+------+--------+----------------------------------------------+
        """
        project, forecast_model, f1, tz1, tz2, u1, u2, u3, t1 = ProjectQueriesTestCase._set_up_as_of_case()
        tz1str = tz1.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        tz2str = tz2.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        model = forecast_model.abbreviation
        season = project.timezero_to_season_name()[tz1]

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        predictions = [
            {"unit": u1.name, "target": t1.name, "class": "named", "prediction": {"family": "pois", "param1": 1.1}},
            {"unit": u2.name, "target": t1.name, "class": "point", "prediction": {"value": 5}},
            {"unit": u2.name, "target": t1.name, "class": "sample", "prediction": None},  # retracted
            {"unit": u3.name, "target": t1.name, "class": "bin",  # all updated
             "prediction": {"cat": [0, 2, 50], "prob": [0.5, 0.3, 0.2]}},
            {"unit": u3.name, "target": t1.name, "class": "quantile",
             "prediction": {"quantile": [0.25, 0.75], "value": [0, 50]}}
        ]
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, is_validate_cats=False)
        f2.issued_at = datetime.datetime.combine(tz2.timezero_date, datetime.time(), tzinfo=datetime.timezone.utc)
        f2.save()

        # model,timezero,season,unit,target,class,value,cat,prob,sample,quantile,family,param1,param2,param3
        exp_rows = [  # all rows from top (baseline) table
            [model, tz1str, season, u1.name, t1.name, 'named', '', '', '', '', '', 'pois', 1.1, '', ''],
            [model, tz1str, season, u2.name, t1.name, 'point', 5, '', '', '', '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 0, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 2, '', '', '', '', ''],
            [model, tz1str, season, u2.name, t1.name, 'sample', '', '', '', 5, '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 0, 0.0, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 2, 0.1, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 50, 0.9, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 0, '', '', '', 0.25, '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 50, '', '', '', 0.75, '', '', '', ''],
        ]
        act_rows = list(query_forecasts_for_project(project,
                                                    {'as_of': f1.issued_at.isoformat()}))[1:]  # skip header
        self.assertEqual(sorted(exp_rows), sorted(act_rows))

        exp_rows = [  # dotted rows
            [model, tz1str, season, u1.name, t1.name, 'named', '', '', '', '', '', 'pois', 1.1, '', ''],
            [model, tz1str, season, u2.name, t1.name, 'point', 5, '', '', '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 0, 0.5, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 2, 0.3, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'bin', '', 50, 0.2, '', '', '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 0, '', '', '', 0.25, '', '', '', ''],
            [model, tz1str, season, u3.name, t1.name, 'quantile', 50, '', '', '', 0.75, '', '', '', ''],
        ]
        act_rows = list(query_forecasts_for_project(project,
                                                    {'as_of': f2.issued_at.isoformat()}))[1:]  # skip header
        self.assertEqual(sorted(exp_rows), sorted(act_rows))
