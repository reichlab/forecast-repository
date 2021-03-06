import datetime
import json
import logging
from numbers import Number
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import BotoCoreError
from django.test import TestCase

from forecast_app.models import TimeZero, BinDistribution, NamedDistribution, PointPrediction, SampleDistribution, QuantileDistribution, Forecast, Job, Unit, Target, Project
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc_io import make_cdc_units_and_targets, load_cdc_csv_forecast_file
from utils.forecast import PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS, load_predictions_from_json_io_dict
from utils.make_minimal_projects import _make_docs_project
from utils.project import create_project_from_json
from utils.project_truth import TRUTH_CSV_HEADER, load_truth_data
from utils.project_queries import FORECAST_CSV_HEADER, query_forecasts_for_project, _forecasts_query_worker, \
    validate_truth_query, _truth_query_worker, query_truth_for_project
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

        # case: as_of is not a string, or is not a date in YYYY_MM_DD_DATE_FORMAT
        error_messages, _ = validate_forecasts_query(self.project, {'as_of': -1})
        self.assertEqual(1, len(error_messages))
        self.assertIn(f"'as_of' was not a string", error_messages[0])

        error_messages, _ = validate_forecasts_query(self.project, {'as_of': '20201011'})
        self.assertEqual(1, len(error_messages))
        self.assertIn(f"'as_of' was not in YYYY-MM-DD format", error_messages[0])

        try:
            validate_forecasts_query(self.project, {'as_of': '2020-10-11'})
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

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
                 'types': list(PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS.values())}
        error_messages, _ = validate_forecasts_query(self.project, query)
        self.assertEqual(0, len(error_messages))


    def test_query_forecasts_for_project(self):
        model = self.forecast_model.abbreviation
        tz = self.time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        timezero_to_season_name = self.project.timezero_to_season_name()
        seas = timezero_to_season_name[self.time_zero]

        # ---- case: all BinDistributions in project. check cat and prob columns ----
        rows = list(query_forecasts_for_project(
            self.project, {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution]]}))  # list for generator
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))

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
        rows = list(query_forecasts_for_project(
            self.project, {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[NamedDistribution]]}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))

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
        rows = list(query_forecasts_for_project(
            self.project, {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction]]}))
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

        # ---- case: all SampleDistributions in project. check sample column ----
        rows = list(query_forecasts_for_project(
            self.project, {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleDistribution]]}))
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

        # ---- case: all QuantileDistributions in project. check quantile and value columns ----
        rows = list(query_forecasts_for_project(
            self.project, {'types': [PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[QuantileDistribution]]}))
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
        self.assertEqual(17, len(rows))

        # ---- case: only one target ----
        rows = list(query_forecasts_for_project(self.project, {'targets': ['above baseline']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
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
        rows = list(query_forecasts_for_project(self.project, {}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin) * 2,
                         len(rows))

        # ---- case: only one timezero ----
        rows = list(query_forecasts_for_project(self.project, {'timezeros': ['2011-10-22']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin),
                         len(rows))

        # ---- case: only one model ----
        rows = list(query_forecasts_for_project(self.project, {'models': ['abbrev']}))
        self.assertEqual(FORECAST_CSV_HEADER, rows.pop(0))
        self.assertEqual(len(exp_rows_quantile + exp_rows_sample + exp_rows_point + exp_rows_named + exp_rows_bin),
                         len(rows))


    def test_query_forecasts_for_project_max_num_rows(self):
        try:
            list(query_forecasts_for_project(self.project, {}, max_num_rows=62))  # actual number of rows = 62
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        with self.assertRaises(RuntimeError) as context:
            list(query_forecasts_for_project(self.project, {}, max_num_rows=61))
        self.assertIn("number of rows exceeded maximum", str(context.exception))


    def test_as_of_versions(self):
        # tests the case in [Add forecast versioning](https://github.com/reichlab/forecast-repository/issues/273):
        #
        # Here's an example database with versions (header is timezeros, rows are forecast `issue_date`s). Each forecast
        # only has one point prediction:
        #
        # +-----+-----+-----+
        # |10/2 |10/9 |10/16|
        # |tz1  |tz2  |tz3  |
        # +=====+=====+=====+
        # |10/2 |     |     |
        # |f1   | -   | -   |  2.1
        # +-----+-----+-----+
        # |     |     |10/17|
        # |-    | -   |f2   |  2.0
        # +-----+-----+-----+
        # |10/20|10/20|     |
        # |f3   | f4  | -   |  3.567 | 10
        # +-----+-----+-----+
        #
        # Here are some `as_of` examples (which forecast version would be used as of that date):
        #
        # +-----+----+----+----+
        # |as_of|tz1 |tz2 |tz3 |
        # +-----+----+----+----+
        # |10/1 | -  | -  | -  |
        # |10/3 | f1 | -  | -  |
        # |10/18| f1 | -  | f2 |
        # |10/20| f3 | f4 | f2 |
        # |10/21| f3 | f4 | f2 |
        # +-----+----+----+----+

        # set up database
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)  # atomic
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
        load_predictions_from_json_io_dict(f1, json_io_dict, False)
        f1.issue_date = tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz3)
        json_io_dict = {"predictions": [{"unit": "location2",
                                         "target": "pct next week",
                                         "class": "point",
                                         "prediction": {"value": 2.0}}]}
        load_predictions_from_json_io_dict(f2, json_io_dict, False)
        f2.issue_date = tz3.timezero_date + datetime.timedelta(days=1)
        f2.save()

        f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3', time_zero=tz1)
        json_io_dict = {"predictions": [{"unit": "location3",
                                         "target": "pct next week",
                                         "class": "point",
                                         "prediction": {"value": 3.567}}]}
        load_predictions_from_json_io_dict(f3, json_io_dict, False)
        f3.issue_date = tz1.timezero_date + datetime.timedelta(days=18)
        f3.save()

        f4 = Forecast.objects.create(forecast_model=forecast_model, source='f4', time_zero=tz2)
        json_io_dict = {"predictions": [{"unit": "location3",
                                         "target": "cases next week",
                                         "class": "point",
                                         "prediction": {"value": 10}}]}
        load_predictions_from_json_io_dict(f4, json_io_dict, False)
        f4.issue_date = f3.issue_date
        f4.save()

        # case: default (no `as_of`): no f1 (f3 is newer)
        exp_rows = [['2011-10-16', 'location2', 'pct next week', 'point', 2.0],
                    ['2011-10-02', 'location3', 'pct next week', 'point', 3.567],
                    ['2011-10-09', 'location3', 'cases next week', 'point', 10]]
        act_rows = list(query_forecasts_for_project(project, {}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(exp_rows, act_rows)

        # case: 10/20: same as default
        act_rows = list(query_forecasts_for_project(project, {'as_of': '2011-10-20'}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(exp_rows, act_rows)

        # case: 10/21: same as default
        act_rows = list(query_forecasts_for_project(project, {'as_of': '2011-10-21'}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(exp_rows, act_rows)

        # case: 10/1: none
        exp_rows = []
        act_rows = list(query_forecasts_for_project(project, {'as_of': '2011-10-01'}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(exp_rows, act_rows)

        # case: 10/3: just f1
        exp_rows = [['2011-10-02', 'location1', 'pct next week', 'point', 2.1]]
        act_rows = list(query_forecasts_for_project(project, {'as_of': '2011-10-03'}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(exp_rows, act_rows)

        # case: 10/18: f1 and f2
        exp_rows = [['2011-10-02', 'location1', 'pct next week', 'point', 2.1],
                    ['2011-10-16', 'location2', 'pct next week', 'point', 2.0]]
        act_rows = list(query_forecasts_for_project(project, {'as_of': '2011-10-18'}))
        act_rows = [row[1:2] + row[3:7] for row in act_rows[1:]]  # 'timezero', 'unit', 'target', 'class', 'value'
        self.assertEqual(exp_rows, act_rows)


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

        for key_name in ['units', 'targets', 'timezeros']:
            error_messages, _ = validate_truth_query(self.project, {key_name: -1})
            self.assertEqual(1, len(error_messages))
            self.assertIn(f"'{key_name}' was not a list", error_messages[0])

        # case: bad object id
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
        act_rows = query_truth_for_project(self.project, {})
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
        act_rows = query_truth_for_project(self.project, {'units': ['location1']})
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))

        # case: only one target
        exp_rows = [['2011-10-02', 'location1', 'cases next week', 10],
                    ['2011-10-09', 'location2', 'cases next week', 3],
                    ['2011-10-16', 'location1', 'cases next week', 0]]  # sorted
        act_rows = query_truth_for_project(self.project, {'targets': ['cases next week']})
        self.assertEqual(TRUTH_CSV_HEADER, act_rows.pop(0))
        self._assert_list_of_lists_almost_equal(exp_rows, sorted(act_rows))

        # case: only one timezero
        exp_rows = [['2011-10-02', 'location1', 'Season peak week', '2019-12-15'],
                    ['2011-10-02', 'location1', 'above baseline', True],
                    ['2011-10-02', 'location1', 'cases next week', 10],
                    ['2011-10-02', 'location1', 'pct next week', 4.5432],
                    ['2011-10-02', 'location1', 'season severity', 'moderate']]  # sorted
        act_rows = query_truth_for_project(self.project, {'timezeros': ['2011-10-02']})
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
