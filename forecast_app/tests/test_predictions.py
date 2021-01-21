import datetime
import json
import unittest
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Forecast
from forecast_app.models import ForecastModel, TimeZero
from forecast_app.models.prediction import calc_named_distribution
from forecast_app.tests.test_project_queries import ProjectQueriesTestCase
from utils.forecast import load_predictions_from_json_io_dict, _prediction_dicts_to_validated_db_rows
from utils.make_minimal_projects import _make_docs_project
from utils.project import create_project_from_json
from utils.project_queries import query_truth_for_project
from utils.project_truth import load_truth_data, truth_data_qs
from utils.utilities import get_or_create_super_po_mo_users


#
# initial single file for driving zoltar2 development. todo xx will be split into separate ones as the code develops
#

class PredictionsTestCase(TestCase):
    """
    """


    def test_concrete_subclasses(self):
        """
        this test makes sure the current set of concrete Prediction subclasses hasn't changed since the last time this
        test was updated. it's here as a kind of sanity check to catch the case where the Prediction class hierarchy has
        changed, but not code that depends on the seven (as of this writing) specific subclasses
        """
        from forecast_app.models import Prediction


        concrete_subclasses = Prediction.concrete_subclasses()
        exp_subclasses = {'BinDistribution', 'NamedDistribution', 'PointPrediction', 'SampleDistribution',
                          'QuantileDistribution'}
        self.assertEqual(exp_subclasses, {concrete_subclass.__name__ for concrete_subclass in concrete_subclasses})


    def test_load_predictions_from_json_io_dict(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)

        # test json with no 'predictions'
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(forecast, {}, False)
        self.assertIn("json_io_dict had no 'predictions' key", str(context.exception))

        # load all four types of Predictions, call Forecast.*_qs() functions. see docs-predictionsexp-rows.xlsx.

        # counts from docs-predictionsexp-rows.xlsx: point: 11, named: 3, bin: 30 (3 zero prob), sample: 23
        # = total rows: 67
        #
        # counts based on .json file:
        # - 'pct next week':    point: 3, named: 1 , bin: 3, sample: 5, quantile: 5 = 17
        # - 'cases next week':  point: 2, named: 1 , bin: 3, sample: 3, quantile: 2 = 12
        # - 'season severity':  point: 2, named: 0 , bin: 3, sample: 5, quantile: 0 = 10
        # - 'above baseline':   point: 1, named: 0 , bin: 2, sample: 6, quantile: 0 =  9
        # - 'Season peak week': point: 3, named: 0 , bin: 7, sample: 4, quantile: 3 = 16
        # = total rows: 64 - 2 zero prob = 62

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict, False)
        self.assertEqual(62, forecast.get_num_rows())
        self.assertEqual(16, forecast.bin_distribution_qs().count())  # 18 - 2 zero prob
        self.assertEqual(2, forecast.named_distribution_qs().count())
        self.assertEqual(11, forecast.point_prediction_qs().count())
        self.assertEqual(23, forecast.sample_distribution_qs().count())
        self.assertEqual(10, forecast.quantile_prediction_qs().count())


    def test_prediction_dicts_to_db_rows(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)

        # see above: counts from docs-predictionsexp-rows.xlsx
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            prediction_dicts = json.load(fp)['predictions']  # ignore 'forecast', 'units', and 'targets'
            bin_rows, named_rows, point_rows, sample_rows, quantile_rows = \
                _prediction_dicts_to_validated_db_rows(forecast, prediction_dicts, False)
        self.assertEqual(16, len(bin_rows))  # 18 - 2 zero prob
        self.assertEqual(2, len(named_rows))
        self.assertEqual(11, len(point_rows))
        self.assertEqual(23, len(sample_rows))
        self.assertEqual(10, len(quantile_rows))
        self.assertEqual([['location2', 'pct next week', 1.1, 0.3],
                          ['location2', 'pct next week', 2.2, 0.2],
                          ['location2', 'pct next week', 3.3, 0.5],
                          ['location3', 'cases next week', 2, 0.1],
                          ['location3', 'cases next week', 50, 0.9],
                          ['location1', 'season severity', 'moderate', 0.1],
                          ['location1', 'season severity', 'severe', 0.9],
                          ['location2', 'above baseline', True, 0.9],
                          ['location2', 'above baseline', False, 0.1],
                          ['location1', 'Season peak week', '2019-12-15', 0.01],
                          ['location1', 'Season peak week', '2019-12-22', 0.1],
                          ['location1', 'Season peak week', '2019-12-29', 0.89],
                          ['location2', 'Season peak week', '2019-12-15', 0.01],
                          ['location2', 'Season peak week', '2019-12-22', 0.05],
                          ['location2', 'Season peak week', '2019-12-29', 0.05],
                          ['location2', 'Season peak week', '2020-01-05', 0.89]],
                         bin_rows)
        self.assertEqual([['location1', 'pct next week', 'norm', 1.1, 2.2, None],
                          ['location1', 'cases next week', 'pois', 1.1, None, None]],
                         named_rows)
        self.assertEqual([['location1', 'pct next week', 2.1],
                          ['location2', 'pct next week', 2.0],
                          ['location3', 'pct next week', 3.567],
                          ['location2', 'cases next week', 5],
                          ['location3', 'cases next week', 10],
                          ['location1', 'season severity', 'mild'],
                          ['location2', 'season severity', 'moderate'],
                          ['location1', 'above baseline', True],
                          ['location1', 'Season peak week', '2019-12-22'],
                          ['location2', 'Season peak week', '2020-01-05'],
                          ['location3', 'Season peak week', '2019-12-29']],
                         point_rows)
        self.assertEqual([['location3', 'pct next week', 2.3],
                          ['location3', 'pct next week', 6.5],
                          ['location3', 'pct next week', 0.0],
                          ['location3', 'pct next week', 10.0234],
                          ['location3', 'pct next week', 0.0001],
                          ['location2', 'cases next week', 0],
                          ['location2', 'cases next week', 2],
                          ['location2', 'cases next week', 5],
                          ['location2', 'season severity', 'moderate'],
                          ['location2', 'season severity', 'severe'],
                          ['location2', 'season severity', 'high'],
                          ['location2', 'season severity', 'moderate'],
                          ['location2', 'season severity', 'mild'],
                          ['location2', 'above baseline', True],
                          ['location2', 'above baseline', False],
                          ['location2', 'above baseline', True],
                          ['location3', 'above baseline', False],
                          ['location3', 'above baseline', True],
                          ['location3', 'above baseline', True],
                          ['location1', 'Season peak week', '2020-01-05'],
                          ['location1', 'Season peak week', '2019-12-15'],
                          ['location3', 'Season peak week', '2020-01-06'],
                          ['location3', 'Season peak week', '2019-12-16']],
                         sample_rows)
        self.assertEqual([['location2', 'pct next week', 0.025, 1.0],
                          ['location2', 'pct next week', 0.25, 2.2],
                          ['location2', 'pct next week', 0.5, 2.2],
                          ['location2', 'pct next week', 0.75, 5.0],
                          ['location2', 'pct next week', 0.975, 50.0],
                          ['location3', 'cases next week', 0.25, 0],
                          ['location3', 'cases next week', 0.75, 50],
                          ['location2', 'Season peak week', 0.5, '2019-12-22'],
                          ['location2', 'Season peak week', 0.75, '2019-12-29'],
                          ['location2', 'Season peak week', 0.975, '2020-01-05']],
                         quantile_rows)


    def test_prediction_dicts_to_db_rows_invalid(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)

        # test for invalid unit
        with self.assertRaises(RuntimeError) as context:
            bad_prediction_dicts = [
                {"unit": "bad unit", "target": "1 wk ahead", "class": "BinCat", "prediction": {}}
            ]
            _prediction_dicts_to_validated_db_rows(forecast, bad_prediction_dicts, False)
        self.assertIn('prediction_dict referred to an undefined Unit', str(context.exception))

        # test for invalid target
        with self.assertRaises(RuntimeError) as context:
            bad_prediction_dicts = [
                {"unit": "location1", "target": "bad target", "class": "bad class", "prediction": {}}
            ]
            _prediction_dicts_to_validated_db_rows(forecast, bad_prediction_dicts, False)
        self.assertIn('prediction_dict referred to an undefined Target', str(context.exception))

        # test for invalid prediction_class
        with self.assertRaises(RuntimeError) as context:
            bad_prediction_dicts = [
                {"unit": "location1", "target": "pct next week", "class": "bad class", "prediction": {}}
            ]
            _prediction_dicts_to_validated_db_rows(forecast, bad_prediction_dicts, False)
        self.assertIn('invalid prediction_class', str(context.exception))


    @unittest.skip("todo")
    def test_calc_named_distribution(self):
        abbrev_parms_exp_value = [
            ('norm', None, None, None, None),  # todo xx
            ('lnorm', None, None, None, None),
            ('gamma', None, None, None, None),
            ('beta', None, None, None, None),
            ('pois', None, None, None, None),
            ('nbinom', None, None, None, None),
            ('nbinom2', None, None, None, None),
        ]
        for named_dist_abbrev, param1, param2, param3, exp_val in abbrev_parms_exp_value:
            self.assertEqual(exp_val, calc_named_distribution(named_dist_abbrev, param1, param2, param3))

        with self.assertRaises(RuntimeError) as context:
            calc_named_distribution(None, None, None, None)
        self.assertIn("invalid abbreviation", str(context.exception))


    #
    # test "retracted" and skipped predictions for forecasts
    #

    def test_load_predictions_from_json_io_dict_none_prediction(self):
        # tests load_predictions_from_json_io_dict() where `"prediction": None`
        project, forecast_model, f1, tz1, tz2, u1, u2, u3, t1 = ProjectQueriesTestCase._set_up_as_of_case()

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        predictions = [
            {"unit": u1.name, "target": t1.name, "class": "named", "prediction": None},
            {"unit": u2.name, "target": t1.name, "class": "point", "prediction": None},
            {"unit": u2.name, "target": t1.name, "class": "sample", "prediction": None},
            {"unit": u3.name, "target": t1.name, "class": "bin", "prediction": None},
            {"unit": u3.name, "target": t1.name, "class": "quantile", "prediction": None}
        ]
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, False)
        self.assertEqual(5, f2.get_num_rows())
        self.assertEqual(1, f2.bin_distribution_qs().count())
        self.assertEqual(1, f2.named_distribution_qs().count())
        self.assertEqual(1, f2.point_prediction_qs().count())
        self.assertEqual(1, f2.sample_distribution_qs().count())
        self.assertEqual(1, f2.quantile_prediction_qs().count())


    def test_load_predictions_from_json_io_dict_dups(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        self.assertEqual(62, project.get_num_forecast_rows_all_models(is_oracle=False))

        # case: load the same file that was loaded originally -> should load no new rows (all are dups)
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict_in, False)  # atomic
        self.assertEqual(62, project.get_num_forecast_rows_all_models(is_oracle=False))  # s/b no change

        # case: load the same file, but change one multi-row prediction (a sample) to have partial duplication
        quantile_pred_dict = [pred_dict for pred_dict in json_io_dict_in['predictions']
                              if (pred_dict['unit'] == 'location2')
                              and (pred_dict['target'] == 'pct next week')
                              and (pred_dict['class'] == 'quantile')][0]
        # original: {"quantile": [0.025, 0.25, 0.5, 0.75,  0.975 ],
        #            "value":    [1.0,   2.2,  2.2,  5.0, 50.0  ]}
        quantile_pred_dict['prediction']['value'][0] = 2.2  # was 1.0
        load_predictions_from_json_io_dict(forecast, json_io_dict_in, False)  # atomic
        self.assertEqual(-1, project.get_num_forecast_rows_all_models(is_oracle=False))  # s/b no change


    #
    # test "retracted" and skipped predictions for truth
    #

    @unittest.skip("todo")
    def test_load_truth_data_null_rows(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)  # atomic
        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-null-value.csv'),
                        is_convert_na_none=True)
        exp_rows = [
            (datetime.date(2011, 10, 2), 'location1', 'Season peak week', None, None, None, datetime.date(2019, 12, 15),
             None),
            (datetime.date(2011, 10, 2), 'location1', 'above baseline', None, None, None, None, True),
            (datetime.date(2011, 10, 2), 'location1', 'season severity', None, None, 'moderate', None, None),
            (datetime.date(2011, 10, 2), 'location1', 'cases next week', None, None, None, None, None),  # all None
            (datetime.date(2011, 10, 2), 'location1', 'pct next week', None, None, None, None, None),  # all None
            (datetime.date(2011, 10, 9), 'location2', 'Season peak week', None, None, None, datetime.date(2019, 12, 29),
             None),
            (datetime.date(2011, 10, 9), 'location2', 'above baseline', None, None, None, None, True),
            (datetime.date(2011, 10, 9), 'location2', 'season severity', None, None, 'severe', None, None),
            (datetime.date(2011, 10, 9), 'location2', 'cases next week', 3, None, None, None, None),
            (datetime.date(2011, 10, 9), 'location2', 'pct next week', None, 99.9, None, None, None),
            (
                datetime.date(2011, 10, 16), 'location1', 'Season peak week', None, None, None,
                datetime.date(2019, 12, 22),
                None),
            (datetime.date(2011, 10, 16), 'location1', 'above baseline', None, None, None, None, False),
            (datetime.date(2011, 10, 16), 'location1', 'cases next week', 0, None, None, None, None),
            (datetime.date(2011, 10, 16), 'location1', 'pct next week', None, 0.0, None, None, None)
        ]
        act_rows = truth_data_qs(project) \
            .values_list('forecast__time_zero__timezero_date', 'unit__name', 'target__name', 'value_i', 'value_f',
                         'value_t', 'value_d', 'value_b')
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    @unittest.skip("todo")
    def test_query_truth_for_project_null_rows(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)  # atomic
        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-null-value.csv'),
                        is_convert_na_none=True)

        exp_rows = [-1]  # todo xx
        act_rows = list(query_truth_for_project(project, {}))
        # print('xx', act_rows)
        # [['timezero', 'unit', 'target', 'value'],
        #  ['2011-10-02', 'location1', 'pct next week', None],
        #  ['2011-10-02', 'location1', 'cases next week', None],
        #  ['2011-10-02', 'location1', 'season severity', 'moderate'],
        #  ['2011-10-02', 'location1', 'above baseline', True],
        #  ['2011-10-02', 'location1', 'Season peak week', '2019-12-15'],
        #  ['2011-10-09', 'location2', 'pct next week', 99.9],
        #  ['2011-10-09', 'location2', 'cases next week', 3],
        #  ['2011-10-09', 'location2', 'season severity', 'severe'],
        #  ['2011-10-09', 'location2', 'above baseline', True],
        #  ['2011-10-09', 'location2', 'Season peak week', '2019-12-29'],
        #  ['2011-10-16', 'location1', 'pct next week', 0.0],
        #  ['2011-10-16', 'location1', 'cases next week', 0],
        #  ['2011-10-16', 'location1', 'above baseline', False],
        #  ['2011-10-16', 'location1', 'Season peak week', '2019-12-22']]
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    @unittest.skip("todo")
    def test__upload_truth_worker(self):
        # todo xx _upload_truth_worker(): is_convert_na_none=True: load_truth_data(project, cloud_file_fp, file_name=filename)
        self.fail()  # todo xx


    @unittest.skip("todo")
    def test_load_truth_data_dups(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)  # atomic
        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-null-value.csv'),
                        is_convert_na_none=True)
        self.assertEqual(-1, truth_data_qs(project).count())

        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-null-value.csv'),
                        is_convert_na_none=True)
        self.assertEqual(-1, truth_data_qs(project).count())  # s/b no change
