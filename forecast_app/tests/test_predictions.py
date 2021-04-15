import datetime
import json
import unittest
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Forecast, PredictionElement, PredictionData
from forecast_app.models import ForecastModel, TimeZero
from forecast_app.models.prediction_element import PRED_CLASS_NAME_TO_INT
from forecast_app.tests.test_project_queries import ProjectQueriesTestCase
from utils.forecast import load_predictions_from_json_io_dict, _validated_pred_ele_rows_for_pred_dicts
from utils.make_minimal_projects import _make_docs_project
from utils.project import create_project_from_json
from utils.project_queries import query_truth_for_project, query_forecasts_for_project
from utils.project_truth import load_truth_data, truth_data_qs
from utils.utilities import get_or_create_super_po_mo_users


#
# initial single file for driving zoltar2 development. todo xx will be split into separate ones as the code develops
#

class PredictionsTestCase(TestCase):
    """
    """


    def test_hash_for_prediction_dict(self):
        for exp_hash, prediction_dict in [
            ('845e3d041b6be23a381b6afd263fb113', {"family": "pois", "param1": 1.1}),
            ('2ed5d7d59eb10044644ab28a1b292efb', {"value": 5}),
            ('74135c30ddfd5427c8b1e86b2989a642', {"sample": [0, 2, 5]}),
            ('a74ea3f2472e0aec511eb1f604282220', {"cat": [0, 2, 50], "prob": [0.0, 0.1, 0.9]}),
            ('838e6e3f77075f69eef3bb3d7bcdffdc', {"quantile": [0.25, 0.75], "value": [0, 50]}),
            ('bc55989f596fd157ccc6e3279b1f694a', {"value": "mild"}),
            ('ac263a19694da72f65e903c2ec2000d1', {"cat": ["mild", "moderate", "severe"], "prob": [0.0, 0.1, 0.9]}),
            ('19d0e94bc24114abfa0d07ca41b8b3bf', {"value": True}),
            ('1b98c3c7b5b09d3ba0ea43566d5e9d03', {"cat": [True, False], "prob": [0.9, 0.1]}),
            ('c74e3f626224eeb482368d9fb7a387da',
             {"cat": ["2019-12-15", "2019-12-22", "2019-12-29"], "prob": [0.01, 0.1, 0.89]}),
        ]:
            self.assertEqual(exp_hash, PredictionElement.hash_for_prediction_data_dict(prediction_dict))


    def test_load_predictions_from_json_io_dict_existing_pred_eles(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        json_io_dict = {"predictions": [{"unit": "location1",
                                         "target": "pct next week",
                                         "class": "point",
                                         "prediction": {"value": 2.1}}]}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(forecast, json_io_dict)
        self.assertIn("cannot load data into a non-empty forecast", str(context.exception))


    def test_load_predictions_from_json_io_dict_phase_1(self):
        # tests pass 1/2 of load_predictions_from_json_io_dict(). NB: implicitly covers test_hash_for_prediction_dict()
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict, is_validate_cats=False)

        # test PredictionElement.forecast and is_retract
        self.assertEqual(29, forecast.pred_eles.count())
        self.assertEqual(0, PredictionElement.objects.filter(is_retract=True).count())

        exp_rows = [('point', 'location1', 'pct next week', '2c343e1ea37e8b493c219066a8664276'),
                    ('named', 'location1', 'pct next week', '58a7f8487958446d57333b262aaa8271'),
                    ('point', 'location2', 'pct next week', '2b9db448ae1a3b7065ffee67d4857268'),
                    ('bin', 'location2', 'pct next week', '7d1485af48de540dbcd954ee5cba51cb'),
                    ('quantile', 'location2', 'pct next week', '0d3698e4e39456b8e36c750d73bb6870'),
                    ('point', 'location3', 'pct next week', '5d321ea39f0af08cb3f40a58fa7c54d4'),
                    ('sample', 'location3', 'pct next week', '0b431a76d5ad343981944c4b0792d738'),
                    ('named', 'location1', 'cases next week', '845e3d041b6be23a381b6afd263fb113'),
                    ('point', 'location2', 'cases next week', '2ed5d7d59eb10044644ab28a1b292efb'),
                    ('sample', 'location2', 'cases next week', '74135c30ddfd5427c8b1e86b2989a642'),
                    ('point', 'location3', 'cases next week', 'a6ff82cc0637f67254df41352e1c00f9'),
                    ('bin', 'location3', 'cases next week', 'a74ea3f2472e0aec511eb1f604282220'),
                    ('quantile', 'location3', 'cases next week', '838e6e3f77075f69eef3bb3d7bcdffdc'),
                    ('point', 'location1', 'season severity', 'bc55989f596fd157ccc6e3279b1f694a'),
                    ('bin', 'location1', 'season severity', 'ac263a19694da72f65e903c2ec2000d1'),
                    ('point', 'location2', 'season severity', 'ec5add7ea7a8abf3e68e9570d0b73898'),
                    ('sample', 'location2', 'season severity', '51d07bda3e8a39da714f5767d93704ff'),
                    ('point', 'location1', 'above baseline', '19d0e94bc24114abfa0d07ca41b8b3bf'),
                    ('bin', 'location2', 'above baseline', '1b98c3c7b5b09d3ba0ea43566d5e9d03'),
                    ('sample', 'location2', 'above baseline', 'ae168d5bfdad1463672120d51787fed2'),
                    ('sample', 'location3', 'above baseline', '380b79bea27bfa66e8864cfec9e3403a'),
                    ('point', 'location1', 'Season peak week', 'fad04bc4cd443ca7cd7cd53f5de4fa99'),
                    ('bin', 'location1', 'Season peak week', 'c74e3f626224eeb482368d9fb7a387da'),
                    ('sample', 'location1', 'Season peak week', '2f0fdc8a293046d38eb912601cf0a5cf'),
                    ('point', 'location2', 'Season peak week', '39c511635eb21cfde3657ab144521b94'),
                    ('bin', 'location2', 'Season peak week', '4fa62ed754c3fc9b9ede90926efe8f7f'),
                    ('quantile', 'location2', 'Season peak week', 'd06cb30665b099e471c6dd9d50ba2c30'),
                    ('point', 'location3', 'Season peak week', 'f15fab078daf9adb53f464272b31dbf6'),
                    ('sample', 'location3', 'Season peak week', '213d829834bceaaa4376a79b989161c3'), ]
        pred_data_qs = PredictionElement.objects \
            .filter(forecast=forecast) \
            .values_list('pred_class', 'unit__name', 'target__name', 'data_hash') \
            .order_by('id')
        act_rows = [(PredictionElement.prediction_class_int_as_str(row[0]), row[1], row[2], row[3])
                    for row in pred_data_qs]
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    def test_load_predictions_from_json_io_dict(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)

        # test json with no 'predictions'
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(forecast, {}, is_validate_cats=False)
        self.assertIn("json_io_dict had no 'predictions' key", str(context.exception))

        # test loading all five types of Predictions
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict, is_validate_cats=False)

        # test prediction element counts match number in .json file
        pred_ele_qs = forecast.pred_eles.all()
        pred_data_qs = PredictionData.objects.filter(pred_ele__forecast=forecast)
        self.assertEqual(29, len(pred_ele_qs))
        self.assertEqual(29, len(pred_data_qs))

        # test there's a prediction element for every .json item
        unit_name_to_obj = {unit.name: unit for unit in project.units.all()}
        target_name_to_obj = {target.name: target for target in project.targets.all()}
        for pred_ele_dict in json_io_dict['predictions']:
            unit = unit_name_to_obj[pred_ele_dict['unit']]
            target = target_name_to_obj[pred_ele_dict['target']]
            pred_class_int = PRED_CLASS_NAME_TO_INT[pred_ele_dict['class']]
            data_hash = PredictionElement.hash_for_prediction_data_dict(pred_ele_dict['prediction'])
            pred_ele = pred_ele_qs.filter(pred_class=pred_class_int, unit=unit, target=target, is_retract=False,
                                          data_hash=data_hash).first()
            self.assertIsNotNone(pred_ele)
            self.assertIsNotNone(pred_data_qs.filter(pred_ele=pred_ele).first())


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
            _validated_pred_ele_rows_for_pred_dicts(forecast, bad_prediction_dicts, False, False)
        self.assertIn('prediction_dict referred to an undefined Unit', str(context.exception))

        # test for invalid target
        with self.assertRaises(RuntimeError) as context:
            bad_prediction_dicts = [
                {"unit": "location1", "target": "bad target", "class": "bad class", "prediction": {}}
            ]
            _validated_pred_ele_rows_for_pred_dicts(forecast, bad_prediction_dicts, False, False)
        self.assertIn('prediction_dict referred to an undefined Target', str(context.exception))

        # test for invalid pred_class
        with self.assertRaises(RuntimeError) as context:
            bad_prediction_dicts = [
                {"unit": "location1", "target": "pct next week", "class": "bad class", "prediction": {}}
            ]
            _validated_pred_ele_rows_for_pred_dicts(forecast, bad_prediction_dicts, False, False)
        self.assertIn('invalid pred_class', str(context.exception))


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
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, is_validate_cats=False)
        self.assertEqual(5, f2.pred_eles.count())
        self.assertEqual('', f2.pred_eles.first().data_hash)

        # test loading an initial version that includes retractions (we are sure what this means, but it is valid and
        # should not fail :-)
        f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3', time_zero=tz2)
        load_predictions_from_json_io_dict(f3, {'predictions': predictions}, is_validate_cats=False)
        self.assertEqual(5, f3.pred_eles.count())
        self.assertEqual('', f3.pred_eles.first().data_hash)

        # test querying same
        try:
            rows = list(query_forecasts_for_project(project, {}))  # list for generator
            self.assertEqual(1, len(rows))  # header
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")


    def test_load_predictions_from_json_io_dict_dups(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        tz1 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2020, 10, 4))
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            pred_dicts = json_io_dict['predictions']  # get some prediction elements to work with (29)

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        f1.issue_date = tz1.timezero_date
        f1.save()
        load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': pred_dicts[:-2]})  # all but last 2 PEs

        # case: load the just-loaded file into a separate timezero -> should load all rows (duplicates are only within
        # the same timezero)
        tz2 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
        f2 = Forecast.objects.create(forecast_model=forecast_model, time_zero=tz2)
        load_predictions_from_json_io_dict(f2, {'meta': {}, 'predictions': pred_dicts[:-1]},  # all but last PE
                                           is_validate_cats=False)
        self.assertEqual(27, f1.pred_eles.count())
        self.assertEqual(28, f2.pred_eles.count())
        self.assertEqual(27 + 28, project.num_pred_ele_rows_all_models(is_oracle=False))

        # case: load the same predictions into a different version -> none should load (they're all duplicates)
        f1.issue_date -= datetime.timedelta(days=1)
        f1.save()
        f3 = Forecast.objects.create(forecast_model=forecast_model, time_zero=tz1)
        load_predictions_from_json_io_dict(f3, json_io_dict, is_validate_cats=False)
        self.assertEqual(27, f1.pred_eles.count())
        self.assertEqual(28, f2.pred_eles.count())
        self.assertEqual(2, f3.pred_eles.count())  # 2 were new (non-dup)
        self.assertEqual(27 + 28 + 2, project.num_pred_ele_rows_all_models(is_oracle=False))

        # case: load the same file, but change one multi-row prediction (a sample) to have partial duplication
        f3.issue_date -= datetime.timedelta(days=2)
        f3.save()
        quantile_pred_dict = [pred_dict for pred_dict in json_io_dict['predictions']
                              if (pred_dict['unit'] == 'location2')
                              and (pred_dict['target'] == 'pct next week')
                              and (pred_dict['class'] == 'quantile')][0]
        # original: {"quantile": [0.025, 0.25, 0.5, 0.75,  0.975 ],
        #            "value":    [1.0,   2.2,  2.2,  5.0, 50.0  ]}
        quantile_pred_dict['prediction']['value'][0] = 2.2  # was 1.0
        f4 = Forecast.objects.create(forecast_model=forecast_model, time_zero=tz1)
        load_predictions_from_json_io_dict(f4, json_io_dict, is_validate_cats=False)
        self.assertEqual(1, f4.pred_eles.count())
        self.assertEqual(27 + 28 + 2 + 1, project.num_pred_ele_rows_all_models(is_oracle=False))


    #
    # test "retracted" and skipped predictions for truth
    #

    @unittest.skip("todo")
    def test_load_truth_data_null_rows(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
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
            .values_list('pred_ele__forecast__time_zero__timezero_date',
                         'pred_ele__unit__name', 'pred_ele__target__name',
                         'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    @unittest.skip("todo")
    def test_query_truth_for_project_null_rows(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-null-value.csv'),
                        is_convert_na_none=True)

        exp_rows = [-1]  # todo xx
        act_rows = list(query_truth_for_project(project, {}))
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    @unittest.skip("todo")
    def test__upload_truth_worker(self):
        # todo xx _upload_truth_worker(): is_convert_na_none=True: load_truth_data(project, cloud_file_fp, file_name=filename)
        self.fail()  # todo xx


    @unittest.skip("todo")
    def test_load_truth_data_dups(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-null-value.csv'),
                        is_convert_na_none=True)
        self.assertEqual(-1, truth_data_qs(project).count())

        load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth-null-value.csv'),
                        is_convert_na_none=True)
        self.assertEqual(-1, truth_data_qs(project).count())
