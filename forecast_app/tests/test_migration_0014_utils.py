import datetime
import json
from pathlib import Path

from django.db.models import Q
from django.test import TestCase

from forecast_app.models import Forecast, ForecastModel, PredictionElement, TimeZero
from utils.forecast import load_predictions_from_json_io_dict, json_io_dict_from_forecast
from utils.make_minimal_projects import _make_docs_project
from utils.migration_0014_utils import _copy_new_data_to_old_tables, _delete_new_data, copy_old_data_to_new_tables, \
    delete_old_data, _num_rows_new_data, _num_rows_old_data, _pred_dicts_from_forecast_old, _grouped_version_rows, \
    is_different_old_new_json, pred_dicts_with_implicit_retractions, _forecast_previous_version, forecast_ids_with_no_data_new
from utils.project import create_project_from_json
from utils.project_truth import oracle_model_for_project
from utils.utilities import get_or_create_super_po_mo_users


class Migration0014TestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        cls.project, cls.time_zero, cls.forecast_model, cls.forecast = _make_docs_project(po_user)
        tz2 = cls.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
        cls.forecast2 = Forecast.objects.create(forecast_model=cls.forecast_model,
                                                source='docs-predictions-non-dup.json',
                                                time_zero=tz2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions-non-dup.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(cls.forecast2, json_io_dict_in, is_validate_cats=False)


    # def setUp(self):
    #     _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
    #     self.project, self.time_zero, self.forecast_model, self.forecast = _make_docs_project(po_user)
    #     tz2 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
    #     self.forecast2 = Forecast.objects.create(forecast_model=self.forecast_model,
    #                                              source='docs-predictions-non-dup.json',
    #                                              time_zero=tz2, notes="a small prediction file")
    #     with open('forecast_app/tests/predictions/docs-predictions-non-dup.json') as fp:
    #         json_io_dict_in = json.load(fp)
    #         load_predictions_from_json_io_dict(self.forecast2, json_io_dict_in, is_validate_cats=False)

    def test_copy(self):
        # starts with new data, but no old data. point_count, named_count, bin_count, sample_count, quantile_count:
        # - docs-predictions.json: 11 + 2 + 18 + 23 + 10 = 64
        # - docs-predictions-non-dup.json: 64 - 4 = 60
        self.assertEqual(0, _num_rows_old_data(self.forecast))
        self.assertEqual(0, _num_rows_old_data(self.forecast2))
        self.assertEqual(64, _num_rows_new_data(self.forecast))
        self.assertEqual(60, _num_rows_new_data(self.forecast2))
        self.assertEqual(29, self.forecast.pred_eles.count())
        self.assertEqual(29, self.forecast2.pred_eles.count())

        # copy new data -> old and ensure that there is now old data
        _copy_new_data_to_old_tables(self.project)
        self.assertEqual(64, _num_rows_old_data(self.forecast))
        self.assertEqual(60, _num_rows_old_data(self.forecast2))
        self.assertEqual(64, _num_rows_new_data(self.forecast))
        self.assertEqual(60, _num_rows_new_data(self.forecast2))
        self.assertEqual(29, self.forecast.pred_eles.count())
        self.assertEqual(29, self.forecast2.pred_eles.count())

        # delete the new data to create a real-life migration scenario: we have old data to migrate, and no new data
        _delete_new_data(self.project)
        self.assertEqual(64, _num_rows_old_data(self.forecast))
        self.assertEqual(60, _num_rows_old_data(self.forecast2))
        self.assertEqual(0, _num_rows_new_data(self.forecast))
        self.assertEqual(0, _num_rows_new_data(self.forecast2))
        self.assertEqual(0, self.forecast.pred_eles.count())
        self.assertEqual(0, self.forecast2.pred_eles.count())

        # test the actual migration utility
        copy_old_data_to_new_tables(self.forecast)
        copy_old_data_to_new_tables(self.forecast2)
        self.assertEqual(64, _num_rows_old_data(self.forecast))
        self.assertEqual(60, _num_rows_old_data(self.forecast2))
        self.assertEqual(64, _num_rows_new_data(self.forecast))
        self.assertEqual(60, _num_rows_new_data(self.forecast2))
        self.assertEqual(29, self.forecast.pred_eles.count())
        self.assertEqual(29, self.forecast2.pred_eles.count())

        # back to square one
        delete_old_data(self.forecast)
        delete_old_data(self.forecast2)
        self.assertEqual(0, _num_rows_old_data(self.forecast))
        self.assertEqual(0, _num_rows_old_data(self.forecast2))
        self.assertEqual(64, _num_rows_new_data(self.forecast))
        self.assertEqual(60, _num_rows_new_data(self.forecast2))
        self.assertEqual(29, self.forecast.pred_eles.count())
        self.assertEqual(29, self.forecast2.pred_eles.count())


    def test__pred_dicts_from_forecast_old(self):
        def sort_key(pred_dict):
            return pred_dict['unit'], pred_dict['target'], pred_dict['class']


        _copy_new_data_to_old_tables(self.project)  # copy itself is tested above
        for forecast in Forecast.objects.filter(forecast_model__project=self.project):
            json_io_dict_new = json_io_dict_from_forecast(forecast, None)
            json_io_dict_old = {'meta': {},
                                'predictions': sorted(_pred_dicts_from_forecast_old(forecast), key=sort_key)}
            json_io_dict_new['predictions'].sort(key=sort_key)
            self.assertEqual(json_io_dict_new, json_io_dict_old)


    def test__grouped_version_rows(self):
        self.forecast.issue_date = self.time_zero.timezero_date  # v1
        self.forecast.save()
        f1 = self.forecast
        f2 = self.forecast2
        f3 = Forecast.objects.create(forecast_model=self.forecast_model, source='f3', time_zero=self.time_zero)  # v2

        # is_versions_only = True
        exp_rows = [  # fm_id, tz_id, issue_date, f_id, f_source, f_created_at, rank  # NB: no: f_source, f_created_at
            (f1.forecast_model.pk, f1.time_zero.pk, f1.issue_date, f1.pk, 1),
            (f3.forecast_model.pk, f3.time_zero.pk, f3.issue_date, f3.pk, 2)]
        act_rows = sorted([(row[0], row[1], row[2], row[3], row[6])
                           for row in _grouped_version_rows(self.project, True)])
        self.assertEqual(sorted(exp_rows), act_rows)

        # is_versions_only = False
        exp_rows += [
            (f2.forecast_model.pk, f2.time_zero.pk, f2.issue_date, f2.pk, 1),
        ]
        exp_rows += [(f.forecast_model.pk, f.time_zero.pk, f.issue_date, f.pk, 1)
                     for f in oracle_model_for_project(self.project).forecasts.all()]
        act_rows = sorted([(row[0], row[1], row[2], row[3], row[6])
                           for row in _grouped_version_rows(self.project, False)])
        self.assertEqual(sorted(exp_rows), sorted(act_rows))


    def test__copy_new_data_to_old_tables_versions(self):
        """
        Tests that _copy_new_data_to_old_tables() "merges" previous versions when it copies a forecast's new data.
        """
        # setup from test_data_rows_from_forecast_on_versions()
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='case model', abbreviation='case_model')
        tz1 = project.timezeros.get(timezero_date=datetime.date(2011, 10, 2))

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        f1.issue_date = tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        f2.issue_date = f1.issue_date + datetime.timedelta(days=1)
        f2.save()

        # load f1
        predictions = [
            {"unit": 'location1', "target": 'cases next week', "class": "named",
             "prediction": {"family": "pois", "param1": 1.1}},
            {"unit": 'location2', "target": 'cases next week', "class": "point",
             "prediction": {"value": 5}},
            {"unit": 'location1', "target": 'pct next week', "class": "bin",
             "prediction": {"cat": [1.1, 2.2, 3.3], "prob": [0.3, 0.2, 0.5]}},
        ]
        load_predictions_from_json_io_dict(f1, {'predictions': predictions}, is_validate_cats=False)

        # load f2
        predictions = [
            {"unit": 'location1', "target": 'cases next week', "class": "named",
             "prediction": {"family": "pois", "param1": 2.2}},  # changed
            {"unit": 'location1', "target": 'cases next week', "class": "point",
             "prediction": {"value": 6}},  # new
            {"unit": 'location2', "target": 'cases next week', "class": "point",
             "prediction": None},  # retract
            {"unit": 'location3', "target": 'Season peak week', "class": "sample",  # new
             "prediction": {"sample": ["2020-01-05", "2019-12-15"]}},
            {"unit": 'location1', "target": 'pct next week', "class": "bin",  # dup
             "prediction": {"cat": [1.1, 2.2, 3.3], "prob": [0.3, 0.2, 0.5]}},
            {"unit": 'location1', "target": 'Season peak week', "class": "quantile",  # new
             "prediction": {"quantile": [0.5, 0.75, 0.975], "value": ["2019-12-22", "2019-12-29", "2020-01-05"]}},
        ]
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, is_validate_cats=False)

        # test
        _copy_new_data_to_old_tables(project)
        self.assertIsNone(is_different_old_new_json(f1))
        self.assertIsNone(is_different_old_new_json(f2))


    def test_add_implicit_retractions_json(self):
        # test starts with new data, but no old data. steps:
        # - load new f1 and f2 data
        # - invalidate new f2 by making it a subset of new f1 (delete pred eles)
        # - copy new f1 and f2 to old
        # - delete new f1 and f2 data (puts us into a pre-migrate state)
        # - migrate f1 (old -> new). should NOT error
        # - migrate f2 (old -> new). SHOULD error (subset)
        # - get pred_dicts_with_implicit_retractions(f1, f2)  # uses new data to calc diffs f1->f2, adds diffs to f2
        # - test ""
        # - load diff f2 into f2 new. should NOT error

        def sort_key(pred_dict):
            return pred_dict['unit'], pred_dict['target'], pred_dict['class']


        # setup from test_data_rows_from_forecast_on_versions()
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='case model', abbreviation='case_model')
        tz1 = project.timezeros.get(timezero_date=datetime.date(2011, 10, 2))

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        f1.issue_date = tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        f2.issue_date = f1.issue_date + datetime.timedelta(days=1)
        f2.save()

        # load f1
        predictions = [
            {"unit": 'location1', "target": 'cases next week', "class": "named",
             "prediction": {"family": "pois", "param1": 1.1}},
            {"unit": 'location2', "target": 'cases next week', "class": "point",
             "prediction": {"value": 5}},
            {"unit": 'location1', "target": 'pct next week', "class": "bin",
             "prediction": {"cat": [1.1, 2.2, 3.3],
                            "prob": [0.3, 0.2, 0.5]}},
            {"unit": "location2", "target": "pct next week", "class": "quantile",
             "prediction": {"quantile": [0.025, 0.25, 0.5, 0.75, 0.975],
                            "value": [1.0, 2.2, 2.2, 5.0, 50.0]}},
        ]
        load_predictions_from_json_io_dict(f1, {'predictions': predictions}, is_validate_cats=False)

        # load f2
        predictions = [
            {"unit": 'location1', "target": 'cases next week', "class": "named",  # changed
             "prediction": {"family": "pois", "param1": 2.2}},
            {"unit": 'location1', "target": 'cases next week', "class": "point",  # new
             "prediction": {"value": 6}},
            {"unit": 'location2', "target": 'cases next week', "class": "point",  # retract
             "prediction": None},
            {"unit": 'location3', "target": 'Season peak week', "class": "sample",  # new
             "prediction": {"sample": ["2020-01-05", "2019-12-15"]}},
            {"unit": 'location1', "target": 'pct next week', "class": "bin",  # dup
             "prediction": {"cat": [1.1, 2.2, 3.3], "prob": [0.3, 0.2, 0.5]}},
            {"unit": 'location1', "target": 'Season peak week', "class": "quantile",  # new
             "prediction": {"quantile": [0.5, 0.75, 0.975], "value": ["2019-12-22", "2019-12-29", "2020-01-05"]}},
            {"unit": "location2", "target": "pct next week", "class": "quantile",  # retract
             "prediction": None}]
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, is_validate_cats=False)

        # copy new to old, first deleting to make f2 invalid (keeping only points and quantiles), and then delete new
        pred_eles_to_keep = PredictionElement.objects \
            .filter(Q(forecast=f2) &
                    (Q(pred_class=PredictionElement.POINT_CLASS) | Q(pred_class=PredictionElement.QUANTILE_CLASS))) \
            .values_list('id', flat=True)
        PredictionElement.objects.filter(forecast=f2).exclude(pk__in=pred_eles_to_keep).delete()

        _copy_new_data_to_old_tables(project)
        PredictionElement.objects.filter(forecast=f1).delete()
        PredictionElement.objects.filter(forecast=f2).delete()

        # migrate f1 to set up state where f1 old has been migrated to f1 new, and f2 old is invalid (subset) and needs
        # fixing by pred_dicts_with_implicit_retractions(). first we test to ensure it's invalid
        copy_old_data_to_new_tables(f1)  # f1 migrates as expected. trust copy_old_data_to_new_tables() b/c tested above
        with self.assertRaisesRegex(RuntimeError, 'invalid forecast. new data is a subset of previous'):
            copy_old_data_to_new_tables(f2)  # f2 fails to migrate as expected

        # now we have f1 migrated but f2 failed due to its being a subset of f1. get corrected f2 with implicit
        # retractions, test that the retractions were added, and verify they load w/o subset error
        try:
            pred_eles_f1_not_in_f2, act_f2_pred_dicts_with_retractions = pred_dicts_with_implicit_retractions(f1, f2)
            act_f2_pred_dicts_with_retractions = sorted(act_f2_pred_dicts_with_retractions, key=sort_key)
            exp_f2_pred_dicts_with_retractions = [
                {'unit': 'location1', 'target': 'Season peak week', 'class': 'quantile',
                 'prediction': {'quantile': [0.5, 0.75, 0.975], 'value': ['2019-12-22', '2019-12-29', '2020-01-05']}},
                {'unit': 'location1', 'target': 'cases next week', 'class': 'named',
                 'prediction': {'family': 'pois', 'param1': 1.1}},
                {'unit': 'location1', 'target': 'cases next week', 'class': 'point',
                 'prediction': {'value': 6}},
                {'unit': 'location1', 'target': 'pct next week', 'class': 'bin',
                 'prediction': {'cat': [1.1, 2.2, 3.3], 'prob': [0.3, 0.2, 0.5]}},
                {'unit': 'location2', 'target': 'cases next week', 'class': 'point', 'prediction': None},  # new
                {'unit': 'location2', 'target': 'pct next week', 'class': 'quantile', 'prediction': None},  # new
            ]
            self.assertEqual(exp_f2_pred_dicts_with_retractions, act_f2_pred_dicts_with_retractions)

            load_predictions_from_json_io_dict(f2, {'meta': {}, 'predictions': act_f2_pred_dicts_with_retractions})
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")


    def test_json_io_dict_from_forecast_is_include_retract(self):
        # setup from test_implicit_retractions_dups_interaction()
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        tz1 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2020, 10, 4))
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        f1.issue_date = tz1.timezero_date
        f1.save()

        predictions = [{"unit": 'location1', "target": 'pct next week', "class": "point", "prediction": None},
                       {"unit": 'location2', "target": 'pct next week', "class": "point", "prediction": {"value": 2.0}}]
        load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': predictions})

        exp_json_io_dict = [
            {"unit": 'location1', "target": 'pct next week', "class": "point", "prediction": None},
            {'unit': 'location2', 'target': 'pct next week', 'class': 'point', 'prediction': {'value': 2.0}}]
        act_json_io_dict = json_io_dict_from_forecast(f1, None, True)['predictions']
        self.assertEqual(exp_json_io_dict, act_json_io_dict)


    def test_add_implicit_retractions_json_3_versions(self):
        """
        Exposes a bug in `pred_dicts_with_implicit_retractions()` where retracted prediction elements were not factored
        in.
        """


        def sort_key(pred_dict):
            return pred_dict['unit'], pred_dict['target'], pred_dict['class']


        # setup from test_implicit_retractions_dups_interaction()
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        tz1 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2020, 10, 4))
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        f1.issue_date = tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        f2.issue_date = f1.issue_date + datetime.timedelta(days=1)
        f2.save()

        predictions = [  # f1
            {"unit": 'location1', "target": 'pct next week', "class": "point", "prediction": None}
        ]
        load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': predictions})

        predictions = [  # f2
            # this one deleted after loading to become an implict retraction in old data:
            {"unit": 'location1', "target": 'pct next week', "class": "point", "prediction": {"value": 1.0}},  # mod
        ]
        load_predictions_from_json_io_dict(f2, {'meta': {}, 'predictions': predictions})

        # create the implicit retraction in new data
        PredictionElement.objects.filter(forecast=f2, unit__name='location1', target__name='pct next week',
                                         pred_class=PredictionElement.POINT_CLASS) \
            .delete()

        # copy new -> old and then delete f2 new to set up for pred_dicts_with_implicit_retractions() call
        _copy_new_data_to_old_tables(project)
        PredictionElement.objects.filter(forecast=f2).delete()

        # test pred_dicts_with_implicit_retractions()
        exp_pred_dicts_with_retractions = [
            {"unit": 'location1', "target": 'pct next week', "class": "point", "prediction": None},
        ]
        pred_eles_f1_not_in_f2, act_pred_dicts_with_retractions = pred_dicts_with_implicit_retractions(f1, f2)
        sorted(act_pred_dicts_with_retractions, key=sort_key)
        self.assertEqual(exp_pred_dicts_with_retractions, act_pred_dicts_with_retractions)


    def test__forecast_previous_version(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='case model', abbreviation='case_model')
        tz1 = project.timezeros.get(timezero_date=datetime.date(2011, 10, 2))

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        f1.issue_date = tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        f2.issue_date = f1.issue_date + datetime.timedelta(days=1)
        f2.save()

        f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3', time_zero=tz1)
        f3.issue_date = f1.issue_date + datetime.timedelta(days=2)
        f3.save()

        self.assertEqual(None, _forecast_previous_version(f1))
        self.assertEqual(f1, _forecast_previous_version(f2))
        self.assertEqual(f2, _forecast_previous_version(f3))


    def test_forecast_ids_with_no_data_new(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        tz1 = self.project.timezeros.get(timezero_date=datetime.date(2011, 10, 2))
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            pred_dicts = json_io_dict['predictions']  # get some prediction elements to work with (29)

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        f1.issue_date = tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1)
        f2.issue_date = f1.issue_date + datetime.timedelta(days=1)
        f2.save()

        self.assertEqual([f1.pk, f2.pk], forecast_ids_with_no_data_new())

        load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': pred_dicts[:2]})
        self.assertEqual([f2.pk], forecast_ids_with_no_data_new())

        load_predictions_from_json_io_dict(f2, {'meta': {}, 'predictions': pred_dicts[:4]})
        self.assertEqual([], forecast_ids_with_no_data_new())
