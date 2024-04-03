import datetime
import json
import time
from pathlib import Path

import django
from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APIClient

from forecast_app.models import Forecast, TimeZero, ForecastModel
from utils.forecast import load_predictions_from_json_io_dict, json_io_dict_from_forecast, cache_forecast_metadata, \
    forecast_metadata, data_rows_from_forecast
from utils.make_minimal_projects import _make_docs_project
from utils.project import models_summary_table_rows_for_project, latest_forecast_ids_for_project, \
    create_project_from_json, latest_forecast_cols_for_project
from utils.utilities import get_or_create_super_po_mo_users


class ForecastVersionsTestCase(TestCase):
    """
    Forecast queries:
    - query_forecasts_for_project(): handles optional `as_of`

    Project detail:
    - utils.project.models_summary_table_rows_for_project()

    Web ui:
    - TBC

    API:
    - serializers: include `issued_at`
    - model detail: include all versions
    - forecast queries: optional `issued_at`
    - TBC
    """


    def setUp(self):  # runs before every test. done here instead of setUpTestData(cls) b/c below tests modify the db
        self.client = APIClient()
        _, _, self.po_user, self.po_user_password, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        self.project, self.time_zero, self.forecast_model, self.forecast = _make_docs_project(self.po_user)
        self.tz1 = self.project.timezeros.get(timezero_date=datetime.date(2011, 10, 2))
        self.tz2 = self.project.timezeros.get(timezero_date=datetime.date(2011, 10, 9))
        self.tz3 = self.project.timezeros.get(timezero_date=datetime.date(2011, 10, 16))


    def test_multiple_forecasts_per_timezero(self):
        # note: we do not test forecast.issued_at b/c: 1) we can trust Forecast.created_at and Forecast.issued_at are
        # correct via auto_now_add, and 2) these are not always equal due to timezone differences:
        # self.forecast.created_at.date(), self.forecast.issued_at

        # test starting version counts
        self.assertEqual(1, len(Forecast.objects.filter(time_zero=self.time_zero, forecast_model__is_oracle=False)))
        self.assertEqual(0, len(Forecast.objects.filter(time_zero=self.tz2, forecast_model__is_oracle=False)))

        # add a second forecast to tz1, test count. first change issued_at so that we don't have an integrity error
        # (i.e., that it looks like the same version)
        self.forecast.issued_at -= datetime.timedelta(days=1)  # make it an older version
        self.forecast.save()

        forecast2 = Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)
        self.assertEqual(2, len(Forecast.objects.filter(time_zero=self.time_zero, forecast_model__is_oracle=False)))
        self.assertEqual(0, len(Forecast.objects.filter(time_zero=self.tz2, forecast_model__is_oracle=False)))

        # delete one of the forecasts, test count
        forecast2.delete()
        self.assertEqual(1, len(Forecast.objects.filter(time_zero=self.time_zero, forecast_model__is_oracle=False)))
        self.assertEqual(0, len(Forecast.objects.filter(time_zero=self.tz2, forecast_model__is_oracle=False)))

        # test that there is only one "version" of every forecast: exactly 0 or 1 forecasts are allowed with the same
        # (timezero_id, issued_at) 2-tuple per model. do so by trying to add a second forecast for that combination.
        # note that we do not test the exact message in context b/c it varies depending on the database/vendor:
        # - sqlite3: "UNIQUE constraint failed"
        # - postgres: "duplicate key value violates unique constraint"
        f1 = Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)
        with self.assertRaises(django.db.utils.IntegrityError) as context:
            Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero,
                                    issued_at=f1.issued_at)


    def test_models_summary_table_rows_for_project_case_1(self):
        # case 1/3: three timezeros, two forecasts, no versions:
        # tz1 -- f1 - tz1.date (issued_at)
        # tz2 -- f2 - tz2.date
        # tz3 -- x  - x
        f1 = self.forecast
        # per https://stackoverflow.com/questions/1937622/convert-date-to-datetime-in-python/1937636 :
        f1.issued_at = datetime.datetime.combine(self.tz1.timezero_date, datetime.time(), tzinfo=datetime.timezone.utc)
        f1.save()

        f2 = Forecast.objects.create(forecast_model=self.forecast_model, source='f2', time_zero=self.tz2,
                                     issued_at=datetime.datetime.combine(self.tz2.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))

        # NB: we have to work around a Django bug where DateField and DateTimeField come out of the database as either
        # datetime.date/datetime.datetime objects (postgres) or strings (sqlite3). also have to convert datetime to
        # comparable format using utctimetuple()
        exp_row = (self.forecast_model, 2,
                   str(f1.time_zero.timezero_date),  # oldest_forecast_tz_date
                   str(f2.time_zero.timezero_date),  # newest_forecast_tz_date
                   f2.id, f2.created_at.utctimetuple())  # id and created_at of ""
        act_rows = models_summary_table_rows_for_project(self.project)
        self.assertEqual(1, len(act_rows))

        act_row = (act_rows[0][0], act_rows[0][1],
                   str(act_rows[0][2]),  # oldest_forecast_tz_date
                   str(act_rows[0][3]),  # newest_forecast_tz_date
                   act_rows[0][4], act_rows[0][5].utctimetuple())  # id and created_at of ""
        self.assertEqual(exp_row, act_row)

        # test `latest_forecast_ids_for_project()` b/c it's convenient here
        exp_fm_tz_ids_to_f_id = {(self.forecast_model.id, self.tz1.id): f1.id,
                                 (self.forecast_model.id, self.tz2.id): f2.id}
        act_fm_tz_ids_to_f_id = latest_forecast_ids_for_project(self.project, False)
        self.assertEqual(exp_fm_tz_ids_to_f_id, act_fm_tz_ids_to_f_id)


    def test_models_summary_table_rows_for_project_case_2(self):
        # case 2/3: three timezeros, three forecasts, oldest tz has two versions:
        # tz1 -- f1 - tz1.date (issued_at)  v.1/2
        #     \- f2 - tz1.date + 1           v.2/2
        # tz2 -- f3 - tz2.date
        # tz3 -- x
        f1 = self.forecast
        f1.issued_at = datetime.datetime.combine(self.tz1.timezero_date, datetime.time(), tzinfo=datetime.timezone.utc)
        f1.save()

        f2 = Forecast.objects.create(forecast_model=self.forecast_model, source='f2', time_zero=self.tz1,
                                     issued_at=f1.issued_at + datetime.timedelta(days=1))

        f3 = Forecast.objects.create(forecast_model=self.forecast_model, source='f3', time_zero=self.tz2,
                                     issued_at=datetime.datetime.combine(self.tz3.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))

        exp_row = (self.forecast_model, 3,
                   str(f1.time_zero.timezero_date),  # oldest_forecast_tz_date
                   str(f3.time_zero.timezero_date),  # newest_forecast_tz_date
                   f3.id, f3.created_at.utctimetuple())  # id and created_at of ""
        act_rows = models_summary_table_rows_for_project(self.project)
        self.assertEqual(1, len(act_rows))

        act_row = (act_rows[0][0], act_rows[0][1],
                   str(act_rows[0][2]),  # oldest_forecast_tz_date
                   str(act_rows[0][3]),  # newest_forecast_tz_date
                   act_rows[0][4], act_rows[0][5].utctimetuple())  # id and created_at of ""
        self.assertEqual(exp_row, act_row)

        # test `latest_forecast_ids_for_project()` b/c it's convenient here
        exp_fm_tz_ids_to_f_id = {(self.forecast_model.id, self.tz1.id): f2.id,
                                 (self.forecast_model.id, self.tz2.id): f3.id}
        act_fm_tz_ids_to_f_id = latest_forecast_ids_for_project(self.project, False)
        self.assertEqual(exp_fm_tz_ids_to_f_id, act_fm_tz_ids_to_f_id)


    def test_models_summary_table_rows_for_project_case_3(self):
        # case 3/3: three timezeros, four forecasts, newest tz has two versions:
        # tz1 -- f1 - tz1.date (issued_at)
        # tz2 -- f2 - tz2.date
        # tz3 -- f3 - tz3.date       v.1/2
        #     \- f4 - tz3.date + 1   v.2/2
        f1 = self.forecast
        f1.issued_at = datetime.datetime.combine(self.tz1.timezero_date, datetime.time(), tzinfo=datetime.timezone.utc)
        f1.save()

        f2 = Forecast.objects.create(forecast_model=self.forecast_model, source='f2', time_zero=self.tz2,
                                     issued_at=datetime.datetime.combine(self.tz2.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))
        f3 = Forecast.objects.create(forecast_model=self.forecast_model, source='f3', time_zero=self.tz3,
                                     issued_at=datetime.datetime.combine(self.tz3.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))
        f4 = Forecast.objects.create(forecast_model=self.forecast_model, source='f4', time_zero=self.tz3,
                                     issued_at=f3.issued_at + datetime.timedelta(days=1))

        exp_row = (self.forecast_model, 4,
                   str(f1.time_zero.timezero_date),  # oldest_forecast_tz_date
                   str(f4.time_zero.timezero_date),  # newest_forecast_tz_date
                   f4.id, f4.created_at.utctimetuple())  # id and created_at of ""
        act_rows = models_summary_table_rows_for_project(self.project)
        self.assertEqual(1, len(act_rows))

        act_row = (act_rows[0][0], act_rows[0][1],
                   str(act_rows[0][2]),  # oldest_forecast_tz_date
                   str(act_rows[0][3]),  # newest_forecast_tz_date
                   act_rows[0][4], act_rows[0][5].utctimetuple())  # id and created_at of ""
        self.assertEqual(exp_row, act_row)

        # test `latest_forecast_ids_for_project()` b/c it's convenient here
        act_fm_tz_ids_to_f_id = latest_forecast_ids_for_project(self.project, False)
        exp_fm_tz_ids_to_f_id = {(self.forecast_model.id, self.tz1.id): f1.id,
                                 (self.forecast_model.id, self.tz2.id): f2.id,
                                 (self.forecast_model.id, self.tz3.id): f4.id}
        self.assertEqual(exp_fm_tz_ids_to_f_id, act_fm_tz_ids_to_f_id)


    # copied from test_views_and_rest_api.ViewsTestCase._authenticate_jwt_user
    def _authenticate_jwt_user(self, user, password):
        jwt_auth_url = reverse('auth-jwt-get')
        jwt_auth_resp = self.client.post(jwt_auth_url, {'username': user.username, 'password': password}, format='json')
        jwt_token = jwt_auth_resp.data['token']
        self.client.credentials(HTTP_AUTHORIZATION='JWT ' + jwt_token)
        return jwt_token


    def test_implicit_retractions(self):  # todo xx make sure subset rules are tested - uncomment special cases
        """
        Tests this forecast version rule: "An uploaded forecast version cannot imply any retracted prediction elements
        in existing versions." We test the four cases (including out-of-order upload ones): Consider uploading a
        version ("fn" - f new) when there are two existing ones (f1 and f2):

        forecast_id | issued_at | case
        ------------+------------+-----
        -            10/3         a) uploaded version has oldest issued_at
        f1           10/4
        -            10/5         b) uploaded version is between two issued_ats
        f2           10/6
        -            10/7         c) uploaded version has newest issued_at

        For these cases, uploading fn violates this rule if ("PE" = "prediction element"s "):
        - a)   fn's PEs  are a superset of  f1's PEs   [nnn][11]       [2222]        # } "visual" order
        - b1)  ""        are a subset of    f1's PEs        [11]  [n]  [2222]        # }   - left-to-right issued_at
        - b2)  ""        are a superset of  f2's PEs        [11][nnnnn][2222]        # }   - f1: 2 PEs, f2: 4 PEs
        - c)   ""        are a subset of    f2's PEs        [11]       [2222][nnn]   # }   - fn: various # PEs

        (Cases a and b2 are examples of nick's "Case B" out-of-order situations where the existing forecast with the
        later issue date and same timezero is missing prediction elements that are present in the new forecast with an
        earlier issue date.)
        """
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        tz1 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2020, 10, 4))
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            pred_dicts = json_io_dict['predictions']  # get some prediction elements to work with (29)

        # load progressively more data into f1 and f2. NB: it is OK to create f2 before we've loaded f1 b/c
        # _is_pred_eles_subset_prev_versions() handles the special case of. o/w it would be invalid (version validation will fail b/c
        # f2 will be empty and therefore f1 will be a superset of it)
        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1,
                                     issued_at=datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))
        # leave that one day gap between f1 and f2:
        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1,
                                     issued_at=f1.issued_at + datetime.timedelta(days=2))

        load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': pred_dicts[:2]})
        load_predictions_from_json_io_dict(f2, {'meta': {}, 'predictions': pred_dicts[:4]})

        # case a). NB: we cannot test this case b/c we added a second forecast version rule that supersedes it
        # ("found an earlier non-empty version")
        # f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3_case_a', time_zero=tz1)
        # f3.issued_at = f1.issued_at - datetime.timedelta(days=1)
        # f3.save()
        # with self.assertRaisesRegex(RuntimeError, 'forecast is a subset of next version'):
        #     load_predictions_from_json_io_dict(f3, {'meta': {}, 'predictions': pred_dicts[:3]})
        # f3.delete()

        # case b1). NB: cannot test for the same reason ("found an earlier non-empty version")
        # f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3_case_b2', time_zero=tz1)
        # f3.issued_at = f1.issued_at + datetime.timedelta(days=1)
        # f3.save()
        # with self.assertRaisesRegex(RuntimeError, 'previous version is a subset of forecast'):
        #     load_predictions_from_json_io_dict(f3, {'meta': {}, 'predictions': pred_dicts[:1]})
        # f3.delete()

        # case b2). NB: cannot test for the same reason ("found an earlier non-empty version")
        # f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3_case_b1', time_zero=tz1)
        # f3.issued_at = f1.issued_at + datetime.timedelta(days=1)
        # f3.save()
        # with self.assertRaisesRegex(RuntimeError, 'forecast is a subset of next version'):
        #     load_predictions_from_json_io_dict(f3, {'meta': {}, 'predictions': pred_dicts[:5]})
        # f3.delete()

        # case c)
        f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3_case_c', time_zero=tz1,
                                     issued_at=f2.issued_at + datetime.timedelta(days=1))
        with self.assertRaisesRegex(RuntimeError, 'new data is a subset of previous'):
            load_predictions_from_json_io_dict(f3, {'meta': {}, 'predictions': pred_dicts[:3]})


    def test_implicit_retractions_dups_interaction(self):
        """
        Tests the case where there are more than two versions, the second forecast had duplicates of the first, and the
        third has implicit retractions (i.e., had removed prediction elements) that were in the second forecast's
        removed duplicates. This test therefore ensures that the code that detects implicit retractions is joining all
        previous versions' data to essentially reassemble the original data.

        forecast_id | issued_at | case
        ------------+------------+-----
        f1           10/4          1st upload, no previous, so no duplicates to remove. all data is kept
        f2           10/5          2nd upload, some duplicates of f1, so only non-duplicates are kept
        f3           10/6          3rd upload, implicitly retracts by removing pred eles from f2's /duplicates/ of f1
        """
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        tz1 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2020, 10, 4))
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            pred_dicts = json_io_dict['predictions']  # get some prediction elements to work with (29)

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1,
                                     issued_at=datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))
        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1,
                                     issued_at=f1.issued_at + datetime.timedelta(days=1))
        f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3', time_zero=tz1,
                                     issued_at=f1.issued_at + datetime.timedelta(days=2))

        load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': pred_dicts[:2]})  # 1st upload. no dups
        load_predictions_from_json_io_dict(f2, {'meta': {}, 'predictions': pred_dicts[:4]})  # 0 & 1 are dups
        with self.assertRaisesRegex(RuntimeError, 'new data is a subset of previous'):
            load_predictions_from_json_io_dict(f3, {'meta': {}, 'predictions': pred_dicts[2:4]})  # 0 & 1 missing


    def test_non_subset_forecast_version_rules(self):
        """
        Tests these forecast rules:
        - cannot load empty data
        - cannot load 100% duplicate data
        - cannot position a new forecast before any existing versions
        - editing a version's issued_at cannot reposition it before any existing forecasts
        - cannot load data into a non-empty forecast
        - cannot delete a forecast that has any newer versions
        """
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        tz1 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2020, 10, 4))
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            pred_dicts = json_io_dict['predictions']  # get some prediction elements to work with (29)

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1,
                                     issued_at=datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))
        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1,
                                     issued_at=f1.issued_at + datetime.timedelta(days=1))

        # test "cannot load empty data"
        with self.assertRaisesRegex(RuntimeError, "cannot load empty data"):
            load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': []})  # no data

        # test "cannot load 100% duplicate data"
        load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': pred_dicts[:2]})
        with self.assertRaisesRegex(RuntimeError, "cannot load 100% duplicate data"):
            load_predictions_from_json_io_dict(f2, {'meta': {}, 'predictions': pred_dicts[:2]})

        # test "cannot position a new forecast before any existing versions"
        with self.assertRaisesRegex(RuntimeError, "you cannot position a new forecast before any existing versions"):
            Forecast.objects.create(forecast_model=forecast_model, time_zero=tz1,
                                    issued_at=datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                                        tzinfo=datetime.timezone.utc))

        # test "editing a version's issued_at cannot reposition it before any existing forecasts"
        with self.assertRaisesRegex(RuntimeError, "editing a version's issued_at cannot reposition it before any "
                                                  "existing forecasts"):
            # before an existing one:
            f3 = Forecast.objects.create(forecast_model=forecast_model, source='f3', time_zero=tz1)
            f3.issued_at = f1.issued_at + datetime.timedelta(days=-1)
            f3.save()

            # test "cannot load data into a non-empty forecast"
        with self.assertRaisesRegex(RuntimeError, "cannot load data into a non-empty forecast"):
            load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': pred_dicts[:2]})

        # test "cannot delete a forecast that has any newer versions"
        with self.assertRaisesRegex(RuntimeError, "you cannot delete a forecast that has any newer versions"):
            f1.delete()


    def test_json_io_dict_from_forecast_on_versions(self):
        def sort_key(pred_dict):
            return pred_dict['unit'], pred_dict['target'], pred_dict['class']


        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        tz1 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2020, 10, 4))
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')

        # create and load f1
        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1,
                                     issued_at=datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))

        predictions_1 = [  # docs-predictions.json: 0 and 1
            {"unit": "loc1", "target": "pct next week", "class": "point",
             "prediction": {"value": 2.1}},  # [0]
            {"unit": "loc1", "target": "pct next week", "class": "named",
             "prediction": {"family": "norm", "param1": 1.1, "param2": 2.2}},  # [1]
        ]
        load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': predictions_1})

        # create and load f2
        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1,
                                     issued_at=f1.issued_at + datetime.timedelta(days=1))

        predictions_2 = [  # docs-predictions.json: 0 through 2
            {"unit": "loc1", "target": "pct next week", "class": "point",
             "prediction": {"value": 2.1}},  # [0] dup
            {"unit": "loc1", "target": "pct next week", "class": "named",
             "prediction": {"family": "norm", "param1": 3.3, "param2": 2.2}},  # [1] param1 changed
            {"unit": "loc2", "target": "pct next week", "class": "point",
             "prediction": {"value": 2.0}},  # [2] new
        ]
        load_predictions_from_json_io_dict(f2, {'meta': {}, 'predictions': predictions_2})

        # test
        exp_predictions = sorted(predictions_1, key=sort_key)
        act_predictions = sorted(json_io_dict_from_forecast(f1, None)['predictions'], key=sort_key)  # ignore meta
        self.assertEqual(exp_predictions, act_predictions)

        exp_predictions = sorted(predictions_2, key=sort_key)
        act_predictions = sorted(json_io_dict_from_forecast(f2, None)['predictions'], key=sort_key)  # ignore meta
        self.assertEqual(exp_predictions, act_predictions)


    def test_json_io_dict_from_forecast_on_versions_as_of(self):
        """
        exposes a bug when running against sqlite where _query_forecasts_sql_for_pred_class() was using
        `as_of.isoformat()`, which sqlite does not like
        """
        forecasts = []
        # order is important:
        for json_preds_file in ['forecast_app/tests/predictions/docs-predictions-mix-retract-dup-edit.json',
                                'forecast_app/tests/predictions/docs-predictions-all-retracted.json']:
            json_file_path = Path(json_preds_file)
            with open(json_file_path) as json_fp:
                json_io_dict = json.load(json_fp)
                forecast = Forecast.objects.create(forecast_model=self.forecast_model, source=json_file_path.name,
                                                   time_zero=self.tz1)
                load_predictions_from_json_io_dict(forecast, json_io_dict, is_skip_validation=False)  # atomic
                forecasts.append(forecast)
                time.sleep(1)  # give issued_at a second

        f0_json = json_io_dict_from_forecast(self.forecast, None, True)  # is_include_retract
        f1_json = json_io_dict_from_forecast(forecasts[0], None, True)
        f2_json = json_io_dict_from_forecast(forecasts[1], None, True)
        with open('forecast_app/tests/predictions/exp-json-io-dict-from-forecast-as-of.json') as fp:
            exp_json_io_dicts = json.load(fp)
            for exp_pred_dict, act_pred_dict in [(exp_json_io_dicts[0], f0_json),
                                                 (exp_json_io_dicts[1], f1_json),
                                                 (exp_json_io_dicts[2], f2_json)]:
                self.assertEqual(exp_pred_dict, act_pred_dict)


    def test_cache_forecast_metadata_on_versions(self):
        """
        Tests that metadata is correctly calculated when there are forecast versions.
        """
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='case model', abbreviation='case_model')
        tz1 = project.timezeros.get(timezero_date=datetime.date(2011, 10, 2))

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1,
                                     issued_at=datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))
        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1,
                                     issued_at=f1.issued_at + datetime.timedelta(days=1))

        # load f1
        predictions = [
            {"unit": 'loc1', "target": 'cases next week', "class": "named",
             "prediction": {"family": "pois", "param1": 1.1}},
            {"unit": 'loc2', "target": 'cases next week', "class": "point", "prediction": {"value": 5}},
        ]
        load_predictions_from_json_io_dict(f1, {'predictions': predictions}, is_validate_cats=False)

        # load f2
        predictions = [
            {"unit": 'loc1', "target": 'cases next week', "class": "named", "prediction": None},  # retract
            {"unit": 'loc2', "target": 'cases next week', "class": "point", "prediction": None},  # retract
            {"unit": 'loc3', "target": 'Season peak week', "class": "sample",  # new
             "prediction": {"sample": ["2020-01-05", "2019-12-15"]}},
            {"unit": 'loc1', "target": 'pct next week', "class": "bin",  # new
             "prediction": {"cat": [1.1, 2.2, 3.3],
                            "prob": [0.3, 0.2, 0.5]}},
            {"unit": 'loc1', "target": 'Season peak week', "class": "quantile",  # new
             "prediction": {"quantile": [0.5, 0.75, 0.975],
                            "value": ["2019-12-22", "2019-12-29", "2020-01-05"]}},
        ]
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, is_validate_cats=False)

        # cache both
        cache_forecast_metadata(f1)
        cache_forecast_metadata(f2)

        # test f1
        # forecast_meta_prediction (pnbsq), forecast_meta_unit_qs, forecast_meta_target_qs:
        exp_meta = ((1, 1, 0, 0, 0, 0, 0, 0), {'loc1', 'loc2'}, {'cases next week'})
        act_meta = forecast_metadata(f1)
        act_fmp_counts = act_meta[0].point_count, act_meta[0].named_count, act_meta[0].bin_count, \
            act_meta[0].sample_count, act_meta[0].quantile_count, \
            act_meta[0].mean_count, act_meta[0].median_count, act_meta[0].mode_count
        act_fm_units = set([fmu.unit.abbreviation for fmu in act_meta[1]])
        act_fm_targets = set([fmu.target.name for fmu in act_meta[2]])
        self.assertEqual(exp_meta[0], act_fmp_counts)
        self.assertEqual(exp_meta[1], act_fm_units)
        self.assertEqual(exp_meta[2], act_fm_targets)

        # test f2
        # forecast_meta_prediction (pnbsq), forecast_meta_unit_qs, forecast_meta_target_qs:
        exp_meta = ((0, 0, 1, 1, 1, 0, 0, 0), {'loc1', 'loc3'}, {'pct next week', 'Season peak week'})
        act_meta = forecast_metadata(f2)
        act_fmp_counts = act_meta[0].point_count, act_meta[0].named_count, act_meta[0].bin_count, \
            act_meta[0].sample_count, act_meta[0].quantile_count, \
            act_meta[0].mean_count, act_meta[0].median_count, act_meta[0].mode_count
        act_fm_units = set([fmu.unit.abbreviation for fmu in act_meta[1]])
        act_fm_targets = set([fmu.target.name for fmu in act_meta[2]])
        self.assertEqual(exp_meta[0], act_fmp_counts)
        self.assertEqual(exp_meta[1], act_fm_units)
        self.assertEqual(exp_meta[2], act_fm_targets)


    def test_data_rows_from_forecast_on_versions(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='case model', abbreviation='case_model')
        tz1 = project.timezeros.get(timezero_date=datetime.date(2011, 10, 2))

        u1 = project.units.get(abbreviation='loc1')
        u2 = project.units.get(abbreviation='loc2')
        u3 = project.units.get(abbreviation='loc3')
        t1 = project.targets.get(name='cases next week')
        t2 = project.targets.get(name='pct next week')
        t3 = project.targets.get(name='Season peak week')

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1,
                                     issued_at=datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))
        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=tz1,
                                     issued_at=f1.issued_at + datetime.timedelta(days=1))

        # load f1
        predictions = [
            {"unit": 'loc1', "target": 'cases next week', "class": "named",
             "prediction": {"family": "pois", "param1": 1.1}},
            {"unit": 'loc2', "target": 'cases next week', "class": "point",
             "prediction": {"value": 5}},
            {"unit": 'loc1', "target": 'pct next week', "class": "bin",
             "prediction": {"cat": [1.1, 2.2, 3.3], "prob": [0.3, 0.2, 0.5]}},
        ]
        load_predictions_from_json_io_dict(f1, {'predictions': predictions}, is_validate_cats=False)

        # load f2
        predictions = [
            {"unit": 'loc1', "target": 'cases next week', "class": "named", "prediction":
                {"family": "pois", "param1": 2.2}},  # changed
            {"unit": 'loc1', "target": 'cases next week', "class": "point",
             "prediction": {"value": 6}},  # new
            {"unit": 'loc2', "target": 'cases next week', "class": "point",
             "prediction": None},  # retract
            {"unit": 'loc3', "target": 'Season peak week', "class": "sample",  # new
             "prediction": {"sample": ["2020-01-05", "2019-12-15"]}},
            {"unit": 'loc1', "target": 'pct next week', "class": "bin",  # dup
             "prediction": {"cat": [1.1, 2.2, 3.3], "prob": [0.3, 0.2, 0.5]}},
            {"unit": 'loc1', "target": 'Season peak week', "class": "quantile",  # new
             "prediction": {"quantile": [0.5, 0.75, 0.975], "value": ["2019-12-22", "2019-12-29", "2020-01-05"]}},
        ]
        load_predictions_from_json_io_dict(f2, {'predictions': predictions}, is_validate_cats=False)

        # rows: 8-tuple: (data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample,
        #                 data_rows_mean, data_rows_median, data_rows_mode)
        f_loc_targ_to_exp_rows = {
            (f1, u1, t1): ([], [('loc1', 'cases next week', 'pois', 1.1, None, None)], [], [], [], [], [], []),
            (f1, u1, t2): ([('loc1', 'pct next week', 1.1, 0.3),
                            ('loc1', 'pct next week', 2.2, 0.2),
                            ('loc1', 'pct next week', 3.3, 0.5)],
                           [], [], [], [], [], [], []),
            (f1, u1, t3): ([], [], [], [], [], [], [], []),
            (f1, u2, t1): ([], [], [('loc2', 'cases next week', 5)], [], [], [], [], []),
            (f1, u2, t2): ([], [], [], [], [], [], [], []),
            (f1, u2, t3): ([], [], [], [], [], [], [], []),
            (f1, u3, t1): ([], [], [], [], [], [], [], []),
            (f1, u3, t2): ([], [], [], [], [], [], [], []),
            (f1, u3, t3): ([], [], [], [], [], [], [], []),
            (f2, u1, t1): ([],
                           [('loc1', 'cases next week', 'pois', 2.2, None, None)],
                           [('loc1', 'cases next week', 6)],
                           [], [], [], [], []),
            (f2, u1, t2): ([('loc1', 'pct next week', 1.1, 0.3),
                            ('loc1', 'pct next week', 2.2, 0.2),
                            ('loc1', 'pct next week', 3.3, 0.5)],
                           [], [], [], [], [], [], []),  # will fail if doesn't merge w/older version
            (f2, u1, t3): ([], [], [],
                           [('loc1', 'Season peak week', 0.5, '2019-12-22'),
                            ('loc1', 'Season peak week', 0.75, '2019-12-29'),
                            ('loc1', 'Season peak week', 0.975, '2020-01-05')],
                           [], [], [], []),
            (f2, u2, t1): ([], [], [], [], [], [], [], []),
            (f2, u2, t2): ([], [], [], [], [], [], [], []),
            (f2, u2, t3): ([], [], [], [], [], [], [], []),
            (f2, u3, t1): ([], [], [], [], [], [], [], []),
            (f2, u3, t2): ([], [], [], [], [], [], [], []),
            (f2, u3, t3): ([], [], [], [],
                           [('loc3', 'Season peak week', '2020-01-05'),
                            ('loc3', 'Season peak week', '2019-12-15')], [], [], []),
        }
        for (forecast, unit, target), exp_rows in f_loc_targ_to_exp_rows.items():
            act_rows = data_rows_from_forecast(forecast, unit, target)
            self.assertEqual(exp_rows, act_rows)


    def test_latest_forecast_cols_for_project(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        tz1 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2020, 10, 4))
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1, notes='f1 notes',
                                     issued_at=datetime.datetime.combine(tz1.timezero_date, datetime.time(),
                                                                         tzinfo=datetime.timezone.utc))

        f2 = Forecast.objects.create(forecast_model=forecast_model, source='f\n2', time_zero=tz1, notes='f2\nnotes',
                                     issued_at=f1.issued_at + datetime.timedelta(days=1))

        # case: no columns (just 'id')
        exp_forecast_cols = [(f2.pk,)]
        act_forecast_cols = list(latest_forecast_cols_for_project(project, is_incl_fm_id=False, is_incl_tz_id=False,
                                                                  is_incl_issued_at=False, is_incl_created_at=False,
                                                                  is_incl_source=False, is_incl_notes=False))
        self.assertEqual(exp_forecast_cols, act_forecast_cols)

        # case: just source column
        exp_forecast_cols = [(f2.pk, f2.source)]
        act_forecast_cols = list(latest_forecast_cols_for_project(project, is_incl_fm_id=False, is_incl_tz_id=False,
                                                                  is_incl_issued_at=False, is_incl_created_at=False,
                                                                  is_incl_source=True, is_incl_notes=False))
        self.assertEqual(exp_forecast_cols, act_forecast_cols)

        # case: all columns. NB: utctimetuple() makes sqlite comparisons work
        exp_forecast_cols = [[f2.pk, f2.forecast_model.pk, f2.time_zero.pk, f2.issued_at.utctimetuple(),
                              f2.created_at.utctimetuple(), f2.source, f2.notes]]  # list, not tuple
        act_forecast_cols = list(latest_forecast_cols_for_project(project))  # list for generator
        act_forecast_cols[0] = list(act_forecast_cols[0])  # tuple -> list so I can assign:
        act_forecast_cols[0][3] = act_forecast_cols[0][3].utctimetuple()
        act_forecast_cols[0][4] = act_forecast_cols[0][4].utctimetuple()
        self.assertEqual(exp_forecast_cols, act_forecast_cols)
