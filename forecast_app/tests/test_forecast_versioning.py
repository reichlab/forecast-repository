import datetime
from unittest.mock import patch

import django
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from forecast_app.models import Forecast
from utils.make_minimal_projects import _make_docs_project
from utils.project import models_summary_table_rows_for_project, latest_forecast_ids_for_project
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
    - serializers: include `issue_date`
    - model detail: include all versions
    - forecast queries: optional `issue_date`
    - TBC
    """


    def setUp(self):  # runs before every test. done here instead of setUpTestData(cls) b/c below tests modify the db
        self.client = APIClient()
        _, _, self.po_user, self.po_user_password, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        self.project, self.time_zero, self.forecast_model, self.forecast = _make_docs_project(self.po_user)
        self.tz1 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 2)).first()
        self.tz2 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
        self.tz3 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 16)).first()


    def test_multiple_forecasts_per_timezero(self):
        # note: we do not test forecast.issue_date b/c: 1) we can trust Forecast.created_at and Forecast.issue_date are
        # correct via auto_now_add, and 2) these are not always equal due to timezone differences:
        # self.forecast.created_at.date(), self.forecast.issue_date

        # test starting version counts
        self.assertEqual(1, len(Forecast.objects.filter(time_zero=self.time_zero, forecast_model__is_oracle=False)))
        self.assertEqual(0, len(Forecast.objects.filter(time_zero=self.tz2, forecast_model__is_oracle=False)))

        # add a second forecast to tz1, test count. first change issue_date so that we don't have an integrity error
        # (i.e., that it looks like the same version)
        self.forecast.issue_date -= datetime.timedelta(days=1)  # make it an older version
        self.forecast.save()

        forecast2 = Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)
        self.assertEqual(2, len(Forecast.objects.filter(time_zero=self.time_zero, forecast_model__is_oracle=False)))
        self.assertEqual(0, len(Forecast.objects.filter(time_zero=self.tz2, forecast_model__is_oracle=False)))

        # delete one of the forecasts, test count
        forecast2.delete()
        self.assertEqual(1, len(Forecast.objects.filter(time_zero=self.time_zero, forecast_model__is_oracle=False)))
        self.assertEqual(0, len(Forecast.objects.filter(time_zero=self.tz2, forecast_model__is_oracle=False)))

        # test that there is only one "version" of every forecast: exactly 0 or 1 forecasts are allowed with the same
        # (timezero_id, issue_date) 2-tuple per model. do so by trying to add a second forecast for that combination.
        # note that this could technically fail if the date changes between the two Forecast.objects.create() calls
        # because Forecast.issue_date is auto_now_add. note that we do not test the exact message in context b/c it
        # varies depending on the database/vendor:
        # - sqlite3: "UNIQUE constraint failed"
        # - postgres: "duplicate key value violates unique constraint"
        Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)
        with self.assertRaises(django.db.utils.IntegrityError) as context:
            Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)


    def test_multiple_forecasts_per_timezero_api_upload(self):
        # similar to test_api_upload_forecast(). to avoid the requirement of RQ, redis, and S3, we patch _upload_file()
        # to return (is_error, job) with desired return args. NB: this test is vulnerable to an edge case where
        # midnight is spanned between when setUp() creates its forecast and post() creates the second one to test
        # integrity constraints. in that case the two forecasts will have different issue_dates and therefore not
        # conflict, and this test will fail
        with patch('forecast_app.views._upload_file') as upload_file_mock:
            upload_forecast_url = reverse('api-forecast-list', args=[str(self.forecast_model.pk)])
            data_file = SimpleUploadedFile('file.csv', b'file_content', content_type='text/csv')

            # case: existing_forecast_for_time_zero
            jwt_token = self._authenticate_jwt_user(self.po_user, self.po_user_password)
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': '2011-10-02',  # self.tz1
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
            self.assertIn("new forecast was not a unique version", json_response.json()['error'])


    def test_models_summary_table_rows_for_project_case_1(self):
        # case 1/3: three timezeros, two forecasts, no versions:
        # tz1 -- f1 - tz1.date (issue_date)
        # tz2 -- f2 - tz2.date
        # tz3 -- x  - x
        f1 = self.forecast
        f1.issue_date = self.tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=self.forecast_model, source='f2', time_zero=self.tz2)
        f2.issue_date = self.tz2.timezero_date
        f2.save()

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
        # tz1 -- f1 - tz1.date (issue_date)  v.1/2
        #     \- f2 - tz1.date + 1           v.2/2
        # tz2 -- f3 - tz2.date
        # tz3 -- x
        f1 = self.forecast
        f1.issue_date = self.tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=self.forecast_model, source='f2', time_zero=self.tz1)
        f2.issue_date = self.tz1.timezero_date + datetime.timedelta(days=1)
        f2.save()

        f3 = Forecast.objects.create(forecast_model=self.forecast_model, source='f3', time_zero=self.tz2)
        f3.issue_date = self.tz2.timezero_date
        f3.save()

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
        # tz1 -- f1 - tz1.date (issue_date)
        # tz2 -- f2 - tz2.date
        # tz3 -- f3 - tz3.date       v.1/2
        #     \- f4 - tz3.date + 1   v.2/2
        f1 = self.forecast
        f1.issue_date = self.tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=self.forecast_model, source='f2', time_zero=self.tz2)
        f2.issue_date = self.tz2.timezero_date
        f2.save()

        f3 = Forecast.objects.create(forecast_model=self.forecast_model, source='f3', time_zero=self.tz3)
        f3.issue_date = self.tz3.timezero_date
        f3.save()

        f4 = Forecast.objects.create(forecast_model=self.forecast_model, source='f4', time_zero=self.tz3)
        f4.issue_date = self.tz3.timezero_date + datetime.timedelta(days=1)
        f4.save()

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
