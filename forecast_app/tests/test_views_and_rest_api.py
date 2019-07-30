import datetime
import json
import logging
from pathlib import Path
from unittest.mock import patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from forecast_app.api_views import SCORE_CSV_HEADER_PREFIX
from forecast_app.models import Project, ForecastModel, TimeZero, Forecast
from forecast_app.models.upload_file_job import UploadFileJob
from utils.cdc import load_cdc_csv_forecast_file, CDC_CSV_HEADER
from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, get_or_create_super_po_mo_users, \
    CDC_CONFIG_DICT
from utils.utilities import YYYYMMDD_DATE_FORMAT


# todo has no affect on errors like:
# WARNING 2018-11-02 10:48:20,606 exception 5530 140735224639488 Forbidden (Permission denied): /api/project/2/template/
logging.getLogger().setLevel(logging.ERROR)


class ViewsTestCase(TestCase):
    """
    Tests view and API authorization.
    """


    def setUp(self):
        self.client = APIClient()


    @classmethod
    def setUpTestData(cls):
        # users
        cls.superuser, cls.superuser_password, cls.po_user, cls.po_user_password, cls.mo_user, cls.mo_user_password \
            = get_or_create_super_po_mo_users(create_super=True)

        # public_project
        cls.public_project = Project.objects.create(name='public project name', is_public=True,
                                                    owner=cls.po_user, config_dict=CDC_CONFIG_DICT)
        cls.public_project.model_owners.add(cls.mo_user)
        cls.public_project.save()
        make_cdc_locations_and_targets(cls.public_project)

        TimeZero.objects.create(project=cls.public_project, timezero_date=datetime.date(2017, 1, 1))
        cls.public_project.load_truth_data(Path('forecast_app/tests/truth_data/truths-ok.csv'), 'truths-ok.csv')

        cls.public_tz1 = TimeZero.objects.create(project=cls.public_project, timezero_date=datetime.date(2017, 12, 1),
                                                 data_version_date=None)
        cls.public_tz2 = TimeZero.objects.create(project=cls.public_project, timezero_date=datetime.date(2017, 12, 2),
                                                 data_version_date=None)

        cls.upload_file_job = UploadFileJob.objects.create(user=cls.po_user)

        # private_project
        cls.private_project = Project.objects.create(name='private project name', is_public=False,
                                                     owner=cls.po_user, config_dict=CDC_CONFIG_DICT)
        cls.private_project.model_owners.add(cls.mo_user)
        cls.private_project.save()
        make_cdc_locations_and_targets(cls.private_project)
        cls.private_tz1 = TimeZero.objects.create(project=cls.private_project,
                                                  timezero_date=datetime.date(2017, 12, 3),
                                                  data_version_date=None)
        cls.private_tz2 = TimeZero.objects.create(project=cls.private_project,
                                                  timezero_date=datetime.date(2017, 12, 4),
                                                  data_version_date=None)

        cls.public_project2 = Project.objects.create(name='public project 2', is_public=True,
                                                     owner=cls.po_user, config_dict=CDC_CONFIG_DICT)
        cls.public_project2.model_owners.add(cls.mo_user)
        cls.public_project2.save()

        # public_model
        cls.csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')
        cls.public_model = ForecastModel.objects.create(project=cls.public_project, name='public model',
                                                        description='', home_url='http://example.com',
                                                        owner=cls.mo_user)
        cls.public_forecast = load_cdc_csv_forecast_file(cls.public_model, cls.csv_file_path, cls.public_tz1)

        # private_model
        cls.private_model = ForecastModel.objects.create(project=cls.private_project, name='private model',
                                                         description='', home_url='http://example.com',
                                                         owner=cls.mo_user)
        cls.private_forecast = load_cdc_csv_forecast_file(cls.private_model, cls.csv_file_path, cls.private_tz1)

        # user/response pairs for testing authorization
        cls.OK_ALL = [(None, status.HTTP_200_OK),
                      (cls.po_user, status.HTTP_200_OK),
                      (cls.mo_user, status.HTTP_200_OK),
                      (cls.superuser, status.HTTP_200_OK)]
        cls.ONLY_PO_MO = [(None, status.HTTP_403_FORBIDDEN),
                          (cls.po_user, status.HTTP_200_OK),
                          (cls.mo_user, status.HTTP_200_OK),
                          (cls.superuser, status.HTTP_200_OK)]
        cls.ONLY_PO_MO_302 = [(None, status.HTTP_403_FORBIDDEN),
                              (cls.po_user, status.HTTP_302_FOUND),
                              (cls.mo_user, status.HTTP_302_FOUND),
                              (cls.superuser, status.HTTP_302_FOUND)]
        cls.ONLY_PO = [(None, status.HTTP_403_FORBIDDEN),
                       (cls.po_user, status.HTTP_200_OK),
                       (cls.mo_user, status.HTTP_403_FORBIDDEN),
                       (cls.superuser, status.HTTP_200_OK)]
        cls.ONLY_PO_302 = [(None, status.HTTP_403_FORBIDDEN),
                           (cls.po_user, status.HTTP_302_FOUND),
                           (cls.mo_user, status.HTTP_403_FORBIDDEN),
                           (cls.superuser, status.HTTP_302_FOUND)]
        cls.ONLY_SU_200 = [(None, status.HTTP_403_FORBIDDEN),
                           (cls.po_user, status.HTTP_403_FORBIDDEN),
                           (cls.mo_user, status.HTTP_403_FORBIDDEN),
                           (cls.superuser, status.HTTP_200_OK)]
        cls.ONLY_SU_302 = [(None, status.HTTP_403_FORBIDDEN),
                           (cls.po_user, status.HTTP_403_FORBIDDEN),
                           (cls.mo_user, status.HTTP_403_FORBIDDEN),
                           (cls.superuser, status.HTTP_302_FOUND)]


    @patch('forecast_app.models.forecast.Forecast.delete')  # 'delete-forecast'
    # 'create-project' -> form
    # 'edit-project' -> form
    @patch('forecast_app.models.project.Project.delete')  # 'delete-project'
    # 'create-model' -> form
    # 'edit-model' -> form
    @patch('forecast_app.models.forecast_model.ForecastModel.delete')  # 'delete-model'
    def test_url_access(self, mock_delete_model, mock_delete_project, mock_delete_forecast):
        url_to_exp_user_status_code_pairs = {
            reverse('index'): self.OK_ALL,
            reverse('about'): self.OK_ALL,
            reverse('docs'): self.OK_ALL,

            reverse('user-detail', args=[str(self.po_user.pk)]): self.ONLY_PO,
            reverse('upload-file-job-detail', args=[str(self.upload_file_job.pk)]): self.ONLY_PO,

            reverse('zadmin'): self.ONLY_SU_200,
            reverse('empty-rq'): self.ONLY_SU_302,
            reverse('clear-row-count-caches'): self.ONLY_SU_302,
            reverse('update-row-count-caches'): self.ONLY_SU_302,
            reverse('clear-score-csv-file-caches'): self.ONLY_SU_302,
            reverse('update-score-csv-file-caches'): self.ONLY_SU_302,
            reverse('update-all-scores'): self.ONLY_SU_302,
            reverse('delete-file-jobs'): self.ONLY_SU_302,
            reverse('clear-all-scores'): self.ONLY_SU_302,
            reverse('delete-score-last-updates'): self.ONLY_SU_302,

            reverse('project-detail', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('project-detail', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('project-visualizations', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('project-visualizations', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('project-scores', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('project-scores', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('project-score-data', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('project-score-data', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('download-scores', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('download-scores', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('create-project', args=[]): self.ONLY_PO_MO,
            reverse('edit-project', args=[str(self.public_project.pk)]): self.ONLY_PO,
            reverse('edit-project', args=[str(self.private_project.pk)]): self.ONLY_PO,
            reverse('delete-project', args=[str(self.public_project.pk)]): self.ONLY_PO_302,
            reverse('delete-project', args=[str(self.private_project.pk)]): self.ONLY_PO_302,
            reverse('delete-project', args=[str(self.public_project.pk)]): self.ONLY_PO_302,
            reverse('delete-project', args=[str(self.private_project.pk)]): self.ONLY_PO_302,

            reverse('truth-data-detail', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('truth-data-detail', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('delete-truth', args=[str(self.public_project.pk)]): self.ONLY_PO_302,
            reverse('delete-truth', args=[str(self.private_project.pk)]): self.ONLY_PO_302,
            reverse('upload-truth', args=[str(self.public_project.pk)]): self.ONLY_PO,
            reverse('upload-truth', args=[str(self.private_project.pk)]): self.ONLY_PO,
            reverse('download-truth', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('download-truth', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,

            reverse('model-detail', args=[str(self.public_model.pk)]): self.OK_ALL,
            reverse('model-detail', args=[str(self.private_model.pk)]): self.ONLY_PO_MO,
            reverse('create-model', args=[str(self.public_project.pk)]): self.ONLY_PO_MO,
            reverse('create-model', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('edit-model', args=[str(self.public_model.pk)]): self.ONLY_PO_MO,
            reverse('edit-model', args=[str(self.private_model.pk)]): self.ONLY_PO_MO,
            reverse('delete-model', args=[str(self.public_model.pk)]): self.ONLY_PO_MO_302,
            reverse('delete-model', args=[str(self.private_model.pk)]): self.ONLY_PO_MO_302,

            reverse('forecast-detail', args=[str(self.public_forecast.pk)]): self.OK_ALL,
            reverse('forecast-detail', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO,
            reverse('delete-forecast', args=[str(self.public_forecast.pk)]): self.ONLY_PO_MO_302,
            reverse('delete-forecast', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO_302,
            reverse('upload-forecast', args=[str(self.public_model.pk), str(self.public_tz1.pk)]): self.ONLY_PO_MO,
            reverse('upload-forecast', args=[str(self.private_model.pk), str(self.public_tz1.pk)]): self.ONLY_PO_MO,
            reverse('download-forecast', args=[str(self.public_forecast.pk)]): self.OK_ALL,
            reverse('download-forecast', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO,
        }

        # 'download-forecast' returns BAD_REQ_400 b/c they expect a POST with a 'format' parameter, and we don't pass
        # the correct query params. however, 400 does indicate that the code passed the authorization portion
        for url, user_exp_status_code_list in url_to_exp_user_status_code_pairs.items():
            for user, exp_status_code in user_exp_status_code_list:
                self.client.logout()  # AnonymousUser
                if user:
                    password = self.po_user_password if user == self.po_user \
                        else self.mo_user_password if user == self.mo_user \
                        else self.superuser_password
                    self.client.login(username=user.username, password=password)
                response = self.client.get(url, data={'location': None, 'target': None})
                self.assertEqual(exp_status_code, response.status_code)


    def test_edit_delete_upload_create_links(self):
        url_to_exp_content = {
            # model detail page for public model
            reverse('model-detail', args=[str(self.public_model.pk)]): {
                reverse('edit-model', args=[str(self.public_model.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
                reverse('delete-model', args=[str(self.public_model.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
                reverse('delete-forecast', args=[str(self.public_forecast.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
                reverse('upload-forecast', args=[str(self.public_model.pk), str(self.public_tz2.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
            },
            # model detail page for private model. this is the only private model test as we depend on other tests
            # to check accessibility. this is a sanity check, in other words :-)
            reverse('model-detail', args=[str(self.private_model.pk)]): {
                reverse('edit-model', args=[str(self.private_model.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
                reverse('delete-model', args=[str(self.private_model.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
                reverse('delete-forecast', args=[str(self.private_forecast.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
                reverse('upload-forecast', args=[str(self.private_model.pk), str(self.private_tz2.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
            },
            # home page
            reverse('index', args=[]): {
                reverse('create-project', args=[]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
            },
            # project detail - public_project (has truth)
            reverse('project-detail', args=[str(self.public_project.pk)]): {
                reverse('edit-project', args=[str(self.public_project.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True)],
                reverse('delete-project', args=[str(self.public_project.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True)],
                reverse('create-model', args=[str(self.public_project.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
            },
            # project detail - public_project2 (no truth)
            reverse('project-detail', args=[str(self.public_project2.pk)]): {
                reverse('upload-truth', args=[str(self.public_project2.pk)]):  # no truth -> upload link
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True)],
            },
            reverse('truth-data-detail', args=[str(self.public_project.pk)]): {
                reverse('delete-truth', args=[str(self.public_project.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True)],
            },
            # user detail - public_project (has truth)
            reverse('user-detail', args=[str(self.po_user.pk)]): {
                reverse('edit-user', args=[str(self.po_user.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True)],
            },
        }
        for url, url_to_user_access_pairs in url_to_exp_content.items():
            for exp_url, user_access_pairs in url_to_user_access_pairs.items():
                for user, is_accessible in user_access_pairs:
                    self.client.logout()  # AnonymousUser should not see any edit/delete/upload/create buttons
                    response = self.client.get(url)
                    self.assertNotIn(exp_url, str(response.content))

                    password = self.po_user_password if user == self.po_user \
                        else self.mo_user_password if user == self.mo_user \
                        else self.superuser_password
                    self.client.login(username=user.username, password=password)
                    response = self.client.get(url)
                    if is_accessible:
                        self.assertIn(exp_url, str(response.content))
                    else:
                        self.assertNotIn(exp_url, str(response.content))


    # https://stackoverflow.com/questions/47576635/django-rest-framework-jwt-unit-test
    def test_api_jwt_auth(self):
        # recall from base.py: ROOT_URLCONF = 'forecast_repo.urls'
        jwt_auth_url = reverse('auth-jwt-get')

        # test invalid user
        resp = self.client.post(jwt_auth_url, {'username': self.po_user.username, 'password': 'badpass'}, format='json')
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)

        # test valid user: self.po_user, self.po_user_password
        resp = self.client.post(jwt_auth_url, {'username': self.po_user.username, 'password': self.po_user_password},
                                format='json')
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertTrue('token' in resp.data)
        token = resp.data['token']
        # e.g., eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxLCJ1c2VybmFtZSI6InByb2plY3Rfb3duZXIxIiwiZXhwIjoxNTM1MzgwMjM0LCJlbWFpbCI6IiJ9.T_mHlvd3EjeAPhKRZwipyLhklV5StBQ_tRJ9YR-v8sA


    def test_api_get_endpoints(self):
        url_to_exp_user_status_code_pairs = {
            reverse('api-root'): self.OK_ALL,
            reverse('api-project-list'): self.OK_ALL,
            reverse('api-project-detail', args=[self.public_project.pk]): self.OK_ALL,
            reverse('api-project-detail', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-score-data', args=[self.public_project.pk]): self.OK_ALL,
            reverse('api-score-data', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-user-detail', args=[self.po_user.pk]): self.ONLY_PO,
            reverse('api-upload-file-job-detail', args=[self.upload_file_job.pk]): self.ONLY_PO,
            reverse('api-model-detail', args=[self.public_model.pk]): self.OK_ALL,
            reverse('api-model-detail', args=[self.private_model.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-list', args=[self.public_model.pk]): self.OK_ALL,
            reverse('api-forecast-list', args=[self.private_model.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-detail', args=[self.public_forecast.pk]): self.OK_ALL,
            reverse('api-forecast-detail', args=[self.private_forecast.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-data', args=[self.public_forecast.pk]): self.OK_ALL,
            reverse('api-forecast-data', args=[self.private_forecast.pk]): self.ONLY_PO_MO,
        }
        for url, user_exp_status_code_list in url_to_exp_user_status_code_pairs.items():
            for user, exp_status_code in user_exp_status_code_list:
                # authenticate using JWT. used instead of web API self.client.login() authentication elsewhere b/c
                # base.py configures JWT: REST_FRAMEWORK > DEFAULT_AUTHENTICATION_CLASSES > JSONWebTokenAuthentication
                self.client.logout()  # AnonymousUser
                if user:
                    self.authenticate_jwt_user(user)
                response = self.client.get(url)
                self.assertEqual(exp_status_code, response.status_code)


    def test_api_get_project_list_authorization(self):
        # verify filtering based on user authorization

        # anonymous access: self.public_project, self.public_project2
        self.client.logout()  # AnonymousUser
        # a rest_framework.utils.serializer_helpers.ReturnDict:
        response = self.client.get(reverse('api-project-list'), format='json')
        self.assertEqual({self.public_project.id, self.public_project2.id},
                         {proj_resp_dict['id'] for proj_resp_dict in response.data})

        # authorized access: self.mo_user: self.public_project, self.private_project, self.public_project2
        self.authenticate_jwt_user(self.mo_user)
        response = self.client.get(reverse('api-project-list'), format='json')
        self.assertEqual({self.public_project.id, self.private_project.id, self.public_project2.id},
                         {proj_resp_dict['id'] for proj_resp_dict in response.data})


    def test_api_get_endpoint_keys(self):
        """
        Tests returned value keys as a content sanity check.
        """
        # 'api-root' - a rest_framework.response.Response:
        response = self.client.get(reverse('api-root'), format='json')
        self.assertEqual(['projects'], list(response.data.keys()))

        # 'api-project-list' - a rest_framework.utils.serializer_helpers.ReturnList:
        #  (per-user authorization tested in test_api_project_list_authorization())
        response = self.client.get(reverse('api-project-list'), format='json')
        self.assertEqual(2, len(response.data))

        # 'api-project-detail' - a rest_framework.utils.serializer_helpers.ReturnDict:
        response = self.client.get(reverse('api-project-detail', args=[self.public_project.pk]), format='json')
        exp_keys = ['id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'core_data', 'config_dict',
                    'truth', 'model_owners', 'score_data', 'models', 'locations', 'targets', 'timezeros']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-user-detail' - a rest_framework.response.Response:
        # (o/w AttributeError: 'HttpResponseForbidden' object has no attribute 'data')
        self.client.login(username=self.po_user.username, password=self.po_user_password)
        response = self.client.get(reverse('api-user-detail', args=[self.po_user.pk]), format='json')
        exp_keys = ['id', 'url', 'username', 'owned_models', 'projects_and_roles']
        self.client.logout()  # AnonymousUser
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-model-detail' - a rest_framework.response.Response:
        response = self.client.get(reverse('api-model-detail', args=[self.public_model.pk]), format='json')
        exp_keys = ['id', 'url', 'project', 'owner', 'name', 'abbreviation', 'description', 'home_url', 'aux_data_url',
                    'forecasts']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-forecast-list' - a rest_framework.response.Response:
        response = self.client.get(reverse('api-forecast-list', args=[self.public_model.pk]), format='json')
        response_dicts = json.loads(response.content)
        exp_keys = ['id', 'url', 'forecast_model', 'csv_filename', 'time_zero', 'forecast_data']
        self.assertEqual(1, len(response_dicts))
        self.assertEqual(exp_keys, list(response_dicts[0].keys()))

        # 'api-forecast-detail' - a rest_framework.response.Response:
        response = self.client.get(reverse('api-forecast-detail', args=[self.public_forecast.pk]), format='json')
        exp_keys = ['id', 'url', 'forecast_model', 'csv_filename', 'time_zero', 'forecast_data']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-forecast-data' - a django.http.response.JsonResponse:
        # (note that we only check top-level keys b/c we know json_response_for_forecast() uses
        # json_io_dict_from_forecast(), which is tested separately)
        response = self.client.get(reverse('api-forecast-data', args=[self.public_forecast.pk]), format='json')
        response_dict = json.loads(response.content)
        exp_keys = ['forecast', 'locations', 'targets', 'predictions']
        self.assertEqual(exp_keys, list(response_dict.keys()))


    def test_api_delete_endpoints(self):
        # anonymous delete: self.public_forecast -> disallowed
        self.client.logout()  # AnonymousUser
        response = self.client.delete(reverse('api-forecast-detail', args=[self.public_forecast.pk]))
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        # authorized self.mo_user: delete private_forecast2 (new Forecast) -> allowed
        self.authenticate_jwt_user(self.mo_user)
        self.assertEqual(1, self.private_model.forecasts.count())

        private_forecast2 = load_cdc_csv_forecast_file(self.private_model, self.csv_file_path, self.private_tz1)
        private_forecast2_pk = private_forecast2.pk
        self.assertEqual(2, self.private_model.forecasts.count())

        response = self.client.delete(reverse('api-forecast-detail', args=[private_forecast2.pk]))
        self.assertEqual(status.HTTP_204_NO_CONTENT, response.status_code)
        self.assertIsNone(Forecast.objects.filter(pk=private_forecast2_pk).first())  # is deleted
        self.assertEqual(1, self.private_model.forecasts.count())  # is no longer in list


    def test_data_download_formats(self):
        """
        Test forecast_data().
        """
        # forecast data as JSON. a django.http.response.JsonResponse:
        response = self.client.get(reverse('api-forecast-data', args=[self.public_forecast.pk]))
        response_dict = json.loads(response.content)  # will fail if not JSON
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], "application/json")
        self.assertEqual(response['Content-Disposition'], 'attachment; filename="EW1-KoTsarima-2017-01-17.csv.json"')
        self.assertEqual(list(response_dict), ['forecast', 'locations', 'targets', 'predictions'])
        self.assertEqual(len(response_dict['locations']), 11)

        # score data as CSV. a django.http.response.HttpResponse
        response = self.client.get(reverse('download-scores', args=[self.public_project.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], "text/csv")
        self.assertEqual(response['Content-Disposition'], 'attachment; filename="public_project_name-scores.csv"')
        split_content = response.content.decode("utf-8").split('\r\n')
        self.assertEqual(split_content[0], ','.join(SCORE_CSV_HEADER_PREFIX))
        self.assertEqual(len(split_content), 2)  # no score data


    def test_api_upload_forecast(self):
        # to avoid the requirement of RQ, redis, and S3, we patch _upload_file() to return (is_error, upload_file_job)
        # with desired return args
        with patch('forecast_app.views._upload_file') as upload_file_mock:
            upload_forecast_url = reverse('api-forecast-list', args=[str(self.public_model.pk)])
            data_file = SimpleUploadedFile('file.csv', b'file_content', content_type='text/csv')
            jwt_token = self.authenticate_jwt_user(self.mo_user)

            # case: no 'data_file'
            json_response = self.client.post(upload_forecast_url, {
                'Authorization': 'JWT {}'.format(jwt_token),
                'timezero_date': self.public_tz2.timezero_date.strftime(YYYYMMDD_DATE_FORMAT),
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: no 'timezero_date'
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': 'JWT {}'.format(jwt_token),
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: invalid 'timezero_date' format - YYYYMMDD_DATE_FORMAT
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': 'JWT {}'.format(jwt_token),
                'timezero_date': 'x20171202',
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: existing_forecast_for_time_zero
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': 'JWT {}'.format(jwt_token),
                'timezero_date': self.public_tz1.timezero_date.strftime(YYYYMMDD_DATE_FORMAT),  # public_tz1
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: blue sky: _upload_file() -> NOT is_error
            upload_file_mock.return_value = False, UploadFileJob.objects.create()  # is_error, upload_file_job
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': 'JWT {}'.format(jwt_token),
                'timezero_date': self.public_tz2.timezero_date.strftime(YYYYMMDD_DATE_FORMAT),
            }, format='multipart')
            self.assertEqual(status.HTTP_200_OK, json_response.status_code)

            call_dict = upload_file_mock.call_args[1]
            self.assertIn('forecast_model_pk', call_dict)
            self.assertIn('timezero_pk', call_dict)
            self.assertEqual(self.public_model.pk, call_dict['forecast_model_pk'])
            self.assertEqual(self.public_tz2.pk, call_dict['timezero_pk'])

            act_time_zero = TimeZero.objects.get(pk=call_dict['timezero_pk'])
            self.assertEqual(self.public_tz2.timezero_date, act_time_zero.timezero_date)

            # case: _upload_file() -> is_error
            upload_file_mock.return_value = True, None  # is_error, upload_file_job
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': 'JWT {}'.format(jwt_token),
                'timezero_date': self.public_tz2.timezero_date.strftime(YYYYMMDD_DATE_FORMAT),
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: blue sky: auto-creates time_zero if not found. no data_version_date
            upload_file_mock.return_value = False, UploadFileJob.objects.create()  # is_error, upload_file_job
            new_timezero_date = '19621022'
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': 'JWT {}'.format(jwt_token),
                'timezero_date': new_timezero_date,  # doesn't exist
            }, format='multipart')
            self.assertEqual(status.HTTP_200_OK, json_response.status_code)

            call_dict = upload_file_mock.call_args[1]
            act_time_zero = TimeZero.objects.get(pk=call_dict['timezero_pk'])
            self.assertEqual(new_timezero_date, act_time_zero.timezero_date.strftime(YYYYMMDD_DATE_FORMAT))

            # case: blue sky: auto-creates time_zero if not found. valid data_version_date
            act_time_zero.delete()
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': 'JWT {}'.format(jwt_token),
                'timezero_date': new_timezero_date,  # doesn't exist
                'data_version_date': self.public_tz2.timezero_date.strftime(YYYYMMDD_DATE_FORMAT)
            }, format='multipart')
            self.assertEqual(status.HTTP_200_OK, json_response.status_code)

            call_dict = upload_file_mock.call_args[1]
            act_time_zero = TimeZero.objects.get(pk=call_dict['timezero_pk'])
            self.assertEqual(self.public_tz2.timezero_date, act_time_zero.data_version_date)

            # case: blue sky: auto-creates time_zero if not found. invalid data_version_date
            act_time_zero.delete()
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': 'JWT {}'.format(jwt_token),
                'timezero_date': new_timezero_date,  # doesn't exist
                'data_version_date': 'x20171202',
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)


    def authenticate_jwt_user(self, user):
        password = self.po_user_password if user == self.po_user \
            else self.mo_user_password if user == self.mo_user \
            else self.superuser_password
        jwt_auth_url = reverse('auth-jwt-get')
        jwt_auth_resp = self.client.post(jwt_auth_url, {'username': user.username, 'password': password}, format='json')
        jwt_token = jwt_auth_resp.data['token']
        self.client.credentials(HTTP_AUTHORIZATION='JWT ' + jwt_token)
        return jwt_token
