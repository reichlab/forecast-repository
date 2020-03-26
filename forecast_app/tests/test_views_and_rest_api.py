import datetime
import json
import logging
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth.models import User
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient, APIRequestFactory

from forecast_app.models import Project, ForecastModel, TimeZero, Forecast
from forecast_app.models.upload_file_job import UploadFileJob
from forecast_app.serializers import TargetSerializer
from utils.cdc import load_cdc_csv_forecast_file, make_cdc_units_and_targets
from utils.project import delete_project_iteratively, load_truth_data, create_project_from_json
from utils.utilities import YYYY_MM_DD_DATE_FORMAT, get_or_create_super_po_mo_users


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
            = get_or_create_super_po_mo_users(is_create_super=True)

        # public_project
        cls.public_project = Project.objects.create(name='public project name', is_public=True, owner=cls.po_user)
        cls.public_project.model_owners.add(cls.mo_user)
        cls.public_project.save()
        make_cdc_units_and_targets(cls.public_project)

        TimeZero.objects.create(project=cls.public_project, timezero_date=datetime.date(2017, 1, 1))
        load_truth_data(cls.public_project, Path('forecast_app/tests/truth_data/truths-ok.csv'),
                        is_convert_na_none=True)

        cls.public_tz1 = TimeZero.objects.create(project=cls.public_project, timezero_date=datetime.date(2017, 12, 1),
                                                 data_version_date=None)
        cls.public_tz2 = TimeZero.objects.create(project=cls.public_project, timezero_date=datetime.date(2017, 12, 2),
                                                 data_version_date=None)

        cls.upload_file_job = UploadFileJob.objects.create(user=cls.po_user)

        # private_project
        cls.private_project = Project.objects.create(name='private project name', is_public=False, owner=cls.po_user)
        cls.private_project.model_owners.add(cls.mo_user)
        cls.private_project.save()
        make_cdc_units_and_targets(cls.private_project)
        cls.private_tz1 = TimeZero.objects.create(project=cls.private_project,
                                                  timezero_date=datetime.date(2017, 12, 3),
                                                  data_version_date=None)
        cls.private_tz2 = TimeZero.objects.create(project=cls.private_project,
                                                  timezero_date=datetime.date(2017, 12, 4),
                                                  data_version_date=None)

        cls.public_project2 = Project.objects.create(name='public project 2', is_public=True, owner=cls.po_user)
        # cls.public_project2.model_owners.add(cls.mo_user)
        cls.public_project2.save()

        # public_model
        cls.csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')  # EW01 2017

        # create some models to bump up ID in case of accidental passing where model IDs == project IDs :-)
        ForecastModel.objects.create(project=cls.public_project, name='public model', description='',
                                     home_url='http://example.com', owner=cls.mo_user)
        ForecastModel.objects.create(project=cls.public_project, name='public model', description='',
                                     home_url='http://example.com', owner=cls.mo_user)
        ForecastModel.objects.create(project=cls.public_project, name='public model', description='',
                                     home_url='http://example.com', owner=cls.mo_user)

        ForecastModel.objects.create(project=cls.public_project, name='public model', description='',
                                     home_url='http://example.com', owner=cls.mo_user)
        cls.public_model = ForecastModel.objects.create(project=cls.public_project, name='public model',
                                                        description='', home_url='http://example.com',
                                                        owner=cls.mo_user)
        cls.public_forecast = load_cdc_csv_forecast_file(2016, cls.public_model, cls.csv_file_path, cls.public_tz1)

        # private_model
        cls.private_model = ForecastModel.objects.create(project=cls.private_project, name='private model',
                                                         description='', home_url='http://example.com',
                                                         owner=cls.mo_user)
        cls.private_forecast = load_cdc_csv_forecast_file(2016, cls.private_model, cls.csv_file_path, cls.private_tz1)

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


    # the following @patch calls stop CRUD calls from actually taking place. all we care about here is access permissions
    @patch('forecast_app.models.forecast.Forecast.delete')  # 'delete-forecast'
    # 'create-project-from-form' -> form
    # 'edit-project-from-form' -> form
    @patch('utils.project.delete_project_iteratively')  # 'delete-project'
    # 'create-model' -> form
    # 'edit-model' -> form
    @patch('forecast_app.models.forecast_model.ForecastModel.delete')  # 'delete-model'
    def test_url_get_general(self, mock_delete_model, mock_delete_project, mock_delete_forecast):
        url_to_exp_user_status_code_pairs = {
            reverse('index'): self.OK_ALL,
            reverse('about'): self.OK_ALL,

            reverse('user-detail', args=[str(self.po_user.pk)]): self.ONLY_PO,
            reverse('edit-user', args=[str(self.po_user.pk)]): self.ONLY_PO,
            reverse('change-password'): self.ONLY_PO_MO,
            reverse('upload-file-job-detail', args=[str(self.upload_file_job.pk)]): self.ONLY_PO,

            reverse('zadmin'): self.ONLY_SU_200,
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
            reverse('download-project-scores', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('download-project-scores', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('project-config', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('project-config', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('create-project-from-form', args=[]): self.ONLY_PO_MO,
            reverse('create-project-from-file', args=[]): self.ONLY_PO_MO,
            reverse('edit-project-from-form', args=[str(self.public_project.pk)]): self.ONLY_PO,
            reverse('edit-project-from-form', args=[str(self.private_project.pk)]): self.ONLY_PO,
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
                response = self.client.get(url, data={'unit': None, 'target': None})
                self.assertEqual(exp_status_code, response.status_code)


    def test_url_edit_delete_upload_create_links(self):
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
            # project list
            reverse('projects', args=[]): {
                reverse('create-project-from-form', args=[]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
                reverse('create-project-from-file', args=[]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
            },
            # project detail - public_project (has truth)
            reverse('project-detail', args=[str(self.public_project.pk)]): {
                reverse('edit-project-from-form', args=[str(self.public_project.pk)]):
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
                reverse('change-password'):
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


    def test_url_post_edit_project_from_file(self):
        # for both 'edit-project-from-file-preview' and 'edit-project-from-file-execute', only po_user and superuser can
        # POST, and to both public and private projects. anonymous and mo_user cannot POST to none
        for url_name in ['edit-project-from-file-preview', 'edit-project-from-file-preview']:
            for proj_pk in [self.public_project.pk, self.private_project.pk]:
                url = reverse(url_name, args=[str(proj_pk)])

                self.client.logout()  # AnonymousUser
                response = self.client.post(url)
                self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

                self.client.login(username=self.mo_user.username, password=self.mo_user_password)
                response = self.client.post(url)
                self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

                self.client.login(username=self.po_user.username, password=self.po_user_password)
                response = self.client.post(url)
                self.assertEqual(status.HTTP_200_OK, response.status_code)

                self.client.login(username=self.superuser.username, password=self.superuser_password)
                response = self.client.post(url)
                self.assertEqual(status.HTTP_200_OK, response.status_code)


    def test_delete_project_interactively(self):
        # delete_project_iteratively() should delete its project
        project2 = Project.objects.create(owner=self.po_user)
        self.assertIsNotNone(project2.pk)
        delete_project_iteratively(project2)
        self.assertIsNone(project2.pk)

        # views.delete_project() should call delete_project_iteratively()
        project2 = Project.objects.create(owner=self.po_user)
        self.client.login(username=self.po_user.username, password=self.po_user_password)
        with patch('utils.project.delete_project_iteratively') as del_proj_iter_mock:
            self.client.delete(reverse('delete-project', args=[str(project2.pk)]))
            del_proj_iter_mock.assert_called_once()
            args = del_proj_iter_mock.call_args[0]
            self.assertEqual(project2, args[0])

        # api_views.ProjectDetail.delete() should call delete_project_iteratively()
        project2 = Project.objects.create(owner=self.po_user)
        with patch('utils.project.delete_project_iteratively') as del_proj_iter_mock:
            self.client.delete(reverse('delete-project', args=[str(project2.pk)]), {
                'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
            })
            del_proj_iter_mock.assert_called_once()
            args = del_proj_iter_mock.call_args[0]
            self.assertEqual(project2, args[0])


    def test_data_download_formats(self):
        """
        Test forecast_data(). recall all API endpoints require an authorized user
        """
        # forecast data as JSON
        self._authenticate_jwt_user(self.po_user, self.po_user_password)
        response = self.client.get(reverse('api-forecast-data', args=[self.public_forecast.pk]))
        response_dict = json.loads(response.content)  # will fail if not JSON
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual("application/json", response['Content-Type'])
        self.assertEqual('attachment; filename="EW1-KoTsarima-2017-01-17.csv.json"', response['Content-Disposition'])
        self.assertEqual({'meta', 'predictions'}, set(response_dict))
        self.assertEqual({'forecast', 'units', 'targets'}, set(response_dict['meta']))
        self.assertEqual(11, len(response_dict['meta']['units']))

        # score data as CSV
        response = self.client.get(reverse('download-project-scores', args=[self.public_project.pk]))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual("text/csv", response['Content-Type'])
        self.assertEqual('attachment; filename="public_project_name-scores.csv"', response['Content-Disposition'])
        split_content = response.content.decode("utf-8").split('\r\n')
        self.assertEqual(','.join(['model', 'timezero', 'season', 'unit', 'target']), split_content[0])
        self.assertEqual(2, len(split_content))  # no score data


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
        # e.g., eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxLCJ1c2VybmFtZSI6InByb2plY3Rfb3duZXIxIiwiZXhwIjoxNTg0NTY3NTY2LCJlbWFpbCI6IiJ9.ClTxMfIGcVxFoZKOLPEbZB54RgRksvZCxntY46m5bwQ
        token_split = resp.data['token'].split('.')  # header.payload.signature. only header is deterministic
        self.assertEqual('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9', token_split[0])


    # update this when this changes: forecast_app/api_urls.py
    def test_api_get_endpoints(self):
        unit_us_nat = self.public_project.units.filter(name='US National').first()
        target_1wk = self.public_project.targets.filter(name='1 wk ahead').first()
        url_to_exp_user_status_code_pairs = {
            reverse('api-root'): self.ONLY_PO_MO,
            reverse('api-user-detail', args=[self.po_user.pk]): self.ONLY_PO,
            reverse('api-upload-file-job-detail', args=[self.upload_file_job.pk]): self.ONLY_PO,

            reverse('api-project-list'): self.ONLY_PO_MO,
            reverse('api-project-detail', args=[self.public_project.pk]): self.ONLY_PO_MO,
            reverse('api-project-detail', args=[self.private_project.pk]): self.ONLY_PO_MO,

            reverse('api-unit-list', args=[self.public_project.pk]): self.ONLY_PO_MO,
            reverse('api-unit-list', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-unit-detail', args=[unit_us_nat.pk]): self.ONLY_PO_MO,

            reverse('api-target-list', args=[self.public_project.pk]): self.ONLY_PO_MO,
            reverse('api-target-list', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-target-detail', args=[target_1wk.pk]): self.ONLY_PO_MO,

            reverse('api-timezero-list', args=[self.public_project.pk]): self.ONLY_PO_MO,
            reverse('api-timezero-list', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-timezero-detail', args=[self.public_tz1.pk]): self.ONLY_PO_MO,

            reverse('api-truth-detail', args=[self.public_project.pk]): self.ONLY_PO_MO,
            reverse('api-truth-detail', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-truth-data', args=[self.public_project.pk]): self.ONLY_PO_MO,
            reverse('api-truth-data', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-score-data', args=[self.public_project.pk]): self.ONLY_PO_MO,
            reverse('api-score-data', args=[self.private_project.pk]): self.ONLY_PO_MO,

            reverse('api-model-list', args=[self.public_project.pk]): self.ONLY_PO_MO,
            reverse('api-model-list', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-model-detail', args=[self.public_model.pk]): self.ONLY_PO_MO,
            reverse('api-model-detail', args=[self.private_model.pk]): self.ONLY_PO_MO,

            reverse('api-forecast-list', args=[self.public_model.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-list', args=[self.private_model.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-detail', args=[self.public_forecast.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-detail', args=[self.private_forecast.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-data', args=[self.public_forecast.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-data', args=[self.private_forecast.pk]): self.ONLY_PO_MO,
        }
        for url, user_exp_status_code_list in url_to_exp_user_status_code_pairs.items():
            for user, exp_status_code in user_exp_status_code_list:
                # authenticate using JWT. used instead of web API self.client.login() authentication elsewhere b/c
                # base.py configures JWT: REST_FRAMEWORK > DEFAULT_AUTHENTICATION_CLASSES > JSONWebTokenAuthentication
                self.client.logout()  # AnonymousUser
                if user:
                    password = self.po_user_password if user == self.po_user \
                        else self.mo_user_password if user == self.mo_user \
                        else self.superuser_password
                    self._authenticate_jwt_user(user, password)
                response = self.client.get(url)
                self.assertEqual(exp_status_code, response.status_code)


    def test_api_get_project_list_authorization(self):
        # verify filtering based on user authorization
        # recall all API endpoints require an authorized user

        # anonymous access: error
        response = self.client.get(reverse('api-project-list'), format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        # authorized access: self.mo_user: self.public_project, self.private_project, self.public_project2
        response = self.client.get(reverse('api-project-list'),
                                   {
                                       'Authorization': f'JWT {self._authenticate_jwt_user(self.mo_user, self.mo_user_password)}'},
                                   format='json')
        self.assertEqual({self.public_project.id, self.private_project.id, self.public_project2.id},
                         {proj_resp_dict['id'] for proj_resp_dict in response.data})

        # authorized access: self.po_user: self.public_project, self.private_project, self.public_project2
        response = self.client.get(reverse('api-project-list'),
                                   {
                                       'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}'},
                                   format='json')
        self.assertEqual({self.public_project.id, self.private_project.id, self.public_project2.id},
                         {proj_resp_dict['id'] for proj_resp_dict in response.data})


    def test_api_get_endpoint_keys(self):
        """
        Tests returned value keys as a content sanity check. recall all API endpoints require an authorized user.
        (per-user authorization tested in test_api_project_list_authorization())
        """
        # todo xx why does this work for all tests here when they don't pass the token!?
        self._authenticate_jwt_user(self.po_user, self.po_user_password)

        response = self.client.get(reverse('api-root'), format='json')
        self.assertEqual(['projects'], list(response.data))

        response = self.client.get(reverse('api-user-detail', args=[self.po_user.pk]), format='json')
        self.assertEqual(['id', 'url', 'username', 'owned_models', 'projects_and_roles'],
                         list(response.data))

        response = self.client.get(reverse('api-upload-file-job-detail', args=[self.upload_file_job.pk]))
        self.assertEqual(['id', 'url', 'status', 'user', 'created_at', 'updated_at', 'failure_message', 'filename',
                          'input_json', 'output_json'],
                         list(response.data))

        response = self.client.get(reverse('api-project-list'), format='json')
        self.assertEqual(3, len(response.data))  # assume contents are checked below

        response = self.client.get(reverse('api-project-detail', args=[self.public_project.pk]), format='json')
        self.assertEqual(['id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'time_interval_type',
                          'visualization_y_label', 'core_data', 'truth', 'model_owners', 'score_data', 'models',
                          'units', 'targets', 'timezeros'],
                         list(response.data))

        response = self.client.get(reverse('api-unit-list', args=[self.public_project.pk]), format='json')
        self.assertEqual(11, len(response.data))  # assume contents are checked below

        response = self.client.get(reverse('api-target-list', args=[self.public_project.pk]), format='json')
        self.assertEqual(7, len(response.data))  # assume contents are checked below

        response = self.client.get(reverse('api-timezero-list', args=[self.public_project.pk]), format='json')
        self.assertEqual(3, len(response.data))  # assume contents are checked below

        response = self.client.get(reverse('api-model-list', args=[self.public_project.pk]), format='json')
        self.assertEqual(5, len(response.data))  # assume contents are checked below

        response = self.client.get(reverse('api-truth-detail', args=[self.public_project.pk]), format='json')
        self.assertEqual(['id', 'url', 'project', 'truth_csv_filename', 'truth_data'],
                         list(response.data))

        response = self.client.get(reverse('api-truth-data', args=[self.public_project.pk]), format='json')
        self.assertEqual(341, len(response.content))

        response = self.client.get(reverse('api-score-data', args=[self.public_project.pk]), format='json')
        self.assertEqual(35, len(response.content))  # just SCORE_CSV_HEADER_PREFIX due to no scores

        unit_us_nat = self.public_project.units.filter(name='US National').first()
        response = self.client.get(reverse('api-unit-detail', args=[unit_us_nat.pk]))
        self.assertEqual(['id', 'url', 'name'], list(response.data))

        target_1wk = self.public_project.targets.filter(name='1 wk ahead').first()
        response = self.client.get(reverse('api-target-detail', args=[target_1wk.pk]))
        self.assertEqual(['id', 'url', 'name', 'type', 'description', 'is_step_ahead', 'step_ahead_increment', 'unit',
                          'cats'], list(response.data))

        response = self.client.get(reverse('api-timezero-detail', args=[self.public_tz1.pk]))
        self.assertEqual(['id', 'url', 'timezero_date', 'data_version_date', 'is_season_start', 'season_name'],
                         list(response.data))

        response = self.client.get(reverse('api-model-detail', args=[self.public_model.pk]), format='json')
        exp_keys = ['id', 'url', 'project', 'owner', 'name', 'abbreviation', 'description', 'home_url', 'aux_data_url',
                    'forecasts']
        self.assertEqual(exp_keys, list(response.data))

        response = self.client.get(reverse('api-forecast-list', args=[self.public_model.pk]), format='json')
        response_dicts = json.loads(response.content)
        exp_keys = ['id', 'url', 'forecast_model', 'source', 'time_zero', 'created_at', 'forecast_data']
        self.assertEqual(1, len(response_dicts))
        self.assertEqual(exp_keys, list(response_dicts[0]))

        response = self.client.get(reverse('api-forecast-detail', args=[self.public_forecast.pk]), format='json')
        exp_keys = ['id', 'url', 'forecast_model', 'source', 'time_zero', 'created_at', 'forecast_data']
        self.assertEqual(exp_keys, list(response.data))

        # note that we only check top-level keys b/c we know json_response_for_forecast() uses
        # json_io_dict_from_forecast(), which is tested separately
        response = self.client.get(reverse('api-forecast-data', args=[self.public_forecast.pk]), format='json')
        response_dict = json.loads(response.content)
        self.assertEqual({'meta', 'predictions'}, set(response_dict))
        self.assertEqual({'forecast', 'units', 'targets'}, set(response_dict['meta']))


    def test_target_serialization_api_target_detail(self):
        self._authenticate_jwt_user(self.po_user, self.po_user_password)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), self.po_user)

        # test 'api-target-detail' | 'pct next week'
        pct_next_week_target = project.targets.filter(name='pct next week').first()
        response = self.client.get(reverse('api-target-detail', args=[pct_next_week_target.pk]))
        self.assertEqual({'id', 'url', 'name', 'description', 'type', 'is_step_ahead', 'step_ahead_increment', 'unit',
                          'range', 'cats'}, set(response.data))

        # test 'api-target-detail' | 'cases next week'
        cases_next_week_target = project.targets.filter(name='cases next week').first()
        response = self.client.get(reverse('api-target-detail', args=[cases_next_week_target.pk]))
        self.assertEqual({'id', 'url', 'name', 'description', 'type', 'is_step_ahead', 'step_ahead_increment', 'unit',
                          'range', 'cats'}, set(response.data))

        # test 'api-target-detail' | 'season severity'
        season_severity_target = project.targets.filter(name='season severity').first()
        response = self.client.get(reverse('api-target-detail', args=[season_severity_target.pk]))
        self.assertEqual({'id', 'url', 'name', 'description', 'type', 'is_step_ahead', 'cats'}, set(response.data))

        # test 'api-target-detail' | 'above baseline'
        above_baseline_target = project.targets.filter(name='above baseline').first()
        response = self.client.get(reverse('api-target-detail', args=[above_baseline_target.pk]))
        self.assertEqual({'id', 'url', 'name', 'description', 'type', 'is_step_ahead'}, set(response.data))

        # test 'api-target-detail' | 'Season peak week'
        season_peak_week_target = project.targets.filter(name='Season peak week').first()
        response = self.client.get(reverse('api-target-detail', args=[season_peak_week_target.pk]))
        self.assertEqual({'id', 'url', 'name', 'description', 'type', 'is_step_ahead', 'unit', 'cats'},
                         set(response.data))


    def test_target_serialization_api_target_list(self):
        self._authenticate_jwt_user(self.po_user, self.po_user_password)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), self.po_user)

        # test TargetSerializer being passed one vs. many instances - this drives complicated DRF functionality.
        # note: using APIRequestFactory was the only way I could find to pass a request object. o/w you get:
        #   AssertionError: `HyperlinkedIdentityField` requires the request in the serializer context.
        request = APIRequestFactory().get('/')

        # test serializing a few single Targets ('pct next week' and 'Season peak week')
        pct_next_week_target = project.targets.filter(name='pct next week').first()
        target_serializer = TargetSerializer(pct_next_week_target, context={'request': request})
        # -> <class 'forecast_app.serializers.TargetSerializer'>
        self.assertEqual({'name', 'id', 'step_ahead_increment', 'url', 'is_step_ahead', 'range', 'description', 'unit',
                          'type', 'cats'}, set(target_serializer.data))
        self.assertEqual([0.0, 100.0], target_serializer.data['range'])  # sanity-check

        season_peak_week_target = project.targets.filter(name='Season peak week').first()
        target_serializer = TargetSerializer(season_peak_week_target, context={'request': request})
        self.assertEqual({'description', 'is_step_ahead', 'url', 'name', 'type', 'id', 'cats', 'unit'},
                         set(target_serializer.data))
        self.assertEqual(f"http://testserver/api/target/{target_serializer.data['id']}/",
                         target_serializer.data['url'])  # sanity-check

        # test serializing a multiple Targets
        target_serializer_multi = TargetSerializer(project.targets, many=True, context={'request': request})
        # -> <class 'rest_framework.serializers.ListSerializer'>
        self.assertEqual(5, len(target_serializer_multi.data))  # 5 targets
        self.assertEqual(target_serializer.data, target_serializer_multi.data[4])  # single matches multi

        # finally, test serializing a multiple Targets via endpoints
        response = self.client.get(reverse('api-target-list', args=[project.pk]), format='json')
        self.assertEqual(5, len(response.data))
        self.assertEqual(target_serializer.data, response.data[4])  # single matches multi


    def test_api_delete_forecast(self):
        # anonymous delete: self.public_forecast -> disallowed
        response = self.client.delete(reverse('api-forecast-detail', args=[self.public_forecast.pk]))
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        # authorized self.mo_user: delete private_forecast2 (new Forecast) -> allowed
        self._authenticate_jwt_user(self.mo_user, self.mo_user_password)
        self.assertEqual(1, self.private_model.forecasts.count())

        private_forecast2 = load_cdc_csv_forecast_file(2016, self.private_model, self.csv_file_path, self.private_tz1)
        private_forecast2_pk = private_forecast2.pk
        self.assertEqual(2, self.private_model.forecasts.count())

        response = self.client.delete(reverse('api-forecast-detail', args=[private_forecast2.pk]))
        self.assertEqual(status.HTTP_204_NO_CONTENT, response.status_code)
        self.assertIsNone(Forecast.objects.filter(pk=private_forecast2_pk).first())  # is deleted
        self.assertEqual(1, self.private_model.forecasts.count())  # is no longer in list


    def test_api_create_project(self):
        # case: not authorized. recall that any logged-in user can create
        json_response = self.client.post(reverse('api-project-list'), {
            'project_config': {},
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        # case: authorized
        with open(Path('forecast_app/tests/projects/cdc-project.json'), 'rb') as fp:
            project_dict = json.load(fp)
        json_response = self.client.post(reverse('api-project-list'), {
            'project_config': project_dict,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_200_OK, json_response.status_code)

        # spot-check response
        proj_json = json_response.json()
        self.assertEqual({'id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'time_interval_type',
                          'visualization_y_label', 'core_data', 'truth', 'model_owners', 'score_data', 'models',
                          'units',
                          'targets', 'timezeros'},
                         set(proj_json.keys()))
        self.assertEqual('CDC Flu challenge', proj_json['name'])


    def test_api_delete_project(self):
        # create a project to delete
        project2 = Project.objects.create(owner=self.po_user)

        # case: not authorized
        joe_user = User.objects.create_user(username='joe', password='password')
        response = self.client.delete(reverse('api-project-detail', args=[project2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(joe_user, "password")}',
        })
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        # case: authorized
        response = self.client.delete(reverse('api-project-detail', args=[project2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        })
        self.assertEqual(status.HTTP_204_NO_CONTENT, response.status_code)


    def test_api_edit_project(self):
        # create a project to edit
        project2 = Project.objects.create(owner=self.po_user)
        self.assertEqual('', project2.name)

        # case: not authorized
        joe_user = User.objects.create_user(username='joe', password='password')
        json_response = self.client.post(reverse('api-project-detail', args=[project2.pk]), {
            'project_config': {},
            'Authorization': f'JWT {self._authenticate_jwt_user(joe_user, "password")}',
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        # case: authorized
        with open(Path('forecast_app/tests/project_diff/docs-project-edited.json')) as fp:
            edited_config_dict = json.load(fp)  # makes the same changes as _make_some_changes()
        json_response = self.client.post(reverse('api-project-detail', args=[project2.pk]), {
            'project_config': edited_config_dict,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_200_OK, json_response.status_code)

        # spot-check response
        proj_json = json_response.json()
        self.assertEqual('new project name', proj_json['name'])


    def test_api_create_model(self):
        project2 = Project.objects.create(owner=self.po_user)

        # case: not authorized. recall user must be a superuser, project owner, or model owner
        json_response = self.client.post(reverse('api-model-list', args=[project2.pk]), {
            'model_config': {},
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        # case: no 'model_config'
        json_response = self.client.post(reverse('api-model-list', args=[project2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
        self.assertEqual({'error': "No 'model_config' data."}, json_response.json())

        # case: bad 'model_config': missing expected_keys:
        #   {'name', 'abbreviation', 'team_name', 'description', 'home_url', 'aux_data_url'}
        model_config = {}
        json_response = self.client.post(reverse('api-model-list', args=[project2.pk]), {
            'model_config': model_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

        # case: authorized
        model_config = {'name': 'a model_name', 'abbreviation': 'an abbreviation', 'team_name': 'a team_name',
                        'description': 'a description', 'home_url': 'http://example.com/',
                        'aux_data_url': 'http://example.com/'}
        json_response = self.client.post(reverse('api-model-list', args=[project2.pk]), {
            'model_config': model_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_200_OK, json_response.status_code)
        self.assertEqual(set(json_response.json().keys()),
                         {'project', 'home_url', 'forecasts', 'aux_data_url', 'abbreviation', 'description', 'owner',
                          'url', 'id', 'name'})

        # spot-check response
        model_json = json_response.json()
        self.assertEqual({'id', 'url', 'project', 'owner', 'name', 'abbreviation', 'description', 'home_url',
                          'aux_data_url', 'forecasts'},
                         set(model_json.keys()))
        self.assertEqual('a model_name', model_json['name'])


    def test_api_delete_model(self):
        # create a model to delete
        project2 = Project.objects.create(owner=self.po_user)
        forecast_model2 = ForecastModel.objects.create(project=project2, owner=self.po_user)

        # case: not authorized
        joe_user = User.objects.create_user(username='joe', password='password')
        response = self.client.delete(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(joe_user, "password")}',
        })
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        # case: authorized
        response = self.client.delete(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        })
        self.assertEqual(status.HTTP_204_NO_CONTENT, response.status_code)


    def test_api_create_timezero(self):
        project2 = Project.objects.create(owner=self.po_user)

        # case: not authorized. recall user must be a superuser, project owner, or model owner
        json_response = self.client.post(reverse('api-timezero-list', args=[project2.pk]), {
            'timezero_config': {},
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        # case: no 'timezero_config'
        json_response = self.client.post(reverse('api-timezero-list', args=[project2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
        self.assertEqual({'error': "No 'timezero_config' data."}, json_response.json())

        # case: bad 'timezero_config': missing expected_keys:
        #   {'timezero_date', 'data_version_date', 'is_season_start', 'season_name'}
        timezero_config = {}
        json_response = self.client.post(reverse('api-timezero-list', args=[project2.pk]), {
            'timezero_config': timezero_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

        # case: blue sky:  no data_version_date, yes season
        timezero_config = {'timezero_date': '2017-12-01',
                           'data_version_date': None,
                           'is_season_start': True,
                           'season_name': 'tis the season'}
        json_response = self.client.post(reverse('api-timezero-list', args=[project2.pk]), {
            'timezero_config': timezero_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_200_OK, json_response.status_code)
        self.assertEqual(set(json_response.json().keys()),
                         {'id', 'url', 'timezero_date', 'data_version_date', 'is_season_start', 'season_name'})

        # case: blue sky:  yes data_version_date, no season
        timezero_config = {'timezero_date': '2017-12-01',
                           'data_version_date': '2017-12-02',
                           'is_season_start': False}
        json_response = self.client.post(reverse('api-timezero-list', args=[project2.pk]), {
            'timezero_config': timezero_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_200_OK, json_response.status_code)
        self.assertEqual(set(json_response.json().keys()),
                         {'id', 'url', 'timezero_date', 'data_version_date', 'is_season_start', 'season_name'})


    def test_api_upload_forecast(self):
        # to avoid the requirement of RQ, redis, and S3, we patch _upload_file() to return (is_error, upload_file_job)
        # with desired return args
        with patch('forecast_app.views._upload_file') as upload_file_mock:
            upload_forecast_url = reverse('api-forecast-list', args=[str(self.public_model.pk)])
            data_file = SimpleUploadedFile('file.csv', b'file_content', content_type='text/csv')

            # case: not authorized
            joe_user = User.objects.create_user(username='joe', password='password')
            json_response = self.client.post(upload_forecast_url, {
                'Authorization': f'JWT {self._authenticate_jwt_user(joe_user, "password")}',
                'timezero_date': self.public_tz2.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),
            }, format='multipart')
            self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

            # case: no 'data_file'
            jwt_token = self._authenticate_jwt_user(self.mo_user, self.mo_user_password)
            json_response = self.client.post(upload_forecast_url, {
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': self.public_tz2.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: no 'timezero_date'
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: invalid 'timezero_date' format - YYYY_MM_DD_DATE_FORMAT
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': 'x20171202',
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: existing_forecast_for_time_zero
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': self.public_tz1.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),  # public_tz1
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: blue sky: _upload_file() -> NOT is_error
            upload_file_mock.return_value = False, UploadFileJob.objects.create()  # is_error, upload_file_job
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': self.public_tz2.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),
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
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': self.public_tz2.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)

            # case: error: time_zero not found. (does not auto-create)
            upload_file_mock.return_value = False, UploadFileJob.objects.create()  # is_error, upload_file_job
            new_timezero_date = '19621022'
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': new_timezero_date,  # doesn't exist
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)


    def _authenticate_jwt_user(self, user, password):
        jwt_auth_url = reverse('auth-jwt-get')
        jwt_auth_resp = self.client.post(jwt_auth_url, {'username': user.username, 'password': password}, format='json')
        jwt_token = jwt_auth_resp.data['token']
        self.client.credentials(HTTP_AUTHORIZATION='JWT ' + jwt_token)
        return jwt_token
