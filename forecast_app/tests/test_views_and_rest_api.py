import json
from pathlib import Path
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from forecast_app.models import Project, ForecastModel, TimeZero
from forecast_app.tests.test_project import TEST_CONFIG_DICT
from utils.make_cdc_flu_challenge_project import get_or_create_super_po_mo_users


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
                                                    owner=cls.po_user, config_dict=TEST_CONFIG_DICT)
        cls.public_project.model_owners.add(cls.mo_user)
        cls.public_project.save()
        cls.public_project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        cls.public_tz1 = TimeZero.objects.create(project=cls.public_project, timezero_date=str('2017-12-01'),
                                                 data_version_date=None)
        cls.public_tz2 = TimeZero.objects.create(project=cls.public_project, timezero_date=str('2017-12-02'),
                                                 data_version_date=None)

        # private_project
        cls.private_project = Project.objects.create(name='private project name', is_public=False,
                                                     owner=cls.po_user, config_dict=TEST_CONFIG_DICT)
        cls.private_project.model_owners.add(cls.mo_user)
        cls.private_project.save()
        cls.private_project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        cls.private_tz1 = TimeZero.objects.create(project=cls.private_project, timezero_date=str('2017-12-03'),
                                                  data_version_date=None)
        cls.private_tz2 = TimeZero.objects.create(project=cls.private_project, timezero_date=str('2017-12-04'),
                                                  data_version_date=None)

        # public project with no template
        cls.public_project2 = Project.objects.create(name='public project 2', is_public=True,
                                                     owner=cls.po_user, config_dict=TEST_CONFIG_DICT)
        cls.public_project2.model_owners.add(cls.mo_user)
        cls.public_project2.save()

        # public_model
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')
        cls.public_model = ForecastModel.objects.create(project=cls.public_project, name='public model',
                                                        description='', home_url='http://example.com',
                                                        owner=cls.mo_user)
        cls.public_forecast = cls.public_model.load_forecast(csv_file_path, cls.public_tz1)

        # private_model
        cls.private_model = ForecastModel.objects.create(project=cls.private_project, name='private model',
                                                         description='', home_url='http://example.com',
                                                         owner=cls.mo_user)
        cls.private_forecast = cls.private_model.load_forecast(csv_file_path, cls.private_tz1)

        # user/response pairs for testing authorization
        cls.OK_ALL = [(None, status.HTTP_200_OK),
                      (cls.po_user, status.HTTP_200_OK),
                      (cls.mo_user, status.HTTP_200_OK),
                      (cls.superuser, status.HTTP_200_OK)]
        cls.BAD_REQ_400_ALL = [(None, status.HTTP_400_BAD_REQUEST),
                               (cls.po_user, status.HTTP_400_BAD_REQUEST),
                               (cls.mo_user, status.HTTP_400_BAD_REQUEST),
                               (cls.superuser, status.HTTP_400_BAD_REQUEST)]
        cls.ONLY_PO_MO = [(None, status.HTTP_403_FORBIDDEN),
                          (cls.po_user, status.HTTP_200_OK),
                          (cls.mo_user, status.HTTP_200_OK),
                          (cls.superuser, status.HTTP_200_OK)]
        cls.ONLY_PO_MO_302 = [(None, status.HTTP_403_FORBIDDEN),
                              (cls.po_user, status.HTTP_302_FOUND),
                              (cls.mo_user, status.HTTP_302_FOUND),
                              (cls.superuser, status.HTTP_302_FOUND)]
        cls.ONLY_PO_MO_400 = [(None, status.HTTP_403_FORBIDDEN),
                              (cls.po_user, status.HTTP_400_BAD_REQUEST),
                              (cls.mo_user, status.HTTP_400_BAD_REQUEST),
                              (cls.superuser, status.HTTP_400_BAD_REQUEST)]
        cls.ONLY_PO = [(None, status.HTTP_403_FORBIDDEN),
                       (cls.po_user, status.HTTP_200_OK),
                       (cls.mo_user, status.HTTP_403_FORBIDDEN),
                       (cls.superuser, status.HTTP_200_OK)]
        cls.ONLY_PO_302 = [(None, status.HTTP_403_FORBIDDEN),
                           (cls.po_user, status.HTTP_302_FOUND),
                           (cls.mo_user, status.HTTP_403_FORBIDDEN),
                           (cls.superuser, status.HTTP_302_FOUND)]


    @patch('forecast_app.models.forecast.Forecast.delete')  # 'delete-forecast'
    @patch('forecast_app.models.forecast_model.ForecastModel.load_forecast')  # 'upload_forecast'
    # 'create-project' -> form
    # 'edit-project' -> form
    @patch('forecast_app.models.project.Project.delete')  # 'delete-project'
    # 'create-model' -> form
    # 'edit-model' -> form
    @patch('forecast_app.models.forecast_model.ForecastModel.delete')  # 'delete-model'
    def test_url_access(self, mock_delete_model, mock_delete_project, mock_load_forecast, mock_delete_forecast):
        url_to_exp_user_status_code_pairs = {
            reverse('index'): self.OK_ALL,
            reverse('about'): self.OK_ALL,
            reverse('docs'): self.OK_ALL,
            reverse('project-detail', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('project-detail', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('project-visualizations', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('project-visualizations', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('template-data-detail', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('template-data-detail', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('download-template-json', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('download-template-json', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('delete-template', args=[str(self.public_project.pk)]): self.ONLY_PO_302,
            reverse('delete-template', args=[str(self.private_project.pk)]): self.ONLY_PO_302,
            reverse('upload-template', args=[str(self.public_project.pk)]): self.ONLY_PO,
            reverse('upload-template', args=[str(self.private_project.pk)]): self.ONLY_PO,
            reverse('model-detail', args=[str(self.public_model.pk)]): self.OK_ALL,
            reverse('model-detail', args=[str(self.private_model.pk)]): self.ONLY_PO_MO,
            reverse('user-detail', args=[str(self.po_user.pk)]): self.OK_ALL,
            reverse('forecast-detail', args=[str(self.public_forecast.pk)]): self.OK_ALL,
            reverse('forecast-detail', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO,
            reverse('download-forecast-json', args=[str(self.public_forecast.pk)]): self.OK_ALL,
            reverse('download-forecast-json', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO,
            reverse('delete-forecast', args=[str(self.public_forecast.pk)]): self.ONLY_PO_MO_302,
            reverse('delete-forecast', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO_302,
            reverse('upload-forecast', args=[str(self.public_model.pk), str(self.public_tz1.pk)]): self.ONLY_PO_MO,
            reverse('upload-forecast', args=[str(self.private_model.pk), str(self.public_tz1.pk)]): self.ONLY_PO_MO,
            reverse('create-project', args=[]): self.ONLY_PO_MO,
            reverse('edit-project', args=[str(self.public_project.pk)]): self.ONLY_PO,
            reverse('edit-project', args=[str(self.private_project.pk)]): self.ONLY_PO,
            reverse('delete-project', args=[str(self.public_project.pk)]): self.ONLY_PO_302,
            reverse('delete-project', args=[str(self.private_project.pk)]): self.ONLY_PO_302,
            reverse('delete-project', args=[str(self.public_project.pk)]): self.ONLY_PO_302,
            reverse('delete-project', args=[str(self.private_project.pk)]): self.ONLY_PO_302,
            reverse('create-model', args=[str(self.public_project.pk)]): self.ONLY_PO_MO,
            reverse('create-model', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('edit-model', args=[str(self.public_model.pk)]): self.ONLY_PO_MO,
            reverse('edit-model', args=[str(self.private_model.pk)]): self.ONLY_PO_MO,
            reverse('delete-model', args=[str(self.public_model.pk)]): self.ONLY_PO_MO_302,
            reverse('delete-model', args=[str(self.private_model.pk)]): self.ONLY_PO_MO_302,
            reverse('forecast-sparkline', args=[str(self.public_forecast.pk)]): self.BAD_REQ_400_ALL,
            reverse('forecast-sparkline', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO_400,
        }
        # NB: re: 'forecast-sparkline' URIs: 1) BAD_REQ_400 is expected b/c we don't pass the correct query params.
        # however, 400 does indicate that the code passed the authorization portion. 2) the 'data' arg is only for the
        # two 'forecast-sparkline' cases, but it doesn't hurt to pass it for all cases, so we do b/c it's simpler :-)
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
            # user detail
            reverse('index', args=[]): {
                reverse('create-project', args=[]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True)],
            },
            # project detail - public_project
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
            # project detail - public_project2 (no template)
            reverse('project-detail', args=[str(self.public_project2.pk)]): {
                reverse('upload-template', args=[str(self.public_project2.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True)],
            },
            # template data detail - public_project
            reverse('template-data-detail', args=[str(self.public_project.pk)]): {
                reverse('delete-template', args=[str(self.public_project.pk)]):
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


    def test_api_endpoints(self):
        url_to_exp_user_status_code_pairs = {
            reverse('api-root'): self.OK_ALL,
            reverse('api-project-list'): self.OK_ALL,
            reverse('api-project-detail', args=[self.public_project.pk]): self.OK_ALL,
            reverse('api-project-detail', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-template-detail', args=[self.public_project.pk]): self.OK_ALL,
            reverse('api-template-detail', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-template-data', args=[self.public_project.pk]): self.OK_ALL,
            reverse('api-template-data', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-user-detail', args=[self.po_user.pk]): self.OK_ALL,
            reverse('api-user-detail', args=[self.mo_user.pk]): self.OK_ALL,
            reverse('api-model-detail', args=[self.public_model.pk]): self.OK_ALL,
            reverse('api-model-detail', args=[self.private_model.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-detail', args=[self.public_forecast.pk]): self.OK_ALL,
            reverse('api-forecast-detail', args=[self.private_forecast.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-data', args=[self.public_forecast.pk]): self.OK_ALL,
            reverse('api-forecast-data', args=[self.private_forecast.pk]): self.ONLY_PO_MO,
        }
        for idx, (url, user_exp_status_code_list) in enumerate(url_to_exp_user_status_code_pairs.items()):
            for user, exp_status_code in user_exp_status_code_list:
                self.client.logout()  # AnonymousUser
                if user:
                    password = self.po_user_password if user == self.po_user \
                        else self.mo_user_password if user == self.mo_user \
                        else self.superuser_password
                    self.client.login(username=user.username, password=password)

                response = self.client.get(url)
                self.assertEqual(exp_status_code, response.status_code)


    def test_api_endpoint_keys(self):
        """
        Tests returned value keys as a content sanity check.
        """
        # 'api-root'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-root'), format='json')
        self.assertEqual(['projects'], list(response.data.keys()))

        # 'api-project-list'
        # a rest_framework.utils.serializer_helpers.ReturnList:
        response = self.client.get(reverse('api-project-list'), format='json')
        self.assertEqual(3, len(response.data))

        # 'api-project-detail'
        # a rest_framework.utils.serializer_helpers.ReturnDict:
        response = self.client.get(reverse('api-project-detail', args=[self.public_project.pk]), format='json')
        exp_keys = ['id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'core_data', 'config_dict',
                    'template', 'model_owners', 'models', 'targets', 'timezeros']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-template-detail'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-template-detail', args=[self.public_project.pk]), format='json')
        exp_keys = ['id', 'url', 'project', 'csv_filename', 'template_data']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-template-data'
        # a django.http.response.JsonResponse:
        response = self.client.get(reverse('api-template-data', args=[self.public_project.pk]), format='json')
        response_dict = json.loads(response.content)

        # check top-level keys
        exp_keys = ['metadata', 'locations']
        self.assertEqual(exp_keys, list(response_dict.keys()))

        # check metadata
        proj_detail_resp = self.client.get(reverse('api-project-detail', args=[self.public_project.pk]), format='json')
        proj_detail_dict = json.loads(proj_detail_resp.content)
        self.assertEqual(proj_detail_dict, response_dict['metadata'])

        # check data keys
        exp_location_names = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                              'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9',
                              'US National']
        self.assertEqual(exp_location_names, [location['name'] for location in response_dict['locations']])

        # 'api-user-detail'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-user-detail', args=[self.po_user.pk]), format='json')
        exp_keys = ['id', 'url', 'username', 'owned_models', 'projects_and_roles']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-model-detail'
        # a rest_framework.response.Response
        response = self.client.get(reverse('api-model-detail', args=[self.public_model.pk]), format='json')
        exp_keys = ['id', 'url', 'project', 'owner', 'name', 'description', 'home_url', 'aux_data_url', 'forecasts']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-forecast-detail'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-forecast-detail', args=[self.public_forecast.pk]), format='json')
        exp_keys = ['id', 'url', 'forecast_model', 'csv_filename', 'time_zero', 'forecast_data']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-forecast-data'
        # a django.http.response.JsonResponse:
        response = self.client.get(reverse('api-forecast-data', args=[self.public_forecast.pk]), format='json')
        response_dict = json.loads(response.content)

        # check top-level keys
        exp_keys = ['metadata', 'locations']
        self.assertEqual(exp_keys, list(response_dict.keys()))

        # check metadata
        forecast_detail_resp = self.client.get(reverse('api-forecast-detail', args=[self.public_forecast.pk]),
                                               format='json')
        forecast_detail_dict = json.loads(forecast_detail_resp.content)
        self.assertEqual(forecast_detail_dict, response_dict['metadata'])

        # check data keys
        exp_location_names = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                              'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9',
                              'US National']
        self.assertEqual(exp_location_names, [location['name'] for location in response_dict['locations']])
