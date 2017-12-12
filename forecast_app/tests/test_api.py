import json
from pathlib import Path

from django.contrib.auth.models import User
from django.test import TestCase
from rest_framework import status
from rest_framework.reverse import reverse
from rest_framework.test import APIClient

from forecast_app.models import Project, ForecastModel, TimeZero
from forecast_app.tests.test_project import TEST_CONFIG_DICT


class ApiTestCase(TestCase):
    """
    Tests the REST API:

    - api/
    - api/projects/
    - api/project/1/
    - api/projects/1/template_data/
    - api/users/
    - api/user/1/
    - api/model/1/
    - api/forecast/1/
    - api/forecast/1/data/

    """


    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.create_user('user1')

        cls.project = Project.objects.create(config_dict=TEST_CONFIG_DICT)
        cls.project.load_template(Path('2016-2017_submission_template.csv'))

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)

        csv_file_path = Path('model_error/ensemble/EW1-KoTstable-2017-01-17.csv')
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date="2017-01-01")
        cls.forecast = cls.forecast_model.load_forecast(csv_file_path, time_zero)


    def setUp(self):
        self.client = APIClient()


    def test_api_can_create_a_bucketlist(self):
        """
        Just tests that endpoints exist and that they return the right keys. todo test content as well
        """
        # 'api-root'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-root'), format="json")
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.assertEqual(['projects', 'users'], list(response.data.keys()))

        # 'api-project-list'
        # a rest_framework.utils.serializer_helpers.ReturnList:
        response = self.client.get(reverse('api-project-list'), format="json")
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.assertEqual(1, len(response.data))

        # 'api-project-detail'
        # a rest_framework.utils.serializer_helpers.ReturnDict:
        response = self.client.get(reverse('api-project-detail', args=[self.project.pk]), format="json")
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        exp_keys = ['id', 'url', 'owner', 'name', 'description', 'home_url', 'core_data', 'config_dict',
                    'template_csv_file_name', 'template_data', 'model_owners', 'forecastmodel_set', 'target_set',
                    'timezero_set']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-template-data'
        # a django.http.response.JsonResponse:
        response = self.client.get(reverse('api-template-data', args=[self.project.pk]), format="json")
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        data = json.loads(response.content)
        exp_keys = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4', 'HHS Region 5',
                    'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(exp_keys, list(data.keys()))

        # 'api-user-list'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-user-list'), format="json")
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.assertEqual(1, len(response.data))

        # 'api-user-detail'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-user-detail', args=[self.user.pk]), format="json")
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        exp_keys = ['id', 'url', 'username', 'owned_models', 'projects_and_roles']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-model-detail'
        # a rest_framework.response.Response
        response = self.client.get(reverse('api-model-detail', args=[self.forecast_model.pk]), format="json")
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        exp_keys = ['id', 'url', 'project', 'owner', 'name', 'description', 'home_url', 'aux_data_url', 'forecasts']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-forecast-detail'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-forecast-detail', args=[self.forecast.pk]), format="json")
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        exp_keys = ['id', 'url', 'forecast_model', 'csv_filename', 'time_zero', 'forecast_data']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-forecast-data'
        # a django.http.response.JsonResponse:
        response = self.client.get(reverse('api-forecast-data', args=[self.forecast.pk]), format="json")
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        data = json.loads(response.content)
        exp_keys = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4', 'HHS Region 5',
                    'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(exp_keys, list(data.keys()))
