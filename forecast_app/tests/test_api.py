import json

from rest_framework.reverse import reverse
from rest_framework.test import APIClient

from forecast_app.tests.test_views import ViewsTestCase


# ViewsTestCase.setUpTestData() gives us users, private and public projects, models, forecasts, etc.
class ApiTestCase(ViewsTestCase):
    """
    Tests the REST API. Endpoints:
    
    - api/                           # 'api-root'
    - api/projects/                  # 'api-project-list'
    - api/project/1/                 # 'api-project-detail'
    - api/projects/1/template_data/  # 'api-template-data'
    - api/users/                     # 'api-user-list'
    - api/user/1/                    # 'api-user-detail'
    - api/model/1/                   # 'api-model-detail'
    - api/forecast/1/                # 'api-forecast-detail'
    - api/forecast/1/data/           # 'api-forecast-data'
    """


    def setUp(self):
        self.client = APIClient()


    def test_api_endpoints(self):
        url_to_exp_user_status_code_pairs = {
            reverse('api-root'): self.OK_ALL,
            reverse('api-project-list'): self.OK_ALL,
            reverse('api-project-detail', args=[self.public_project.pk]): self.OK_ALL,
            reverse('api-project-detail', args=[self.private_project.pk]): self.ONLY_PO_MO,
            reverse('api-template-data', args=[self.public_project.pk]): self.OK_ALL,
            reverse('api-template-data', args=[self.private_project.pk]): self.ONLY_PO_MO,  # 5
            reverse('api-user-list'): self.OK_ALL,
            reverse('api-user-detail', args=[self.po_user.pk]): self.OK_ALL,
            reverse('api-user-detail', args=[self.mo_user.pk]): self.OK_ALL,
            reverse('api-model-detail', args=[self.public_model.pk]): self.OK_ALL,
            reverse('api-model-detail', args=[self.private_model.pk]): self.ONLY_PO_MO,
            reverse('api-forecast-detail', args=[self.public_forecast.pk]): self.OK_ALL,  # 10
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
                if exp_status_code != response.status_code: print('yy', idx, url, user, '.', exp_status_code, response.status_code)
                self.assertEqual(exp_status_code, response.status_code)


    def test_api_endpoint_keys(self):
        """
        Tests returned value keys as a content sanity check.
        """
        # 'api-root'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-root'), format='json')
        self.assertEqual(['users', 'projects'], list(response.data.keys()))

        # 'api-project-list'
        # a rest_framework.utils.serializer_helpers.ReturnList:
        response = self.client.get(reverse('api-project-list'), format='json')
        self.assertEqual(2, len(response.data))

        # 'api-project-detail'
        # a rest_framework.utils.serializer_helpers.ReturnDict:
        response = self.client.get(reverse('api-project-detail', args=[self.public_project.pk]), format='json')
        exp_keys = ['id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'core_data', 'config_dict',
                    'template_csv_file_name', 'template_data', 'model_owners', 'models', 'targets',
                    'timezeros']
        self.assertEqual(exp_keys, list(response.data.keys()))

        # 'api-template-data'
        # a django.http.response.JsonResponse:
        response = self.client.get(reverse('api-template-data', args=[self.public_project.pk]), format='json')
        data = json.loads(response.content)
        exp_keys = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4', 'HHS Region 5',
                    'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(exp_keys, list(data.keys()))

        # 'api-user-list'
        # a rest_framework.response.Response:
        response = self.client.get(reverse('api-user-list'), format='json')
        self.assertEqual(3, len(response.data))

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
        data = json.loads(response.content)
        exp_keys = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4', 'HHS Region 5',
                    'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(exp_keys, list(data.keys()))
