import datetime
from pathlib import Path
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse
from rest_framework import status

from forecast_app.models import Project, ForecastModel, TimeZero
from forecast_app.tests.test_project import TEST_CONFIG_DICT
from utils.make_cdc_flu_challenge_project import get_or_create_super_po_mo_users


class ViewsTestCase(TestCase):
    """
    Tests view authorization.

    Legend:
    - a: anyone can access

    Otherwise, the url's associated Project.is_public determines access. if public: anyone can access. o/w, only
    Project.owner or a Project.model_owners can access. How the viewed object is associated with the Project:

    - p: project = Project
    - m: project = ForecastModel.project
    - f: project = Forecast.forecast_model.project

    URLs:
    - a   '^$'      'index'
    - a   '^about$  'about'
    - a   '^docs'   'docs'
    -
    - p   '^project/(?P<pk>\d+)$'                          'project-detail'
    - p   '^project/(?P<project_pk>\d+)/visualizations$    'project-visualizations'
    - p   '^project/(?P<project_pk>\d+)/template$'         'template-data-detail'
    - p   '^project/(?P<model_with_cdc_data_pk>\d+)/json'  'download-template-json'
    -
    - m   '^model/(?P<pk>\d+)$'    'model-detail'
    -
    - a   '^user/(?P<pk>\d+)$'     'user-detail'
    -
    - f   '^forecast/(?P<pk>\d+)$'                                             'forecast-detail'
    - f   '^forecast/(?P<model_with_cdc_data_pk>\d+)/json'                     'download-forecast-json'
    - f   '^forecast/(?P<forecast_pk>\d+)/delete$'                             'delete-forecast'
    - f   '^forecast/(?P<forecast_model_pk>\d+)/upload/(?P<timezero_pk>\d+)$'  'upload-forecast'
    -
    - p   '^project/create/(?P<user_pk>\d+)'       'create-project'
    - p   '^project/(?P<project_pk>\d+)/edit/$'    'edit-project'
    - p   '^project/(?P<project_pk>\d+)/delete/$'  'delete-project'
    -
    - p   '^project/(?P<project_pk>\d+)/models/create/$'  'create-model'
    - m   '^model/(?P<model_pk>\d+)/edit/$'               'edit-model'
    - m   '^model/(?P<model_pk>\d+)/delete/$'             'delete-model'
    """


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

        # private_project
        cls.private_project = Project.objects.create(name='private project name', is_public=False,
                                                     owner=cls.po_user, config_dict=TEST_CONFIG_DICT)
        cls.private_project.model_owners.add(cls.mo_user)
        cls.private_project.save()
        cls.private_project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))

        # public_model
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')
        cls.tz_today = TimeZero.objects.create(project=cls.public_project, timezero_date=str(datetime.date.today()),
                                               data_version_date=None)
        cls.public_model = ForecastModel.objects.create(project=cls.public_project, name='public model',
                                                        description='', home_url='http://example.com')
        cls.public_forecast = cls.public_model.load_forecast(csv_file_path, cls.tz_today)

        # private_model
        cls.tz_today = TimeZero.objects.create(project=cls.private_project, timezero_date=str(datetime.date.today()),
                                               data_version_date=None)
        cls.private_model = ForecastModel.objects.create(project=cls.private_project, name='private model',
                                                         description='', home_url='http://example.com')
        cls.private_forecast = cls.private_model.load_forecast(csv_file_path, cls.tz_today)

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
                       (cls.superuser, status.HTTP_403_FORBIDDEN)]
        cls.ONLY_PO_302 = [(None, status.HTTP_403_FORBIDDEN),
                           (cls.po_user, status.HTTP_302_FOUND),
                           (cls.mo_user, status.HTTP_403_FORBIDDEN),
                           (cls.superuser, status.HTTP_403_FORBIDDEN)]


    @patch('forecast_app.models.forecast.Forecast.delete')  # 'delete-forecast'
    @patch('forecast_app.models.forecast_model.ForecastModel.load_forecast')  # 'upload_forecast'
    # # 'create-project' -> form
    # # 'edit-project' -> form
    @patch('forecast_app.models.project.Project.delete')  # 'delete-project'
    # # 'create-model' -> form
    # # 'edit-model' -> form
    # # 'delete-model'
    @patch('forecast_app.models.forecast_model.ForecastModel.delete')  # 'delete-model'
    def test_url_access(self, mock_delete_model, mock_delete_project, mock_load_forecast, mock_delete_forecast):
        url_to_exp_user_status_code_pairs = {
            reverse('index'): self.OK_ALL,
            reverse('about'): self.OK_ALL,
            reverse('docs'): self.OK_ALL,
            reverse('project-detail', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('project-detail', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('project-visualizations', args=[str(self.public_project.pk)]): self.OK_ALL,  # 5
            reverse('project-visualizations', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('template-data-detail', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('template-data-detail', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('download-template-json', args=[str(self.public_project.pk)]): self.OK_ALL,
            reverse('download-template-json', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,  # 10
            reverse('model-detail', args=[str(self.public_model.pk)]): self.OK_ALL,
            reverse('model-detail', args=[str(self.private_model.pk)]): self.ONLY_PO_MO,
            reverse('user-detail', args=[str(self.po_user.pk)]): self.OK_ALL,
            reverse('forecast-detail', args=[str(self.public_forecast.pk)]): self.OK_ALL,
            reverse('forecast-detail', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO,  # 15
            reverse('download-forecast-json', args=[str(self.public_forecast.pk)]): self.OK_ALL,
            reverse('download-forecast-json', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO,
            reverse('delete-forecast', args=[str(self.public_forecast.pk)]): self.ONLY_PO_MO_302,
            reverse('delete-forecast', args=[str(self.private_forecast.pk)]): self.ONLY_PO_MO_302,
            # 20:
            reverse('upload-forecast', args=[str(self.public_forecast.pk), str(self.tz_today.pk)]): self.ONLY_PO_MO,
            reverse('upload-forecast', args=[str(self.private_forecast.pk), str(self.tz_today.pk)]): self.ONLY_PO_MO,
            reverse('create-project', args=[str(self.po_user.pk)]): self.ONLY_PO,
            reverse('edit-project', args=[str(self.public_project.pk)]): self.ONLY_PO,
            reverse('edit-project', args=[str(self.private_project.pk)]): self.ONLY_PO,
            reverse('delete-project', args=[str(self.public_project.pk)]): self.ONLY_PO_302,  # 25
            reverse('delete-project', args=[str(self.private_project.pk)]): self.ONLY_PO_302,
            reverse('create-model', args=[str(self.public_project.pk)]): self.ONLY_PO_MO,
            reverse('create-model', args=[str(self.private_project.pk)]): self.ONLY_PO_MO,
            reverse('edit-model', args=[str(self.public_model.pk)]): self.ONLY_PO_MO,
            reverse('edit-model', args=[str(self.private_model.pk)]): self.ONLY_PO_MO,  # 30
            reverse('delete-model', args=[str(self.public_model.pk)]): self.ONLY_PO_MO_302,
            reverse('delete-model', args=[str(self.private_model.pk)]): self.ONLY_PO_MO_302,
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
