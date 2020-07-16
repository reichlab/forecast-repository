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

from forecast_app.api_views import SCORE_CSV_HEADER_PREFIX, _query_forecasts_worker
from forecast_app.models import Project, ForecastModel, TimeZero, Forecast
from forecast_app.models.job import Job
from forecast_app.serializers import TargetSerializer, TimeZeroSerializer
from forecast_app.views import _delete_forecast_worker
from utils.cdc_io import load_cdc_csv_forecast_file, make_cdc_units_and_targets
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
        cls.superuser, cls.superuser_password, cls.po_user, cls.po_user_password, cls.mo_user, cls.mo_user_password, \
        cls.non_staff_user, cls.non_staff_user_password = get_or_create_super_po_mo_users(is_create_super=True)

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

        cls.job = Job.objects.create(user=cls.po_user)

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
                                     abbreviation='abbrev', home_url='http://example.com', owner=cls.mo_user)
        ForecastModel.objects.create(project=cls.public_project, name='public model2', description='',
                                     abbreviation='abbrev2', home_url='http://example.com', owner=cls.mo_user)
        ForecastModel.objects.create(project=cls.public_project, name='public model3', description='',
                                     abbreviation='abbrev3', home_url='http://example.com', owner=cls.mo_user)

        ForecastModel.objects.create(project=cls.public_project, name='public model4', description='',
                                     abbreviation='abbrev4', home_url='http://example.com', owner=cls.mo_user)
        cls.public_model = ForecastModel.objects.create(project=cls.public_project, name='public model5',
                                                        abbreviation='abbrev5', description='',
                                                        home_url='http://example.com', owner=cls.mo_user)
        cls.public_forecast = load_cdc_csv_forecast_file(2016, cls.public_model, cls.csv_file_path, cls.public_tz1)

        # private_model
        cls.private_model = ForecastModel.objects.create(project=cls.private_project, name='private model',
                                                         abbreviation='abbrev', description='',
                                                         home_url='http://example.com', owner=cls.mo_user)
        cls.private_forecast = load_cdc_csv_forecast_file(2016, cls.private_model, cls.csv_file_path, cls.private_tz1)

        # user/response pairs for testing authorization
        cls.OK_ALL = [(None, status.HTTP_200_OK),
                      (cls.po_user, status.HTTP_200_OK),
                      (cls.mo_user, status.HTTP_200_OK),
                      (cls.superuser, status.HTTP_200_OK),
                      (cls.non_staff_user, status.HTTP_200_OK)]
        cls.ONLY_PO_MO = [(None, status.HTTP_403_FORBIDDEN),
                          (cls.po_user, status.HTTP_200_OK),
                          (cls.mo_user, status.HTTP_200_OK),
                          (cls.superuser, status.HTTP_200_OK),
                          (cls.non_staff_user, status.HTTP_403_FORBIDDEN)]
        cls.ONLY_PO_MO_STAFF = [(None, status.HTTP_403_FORBIDDEN),
                                (cls.po_user, status.HTTP_200_OK),
                                (cls.mo_user, status.HTTP_200_OK),
                                (cls.superuser, status.HTTP_200_OK),
                                (cls.non_staff_user, status.HTTP_200_OK)]
        cls.ONLY_PO_MO_302 = [(None, status.HTTP_403_FORBIDDEN),
                              (cls.po_user, status.HTTP_302_FOUND),
                              (cls.mo_user, status.HTTP_302_FOUND),
                              (cls.superuser, status.HTTP_302_FOUND),
                              (cls.non_staff_user, status.HTTP_403_FORBIDDEN)]
        cls.ONLY_PO = [(None, status.HTTP_403_FORBIDDEN),
                       (cls.po_user, status.HTTP_200_OK),
                       (cls.mo_user, status.HTTP_403_FORBIDDEN),
                       (cls.superuser, status.HTTP_200_OK),
                       (cls.non_staff_user, status.HTTP_403_FORBIDDEN)]
        cls.ONLY_PO_302 = [(None, status.HTTP_403_FORBIDDEN),
                           (cls.po_user, status.HTTP_302_FOUND),
                           (cls.mo_user, status.HTTP_403_FORBIDDEN),
                           (cls.superuser, status.HTTP_302_FOUND),
                           (cls.non_staff_user, status.HTTP_403_FORBIDDEN)]
        cls.ONLY_SU_200 = [(None, status.HTTP_403_FORBIDDEN),
                           (cls.po_user, status.HTTP_403_FORBIDDEN),
                           (cls.mo_user, status.HTTP_403_FORBIDDEN),
                           (cls.superuser, status.HTTP_200_OK),
                           (cls.non_staff_user, status.HTTP_403_FORBIDDEN)]
        cls.ONLY_SU_302 = [(None, status.HTTP_403_FORBIDDEN),
                           (cls.po_user, status.HTTP_403_FORBIDDEN),
                           (cls.mo_user, status.HTTP_403_FORBIDDEN),
                           (cls.superuser, status.HTTP_302_FOUND),
                           (cls.non_staff_user, status.HTTP_403_FORBIDDEN)]


    # the following @patch calls stop CRUD calls from actually taking place. all we care about here is access permissions
    @patch('forecast_app.models.forecast.Forecast.delete')  # 'delete-forecast'
    # 'create-project-from-form' -> form
    # 'edit-project-from-form' -> form
    @patch('utils.project.delete_project_iteratively')  # 'delete-project'
    # 'create-model' -> form
    # 'edit-model' -> form
    @patch('forecast_app.models.forecast_model.ForecastModel.delete')  # 'delete-model'
    @patch('rq.queue.Queue.enqueue')
    def test_url_get_general(self, mock_delete_model, mock_delete_project, mock_delete_forecast, enqueue_mock):
        url_exp_user_status_code_pairs = [
            (reverse('index'), self.OK_ALL),
            (reverse('about'), self.OK_ALL),

            (reverse('user-detail', args=[str(self.po_user.pk)]), self.ONLY_PO),
            (reverse('edit-user', args=[str(self.po_user.pk)]), self.ONLY_PO),
            (reverse('change-password'), self.ONLY_PO_MO_STAFF),
            (reverse('job-detail', args=[str(self.job.pk)]), self.ONLY_PO),

            (reverse('zadmin'), self.ONLY_SU_200),
            (reverse('clear-row-count-caches'), self.ONLY_SU_302),
            (reverse('update-row-count-caches'), self.ONLY_SU_302),
            (reverse('clear-score-csv-file-caches'), self.ONLY_SU_302),
            (reverse('update-score-csv-file-caches'), self.ONLY_SU_302),
            (reverse('update-all-scores'), self.ONLY_SU_302),
            (reverse('delete-file-jobs'), self.ONLY_SU_302),
            (reverse('clear-all-scores'), self.ONLY_SU_302),
            (reverse('delete-score-last-updates'), self.ONLY_SU_302),

            (reverse('project-detail', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('project-detail', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('project-visualizations', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('project-visualizations', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('project-forecasts', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('project-forecasts', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('project-explorer', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('project-explorer', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('project-scores', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('project-scores', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('project-score-data', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('project-score-data', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('download-project-scores', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('download-project-scores', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('project-config', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('project-config', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('create-project-from-form', args=[]), self.ONLY_PO_MO),
            (reverse('create-project-from-file', args=[]), self.ONLY_PO_MO),
            (reverse('edit-project-from-form', args=[str(self.public_project.pk)]), self.ONLY_PO),
            (reverse('edit-project-from-form', args=[str(self.private_project.pk)]), self.ONLY_PO),
            (reverse('delete-project', args=[str(self.public_project.pk)]), self.ONLY_PO_302),
            (reverse('delete-project', args=[str(self.private_project.pk)]), self.ONLY_PO_302),
            (reverse('delete-project', args=[str(self.public_project.pk)]), self.ONLY_PO_302),
            (reverse('delete-project', args=[str(self.private_project.pk)]), self.ONLY_PO_302),

            (reverse('truth-data-detail', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('truth-data-detail', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('delete-truth', args=[str(self.public_project.pk)]), self.ONLY_PO_302),
            (reverse('delete-truth', args=[str(self.private_project.pk)]), self.ONLY_PO_302),
            (reverse('upload-truth', args=[str(self.public_project.pk)]), self.ONLY_PO),
            (reverse('upload-truth', args=[str(self.private_project.pk)]), self.ONLY_PO),
            (reverse('download-truth', args=[str(self.public_project.pk)]), self.OK_ALL),
            (reverse('download-truth', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),

            (reverse('model-detail', args=[str(self.public_model.pk)]), self.OK_ALL),
            (reverse('model-detail', args=[str(self.private_model.pk)]), self.ONLY_PO_MO),
            (reverse('create-model', args=[str(self.public_project.pk)]), self.ONLY_PO_MO),
            (reverse('create-model', args=[str(self.private_project.pk)]), self.ONLY_PO_MO),
            (reverse('edit-model', args=[str(self.public_model.pk)]), self.ONLY_PO_MO),
            (reverse('edit-model', args=[str(self.private_model.pk)]), self.ONLY_PO_MO),
            (reverse('delete-model', args=[str(self.public_model.pk)]), self.ONLY_PO_MO_302),
            (reverse('delete-model', args=[str(self.private_model.pk)]), self.ONLY_PO_MO_302),

            (reverse('forecast-detail', args=[str(self.public_forecast.pk)]), self.OK_ALL),
            (reverse('forecast-detail', args=[str(self.private_forecast.pk)]), self.ONLY_PO_MO),
            (reverse('delete-forecast', args=[str(self.public_forecast.pk)]), self.ONLY_PO_MO_302),
            (reverse('delete-forecast', args=[str(self.private_forecast.pk)]), self.ONLY_PO_MO_302),
            (reverse('upload-forecast', args=[str(self.public_model.pk), str(self.public_tz1.pk)]), self.ONLY_PO_MO),
            (reverse('upload-forecast', args=[str(self.private_model.pk), str(self.public_tz1.pk)]), self.ONLY_PO_MO),
            (reverse('download-forecast', args=[str(self.public_forecast.pk)]), self.OK_ALL),
            (reverse('download-forecast', args=[str(self.private_forecast.pk)]), self.ONLY_PO_MO),
        ]

        # 'download-forecast' returns BAD_REQ_400 b/c they expect a POST with a 'format' parameter, and we don't pass
        # the correct query params. however, 400 does indicate that the code passed the authorization portion
        for url, user_exp_status_code_list in url_exp_user_status_code_pairs:
            for user, exp_status_code in user_exp_status_code_list:
                self.client.logout()  # AnonymousUser
                if user:
                    password = self.po_user_password if user == self.po_user \
                        else self.mo_user_password if user == self.mo_user \
                        else self.non_staff_user_password if user == self.non_staff_user \
                        else self.superuser_password
                    self.client.login(username=user.username, password=password)
                response = self.client.get(url, data={'unit': None, 'target': None})
                self.assertEqual(exp_status_code, response.status_code)


    def test_projects_list_limited_visibility(self):
        # test which projects in the projects list can be see by which users. expected:
        #
        # | AnonymousUser  | [public_project, public_project2]                  | everyone can see public projects
        # | temp_user      |            ""          ""                          | ""
        # | non_staff_user |            ""          ""                          | ""
        # | superuser      | [          ""          ""       , private_project] | super can see all projects
        # | po_user        |            ""          ""       ,      ""          | private_project project owner
        # | mo_user        |            ""          ""       ,      ""          | private_project model owner
        #
        temp_user_password = 'p'
        temp_user = User.objects.create_user(username="temp", password=temp_user_password)
        user_to_password = {
            temp_user: temp_user_password,
            self.po_user: self.po_user_password,
            self.superuser: self.superuser_password,
            self.mo_user: self.mo_user_password,
            self.non_staff_user: self.non_staff_user_password,
        }

        projects_url = reverse('projects', args=[])
        public_project_url = reverse('project-detail', args=[str(self.public_project.pk)])
        public_project2_url = reverse('project-detail', args=[str(self.public_project2.pk)])
        private_project_url = reverse('project-detail', args=[str(self.private_project.pk)])

        # AnonymousUser, temp_user, and non_staff_user can only see self.public_project
        self.client.logout()  # AnonymousUser
        response = self.client.get(projects_url)
        self.assertIn(public_project_url, str(response.content))
        self.assertIn(public_project2_url, str(response.content))
        self.assertNotIn(private_project_url, str(response.content))

        self.client.login(username=temp_user.username, password=user_to_password[temp_user])
        response = self.client.get(projects_url)
        self.assertIn(public_project_url, str(response.content))
        self.assertNotIn(private_project_url, str(response.content))

        self.client.login(username=self.non_staff_user.username, password=user_to_password[self.non_staff_user])
        response = self.client.get(projects_url)
        self.assertIn(public_project_url, str(response.content))
        self.assertNotIn(private_project_url, str(response.content))

        for user in [self.superuser, self.po_user, self.mo_user]:
            self.client.login(username=user.username, password=user_to_password[user])
            response = self.client.get(projects_url)
            self.assertIn(public_project_url, str(response.content))
            self.assertIn(public_project2_url, str(response.content))
            self.assertIn(private_project_url, str(response.content))


    def test_url_edit_delete_upload_create_links(self):
        url_to_exp_content = {
            # model detail page for public model
            reverse('model-detail', args=[str(self.public_model.pk)]): {
                reverse('edit-model', args=[str(self.public_model.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('delete-model', args=[str(self.public_model.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('delete-forecast', args=[str(self.public_forecast.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('upload-forecast', args=[str(self.public_model.pk), str(self.public_tz2.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
            },
            # model detail page for private model. this is the only private model test as we depend on other tests
            # to check accessibility. this is a sanity check, in other words :-)
            reverse('model-detail', args=[str(self.private_model.pk)]): {
                reverse('edit-model', args=[str(self.private_model.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('delete-model', args=[str(self.private_model.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('delete-forecast', args=[str(self.private_forecast.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('upload-forecast', args=[str(self.private_model.pk), str(self.private_tz2.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
            },
            # project list
            reverse('projects', args=[]): {
                reverse('create-project-from-form', args=[]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('create-project-from-file', args=[]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
            },
            # project detail - public_project (has truth)
            reverse('project-detail', args=[str(self.public_project.pk)]): {
                reverse('edit-project-from-form', args=[str(self.public_project.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('delete-project', args=[str(self.public_project.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('create-model', args=[str(self.public_project.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, True),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
            },
            # project detail - public_project2 (no truth)
            reverse('project-detail', args=[str(self.public_project2.pk)]): {
                reverse('upload-truth', args=[str(self.public_project2.pk)]):  # no truth -> upload link
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
            },
            reverse('truth-data-detail', args=[str(self.public_project.pk)]): {
                reverse('delete-truth', args=[str(self.public_project.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
            },
            # user detail - public_project (has truth)
            reverse('user-detail', args=[str(self.po_user.pk)]): {
                reverse('edit-user', args=[str(self.po_user.pk)]):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
                reverse('change-password'):
                    [(self.po_user, True),
                     (self.mo_user, False),
                     (self.superuser, True),
                     (self.non_staff_user, False)],
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
                        else self.non_staff_user_password if user == self.non_staff_user \
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

                self.client.login(username=self.non_staff_user.username, password=self.non_staff_user_password)
                response = self.client.post(url)
                self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)


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
        split_content = response.content.decode().split('\r\n')
        self.assertEqual(','.join(SCORE_CSV_HEADER_PREFIX), split_content[0])
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
        url_exp_user_status_code_pairs = [
            (reverse('api-root'), self.ONLY_PO_MO_STAFF),

            (reverse('api-project-list'), self.ONLY_PO_MO_STAFF),
            (reverse('api-project-detail', args=[self.public_project.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-project-detail', args=[self.private_project.pk]), self.ONLY_PO_MO),
            (reverse('api-unit-list', args=[self.public_project.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-unit-list', args=[self.private_project.pk]), self.ONLY_PO_MO),
            (reverse('api-target-list', args=[self.public_project.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-target-list', args=[self.private_project.pk]), self.ONLY_PO_MO),
            (reverse('api-timezero-list', args=[self.public_project.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-timezero-list', args=[self.private_project.pk]), self.ONLY_PO_MO),
            (reverse('api-model-list', args=[self.public_project.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-model-list', args=[self.private_project.pk]), self.ONLY_PO_MO),
            (reverse('api-truth-detail', args=[self.public_project.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-truth-detail', args=[self.private_project.pk]), self.ONLY_PO_MO),
            (reverse('api-truth-data-download', args=[self.public_project.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-truth-data-download', args=[self.private_project.pk]), self.ONLY_PO_MO),
            (reverse('api-score-data-download', args=[self.public_project.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-score-data-download', args=[self.private_project.pk]), self.ONLY_PO_MO),

            (reverse('api-user-detail', args=[self.po_user.pk]), self.ONLY_PO),
            (reverse('api-job-detail', args=[self.job.pk]), self.ONLY_PO),
            (reverse('api-unit-detail', args=[unit_us_nat.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-target-detail', args=[target_1wk.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-timezero-detail', args=[self.public_tz1.pk]), self.ONLY_PO_MO_STAFF),

            (reverse('api-model-detail', args=[self.public_model.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-model-detail', args=[self.private_model.pk]), self.ONLY_PO_MO),
            (reverse('api-forecast-list', args=[self.public_model.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-forecast-list', args=[self.private_model.pk]), self.ONLY_PO_MO),

            (reverse('api-forecast-detail', args=[self.public_forecast.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-forecast-detail', args=[self.private_forecast.pk]), self.ONLY_PO_MO),
            (reverse('api-forecast-data', args=[self.public_forecast.pk]), self.ONLY_PO_MO_STAFF),
            (reverse('api-forecast-data', args=[self.private_forecast.pk]), self.ONLY_PO_MO),
        ]
        for url, user_exp_status_code_list in url_exp_user_status_code_pairs:
            for user, exp_status_code in user_exp_status_code_list:
                # authenticate using JWT. used instead of web API self.client.login() authentication elsewhere b/c
                # base.py configures JWT: REST_FRAMEWORK > DEFAULT_AUTHENTICATION_CLASSES > JSONWebTokenAuthentication
                self.client.logout()  # AnonymousUser
                if user:
                    password = self.po_user_password if user == self.po_user \
                        else self.mo_user_password if user == self.mo_user \
                        else self.non_staff_user_password if user == self.non_staff_user \
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

        response = self.client.get(reverse('api-job-detail', args=[self.job.pk]))
        self.assertEqual(['id', 'url', 'status', 'user', 'created_at', 'updated_at', 'failure_message', 'input_json',
                          'output_json'],
                         list(response.data))

        response = self.client.get(reverse('api-project-list'), format='json')
        self.assertEqual(3, len(response.data))  # assume contents are checked below

        response = self.client.get(reverse('api-project-detail', args=[self.public_project.pk]), format='json')
        self.assertEqual(['id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'logo_url', 'core_data',
                          'time_interval_type', 'visualization_y_label', 'truth', 'model_owners', 'score_data',
                          'models', 'units', 'targets', 'timezeros'],
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
        self.assertEqual(['id', 'url', 'project', 'truth_csv_filename', 'truth_updated_at', 'truth_data'],
                         list(response.data))

        response = self.client.get(reverse('api-truth-data-download', args=[self.public_project.pk]), format='json')
        self.assertEqual(341, len(response.content))

        response = self.client.get(reverse('api-score-data-download', args=[self.public_project.pk]), format='json')
        # just SCORE_CSV_HEADER_PREFIX due to no scores:
        self.assertEqual(','.join(SCORE_CSV_HEADER_PREFIX), response.content.decode().strip())

        unit_us_nat = self.public_project.units.filter(name='US National').first()
        response = self.client.get(reverse('api-unit-detail', args=[unit_us_nat.pk]))
        self.assertEqual(['id', 'url', 'name'], list(response.data))

        target_1wk = self.public_project.targets.filter(name='1 wk ahead').first()
        response = self.client.get(reverse('api-target-detail', args=[target_1wk.pk]))
        self.assertEqual(['id', 'url', 'name', 'type', 'description', 'is_step_ahead', 'step_ahead_increment', 'unit',
                          'cats'], list(response.data))

        response = self.client.get(reverse('api-timezero-detail', args=[self.public_tz1.pk]))
        self.assertEqual(['id', 'url', 'timezero_date', 'data_version_date', 'is_season_start'],
                         list(response.data))  # no 'season_name'

        response = self.client.get(reverse('api-model-detail', args=[self.public_model.pk]), format='json')
        exp_keys = ['id', 'url', 'project', 'owner', 'name', 'abbreviation', 'team_name', 'description',
                    'contributors', 'license', 'notes', 'citation', 'methods', 'home_url', 'aux_data_url',
                    'forecasts']
        self.assertEqual(exp_keys, list(response.data))

        response = self.client.get(reverse('api-forecast-list', args=[self.public_model.pk]), format='json')
        response_dicts = json.loads(response.content)
        exp_keys = ['id', 'url', 'forecast_model', 'source', 'time_zero', 'created_at', 'notes', 'forecast_data']
        self.assertEqual(1, len(response_dicts))
        self.assertEqual(exp_keys, list(response_dicts[0]))

        response = self.client.get(reverse('api-forecast-detail', args=[self.public_forecast.pk]), format='json')
        exp_keys = ['id', 'url', 'forecast_model', 'source', 'time_zero', 'created_at', 'notes', 'forecast_data']
        self.assertEqual(exp_keys, list(response.data))

        # note that we only check top-level keys b/c we know json_response_for_forecast() uses
        # json_io_dict_from_forecast(), which is tested separately
        response = self.client.get(reverse('api-forecast-data', args=[self.public_forecast.pk]), format='json')
        response_dict = json.loads(response.content)
        self.assertEqual({'meta', 'predictions'}, set(response_dict))
        self.assertEqual({'forecast', 'units', 'targets'}, set(response_dict['meta']))


    def test_timezero_serialization_api_timezero_detail(self):
        self._authenticate_jwt_user(self.po_user, self.po_user_password)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), self.po_user)

        # test 'api-timezero-detail' | '2011-10-02'
        timezero = project.timezeros.filter(timezero_date='2011-10-02').first()
        response = self.client.get(reverse('api-timezero-detail', args=[timezero.pk]))
        # yes 'season_name' b/c 'is_season_start':
        self.assertEqual({'is_season_start', 'season_name', 'data_version_date', 'id', 'timezero_date', 'url'},
                         set(response.data))

        # test 'api-timezero-detail' | '2011-10-09'
        timezero = project.timezeros.filter(timezero_date='2011-10-09').first()
        response = self.client.get(reverse('api-timezero-detail', args=[timezero.pk]))
        # no 'season_name' b/c not 'is_season_start':
        self.assertEqual({'timezero_date', 'id', 'data_version_date', 'is_season_start', 'url'}, set(response.data))


    def test_timezero_serialization_api_timezero_list(self):
        self._authenticate_jwt_user(self.po_user, self.po_user_password)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), self.po_user)

        # note: using APIRequestFactory was the only way I could find to pass a request object. o/w you get:
        #   AssertionError: `HyperlinkedIdentityField` requires the request in the serializer context.
        # test serializing multiple timezeros via direct instantiation
        timezero_serializer_multi = TimeZeroSerializer(project.timezeros, many=True,
                                                       context={'request': (APIRequestFactory().request())})
        # -> <class 'rest_framework.serializers.ListSerializer'>
        self.assertEqual(3, len(timezero_serializer_multi.data))  # 3 timezeros

        # spot-check two of them
        tz_2011_10_02_dict = [_ for _ in timezero_serializer_multi.data if _['timezero_date'] == '2011-10-02'][0]
        # yes 'season_name' b/c 'is_season_start':
        self.assertEqual({'id', 'url', 'timezero_date', 'data_version_date', 'is_season_start', 'season_name'},
                         set(tz_2011_10_02_dict))

        tz_2011_10_16_dict = [_ for _ in timezero_serializer_multi.data if _['timezero_date'] == '2011-10-16'][0]
        # no 'season_name' b/c not 'is_season_start':
        self.assertEqual({'id', 'url', 'timezero_date', 'data_version_date', 'is_season_start'},
                         set(tz_2011_10_16_dict))

        # finally, test serializing multiple timezeros via endpoints
        response = self.client.get(reverse('api-timezero-list', args=[project.pk]), format='json')
        self.assertEqual(3, len(response.data))

        # spot-check two of them
        tz_2011_10_02_dict = [_ for _ in response.data if _['timezero_date'] == '2011-10-02'][0]
        self.assertEqual({'id', 'url', 'timezero_date', 'data_version_date', 'is_season_start', 'season_name'},
                         set(tz_2011_10_02_dict))

        tz_2011_10_16_dict = [_ for _ in response.data if _['timezero_date'] == '2011-10-16'][0]
        self.assertEqual({'id', 'url', 'timezero_date', 'data_version_date', 'is_season_start'},
                         set(tz_2011_10_16_dict))


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
        request = APIRequestFactory().request()

        # test serializing a few single Targets ('pct next week' and 'Season peak week')
        pct_next_week_target = project.targets.filter(name='pct next week').first()
        pct_next_week_serializer = TargetSerializer(pct_next_week_target, context={'request': request})
        # -> <class 'forecast_app.serializers.TargetSerializer'>
        self.assertEqual({'name', 'id', 'step_ahead_increment', 'url', 'is_step_ahead', 'range', 'description', 'unit',
                          'type', 'cats'}, set(pct_next_week_serializer.data))
        self.assertEqual([0.0, 100.0], pct_next_week_serializer.data['range'])  # sanity-check

        season_peak_week_target = project.targets.filter(name='Season peak week').first()
        season_peak_week_serializer = TargetSerializer(season_peak_week_target, context={'request': request})
        self.assertEqual({'description', 'is_step_ahead', 'url', 'name', 'type', 'id', 'cats', 'unit'},
                         set(season_peak_week_serializer.data))
        self.assertEqual(f"http://testserver/api/target/{season_peak_week_serializer.data['id']}/",
                         season_peak_week_serializer.data['url'])  # sanity-check

        # test serializing multiple Targets
        target_serializer_multi = TargetSerializer(project.targets, many=True, context={'request': request})
        # -> <class 'rest_framework.serializers.ListSerializer'>
        self.assertEqual(5, len(target_serializer_multi.data))  # 5 targets

        season_peak_week_data = [serialized_data for serialized_data in target_serializer_multi.data
                                 if serialized_data['name'] == 'Season peak week'][0]
        self.assertEqual(season_peak_week_serializer.data, season_peak_week_data)  # single matches multi

        # finally, test serializing multiple Targets via endpoints
        response = self.client.get(reverse('api-target-list', args=[project.pk]), format='json')
        self.assertEqual(5, len(response.data))

        season_peak_week_data = [serialized_data for serialized_data in response.data
                                 if serialized_data['name'] == 'Season peak week'][0]
        self.assertEqual(season_peak_week_serializer.data, season_peak_week_data)  # single matches multi


    @patch('rq.queue.Queue.enqueue')
    def test_api_delete_forecast(self, enqueue_mock):
        # anonymous delete: self.public_forecast -> disallowed
        response = self.client.delete(reverse('api-forecast-detail', args=[self.public_forecast.pk]))
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        # authorized self.mo_user: delete private_forecast2 (new Forecast) -> allowed
        self._authenticate_jwt_user(self.mo_user, self.mo_user_password)
        self.assertEqual(1, self.private_model.forecasts.count())

        # note: b/c ForecastDetail.delete enqueues the deletion, there could be a possible race condition in this test.
        # so we just test that delete() calls enqueue_delete_forecast(), and trust that enqueue_delete_forecast()
        # enqueues a _delete_forecast_worker() call (to simple to fail)
        private_forecast2 = load_cdc_csv_forecast_file(2016, self.private_model, self.csv_file_path, self.private_tz1)
        private_forecast2_pk = private_forecast2.pk
        with patch('rq.queue.Queue.enqueue') as mock:
            json_response = self.client.delete(reverse('api-forecast-detail', args=[private_forecast2.pk]))  # enqueues
            response_json = json_response.json()  # JobSerializer
            mock.assert_called_once()
            self.assertEqual('_delete_forecast_worker', mock.call_args[0][0].__name__)

            self.assertEqual(status.HTTP_200_OK, json_response.status_code)
            self.assertEqual({'id', 'url', 'status', 'user', 'created_at', 'updated_at', 'failure_message',
                              'input_json', 'output_json'}, set(response_json.keys()))
            self.assertEqual(Job.QUEUED, response_json['status'])
            self.assertEqual(private_forecast2_pk, response_json['input_json']['forecast_pk'])

        # test _delete_forecast_worker() itself (which is called by workers)
        private_forecast3 = load_cdc_csv_forecast_file(2016, self.private_model, self.csv_file_path, self.private_tz1)
        job = Job.objects.create(user=self.mo_user)  # status = PENDING
        job.input_json = {'forecast_pk': private_forecast3.pk}
        job.save()
        with patch('django.db.models.Model.delete') as mock:
            _delete_forecast_worker(job.pk)
            mock.assert_called_once()

            job.refresh_from_db()
            self.assertEqual(Job.SUCCESS, job.status)


    def test_api_create_project(self):
        # case: not authorized. recall that only staff users can create
        json_response = self.client.post(reverse('api-project-list'), {
            'project_config': {},
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        # case: not authorized. recall that only staff users can create
        json_response = self.client.post(reverse('api-project-list'), {
            'project_config': {},
            'Authorization': f'JWT {self._authenticate_jwt_user(self.non_staff_user, self.non_staff_user_password)}',
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
        response_json = json_response.json()
        self.assertEqual({'id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'logo_url', 'core_data',
                          'time_interval_type', 'visualization_y_label', 'truth', 'model_owners', 'score_data',
                          'models', 'units', 'targets', 'timezeros'},
                         set(response_json.keys()))
        self.assertEqual('CDC Flu challenge', response_json['name'])


    def test_api_delete_project(self):
        # create a project to delete
        project2 = Project.objects.create(owner=self.po_user)

        # case: not authorized
        joe_user = User.objects.create_user(username='joe', password='password')
        response = self.client.delete(reverse('api-project-detail', args=[project2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(joe_user, "password")}',
        })
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        response = self.client.delete(reverse('api-project-detail', args=[project2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.non_staff_user, self.non_staff_user_password)}',
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

        json_response = self.client.post(reverse('api-project-detail', args=[project2.pk]), {
            'project_config': {},
            'Authorization': f'JWT {self._authenticate_jwt_user(self.non_staff_user, self.non_staff_user_password)}',
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
        self.assertEqual('new project name', json_response.json()['name'])


    def test_api_create_model(self):
        project2 = Project.objects.create(owner=self.po_user)
        ok_model_config = {'name': 'a model_name', 'abbreviation': 'an abbreviation', 'team_name': 'a team_name',
                           'contributors': 'the contributors', 'license': 'other', 'notes': 'some notes',
                           'citation': 'the citation', 'methods': 'our methods', 'description': 'a description',
                           'home_url': 'http://example.com/', 'aux_data_url': 'http://example.com/'}

        # case: not authorized. recall user must be a superuser, project owner, or model owner. and: staff
        json_response = self.client.post(reverse('api-model-list', args=[project2.pk]), {
            'model_config': {},
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        json_response = self.client.post(reverse('api-model-list', args=[project2.pk]), {
            'model_config': {},
            'Authorization': f'JWT {self._authenticate_jwt_user(self.non_staff_user, self.non_staff_user_password)}'
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
        self.assertIn("Wrong keys in 'model_config'", json_response.json()['error'])

        # case: bad 'model_config': invalid license
        model_config = dict(ok_model_config)
        model_config['license'] = 'bad license'
        json_response = self.client.post(reverse('api-model-list', args=[project2.pk]), {
            'model_config': model_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
        self.assertIn("invalid license", json_response.json()['error'])

        # case: authorized
        json_response = self.client.post(reverse('api-model-list', args=[project2.pk]), {
            'model_config': ok_model_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_200_OK, json_response.status_code)
        self.assertEqual({'id', 'url', 'project', 'owner', 'name', 'abbreviation', 'team_name', 'description',
                          'contributors', 'license', 'notes', 'citation', 'methods', 'home_url', 'aux_data_url',
                          'forecasts'}, set(json_response.json().keys()))

        # check response contents
        response_json = json_response.json()
        self.assertEqual(
            {'id', 'url', 'project', 'owner', 'name', 'abbreviation', 'team_name', 'description', 'contributors',
             'license', 'notes', 'citation', 'methods', 'home_url', 'aux_data_url', 'forecasts', },
            set(response_json.keys()))
        self.assertEqual('a model_name', response_json['name'])

        act_model_config = {'name': response_json['name'], 'abbreviation': response_json['abbreviation'],
                            'team_name': response_json['team_name'], 'contributors': response_json['contributors'],
                            'license': response_json['license'], 'notes': response_json['notes'],
                            'citation': response_json['citation'], 'methods': response_json['methods'],
                            'description': response_json['description'], 'home_url': response_json['home_url'],
                            'aux_data_url': response_json['aux_data_url']}
        self.assertEqual(ok_model_config, act_model_config)


    def test_api_edit_model(self):
        # pretty much identical to `test_api_create_model()` :-/
        project2 = Project.objects.create(owner=self.po_user)
        forecast_model2 = ForecastModel.objects.create(project=project2, name='name', abbreviation='abbrev',
                                                       owner=self.po_user)
        ok_model_config = {'name': 'a model_name', 'abbreviation': 'an abbreviation', 'team_name': 'a team_name',
                           'contributors': 'the contributors', 'license': 'other', 'notes': 'some notes',
                           'citation': 'the citation', 'methods': 'our methods', 'description': 'a description',
                           'home_url': 'http://example.com/', 'aux_data_url': 'http://example.com/'}

        # case: not authorized. recall user must be a superuser, project owner, or model owner
        json_response = self.client.put(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'model_config': {},
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        # case: no 'model_config'
        json_response = self.client.put(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
        self.assertEqual({'error': "No 'model_config' data."}, json_response.json())

        # case: bad 'model_config': missing expected_keys:
        #   {'name', 'abbreviation', 'team_name', 'description', 'home_url', 'aux_data_url'}
        model_config = {}
        json_response = self.client.put(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'model_config': model_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
        self.assertIn("Wrong keys in 'model_config'", json_response.json()['error'])

        # case: bad 'model_config': invalid license
        model_config = dict(ok_model_config)
        model_config['license'] = 'bad license'
        json_response = self.client.put(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'model_config': model_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
        self.assertIn("invalid license", json_response.json()['error'])

        # case: authorized
        json_response = self.client.put(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'model_config': ok_model_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_200_OK, json_response.status_code)


    def test_api_delete_model(self):
        # create a model to delete
        project2 = Project.objects.create(owner=self.po_user)
        forecast_model2 = ForecastModel.objects.create(project=project2, name='name', abbreviation='abbrev',
                                                       owner=self.po_user)

        # case: not authorized
        joe_user = User.objects.create_user(username='joe', password='password')
        response = self.client.delete(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(joe_user, "password")}',
        })
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        response = self.client.delete(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.non_staff_user, self.non_staff_user_password)}',
        })
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        # case: authorized
        response = self.client.delete(reverse('api-model-detail', args=[forecast_model2.pk]), {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        })
        self.assertEqual(status.HTTP_204_NO_CONTENT, response.status_code)


    def test_api_create_timezero(self):
        project2 = Project.objects.create(owner=self.po_user)

        # case: not authorized. recall user must be a superuser, project owner, or model owner. and: staff
        json_response = self.client.post(reverse('api-timezero-list', args=[project2.pk]), {
            'timezero_config': {},
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        json_response = self.client.post(reverse('api-timezero-list', args=[project2.pk]), {
            'timezero_config': {},
            'Authorization': f'JWT {self._authenticate_jwt_user(self.non_staff_user, self.non_staff_user_password)}'
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
        self.assertIn("Wrong keys in 'timezero_config'", json_response.json()['error'])

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
        timezero_config = {'timezero_date': '2017-12-02',  # different from above, else errors
                           'data_version_date': '2017-12-02',
                           'is_season_start': False}
        json_response = self.client.post(reverse('api-timezero-list', args=[project2.pk]), {
            'timezero_config': timezero_config,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_200_OK, json_response.status_code)
        self.assertEqual(set(json_response.json().keys()),
                         {'id', 'url', 'timezero_date', 'data_version_date', 'is_season_start'})  # no 'season_name'


    def test_api_upload_truth(self):
        # to avoid the requirement of RQ, redis, and S3, we patch _upload_file() to return (is_error, job)
        # with desired return args
        with patch('forecast_app.views._upload_file') as upload_file_mock:
            # upload_truth_url = reverse('api-upload-truth-data', args=[str(self.public_project.pk)])
            upload_truth_url = reverse('api-truth-detail', args=[str(self.public_project.pk)])
            data_file = SimpleUploadedFile('file.json', b'file_content', content_type='application/csv')

            # case: not authorized
            joe_user = User.objects.create_user(username='joe', password='password')
            json_response = self.client.post(upload_truth_url, {
                'Authorization': f'JWT {self._authenticate_jwt_user(joe_user, "password")}',
            }, format='multipart')
            self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

            json_response = self.client.post(upload_truth_url, {
                'Authorization': f'JWT {self._authenticate_jwt_user(self.non_staff_user, self.non_staff_user_password)}',
            }, format='multipart')
            self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

            # case: no 'data_file'
            jwt_token = self._authenticate_jwt_user(self.po_user, self.po_user_password)
            json_response = self.client.post(upload_truth_url, {
                'Authorization': f'JWT {jwt_token}',
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
            self.assertEqual({'error': "No 'data_file' form field."}, json_response.json())

            # case: _upload_file() -> is_error
            upload_file_mock.return_value = True, None  # is_error, job
            json_response = self.client.post(upload_truth_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
            self.assertEqual({'error': "There was an error uploading the file. The error was: 'True'"},
                             json_response.json())

            # case: blue sky: _upload_file() -> NOT is_error
            job_return_value = Job.objects.create()
            upload_file_mock.return_value = False, job_return_value  # is_error, job
            json_response = self.client.post(upload_truth_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
            }, format='multipart')
            self.assertEqual(status.HTTP_200_OK, json_response.status_code)
            self.assertEqual(job_return_value.id, json_response.json()['id'])


    def test_api_upload_forecast(self):
        # to avoid the requirement of RQ, redis, and S3, we patch _upload_file() to return (is_error, job)
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

            json_response = self.client.post(upload_forecast_url, {
                'Authorization': f'JWT {self._authenticate_jwt_user(self.non_staff_user, self.non_staff_user_password)}',
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
            self.assertEqual({'error': "No 'data_file' form field."}, json_response.json())

            # case: no 'timezero_date'
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
            self.assertIn("No 'timezero_date' form field", json_response.json()['error'])

            # case: invalid 'timezero_date' format - YYYY_MM_DD_DATE_FORMAT
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': 'x20171202',
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
            self.assertIn("Badly formatted 'timezero_date' form field", json_response.json()['error'])

            # case: timezero not found
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': '2017-12-03',  # NOT public_tz1 or public_tz2
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
            self.assertIn("TimeZero not found for 'timezero_date' form field", json_response.json()['error'])

            # case: existing_forecast_for_time_zero
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': '2017-12-01',  # public_tz1
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
            self.assertIn("A forecast already exists", json_response.json()['error'])

            # case: blue sky: _upload_file() -> NOT is_error
            upload_file_mock.return_value = False, Job.objects.create()  # is_error, job
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': self.public_tz2.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),
            }, format='multipart')
            response_dict = json.loads(json_response.content)
            self.assertEqual(status.HTTP_200_OK, json_response.status_code)
            self.assertEqual({'id', 'url', 'status', 'user', 'created_at', 'updated_at', 'failure_message',
                              'input_json', 'output_json'}, set(response_dict.keys()))

            call_dict = upload_file_mock.call_args[1]
            self.assertIn('forecast_model_pk', call_dict)
            self.assertIn('timezero_pk', call_dict)
            self.assertEqual(self.public_model.pk, call_dict['forecast_model_pk'])
            self.assertEqual(self.public_tz2.pk, call_dict['timezero_pk'])

            act_time_zero = TimeZero.objects.get(pk=call_dict['timezero_pk'])
            self.assertEqual(self.public_tz2.timezero_date, act_time_zero.timezero_date)

            # case: _upload_file() -> is_error
            upload_file_mock.return_value = True, None  # is_error, job
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': self.public_tz2.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
            self.assertIn("There was an error uploading the file", json_response.json()['error'])

            # case: error: time_zero not found. (does not auto-create)
            upload_file_mock.return_value = False, Job.objects.create()  # is_error, job
            new_timezero_date = '19621022'
            json_response = self.client.post(upload_forecast_url, {
                'data_file': data_file,
                'Authorization': f'JWT {jwt_token}',
                'timezero_date': new_timezero_date,  # doesn't exist
            }, format='multipart')
            self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
            self.assertIn("Badly formatted 'timezero_date' form field", json_response.json()['error'])


    @patch('rq.queue.Queue.enqueue')
    def test_api_forecast_queries(self, enqueue_mock):
        forecast_queries_url = reverse('api-forecast-queries', args=[str(self.public_project.pk)])
        jwt_token = self._authenticate_jwt_user(self.mo_user, self.mo_user_password)

        # test that GET is not accepted
        response = self.client.get(forecast_queries_url)
        self.assertEqual(status.HTTP_405_METHOD_NOT_ALLOWED, response.status_code)

        # case: no 'query'
        response = self.client.post(forecast_queries_url, {
            'Authorization': f'JWT {jwt_token}',
            # 'query': {},
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertEqual({'error': "No 'query' form field."}, response.json())

        # ensure `validate_forecasts_query()` is called. the actual validate is tested in test_project.py
        with patch('utils.project.validate_forecasts_query', return_value=([], None)) as mock:
            self.client.post(forecast_queries_url, {
                'Authorization': f'JWT {jwt_token}',
                'query': {'hi': 1},
            }, format='json')
            mock.assert_called_once_with(self.public_project, {'hi': 1})

        # case: blue sky: test that POST enqueues and returns a Job
        enqueue_mock.reset_mock()
        json_response = self.client.post(forecast_queries_url, {
            'Authorization': f'JWT {jwt_token}',
            'query': {},
        }, format='json')
        response_json = json_response.json()  # JobSerializer
        self.assertEqual(1, enqueue_mock.call_count)

        self.assertEqual(status.HTTP_200_OK, json_response.status_code)
        self.assertEqual(Job.QUEUED, response_json['status'])

        # case: unauthenticated user (authenticated tested above)
        self.client.logout()  # AnonymousUser
        json_response = self.client.post(forecast_queries_url, {
            'query': {},
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)


    def test__query_forecasts_worker(self):
        # tests the worker directly. above test verifies that it's called from `query_forecasts_endpoint()`

        # case: upload_file() does not error
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.public_project.pk, 'query': {}})
        with patch('utils.cloud_file.upload_file') as mock:
            _query_forecasts_worker(job.pk)
            mock.assert_called_once()

            job.refresh_from_db()
            self.assertEqual(Job.SUCCESS, job.status)

        # case: upload_file() errors
        job = Job.objects.create(user=self.po_user, input_json={'project_pk': self.public_project.pk, 'query': {}})
        with patch('utils.cloud_file.upload_file', side_effect=Exception('foo!')) as mock, \
                patch('forecast_app.notifications.send_notification_email'):
            # patch('forecast_app.models.job.send_notification_for_job'):
            _query_forecasts_worker(job.pk)
            mock.assert_called_once()

            job.refresh_from_db()
            self.assertEqual(Job.FAILED, job.status)
            self.assertIn("_query_forecasts_worker(): error uploading file to cloud", job.failure_message)

        # case: allow actual utils.cloud_file.upload_file(), which calls Bucket.put_object(). we don't actually do this
        # in this test b/c we don't want to hit S3, but it's commented here for debugging:
        # _query_forecasts_worker(job.pk)
        # job.refresh_from_db()
        # self.assertEqual(Job.SUCCESS, job.status)


    def test_api_job_data_download(self):
        job_data_download_url = reverse('api-job-data-download', args=[self.job.pk])  # owner self.po_user

        # case: unauthorized: anonymous
        self.client.logout()  # AnonymousUser
        response = self.client.get(job_data_download_url)
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        # case: unauthorized: mo_user
        self._authenticate_jwt_user(self.mo_user, self.mo_user_password)
        response = self.client.get(job_data_download_url)
        self.assertEqual(status.HTTP_403_FORBIDDEN, response.status_code)

        # case: no Job.input_json
        self._authenticate_jwt_user(self.superuser, self.superuser_password)
        response = self.client.get(job_data_download_url)
        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)

        # case: no 'query' in Job.input_json
        job = Job.objects.create(user=self.po_user, input_json={})
        job_data_download_url = reverse('api-job-data-download', args=[job.pk])
        self._authenticate_jwt_user(self.superuser, self.superuser_password)
        response = self.client.get(job_data_download_url)
        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)

        # case: authorized: superuser + mocked utils.cloud_file.download_file() (no S3 hit)
        with patch('utils.cloud_file.download_file') as mock:
            job = Job.objects.create(user=self.po_user, input_json={'query': {}})
            job_data_download_url = reverse('api-job-data-download', args=[job.pk])
            self._authenticate_jwt_user(self.superuser, self.superuser_password)
            response = self.client.get(job_data_download_url)
            mock.assert_called_once()
            self.assertEqual(status.HTTP_200_OK, response.status_code)

            # case: authorized: self.po_user
            self._authenticate_jwt_user(self.po_user, self.po_user_password)
            response = self.client.get(job_data_download_url)
            self.assertEqual(status.HTTP_200_OK, response.status_code)


    def test_api_put_forecast_source(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)
        self.assertEqual('docs-predictions.json', forecast.source)

        # case: not authorized
        forecast_url = reverse('api-forecast-detail', args=[forecast.pk])
        json_response = self.client.put(forecast_url, {}, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        json_response = self.client.put(forecast_url, {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.non_staff_user, self.non_staff_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_403_FORBIDDEN, json_response.status_code)

        # case: no 'source'
        json_response = self.client.put(forecast_url, {
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        self.assertEqual(status.HTTP_400_BAD_REQUEST, json_response.status_code)
        self.assertEqual({'error': "No 'source' data."}, json_response.json())

        # case: authorized
        new_source = 'new source'
        json_response = self.client.put(forecast_url, {
            'source': new_source,
            'Authorization': f'JWT {self._authenticate_jwt_user(self.po_user, self.po_user_password)}',
        }, format='json')
        forecast.refresh_from_db()
        self.assertEqual(status.HTTP_200_OK, json_response.status_code)
        self.assertEqual(new_source, forecast.source)


    def _authenticate_jwt_user(self, user, password):
        jwt_auth_url = reverse('auth-jwt-get')
        jwt_auth_resp = self.client.post(jwt_auth_url, {'username': user.username, 'password': password}, format='json')
        jwt_token = jwt_auth_resp.data['token']
        self.client.credentials(HTTP_AUTHORIZATION='JWT ' + jwt_token)
        return jwt_token
