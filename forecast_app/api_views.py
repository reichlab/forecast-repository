import datetime
import logging
import tempfile
from wsgiref.util import FileWrapper

import django_rq
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError, ClientError, ConnectionClosedError
from django.contrib.auth.mixins import UserPassesTestMixin
from django.contrib.auth.models import User
from django.http import JsonResponse, HttpResponseForbidden, HttpResponse, HttpResponseBadRequest, \
    HttpResponseNotFound
from django.utils.text import get_valid_filename
from rest_framework import generics, status
from rest_framework.decorators import api_view, renderer_classes
from rest_framework.generics import get_object_or_404
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework_csv.renderers import CSVRenderer

from forecast_app.models import Project, ForecastModel, Forecast, Target
from forecast_app.models.job import Job, JOB_TYPE_QUERY_FORECAST, JOB_TYPE_UPLOAD_TRUTH, \
    JOB_TYPE_UPLOAD_FORECAST, JOB_TYPE_QUERY_SCORE, JOB_TYPE_QUERY_TRUTH
from forecast_app.models.project import TimeZero, Unit
from forecast_app.serializers import ProjectSerializer, UserSerializer, ForecastModelSerializer, ForecastSerializer, \
    TruthSerializer, JobSerializer, TimeZeroSerializer, UnitSerializer, TargetSerializer
from forecast_app.views import is_user_ok_edit_project, is_user_ok_edit_model, is_user_ok_create_model, \
    _upload_truth_worker, enqueue_delete_forecast, is_user_ok_delete_forecast, is_user_ok_create_project, \
    is_user_ok_view_project
from forecast_repo.settings.base import QUERY_FORECAST_QUEUE_NAME
from utils.forecast import json_io_dict_from_forecast
from utils.project import create_project_from_json, config_dict_from_project
from utils.project_diff import execute_project_config_diff, project_config_diff
from utils.project_queries import _forecasts_query_worker, _scores_query_worker, _truth_query_worker
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


logger = logging.getLogger(__name__)


#
# Root view
#

@api_view(['GET'])
def api_root(request, format=None):
    if not request.user.is_authenticated:
        return HttpResponseForbidden()

    return Response({
        'projects': reverse('api-project-list', request=request, format=format),
    })


#
# List- and detail-related views
#

class ProjectList(UserPassesTestMixin, generics.ListAPIView):
    """
    View that returns a list of Projects. Filters out those projects that the requesting user is not authorized to view.
    Note that this means API users have more limited access than the web home page, which lists all projects regardless
    of whether the user is not authorized to view or not. POST to this view to create a new Project from a configuration
    file.
    """
    serializer_class = ProjectSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def get_queryset(self):
        return [project for project in Project.objects.all() if is_user_ok_view_project(self.request.user, project)]


    def test_func(self):
        return self.request.user.is_authenticated


    def post(self, request, *args, **kwargs):
        """
        Creates a new Project based on a project config file ala create_project_from_json(). Runs in the calling thread
        and therefore blocks. POST form fields:
        - request.data (required) must have a 'project_config' field containing a dict valid for
            create_project_from_json(). NB: this is different from other API args in this file in that it takes all
            required information as data, whereas others take their main data as a file in request.FILES, plus some
            additional data in request.data.
        """
        if not is_user_ok_create_project(request.user):
            return HttpResponseForbidden()
        elif 'project_config' not in request.data:
            return JsonResponse({'error': "No 'project_config' data."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            new_project = create_project_from_json(request.data['project_config'], request.user)
            project_serializer = ProjectSerializer(new_project, context={'request': request})
            return JsonResponse(project_serializer.data)
        except Exception as ex:
            return JsonResponse({'error': str(ex)}, status=status.HTTP_400_BAD_REQUEST)


class ProjectDetail(UserPassesTestMixin, generics.RetrieveDestroyAPIView):
    """
    View that returns a Project's details. DELETE to delete the project. POST to this view to edit a Project via "diffs"
    from a configuration file.
    """
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        project = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, project)


    def delete(self, request, *args, **kwargs):
        """
        Deletes this project. Runs in the calling thread and therefore blocks.
        """
        project = self.get_object()
        if not is_user_ok_edit_project(request.user, project):  # only the project owner can delete the project
            return HttpResponseForbidden()

        # imported here so that tests can patch via mock:
        from utils.project import delete_project_iteratively


        # we call our own delete_project_iteratively() instead of using DestroyModelMixin.destroy(), which calls
        # instance.delete()
        delete_project_iteratively(project)  # more memory-efficient. o/w fails on Heroku for large projects
        return Response(status=status.HTTP_204_NO_CONTENT)


    def post(self, request, *args, **kwargs):
        """
        Edits a Project via "diffs" from a configuration file ala execute_project_config_diff(). Runs in the calling
        thread and therefore blocks. POST form fields:
        - request.data (required) must have a 'project_config' field containing a dict valid for
            execute_project_config_diff(). NB: this is different from other API args in this file in that it takes all
            required information as data, whereas others take their main data as a file in request.FILES, plus some
            additional data in request.data.
        """
        project = self.get_object()
        if not is_user_ok_edit_project(request.user, project):  # only the project owner can edit the project
            return HttpResponseForbidden()
        elif 'project_config' not in request.data:
            return JsonResponse({'error': "No 'project_config' data."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            current_config_dict = config_dict_from_project(project, request)
            new_config_dict = request.data['project_config']
            changes = project_config_diff(current_config_dict, new_config_dict)
            # database_changes = database_changes_for_project_config_diff(project, changes)
            logger.debug(f"ProjectDetail.post(): executing project config diff... changes={changes}")
            execute_project_config_diff(project, changes)
            logger.debug(f"ProjectDetail.post(): done")
            project_serializer = ProjectSerializer(project, context={'request': request})
            return JsonResponse(project_serializer.data)
        except Exception as ex:
            return JsonResponse({'error': str(ex)}, status=status.HTTP_400_BAD_REQUEST)


class ProjectForecastModelList(UserPassesTestMixin, generics.ListAPIView):
    """
    View that returns a list of ForecastModels in a Project. This is different from other Views in this file b/c the
    serialized instances returned (ForecastModelSerializer) are different from this class's serializer_class
    (ForecastModelSerializer). Note that `pk` is the project's pk.
    """
    serializer_class = ForecastModelSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, project)


    def get_queryset(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return project.models.filter(is_oracle=False)


    def post(self, request, *args, **kwargs):
        """
        Creates a new ForecastModel based on a model config dict. Runs in the calling thread and therefore blocks.

        POST form fields:
        - request.data (required) must have a 'model_config' field containing these fields: ['name', 'abbreviation',
            'team_name', 'description', 'contributors', 'license', 'notes', 'citation', 'methods', 'home_url',
            'aux_data_url']
        """
        project = Project.objects.get(pk=self.kwargs['pk'])

        # check authorization, 'model_config'
        if not is_user_ok_create_model(request.user, project):
            return HttpResponseForbidden()
        elif 'model_config' not in request.data:
            return JsonResponse({'error': "No 'model_config' data."}, status=status.HTTP_400_BAD_REQUEST)

        # validate model_config
        model_config = request.data['model_config']
        actual_keys = set(model_config.keys())
        expected_keys = {'name', 'abbreviation', 'team_name', 'description', 'contributors', 'license', 'notes',
                         'citation', 'methods', 'home_url', 'aux_data_url'}
        if actual_keys != expected_keys:
            return JsonResponse({'error': f"Wrong keys in 'model_config'. difference={expected_keys ^ actual_keys}. "
                                          f"expected={expected_keys}, actual={actual_keys}"},
                                status=status.HTTP_400_BAD_REQUEST)

        # validate license
        if not ForecastModel.is_valid_license_abbreviation(model_config['license']):
            valid_license_abbrevs = [choice_abbrev for choice_abbrev, choice_name in ForecastModel.LICENSE_CHOICES]
            return JsonResponse({'error': f"invalid license: {model_config['license']!r}. must be one of: "
                                          f"{valid_license_abbrevs}"},
                                status=status.HTTP_400_BAD_REQUEST)

        try:
            model_init = {'project': project,
                          'owner': request.user,
                          'name': model_config['name'],
                          'abbreviation': model_config['abbreviation'],
                          'team_name': model_config['team_name'],
                          'description': model_config['description'],
                          'contributors': model_config['contributors'],
                          'license': model_config['license'],
                          'notes': model_config['notes'],
                          'citation': model_config['citation'],
                          'methods': model_config['methods'],
                          'home_url': model_config['home_url'],
                          'aux_data_url': model_config['aux_data_url']}
            new_model = ForecastModel.objects.create(**model_init)
            model_serializer = ForecastModelSerializer(new_model, context={'request': request})
            return JsonResponse(model_serializer.data)
        except Exception as ex:
            return JsonResponse({'error': str(ex)}, status=status.HTTP_400_BAD_REQUEST)


class ProjectUnitList(UserPassesTestMixin, generics.ListAPIView):
    """
    View that returns a list of Units in a Project, similar to ProjectTimeZeroList.
    """
    serializer_class = UnitSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, project)


    def get_queryset(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return project.units


class ProjectTargetList(UserPassesTestMixin, generics.ListAPIView):
    """
    View that returns a list of Targets in a Project, similar to ProjectTimeZeroList.
    """
    serializer_class = TargetSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, project)


    def get_queryset(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return project.targets


class ProjectTimeZeroList(UserPassesTestMixin, generics.ListAPIView):
    """
    View that returns a list of TimeZeros in a Project. This is different from other Views in this file b/c the
    serialized instances returned (TimeZeroSerializer) are different from this class's serializer_class
    (TimeZeroSerializer). Note that `pk` is the project's pk.
    """
    serializer_class = TimeZeroSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, project)


    def get_queryset(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return project.timezeros


    def post(self, request, *args, **kwargs):
        """
        Creates a new TimeZero for this project based on a config dict. Runs in the calling thread and therefore blocks.

        POST form fields:
        - request.data (required) must have a 'timezero_config' field containing these fields:
            ['timezero_date', 'data_version_date', 'is_season_start', 'season_name']

        The date format is utils.utilities.YYYY_MM_DD_DATE_FORMAT. 'data_version_date' can be None.
        """
        # check authorization, 'timezero_config'
        project = Project.objects.get(pk=self.kwargs['pk'])
        if not is_user_ok_edit_project(request.user, project):  # only the project owner can add TimeZeros
            return HttpResponseForbidden()
        elif 'timezero_config' not in request.data:
            return JsonResponse({'error': "No 'timezero_config' data."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            time_zero = validate_and_create_timezero(project, request.data['timezero_config'])
            timezero_serializer = TimeZeroSerializer(time_zero, context={'request': request})
            return JsonResponse(timezero_serializer.data)
        except Exception as ex:
            return JsonResponse({'error': str(ex)}, status=status.HTTP_400_BAD_REQUEST)


def validate_and_create_timezero(project, timezero_config, is_validate_only=False):
    """
    Helper that validates and creates a TimeZero in project based on timezero_config.

    :param project: project to add the TimeZero to
    :param timezero_config: dict as documented above, with these fields:
        ['timezero_date', 'data_version_date', 'is_season_start', 'season_name']
    :param is_validate_only: controls whether objects are actually created (is_validate_only=False), or whether only
        validation is done but no creation (is_validate_only=True)
    :return: the new TimeZero, or None if is_validate_only
    """
    # validate timezero_config. optional keys are tested below
    all_keys = set(timezero_config.keys())
    tested_keys = all_keys - {'id', 'url', 'season_name'}  # optional keys
    expected_keys = {'timezero_date', 'data_version_date', 'is_season_start'}  # required keys
    if tested_keys != expected_keys:
        raise RuntimeError(f"Wrong keys in 'timezero_config'. difference={expected_keys ^ tested_keys}. "
                           f"expected={expected_keys}, tested_keys={tested_keys}")

    # test for the optional season_name
    if timezero_config['is_season_start'] and ('season_name' not in timezero_config.keys()):
        raise RuntimeError(f"season_name not found but is required when is_season_start is passed. "
                           f"timezero_config={timezero_config}")

    # valid
    if is_validate_only:
        return None

    # create the TimeZero, first checking for an existing one
    timezero_date = datetime.datetime.strptime(timezero_config['timezero_date'], YYYY_MM_DD_DATE_FORMAT).date()
    existing_timezero = project.timezeros.filter(timezero_date=timezero_date).first()
    if existing_timezero:
        raise RuntimeError(f"found existing TimeZero for timezero_date={timezero_date}")

    dvd_datetime = datetime.datetime.strptime(timezero_config['data_version_date'], YYYY_MM_DD_DATE_FORMAT) \
        if timezero_config['data_version_date'] else None
    data_version_date = datetime.date(dvd_datetime.year, dvd_datetime.month, dvd_datetime.day) \
        if dvd_datetime else None
    is_season_start = timezero_config['is_season_start']
    timezero_init = {'project': project,
                     'timezero_date': timezero_date,
                     'data_version_date': data_version_date,
                     'is_season_start': is_season_start,
                     'season_name': timezero_config['season_name'] if is_season_start else None}
    return TimeZero.objects.create(**timezero_init)


class UserDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        detail_user = self.get_object()
        return self.request.user.is_superuser or (detail_user == self.request.user)


class ForecastModelForecastList(UserPassesTestMixin, generics.ListCreateAPIView):
    """
    View that returns a list of Forecasts in a ForecastModel
    """
    serializer_class = ForecastSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        forecast_model = ForecastModel.objects.get(pk=self.kwargs['pk'])
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, forecast_model.project)


    def get_queryset(self):
        forecast_model = ForecastModel.objects.get(pk=self.kwargs['pk'])
        return forecast_model.forecasts


    def post(self, request, *args, **kwargs):
        """
        Handles uploading a new Forecast to this ForecastModel. POST form fields:
        - 'data_file' (required): The data file to upload. NB: 'data_file' is our naming convention. it could be
            renamed. If multiple files, just uses the first one.
        - 'timezero_date' (required): The TimeZero.timezero_date to use to look up the TimeZero to associate with the
            upload. The date format is utils.utilities.YYYY_MM_DD_DATE_FORMAT. The TimeZero must exist, and will not be
            created if one corresponding to 'timezero_date' isn't found.
        """
        # todo xx merge below with views.upload_forecast() and views.validate_data_file()

        # imported here so that tests can patch via mock:
        from forecast_app.views import _upload_file, _upload_forecast_worker, is_user_ok_upload_forecast
        from forecast_repo.settings.base import MAX_UPLOAD_FILE_SIZE


        # check authorization
        forecast_model = ForecastModel.objects.get(pk=self.kwargs['pk'])
        if not is_user_ok_upload_forecast(request, forecast_model):
            return HttpResponseForbidden()

        # validate 'data_file'
        if 'data_file' not in request.data:
            return JsonResponse({'error': "No 'data_file' form field."}, status=status.HTTP_400_BAD_REQUEST)

        # NB: if multiple files, just uses the first one:
        data_file = request.data['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
        if data_file.size > MAX_UPLOAD_FILE_SIZE:
            message = "File was too large to upload. size={}, max={}.".format(data_file.size, MAX_UPLOAD_FILE_SIZE)
            return JsonResponse({'error': message}, status=status.HTTP_400_BAD_REQUEST)

        # validate 'timezero_date'
        if 'timezero_date' not in request.data:
            return JsonResponse({'error': f"No 'timezero_date' form field. forecast_model={forecast_model}"},
                                status=status.HTTP_400_BAD_REQUEST)

        timezero_date_str = request.data['timezero_date']
        try:
            timezero_date_obj = datetime.datetime.strptime(timezero_date_str, YYYY_MM_DD_DATE_FORMAT)
        except ValueError as ve:
            return JsonResponse({'error': f"Badly formatted 'timezero_date' form field: '{ve!r}'. "
                                          f"forecast_model={forecast_model}"},
                                status=status.HTTP_400_BAD_REQUEST)

        time_zero = forecast_model.project.time_zero_for_timezero_date(timezero_date_obj)
        if not time_zero:
            return JsonResponse({'error': f"TimeZero not found for 'timezero_date' form field: '{timezero_date_obj}'. "
                                          f"forecast_model={forecast_model}"},
                                status=status.HTTP_400_BAD_REQUEST)

        # check for existing forecast
        existing_forecast_for_tz = forecast_model.forecast_for_time_zero(time_zero)
        if existing_forecast_for_tz:
            return JsonResponse({'error': f"A forecast already exists for "
                                          f"time_zero={time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)}. "
                                          f"file_name='{data_file.name}', "
                                          f"existing_forecast={existing_forecast_for_tz}, "
                                          f"forecast_model={forecast_model}"},
                                status=status.HTTP_400_BAD_REQUEST)

        # upload to cloud and enqueue a job to process a new Job
        notes = request.data.get('notes', '')
        is_error, job = _upload_file(request.user, data_file, _upload_forecast_worker,
                                     type=JOB_TYPE_UPLOAD_FORECAST,
                                     forecast_model_pk=forecast_model.pk,
                                     timezero_pk=time_zero.pk, notes=notes)
        if is_error:
            return JsonResponse({'error': f"There was an error uploading the file. The error was: '{is_error}'. "
                                          f"forecast_model={forecast_model}"},
                                status=status.HTTP_400_BAD_REQUEST)

        job_serializer = JobSerializer(job, context={'request': request})
        return JsonResponse(job_serializer.data)


class JobDetailView(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        job = self.get_object()
        return self.request.user.is_superuser or (job.user == self.request.user)


class ForecastModelDetail(UserPassesTestMixin, generics.RetrieveUpdateDestroyAPIView):
    queryset = ForecastModel.objects.all()
    serializer_class = ForecastModelSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        forecast_model = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, forecast_model.project)


    def put(self, request, *args, **kwargs):
        """
        Edits a ForecastModel based on a model config dict. Runs in the calling thread and therefore blocks.

        PUT form fields:
        - request.data (required) must have a 'model_config' field containing these fields: ['name', 'abbreviation',
            'team_name', 'description', 'contributors', 'license', 'notes', 'citation', 'methods', 'home_url',
            'aux_data_url']
        """
        # very similar to `ProjectForecastModelList.post` :-/
        forecast_model = self.get_object()

        # check authorization, 'model_config'
        if not is_user_ok_edit_model(request.user, forecast_model):
            return HttpResponseForbidden()
        elif 'model_config' not in request.data:
            return JsonResponse({'error': "No 'model_config' data."}, status=status.HTTP_400_BAD_REQUEST)

        # validate model_config
        model_config = request.data['model_config']
        actual_keys = set(model_config.keys())
        expected_keys = {'name', 'abbreviation', 'team_name', 'description', 'contributors', 'license', 'notes',
                         'citation', 'methods', 'home_url', 'aux_data_url'}
        if actual_keys != expected_keys:
            return JsonResponse({'error': f"Wrong keys in 'model_config'. difference={expected_keys ^ actual_keys}. "
                                          f"expected={expected_keys}, actual={actual_keys}"},
                                status=status.HTTP_400_BAD_REQUEST)

        # validate license
        if not ForecastModel.is_valid_license_abbreviation(model_config['license']):
            valid_license_abbrevs = [choice_abbrev for choice_abbrev, choice_name in ForecastModel.LICENSE_CHOICES]
            return JsonResponse({'error': f"invalid license: {model_config['license']!r}. must be one of: "
                                          f"{valid_license_abbrevs}"},
                                status=status.HTTP_400_BAD_REQUEST)

        forecast_model.name = model_config['name']
        forecast_model.abbreviation = model_config['abbreviation']
        forecast_model.team_name = model_config['team_name']
        forecast_model.description = model_config['description']
        forecast_model.contributors = model_config['contributors']
        forecast_model.license = model_config['license']
        forecast_model.notes = model_config['notes']
        forecast_model.citation = model_config['citation']
        forecast_model.methods = model_config['methods']
        forecast_model.home_url = model_config['home_url']
        forecast_model.aux_data_url = model_config['aux_data_url']
        forecast_model.save()

        return Response(status=status.HTTP_200_OK)


    def delete(self, request, *args, **kwargs):
        """
        Deletes this model. Runs in the calling thread and therefore blocks.
        """
        forecast_model = self.get_object()
        if not is_user_ok_edit_model(request.user, forecast_model):
            return HttpResponseForbidden()

        response = self.destroy(request, *args, **kwargs)
        return response


class ForecastDetail(UserPassesTestMixin, generics.RetrieveUpdateDestroyAPIView):
    queryset = Forecast.objects.all()
    serializer_class = ForecastSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        forecast = self.get_object()
        return self.request.user.is_authenticated and \
               is_user_ok_view_project(self.request.user, forecast.forecast_model.project)


    def patch(self, request, *args, **kwargs):
        """
        Allows setting a single Forecast field. Currently supported fields are:
        - source: a string
        - notes: a string
        - issue_date: a date in YYYY_MM_DD_DATE_FORMAT
        """
        forecast = self.get_object()
        if not is_user_ok_delete_forecast(request.user, forecast):
            return HttpResponseForbidden()

        if 'source' in request.data:
            forecast.source = request.data['source']
            forecast.save()
            return Response(status=status.HTTP_200_OK)
        elif 'notes' in request.data:
            forecast.notes = request.data['notes']
            forecast.save()
            return Response(status=status.HTTP_200_OK)
        elif 'issue_date' in request.data:
            issue_date_str = request.data['issue_date']
            try:
                issue_date = datetime.datetime.strptime(issue_date_str, YYYY_MM_DD_DATE_FORMAT).date()
                forecast.issue_date = issue_date
                forecast.save()
                return Response(status=status.HTTP_200_OK)
            except ValueError:
                return JsonResponse({'error': f"'issue_date' was not in YYYY-MM-DD format: {issue_date_str!r}"},
                                    status=status.HTTP_400_BAD_REQUEST)
        else:
            return JsonResponse({'error': f"Could not find supported field in data: {list(request.data.keys())}. "
                                          f"Supported fields: 'source', 'issue_date'"},
                                status=status.HTTP_400_BAD_REQUEST)


    # def put(self, request, *args, **kwargs):
    #     """
    #     Handles the case of setting my source. PUT form fields:
    #     - request.data (required) must have a 'source' field containing a string
    #     """
    #     forecast = self.get_object()
    #     if not is_user_ok_delete_forecast(request.user, forecast):  # if ok delete forecast then ok to set source
    #         return HttpResponseForbidden()
    #     elif 'source' not in request.data:
    #         return JsonResponse({'error': "No 'source' data."}, status=status.HTTP_400_BAD_REQUEST)
    #
    #     forecast.source = request.data['source']
    #     forecast.save()
    #     return Response(status=status.HTTP_200_OK)


    def delete(self, request, *args, **kwargs):
        """
        Enqueues the deletion of a Forecast, returning a Job for it.
        """
        forecast = self.get_object()
        if not is_user_ok_delete_forecast(request.user, forecast):
            return HttpResponseForbidden()

        job = enqueue_delete_forecast(request.user, forecast)
        job_serializer = JobSerializer(job, context={'request': request})
        return JsonResponse(job_serializer.data)


class UnitDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Unit.objects.all()
    serializer_class = UnitSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        unit = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, unit.project)


class TargetDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Target.objects.all()
    serializer_class = TargetSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        target = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, target.project)


class TimeZeroDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = TimeZero.objects.all()
    serializer_class = TimeZeroSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        time_zero = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, time_zero.project)


class TruthDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Project.objects.all()
    serializer_class = TruthSerializer


    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        return HttpResponseForbidden()  # replaces: AccessMixin.handle_no_permission() raises PermissionDenied


    def test_func(self):
        project = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, project)


    def post(self, request, *args, **kwargs):
        """
        Enqueues the uploading of the passed data into the project's truth, replacing any existing truth data. Returns
        the Job.

        POST form fields:
        - 'data_file' (required): The data file to upload. NB: 'data_file' is our naming convention. it could be
            renamed. If multiple files, just uses the first one.
        """
        # todo xx merge below with views.upload_forecast() and views.validate_data_file()

        # imported here so that tests can patch via mock:
        from forecast_app.views import _upload_file
        from forecast_repo.settings.base import MAX_UPLOAD_FILE_SIZE


        # check authorization
        project = self.get_object()
        if not is_user_ok_edit_project(request.user, project):  # only the project owner can edit the project's truth
            return HttpResponseForbidden()

        # validate 'data_file'
        if 'data_file' not in request.data:
            return JsonResponse({'error': "No 'data_file' form field."}, status=status.HTTP_400_BAD_REQUEST)

        # NB: if multiple files, just uses the first one:
        data_file = request.data['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
        if data_file.size > MAX_UPLOAD_FILE_SIZE:
            message = "File was too large to upload. size={}, max={}.".format(data_file.size, MAX_UPLOAD_FILE_SIZE)
            return JsonResponse({'error': message}, status=status.HTTP_400_BAD_REQUEST)

        # upload to cloud and enqueue a job to process a new Job
        data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
        is_error, job = _upload_file(request.user, data_file, _upload_truth_worker,
                                     type=JOB_TYPE_UPLOAD_TRUTH,
                                     project_pk=project.pk)
        if is_error:
            return JsonResponse({'error': f"There was an error uploading the file. The error was: '{is_error}'"},
                                status=status.HTTP_400_BAD_REQUEST)

        job_serializer = JobSerializer(job, context={'request': request})
        return JsonResponse(job_serializer.data)


@api_view(['POST'])
def query_forecasts_endpoint(request, pk):
    """
    Enqueues a query of the project's forecasts.

    POST form fields:
    - 'query' (required): a dict specifying the query parameters. see https://docs.zoltardata.com/ for documentation

    :param request: a request
    :param pk: a Project's pk
    :return: the serialized Job
    """
    # imported here so that tests can patch via mock:
    from utils.project_queries import validate_forecasts_query


    return _query_endpoint(request, pk, validate_forecasts_query, JOB_TYPE_QUERY_FORECAST, _forecasts_query_worker)


@api_view(['POST'])
def query_scores_endpoint(request, pk):
    """
    Similar to query_forecasts_endpoint(), enqueues a query of the project's scores.

    POST form fields:
    - 'query' (required): a dict specifying the query parameters. see https://docs.zoltardata.com/ for documentation

    :param request: a request
    :param pk: a Project's pk
    :return: the serialized Job
    """
    # imported here so that tests can patch via mock:
    from utils.project_queries import validate_scores_query


    return _query_endpoint(request, pk, validate_scores_query, JOB_TYPE_QUERY_SCORE, _scores_query_worker)


@api_view(['POST'])
def query_truth_endpoint(request, pk):
    """
    Similar to query_forecasts_endpoint(), enqueues a query of the project's truth.

    POST form fields:
    - 'query' (required): a dict specifying the query parameters. see https://docs.zoltardata.com/ for documentation

    :param request: a request
    :param pk: a Project's pk
    :return: the serialized Job
    """
    # imported here so that tests can patch via mock:
    from utils.project_queries import validate_truth_query


    return _query_endpoint(request, pk, validate_truth_query, JOB_TYPE_QUERY_TRUTH, _truth_query_worker)


def _query_endpoint(request, project_pk, query_validation_fcn, query_job_type, query_worker_fcn):
    """
    `query_forecasts_endpoint()` and `query_scores_endpoint()` helper

    :param request: a request
    :param project_pk: a Project's pk
    :param query_validation_fcn: a function of 2 args (Project and query) that validates the query and returns a
        2-tuple: (error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types)) . notice the second element
        is itself a 5-tuple of validated object IDs. the function is either `validate_forecasts_query` or
        `validate_scores_query`. there are two cases, which determine the return values: 1) valid query: error_messages
        is [], and ID lists are valid integers. 2) invalid query: error_messages is a list of strings, and the ID lists
        are all [].
    :param query_job_type: is either JOB_TYPE_QUERY_FORECAST or JOB_TYPE_QUERY_SCORES. used for the new Job's `type`
    :param query_worker_fcn: an enqueue() helper function of one arg (job_pk). the function is either
    `_forecasts_query_worker` or `_scores_query_worker`
    :return: the serialized Job
    """
    if request.method != 'POST':
        return Response(f"Only POST is allowed at this endpoint", status=status.HTTP_405_METHOD_NOT_ALLOWED)

    # check authorization
    project = get_object_or_404(Project, pk=project_pk)
    if (not request.user.is_authenticated) or not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden()

    # validate 'query'
    if 'query' not in request.data:
        return JsonResponse({'error': "No 'query' form field."}, status=status.HTTP_400_BAD_REQUEST)

    query = request.data['query']
    logger.debug(f"query_forecasts_endpoint(): query={query}")
    error_messages, _ = query_validation_fcn(project, query)
    if error_messages:
        return JsonResponse({'error': f"Invalid query. error_messages='{error_messages}', query={query}"},
                            status=status.HTTP_400_BAD_REQUEST)

    job = _create_query_job(project_pk, query, query_job_type, query_worker_fcn, request)
    job_serializer = JobSerializer(job, context={'request': request})
    logger.debug(f"query_forecasts_endpoint(): query enqueued. job={job}")
    return JsonResponse(job_serializer.data)


def _create_query_job(project_pk, query, query_job_type, query_worker_fcn, request):
    job = Job.objects.create(user=request.user)  # status = PENDING
    job.input_json = {'type': query_job_type, 'project_pk': project_pk, 'query': query}
    job.save()
    queue = django_rq.get_queue(QUERY_FORECAST_QUEUE_NAME)
    queue.enqueue(query_worker_fcn, job.pk)
    job.status = Job.QUEUED
    job.save()
    return job


#
# Forecast data-related views
#

@api_view(['GET'])
@renderer_classes((BrowsableAPIRenderer, JSONRenderer))  # todo xx BrowsableAPIRenderer needed?
def forecast_data(request, pk):
    """
    :return: a Forecast's data as JSON - see load_predictions_from_json_io_dict() for the format
    """
    forecast = get_object_or_404(Forecast, pk=pk)
    if (not request.user.is_authenticated) or \
            not is_user_ok_view_project(request.user, forecast.forecast_model.project):
        return HttpResponseForbidden()

    return json_response_for_forecast(forecast, request)


def json_response_for_forecast(forecast, request):
    """
    :param forecast: a Forecast
    :param request: required for TargetSerializer's 'id' field
    :return: a JsonResponse for forecast
    """
    # note: I tried to use a rest_framework.response.Response, which is supposed to support pretty printing on the
    # client side via something like:
    #   curl -H 'Accept: application/json; indent=4' http://127.0.0.p1:8000/api/project/1/template_data/
    # but when I tried this, returned a delimited string instead of JSON:
    #   return Response(JSONRenderer().render(unit_dicts))
    # https://stackoverflow.com/questions/23195210/how-to-get-pretty-output-from-rest-framework-serializer
    response = JsonResponse(json_io_dict_from_forecast(forecast, request))  # default 'content_type': 'application/json'
    response['Content-Disposition'] = 'attachment; filename="{}.json"'.format(get_valid_filename(forecast.source))
    return response


#
# Job data-related functions
#

@api_view(['GET'])
# @renderer_classes((BrowsableAPIRenderer, CSVRenderer))    # todo xx BrowsableAPIRenderer needed?
@renderer_classes((CSVRenderer,))
def download_job_data(request, pk):
    """
    A note regarding Job "type": Currently there is no Job.type IV, so we have to infer it from Job.input_json, which
    will have a 'query' key if it was created by `query_forecasts_endpoint()`.

    :return: a Job's data as CSV
    """
    job = get_object_or_404(Job, pk=pk)
    if (not request.user.is_authenticated) or ((not request.user.is_superuser) and (not request.user == job.user)):
        return HttpResponseForbidden()

    if (not isinstance(job.input_json, dict)) or ('query' not in job.input_json):
        return HttpResponseBadRequest(f"job.input_json did not contain a `query` key. job={job}")

    return _download_job_data_request(job)


def _download_job_data_request(job):
    """
    :param job: a Job
    :return: the data file corresponding to `job` as a CSV file
    """
    # imported here so that tests can patch via mock:
    from utils.cloud_file import download_file, _file_name_for_object


    with tempfile.TemporaryFile() as cloud_file_fp:  # <class '_io.BufferedRandom'>
        try:
            download_file(job, cloud_file_fp)
            cloud_file_fp.seek(0)  # yes you have to do this!

            # https://stackoverflow.com/questions/16538210/downloading-files-from-amazon-s3-using-django
            csv_filename = get_valid_filename(f'job-{_file_name_for_object(job)}-data.csv')
            wrapper = FileWrapper(cloud_file_fp)
            response = HttpResponse(wrapper, content_type='text/csv')
            # response['Content-Length'] = os.path.getsize('/tmp/'+fname)
            response['Content-Disposition'] = 'attachment; filename="{}"'.format(str(csv_filename))
            return response
        except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError) as aws_exc:
            logger.debug(f"download_job_data(): AWS error: {aws_exc!r}. job={job}")
            return HttpResponseNotFound(f"AWS error: {aws_exc!r}, job={job}")
        except Exception as ex:
            logger.debug(f"download_job_data(): error: {ex!r}. job={job}")
            return HttpResponseNotFound(f"error downloading job data. ex={ex!r}, job={job}")
