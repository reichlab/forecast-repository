import csv
import datetime
import io
import logging
import tempfile
from collections import defaultdict
from itertools import groupby
from pathlib import Path
from wsgiref.util import FileWrapper

import django_rq
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError, ClientError, ConnectionClosedError
from django.contrib.auth.mixins import UserPassesTestMixin
from django.contrib.auth.models import User
from django.db import connection
from django.http import JsonResponse, HttpResponseForbidden, HttpResponse, HttpResponseBadRequest, \
    HttpResponseNotFound
from django.utils.text import get_valid_filename
from rest_framework import generics, status
from rest_framework.decorators import api_view, renderer_classes
from rest_framework.exceptions import PermissionDenied
from rest_framework.generics import get_object_or_404
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework_csv.renderers import CSVRenderer
from rq.timeouts import JobTimeoutException

from forecast_app.models import Project, ForecastModel, Forecast, Score, ScoreValue, PointPrediction, Target
from forecast_app.models.job import Job, JOB_TYPE_QUERY_FORECAST, JOB_TYPE_UPLOAD_TRUTH, \
    JOB_TYPE_UPLOAD_FORECAST
from forecast_app.models.project import TRUTH_CSV_HEADER, TimeZero, Unit
from forecast_app.serializers import ProjectSerializer, UserSerializer, ForecastModelSerializer, ForecastSerializer, \
    TruthSerializer, JobSerializer, TimeZeroSerializer, UnitSerializer, TargetSerializer
from forecast_app.views import is_user_ok_edit_project, is_user_ok_edit_model, is_user_ok_create_model, \
    _upload_truth_worker, enqueue_delete_forecast, is_user_ok_delete_forecast, is_user_ok_create_project, \
    is_user_ok_view_project
from forecast_repo.settings.base import QUERY_FORECAST_QUEUE_NAME
from utils.forecast import json_io_dict_from_forecast
from utils.project import create_project_from_json, config_dict_from_project, query_forecasts_for_project
from utils.project_diff import execute_project_config_diff, project_config_diff
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


logger = logging.getLogger(__name__)


#
# Root view
#

@api_view(['GET'])
def api_root(request, format=None):
    if not request.user.is_authenticated:
        raise PermissionDenied

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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


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
            raise PermissionDenied
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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):
        project = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, project)


    def delete(self, request, *args, **kwargs):
        """
        Deletes this project. Runs in the calling thread and therefore blocks.
        """
        project = self.get_object()
        if not is_user_ok_edit_project(request.user, project):  # only the project owner can delete the project
            raise PermissionDenied

        # imported here so that test_delete_project_iteratively() can patch via mock:
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
            raise PermissionDenied
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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, project)


    def get_queryset(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return project.models


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
            raise PermissionDenied
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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


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
            raise PermissionDenied
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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):
        detail_user = self.get_object()
        return self.request.user.is_superuser or (detail_user == self.request.user)


class ForecastModelForecastList(UserPassesTestMixin, generics.ListCreateAPIView):
    """
    View that returns a list of Forecasts in a ForecastModel
    """
    serializer_class = ForecastSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


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

        # imported here so that test_api_upload_forecast() can patch via mock:
        from forecast_app.views import _upload_file, _upload_forecast_worker, is_user_ok_upload_forecast
        from forecast_repo.settings.base import MAX_UPLOAD_FILE_SIZE


        # check authorization
        forecast_model = ForecastModel.objects.get(pk=self.kwargs['pk'])
        if not is_user_ok_upload_forecast(request, forecast_model):
            raise PermissionDenied

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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):
        job = self.get_object()
        return self.request.user.is_superuser or (job.user == self.request.user)


class ForecastModelDetail(UserPassesTestMixin, generics.RetrieveUpdateDestroyAPIView):
    queryset = ForecastModel.objects.all()
    serializer_class = ForecastModelSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


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
            raise PermissionDenied
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
            raise PermissionDenied

        response = self.destroy(request, *args, **kwargs)
        return response


class ForecastDetail(UserPassesTestMixin, generics.RetrieveUpdateDestroyAPIView):
    queryset = Forecast.objects.all()
    serializer_class = ForecastSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):
        forecast = self.get_object()
        return self.request.user.is_authenticated and \
               is_user_ok_view_project(self.request.user, forecast.forecast_model.project)


    def put(self, request, *args, **kwargs):
        """
        Handles the case of setting my source. PUT form fields:
        - request.data (required) must have a 'source' field containing a string
        """
        forecast = self.get_object()
        if not is_user_ok_delete_forecast(request.user, forecast):  # if ok delete forecast then ok to set source
            raise PermissionDenied
        elif 'source' not in request.data:
            return JsonResponse({'error': "No 'source' data."}, status=status.HTTP_400_BAD_REQUEST)

        forecast.source = request.data['source']
        forecast.save()
        return Response(status=status.HTTP_200_OK)


    def delete(self, request, *args, **kwargs):
        """
        Enqueues the deletion of a Forecast, returning a Job for it.
        """
        forecast = self.get_object()
        if not is_user_ok_delete_forecast(request.user, forecast):
            raise PermissionDenied

        job = enqueue_delete_forecast(request.user, forecast)
        job_serializer = JobSerializer(job, context={'request': request})
        return JsonResponse(job_serializer.data)


class UnitDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Unit.objects.all()
    serializer_class = UnitSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):
        unit = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, unit.project)


class TargetDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Target.objects.all()
    serializer_class = TargetSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):
        target = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, target.project)


class TimeZeroDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = TimeZero.objects.all()
    serializer_class = TimeZeroSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):
        time_zero = self.get_object()
        return self.request.user.is_authenticated and is_user_ok_view_project(self.request.user, time_zero.project)


class TruthDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Project.objects.all()
    serializer_class = TruthSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


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

        # imported here so that test_api_upload_forecast() can patch via mock:
        from forecast_app.views import _upload_file
        from forecast_repo.settings.base import MAX_UPLOAD_FILE_SIZE


        # check authorization
        project = self.get_object()
        if not is_user_ok_edit_project(request.user, project):  # only the project owner can edit the project's truth
            raise PermissionDenied

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
    # imported here so that test_api_forecast_queries() can patch via mock:
    from utils.project import validate_forecasts_query


    if request.method != 'POST':
        return Response(f"Only POST is allowed at this endpoint", status=status.HTTP_405_METHOD_NOT_ALLOWED)

    # check authorization
    project = get_object_or_404(Project, pk=pk)
    if (not request.user.is_authenticated) or not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden()

    # validate 'query'
    if 'query' not in request.data:
        return JsonResponse({'error': "No 'query' form field."}, status=status.HTTP_400_BAD_REQUEST)

    query = request.data['query']
    logger.debug(f"query_forecasts_endpoint(): query={query}")
    error_messages, _ = validate_forecasts_query(project, query)
    if error_messages:
        return JsonResponse({'error': f"Invalid query. error_messages='{error_messages}', query={query}"},
                            status=status.HTTP_400_BAD_REQUEST)

    # create the job
    job = Job.objects.create(user=request.user)  # status = PENDING
    job.input_json = {'type': JOB_TYPE_QUERY_FORECAST, 'project_pk': pk, 'query': query}
    job.save()

    queue = django_rq.get_queue(QUERY_FORECAST_QUEUE_NAME)
    queue.enqueue(_query_forecasts_worker, job.pk)
    job.status = Job.QUEUED
    job.save()

    job_serializer = JobSerializer(job, context={'request': request})
    logger.debug(f"query_forecasts_endpoint(): query enqueued. job={job}")
    return JsonResponse(job_serializer.data)


def _query_forecasts_worker(job_pk):
    """
    enqueue() helper function

    assumes these input_json fields are present and valid:
    - 'project_pk'
    - 'query' (assume has passed `validate_forecasts_query()`)
    """
    # imported here so that test__query_forecasts_worker() can patch via mock:
    from utils.cloud_file import upload_file


    # run the query
    job = get_object_or_404(Job, pk=job_pk)
    project = get_object_or_404(Project, pk=job.input_json['project_pk'])
    query = job.input_json['query']
    try:
        logger.debug(f"_query_forecasts_worker(): querying rows. query={query}. job={job}")
        rows = query_forecasts_for_project(project, query)
    except JobTimeoutException as jte:
        job.status = Job.TIMEOUT
        job.save()
        logger.error(f"_query_forecasts_worker(): Job timeout: {jte!r}. job={job}")
        return
    except Exception as ex:
        job.status = Job.FAILED
        job.failure_message = f"_query_forecasts_worker(): error running query: {ex!r}. job={job}"
        job.save()
        logger.error(f"_query_forecasts_worker(): error: {ex!r}. job={job}")
        return

    # upload the file to cloud storage
    try:
        # we need a BytesIO for upload_file() (o/w it errors: "Unicode-objects must be encoded before hashing"), but
        # writerows() needs a StringIO (o/w "a bytes-like object is required, not 'str'" error), so we use
        # TextIOWrapper. BUT: https://docs.python.org/3.6/library/io.html#io.TextIOWrapper :
        #     Text I/O over a binary storage (such as a file) is significantly slower than binary I/O over the same
        #     storage, because it requires conversions between unicode and binary data using a character codec. This can
        #     become noticeable handling huge amounts of text data like large log files.

        # note: using a context is required o/w is closed and becomes unusable:
        # per https://stackoverflow.com/questions/59079354/how-to-write-utf-8-csv-into-bytesio-in-python3 :
        with io.BytesIO() as bytes_io:
            logger.debug(f"_query_forecasts_worker(): writing {len(rows)} rows. job={job}")
            text_io_wrapper = io.TextIOWrapper(bytes_io, 'utf-8', newline='')
            csv.writer(text_io_wrapper).writerows(rows)
            text_io_wrapper.flush()
            bytes_io.seek(0)

            logger.debug(f"_query_forecasts_worker(): uploading file. job={job}")
            upload_file(job, bytes_io)  # might raise S3 exception
            job.status = Job.SUCCESS
            job.output_json = {'num_rows': len(rows)}  # todo xx temp
            job.save()
            logger.debug(f"_query_forecasts_worker(): done. job={job}")
    except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError) as aws_exc:
        job.status = Job.FAILED
        job.failure_message = f"_query_forecasts_worker(): AWS error: {aws_exc!r}. job={job}"
        job.save()
        logger.error(f"_query_forecasts_worker(): AWS error: {aws_exc!r}. job={job}")
    except Exception as ex:
        job.status = Job.FAILED
        job.failure_message = f"_query_forecasts_worker(): error: {ex!r}. job={job}"
        job.save()


#
# Truth data-related views
#

@api_view(['GET'])
@renderer_classes((BrowsableAPIRenderer, CSVRenderer))  # todo xx BrowsableAPIRenderer needed?
def download_truth_data(request, pk):
    """
    :return: the Project's truth data as CSV. note that the actual data is wrapped by metadata
    """
    project = get_object_or_404(Project, pk=pk)
    if (not request.user.is_authenticated) or not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden()

    return csv_response_for_project_truth_data(project)


def csv_response_for_project_truth_data(project):
    """
    Similar to json_response_for_forecast(), but returns a response with project's truth data formatted as
    CSV. NB: The returned response will contain only those rows that actually loaded from the original CSV file passed
    to Project.load_truth_data(), which will contain fewer rows if some were invalid. For that reason we change the
    filename to hopefully hint at what's going on.
    """
    response = HttpResponse(content_type='text/csv')

    # two cases for deciding the filename to put in download response:
    # 1) original ends with .csv -> orig-name.csv -> orig-name-validated.csv
    # 2) "" does not end "" -> orig-name.csv.foo -> orig-name.csv.foo-validated.csv
    csv_filename_path = Path(project.truth_csv_filename)
    if csv_filename_path.suffix.lower() == '.csv':
        csv_filename = csv_filename_path.stem + '-validated' + csv_filename_path.suffix
    else:
        csv_filename = csv_filename_path.name + '-validated.csv'
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(str(csv_filename))

    writer = csv.writer(response)
    writer.writerow(TRUTH_CSV_HEADER)
    for timezero_date, unit_name, target_name, \
        value_i, value_f, value_t, value_d, value_b in project.get_truth_data_rows():
        timezero_date = timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        truth_value = PointPrediction.first_non_none_value(value_i, value_f, value_t, value_d, value_b)
        writer.writerow([timezero_date, unit_name, target_name, truth_value])

    return response


#
# Score data-related views
#

@api_view(['GET'])
@renderer_classes((BrowsableAPIRenderer, CSVRenderer))  # todo xx BrowsableAPIRenderer needed?
def download_score_data(request, pk):
    """
    :return: the Project's score data as CSV
    """
    project = get_object_or_404(Project, pk=pk)
    if (not request.user.is_authenticated) or not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden()

    if project.score_csv_file_cache.is_file_exists():
        return csv_response_for_cached_project_score_data(project)
    else:
        return csv_response_for_project_score_data(project)


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
# Score data-related functions
#

SCORE_CSV_HEADER_PREFIX = ['model', 'timezero', 'season', 'unit', 'target', 'truth']


def _csv_filename_for_project_scores(project):
    return get_valid_filename(project.name + '-scores.csv')


def csv_response_for_cached_project_score_data(project):
    """
    Similar to csv_response_for_project_score_data(), but returns a response that's loaded from an existing S3 file.

    :param project:
    :return:
    """
    from utils.cloud_file import download_file


    with tempfile.TemporaryFile() as cloud_file_fp:  # <class '_io.BufferedRandom'>
        try:
            download_file(project.score_csv_file_cache, cloud_file_fp)
            cloud_file_fp.seek(0)  # yes you have to do this!

            # https://stackoverflow.com/questions/16538210/downloading-files-from-amazon-s3-using-django
            csv_filename = _csv_filename_for_project_scores(project)
            wrapper = FileWrapper(cloud_file_fp)
            response = HttpResponse(wrapper, content_type='text/csv')
            # response['Content-Length'] = os.path.getsize('/tmp/'+fname)
            response['Content-Disposition'] = f'attachment; filename="{str(csv_filename)}"'
            return response
        except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError) as aws_exc:
            logger.error(f"csv_response_for_cached_project_score_data(): AWS error: {aws_exc!r}. project={project}")
            return None
        except Exception as ex:
            logger.debug(f"csv_response_for_cached_project_score_data(): Error: {ex!r}. project={project}")
            return None


def csv_response_for_project_score_data(project):
    """
    Similar to csv_response_for_project_truth_data(), but returns a response with project's score data formatted as CSV.
    """
    response = HttpResponse(content_type='text/csv')
    csv_filename = _csv_filename_for_project_scores(project)
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(str(csv_filename))

    # recall https://raw.githubusercontent.com/FluSightNetwork/cdc-flusight-ensemble/master/scores/scores.csv:
    #   Model,Year,Epiweek,Season,Model Week,Location,Target,Score,Multi bin score
    writer = csv.writer(response)
    _write_csv_score_data_for_project(writer, project)
    return response


def _write_csv_score_data_for_project(csv_writer, project):
    """
    Writes all ScoreValue data for project into csv_writer. There is one column per ScoreValue BUT: all Scores are on
    one line. Thus, the row 'key' is the (fixed) first five columns:

        `ForecastModel.abbreviation | ForecastModel.name , TimeZero.timezero_date, season, Unit.name, Target.name`

    Followed on the same line by a variable number of ScoreValue.value columns, one for each Score. Score names are in
    the header. An example header and first few rows:

        model,           timezero,    season,    unit,  target,          constant score,  Absolute Error
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      1_biweek_ahead,  1                <blank>
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      1_biweek_ahead,  <blank>          2
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      2_biweek_ahead,  <blank>          1
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      3_biweek_ahead,  <blank>          9
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      4_biweek_ahead,  <blank>          6
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      5_biweek_ahead,  <blank>          8
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      1_biweek_ahead,  <blank>          6
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      2_biweek_ahead,  <blank>          6
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      3_biweek_ahead,  <blank>          37
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      4_biweek_ahead,  <blank>          25
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      5_biweek_ahead,  <blank>          62

    Notes:
    - `season` is each TimeZero's containing season_name, similar to Project.timezeros_in_season().
    -  for the model column we use the model's abbreviation if it's not empty, otherwise we use its name
    - NB: we were using get_valid_filename() to ensure values are CSV-compliant, i.e., no commas, returns, tabs, etc.
      (a function that was as good as any), but we removed it to help performance in the loop
    - we use groupby to group row 'keys' so that all score values are together
    """
    # re: scores order: it is crucial that order matches query ORDER BY ... sv.score_id so that columns match values
    scores = Score.objects.all().order_by('pk')

    # write header
    score_csv_header = SCORE_CSV_HEADER_PREFIX + [score.csv_column_name() for score in scores]
    csv_writer.writerow(score_csv_header)

    # get the raw rows - sorted for groupby()
    logger.debug(f"_write_csv_score_data_for_project(): getting rows: project={project}")
    sql = f"""
        SELECT f.forecast_model_id, f.time_zero_id, sv.unit_id, sv.target_id, sv.score_id, sv.value
        FROM {ScoreValue._meta.db_table} AS sv
            INNER JOIN {Score._meta.db_table} s ON sv.score_id = s.id
            INNER JOIN {Forecast._meta.db_table} AS f ON sv.forecast_id = f.id
            INNER JOIN {ForecastModel._meta.db_table} AS fm ON f.forecast_model_id = fm.id
        WHERE fm.project_id = %s
        ORDER BY f.forecast_model_id, f.time_zero_id, sv.unit_id, sv.target_id, sv.score_id;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        rows = cursor.fetchall()

    # write grouped rows
    logger.debug(f"_write_csv_score_data_for_project(): preparing to iterate. project={project}")
    forecast_model_id_to_obj = {forecast_model.pk: forecast_model for forecast_model in project.models.all()}
    timezero_id_to_obj = {timezero.pk: timezero for timezero in project.timezeros.all()}
    unit_id_to_obj = {unit.pk: unit for unit in project.units.all()}
    target_id_to_obj = {target.pk: target for target in project.targets.all()}
    timezero_to_season_name = project.timezero_to_season_name()

    logger.debug(f"_write_csv_score_data_for_project(): iterating. project={project}")
    tz_unit_targ_pks_to_truth_vals = _tz_unit_targ_pks_to_truth_values(project)
    num_warnings = 0
    for (forecast_model_id, time_zero_id, unit_id, target_id), score_id_value_grouper \
            in groupby(rows, key=lambda _: (_[0], _[1], _[2], _[3])):
        # get truth. should be only one value
        true_value, error_string = _validate_truth(tz_unit_targ_pks_to_truth_vals, time_zero_id, unit_id, target_id)
        if error_string:
            num_warnings += 1
            continue  # skip this (forecast_model_id, time_zero_id, unit_id, target_id) combination's score row

        forecast_model = forecast_model_id_to_obj[forecast_model_id]
        time_zero = timezero_id_to_obj[time_zero_id]
        unit = unit_id_to_obj[unit_id]
        target = target_id_to_obj[target_id]
        # ex score_groups: [(1, 18, 1, 1, 1, 1.0), (1, 18, 1, 1, 2, 2.0)]  # multiple scores per group
        #                  [(1, 18, 1, 2, 2, 0.0)]                         # single score
        score_groups = list(score_id_value_grouper)
        score_id_to_value = {score_group[-2]: score_group[-1] for score_group in score_groups}
        score_values = [score_id_to_value[score.id] if score.id in score_id_to_value else None for score in scores]
        # while name and abbreviation are now both required to be non-empty, we leave the check here just in case:
        csv_writer.writerow([forecast_model.abbreviation if forecast_model.abbreviation else forecast_model.name,
                             time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),
                             timezero_to_season_name[time_zero],
                             unit.name, target.name, true_value]
                            + score_values)

    # print warning count
    logger.debug(f"_write_csv_score_data_for_project(): done. project={project}, num_warnings={num_warnings}")


def _tz_unit_targ_pks_to_truth_values(project):
    """
    Similar to Project.unit_target_name_tz_date_to_truth(), returns project's truth values as a nested dict
    that's organized for easy access using these keys: [timezero_pk][unit_pk][target_id] -> truth_values (a list).
    """
    truth_data_qs = project.truth_data_qs() \
        .order_by('time_zero__id', 'unit__id', 'target__id') \
        .values_list('time_zero__id', 'unit__id', 'target__id',
                     'value_i', 'value_f', 'value_t', 'value_d', 'value_b')

    tz_unit_targ_pks_to_truth_vals = {}  # {timezero_pk: {unit_pk: {target_id: truth_value}}}
    for time_zero_id, unit_target_val_grouper in groupby(truth_data_qs, key=lambda _: _[0]):
        unit_targ_pks_to_truth = {}  # {unit_pk: {target_id: truth_value}}
        tz_unit_targ_pks_to_truth_vals[time_zero_id] = unit_targ_pks_to_truth
        for unit_id, target_val_grouper in groupby(unit_target_val_grouper, key=lambda _: _[1]):
            target_pk_to_truth = defaultdict(list)  # {target_id: truth_value}
            unit_targ_pks_to_truth[unit_id] = target_pk_to_truth
            for _, _, target_id, value_i, value_f, value_t, value_d, value_b in target_val_grouper:
                value = PointPrediction.first_non_none_value(value_i, value_f, value_t, value_d, value_b)
                target_pk_to_truth[target_id].append(value)

    return tz_unit_targ_pks_to_truth_vals


def _validate_truth(timezero_loc_target_pks_to_truth_values, timezero_pk, unit_pk, target_pk):
    """
    :return: 2-tuple of the form: (truth_value, error_string) where error_string is non-None if the inputs were invalid.
        in that case, truth_value is None. o/w truth_value is valid
    """
    if timezero_pk not in timezero_loc_target_pks_to_truth_values:
        return None, 'timezero_pk not in truth'
    elif unit_pk not in timezero_loc_target_pks_to_truth_values[timezero_pk]:
        return None, 'unit_pk not in truth'
    elif target_pk not in timezero_loc_target_pks_to_truth_values[timezero_pk][unit_pk]:
        return None, 'target_pk not in truth'

    truth_values = timezero_loc_target_pks_to_truth_values[timezero_pk][unit_pk][target_pk]
    if len(truth_values) == 0:  # truth not available
        return None, 'truth value not found'
    elif len(truth_values) > 1:
        return None, '>1 truth values found'

    return truth_values[0], None


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
    # imported here so that test_api_job_data_download() can patch via mock:
    from utils.cloud_file import download_file, _file_name_for_object


    job = get_object_or_404(Job, pk=pk)
    if (not request.user.is_authenticated) or ((not request.user.is_superuser) and (not request.user == job.user)):
        return HttpResponseForbidden()

    if (not isinstance(job.input_json, dict)) or ('query' not in job.input_json):
        return HttpResponseBadRequest(f"job.input_json did not contain a `query` key. job={job}")

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
