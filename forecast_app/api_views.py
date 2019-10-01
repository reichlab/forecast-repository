import csv
import datetime
import logging
import tempfile
from itertools import groupby
from pathlib import Path
from wsgiref.util import FileWrapper

from django.contrib.auth.mixins import UserPassesTestMixin
from django.contrib.auth.models import User
from django.db import connection
from django.http import JsonResponse, HttpResponseForbidden, HttpResponse
from django.utils.text import get_valid_filename
from rest_framework import generics, status
from rest_framework.decorators import api_view, renderer_classes
from rest_framework.exceptions import PermissionDenied
from rest_framework.generics import get_object_or_404, RetrieveDestroyAPIView, ListCreateAPIView
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework_csv.renderers import CSVRenderer

from forecast_app.models import Project, ForecastModel, Forecast, Score, ScoreValue
from forecast_app.models.project import TRUTH_CSV_HEADER, TimeZero
from forecast_app.models.upload_file_job import UploadFileJob
from forecast_app.serializers import ProjectSerializer, UserSerializer, ForecastModelSerializer, ForecastSerializer, \
    TruthSerializer, UploadFileJobSerializer
from forecast_app.views import is_user_ok_create_project, is_user_ok_edit_project, is_user_ok_edit_model, \
    is_user_ok_create_model
from utils.cloud_file import download_file
from utils.forecast import json_io_dict_from_forecast
from utils.project import create_project_from_json
from utils.utilities import YYYYMMDD_DATE_FORMAT


logger = logging.getLogger(__name__)


#
# Root view
#

@api_view(['GET'])
def api_root(request, format=None):
    return Response({
        'projects': reverse('api-project-list', request=request, format=format),
    })


#
# List- and detail-related views
#

# was ListCreateAPIView -> def perform_create(self, serializer): serializer.save(owner=self.request.user)
class ProjectList(generics.ListAPIView):
    # View that returns a list of Projects. Filters out those projects that the requesting user is not authorized to view.
    # Note that this means API users have more limited access than the web home page, which lists all projects regardless
    # of whether the user is not authorized to view or not. Granted that a subset of fields is shown in this case, but
    # it's a discrepancy. I tried to implement a per-Project serialization that included the same subset, but DRF fought
    # me and won.
    serializer_class = ProjectSerializer


    def get_queryset(self):
        return [project for project in Project.objects.all() if project.is_user_ok_to_view(self.request.user)]


    def post(self, request, *args, **kwargs):
        """
        Creates a new Project based on a project config file ala create_project_from_json(). Runs in the calling thread
        and therefore blocks. POST form fields:
        - request.data (required) must have a 'project_config' field containing a dict valid for
            create_project_from_json(). NB: this is different from other API args in this file in that it takes all
            required information as data, whereas others take their main data as a file in request.FILES, plus some
            additional data in request.data.
        """
        if not is_user_ok_create_project(request.user):  # any logged-in user can create
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
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        project = self.get_object()
        return project.is_user_ok_to_view(self.request.user)


    def delete(self, request, *args, **kwargs):
        """
        Deletes this project. Runs in the calling thread and therefore blocks.
        """
        project = self.get_object()
        if not is_user_ok_edit_project(request.user, project):
            raise PermissionDenied

        response = self.destroy(request, *args, **kwargs)
        return response


class ProjectForecastModelList(ListCreateAPIView):
    # View that returns a list of ForecastModels in a Project
    serializer_class = ForecastModelSerializer


    def get_queryset(self):
        project = Project.objects.get(pk=self.kwargs['pk'])
        return project.models


    def post(self, request, *args, **kwargs):
        """
        Creates a new ForecastModel based on a model config dict. Runs in the calling thread and therefore blocks.

        POST form fields:
        - request.data (required) must have a 'model_config' field containing these fields: ['name'].
            optional fields: ['abbreviation', 'team_name', 'description', 'home_url', 'aux_data_url']
        """
        project = Project.objects.get(pk=self.kwargs['pk'])

        # check authorization
        if not is_user_ok_create_model(request.user, project):  # any logged-in user can create
            raise PermissionDenied
        elif 'model_config' not in request.data:
            return JsonResponse({'error': "No 'model_config' data."}, status=status.HTTP_400_BAD_REQUEST)

        # validate model_config
        model_config = request.data['model_config']
        actual_keys = set(model_config.keys())
        expected_keys = {'name', 'abbreviation', 'team_name', 'description', 'home_url', 'aux_data_url'}
        if actual_keys != expected_keys:
            return JsonResponse({'error': f"Wrong keys in 'model_config'. expected={expected_keys}, "
                                          f"actual={actual_keys}"},
                                status=status.HTTP_400_BAD_REQUEST)

        try:
            model_init = {'project': project,
                          'owner': request.user,
                          'name': model_config['name'],
                          'abbreviation': model_config['abbreviation'] if 'abbreviation' in model_config else '',
                          'team_name': model_config['team_name'] if 'team_name' in model_config else '',
                          'description': model_config['description'] if 'description' in model_config else '',
                          'home_url': model_config['home_url'] if 'home_url' in model_config else '',
                          'aux_data_url': model_config['aux_data_url'] if 'aux_data_url' in model_config else ''}
            new_model = ForecastModel.objects.create(**model_init)
            model_serializer = ForecastModelSerializer(new_model, context={'request': request})
            return JsonResponse(model_serializer.data)
        except Exception as ex:
            return JsonResponse({'error': str(ex)}, status=status.HTTP_400_BAD_REQUEST)


class UserList(generics.ListAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer


class UserDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):  # return True if the current user can access the view
        detail_user = self.get_object()
        return self.request.user.is_superuser or (detail_user == self.request.user)


class ForecastModelForecastList(ListCreateAPIView):
    # View that returns a list of Forecasts in a ForecastModel
    serializer_class = ForecastSerializer


    def get_queryset(self):
        forecast_model = ForecastModel.objects.get(pk=self.kwargs['pk'])
        return forecast_model.forecasts


    def get(self, request, *args, **kwargs):
        forecast_model = ForecastModel.objects.get(pk=kwargs['pk'])
        if not forecast_model.project.is_user_ok_to_view(request.user):
            raise PermissionDenied

        return self.list(request, *args, **kwargs)


    def post(self, request, *args, **kwargs):
        """
        Handles uploading a new Forecast to this ForecastModel. POST form fields:
        - 'data_file' (required): The data file to upload. NB: 'data_file' is our naming convention. it could be
            renamed. If multiple files, just uses the first one.
        - 'timezero_date' (required): The TimeZero.timezero_date to use to look up the TimeZero to associate with the
            upload. The date format is utils.utilities.YYYYMMDD_DATE_FORMAT. The TimeZero will be created if one
            corresponding to 'timezero_date' isn't found.
        - data_version_date (optional): To be used for the newly-created TimeZero corresponding to 'timezero_date', if
            it didn't exist.
        """
        # todo xx merge below with views.upload_forecast() and views.validate_data_file()

        # imported here so that test_api_upload_forecast() can patch via mock:
        from forecast_app.views import MAX_UPLOAD_FILE_SIZE, _upload_file, process_upload_file_job__forecast, \
            is_user_ok_upload_forecast


        # check authorization
        forecast_model = ForecastModel.objects.get(pk=self.kwargs['pk'])
        if not is_user_ok_upload_forecast(request, forecast_model):
            raise PermissionDenied

        # validate 'data_file'
        if 'data_file' not in request.FILES:
            return JsonResponse({'error': "No 'data_file' form field."}, status=status.HTTP_400_BAD_REQUEST)

        # NB: if multiple files, just uses the first one:
        data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
        if data_file.size > MAX_UPLOAD_FILE_SIZE:
            message = "File was too large to upload. size={}, max={}.".format(data_file.size, MAX_UPLOAD_FILE_SIZE)
            return JsonResponse({'error': message}, status=status.HTTP_400_BAD_REQUEST)

        # validate 'timezero_date'
        if 'timezero_date' not in request.POST:
            return JsonResponse({'error': "No 'timezero_date' form field."}, status=status.HTTP_400_BAD_REQUEST)

        timezero_date_str = request.POST['timezero_date']
        try:
            timezero_date_obj = datetime.datetime.strptime(timezero_date_str, YYYYMMDD_DATE_FORMAT)
        except ValueError as ve:
            return JsonResponse({'error': "Badly formatted 'timezero_date' form field: '{}'".format(ve)},
                                status=status.HTTP_400_BAD_REQUEST)

        time_zero = forecast_model.project.time_zero_for_timezero_date(timezero_date_obj)
        is_created_timezero = False
        if not time_zero:
            time_zero = TimeZero.objects.create(project=forecast_model.project, timezero_date=timezero_date_obj)
            is_created_timezero = True

        # validate optional 'data_version_date'
        if is_created_timezero and ('data_version_date' in request.POST):
            data_version_date_str = request.POST['data_version_date']
            try:
                data_version_date_obj = datetime.datetime.strptime(data_version_date_str, YYYYMMDD_DATE_FORMAT)
                time_zero.data_version_date = data_version_date_obj
                time_zero.save()
            except ValueError as ve:
                return JsonResponse({'error': "Badly formatted 'data_version_date' form field: '{}'".format(ve)},
                                    status=status.HTTP_400_BAD_REQUEST)
        if is_created_timezero:
            logger.info("post(): created time_zero: {}".format(time_zero))

        # check for existing forecast
        existing_forecast_for_time_zero = forecast_model.forecast_for_time_zero(time_zero)
        if existing_forecast_for_time_zero:
            return JsonResponse({'error': "A forecast already exists. time_zero={}, file_name='{}'. Please delete "
                                          "existing data and then upload again."
                                .format(time_zero.timezero_date.strftime(YYYYMMDD_DATE_FORMAT), data_file.name)},
                                status=status.HTTP_400_BAD_REQUEST)

        # upload to cloud and enqueue a job to process a new UploadFileJob
        is_error, upload_file_job = _upload_file(request.user, data_file, process_upload_file_job__forecast,
                                                 forecast_model_pk=forecast_model.pk,
                                                 timezero_pk=time_zero.pk)
        if is_error:
            return JsonResponse({'error': "There was an error uploading the file. The error was: '{}'"
                                .format(is_error)},
                                status=status.HTTP_400_BAD_REQUEST)

        upload_file_job_serializer = UploadFileJobSerializer(upload_file_job, context={'request': request})
        return JsonResponse(upload_file_job_serializer.data)


class UploadFileJobDetailView(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = UploadFileJob.objects.all()
    serializer_class = UploadFileJobSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):  # return True if the current user can access the view
        upload_file_job = self.get_object()
        return self.request.user.is_superuser or (upload_file_job.user == self.request.user)


class ForecastModelDetail(UserPassesTestMixin, generics.RetrieveDestroyAPIView):
    queryset = ForecastModel.objects.all()
    serializer_class = ForecastModelSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):  # return True if the current user can access the view
        forecast_model = self.get_object()
        return forecast_model.project.is_user_ok_to_view(self.request.user)


    def delete(self, request, *args, **kwargs):
        """
        Deletes this model. Runs in the calling thread and therefore blocks.
        """
        forecast_model = self.get_object()
        if not is_user_ok_edit_model(request.user, forecast_model):
            raise PermissionDenied

        response = self.destroy(request, *args, **kwargs)
        return response


class ForecastDetail(RetrieveDestroyAPIView):
    queryset = Forecast.objects.all()
    serializer_class = ForecastSerializer


    def get(self, request, *args, **kwargs):
        forecast = self.get_object()
        if not forecast.forecast_model.project.is_user_ok_to_view(request.user):
            raise PermissionDenied

        return self.retrieve(request, *args, **kwargs)


    def delete(self, request, *args, **kwargs):
        """
        Deletes this forecast. Runs in the calling thread and therefore blocks.
        """
        forecast = self.get_object()
        if not forecast.is_user_ok_to_delete(request.user):
            raise PermissionDenied

        response = self.destroy(request, *args, **kwargs)
        return response


class TruthDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Project.objects.all()
    serializer_class = TruthSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):  # return True if the current user can access the view
        project = self.get_object()
        return project.is_user_ok_to_view(self.request.user)


#
# Truth data-related views
#

@api_view(['GET'])
@renderer_classes((BrowsableAPIRenderer, CSVRenderer))
def truth_data(request, project_pk):
    """
    :return: the Project's truth data as CSV. note that the actual data is wrapped by metadata
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_ok_to_view(request.user):
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
    for timezero_date, location_name, target_name, value in project.get_truth_data_rows():
        timezero_date = timezero_date.strftime(YYYYMMDD_DATE_FORMAT)
        writer.writerow([timezero_date, location_name, target_name, value])

    return response


#
# Score data-related views
#

@api_view(['GET'])
@renderer_classes((BrowsableAPIRenderer, CSVRenderer))
def score_data(request, pk):
    """
    :return: the Project's score data as CSV
    """
    project = get_object_or_404(Project, pk=pk)
    if not project.is_user_ok_to_view(request.user):
        return HttpResponseForbidden()

    if project.score_csv_file_cache.is_file_exists():
        return csv_response_for_cached_project_score_data(project)
    else:
        return csv_response_for_project_score_data(project)


#
# Forecast data-related views
#

@api_view(['GET'])
@renderer_classes((JSONRenderer, BrowsableAPIRenderer))
def forecast_data(request, pk):
    """
    :return: a Forecast's data as JSON - see load_predictions_from_json_io_dict() for the format
    """
    forecast = get_object_or_404(Forecast, pk=pk)
    if not forecast.forecast_model.project.is_user_ok_to_view(request.user):
        return HttpResponseForbidden()

    return json_response_for_forecast(request, forecast)


def json_response_for_forecast(request, forecast):
    """
    :return: a JsonResponse for forecast
    """
    # note: I tried to use a rest_framework.response.Response, which is supposed to support pretty printing on the
    # client side via something like:
    #   curl -H 'Accept: application/json; indent=4' http://127.0.0.p1:8000/api/project/1/template_data/
    # but when I tried this, returned a delimited string instead of JSON:
    #   return Response(JSONRenderer().render(location_dicts))
    # https://stackoverflow.com/questions/23195210/how-to-get-pretty-output-from-rest-framework-serializer
    response = JsonResponse(json_io_dict_from_forecast(forecast))  # defaults to 'content_type' 'application/json'
    response['Content-Disposition'] = 'attachment; filename="{}.json"'.format(get_valid_filename(forecast.source))
    return response


#
# Score data-related functions
#

SCORE_CSV_HEADER_PREFIX = ['model', 'timezero', 'season', 'location', 'target']


def _csv_filename_for_project_scores(project):
    return get_valid_filename(project.name + '-scores.csv')


def csv_response_for_cached_project_score_data(project):
    """
    Similar to csv_response_for_project_score_data(), but returns a response that's loaded from an existing S3 file.

    :param project:
    :return:
    """
    with tempfile.TemporaryFile() as cloud_file_fp:  # <class '_io.BufferedRandom'>
        try:
            download_file(project.score_csv_file_cache, cloud_file_fp)
            cloud_file_fp.seek(0)  # yes you have to do this!

            # https://stackoverflow.com/questions/16538210/downloading-files-from-amazon-s3-using-django
            csv_filename = _csv_filename_for_project_scores(project)
            wrapper = FileWrapper(cloud_file_fp)
            response = HttpResponse(wrapper, content_type='text/csv')
            # response['Content-Length'] = os.path.getsize('/tmp/'+fname)
            response['Content-Disposition'] = 'attachment; filename="{}"'.format(str(csv_filename))
            return response
        except Exception as exc:
            logger.debug("csv_response_for_cached_project_score_data(): Error: {}. project={}".format(exc, project))


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

        `ForecastModel.abbreviation | ForecastModel.name , TimeZero.timezero_date, season, Location.name, Target.name`

    Followed on the same line by a variable number of ScoreValue.value columns, one for each Score. Score names are in
    the header. An example header and first few rows:

        model,           timezero,  season,    location,  target,          constant score,  Absolute Error
        gam_lag1_tops3,  20170423,  2017-2018  TH01,      1_biweek_ahead,  1                <blank>
        gam_lag1_tops3,  20170423,  2017-2018  TH01,      1_biweek_ahead,  <blank>          2
        gam_lag1_tops3,  20170423,  2017-2018  TH01,      2_biweek_ahead,  <blank>          1
        gam_lag1_tops3,  20170423,  2017-2018  TH01,      3_biweek_ahead,  <blank>          9
        gam_lag1_tops3,  20170423,  2017-2018  TH01,      4_biweek_ahead,  <blank>          6
        gam_lag1_tops3,  20170423,  2017-2018  TH01,      5_biweek_ahead,  <blank>          8
        gam_lag1_tops3,  20170423,  2017-2018  TH02,      1_biweek_ahead,  <blank>          6
        gam_lag1_tops3,  20170423,  2017-2018  TH02,      2_biweek_ahead,  <blank>          6
        gam_lag1_tops3,  20170423,  2017-2018  TH02,      3_biweek_ahead,  <blank>          37
        gam_lag1_tops3,  20170423,  2017-2018  TH02,      4_biweek_ahead,  <blank>          25
        gam_lag1_tops3,  20170423,  2017-2018  TH02,      5_biweek_ahead,  <blank>          62

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
    logger.debug("_write_csv_score_data_for_project(): getting rows: project={}".format(project))
    sql = """
        SELECT f.forecast_model_id, f.time_zero_id, sv.location_id, sv.target_id, sv.score_id, sv.value
        FROM {scorevalue_table_name} AS sv
               INNER JOIN {score_table_name} s ON sv.score_id = s.id
               INNER JOIN {forecast_table_name} AS f ON sv.forecast_id = f.id
               INNER JOIN {forecastmodel_table_name} AS fm ON f.forecast_model_id = fm.id
        WHERE fm.project_id = %s
        ORDER BY f.forecast_model_id, f.time_zero_id, sv.location_id, sv.target_id, sv.score_id;
    """.format(scorevalue_table_name=ScoreValue._meta.db_table,
               score_table_name=Score._meta.db_table,
               forecast_table_name=Forecast._meta.db_table,
               forecastmodel_table_name=ForecastModel._meta.db_table)
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        rows = cursor.fetchall()

    # write grouped rows
    logger.debug("_write_csv_score_data_for_project(): preparing to iterate")
    forecast_model_id_to_obj = {forecast_model.pk: forecast_model for forecast_model in project.models.all()}
    timezeros = project.timezeros.all()
    timezero_id_to_obj = {timezero.pk: timezero for timezero in timezeros}
    location_id_to_obj = {location.pk: location for location in project.locations.all()}
    target_id_to_obj = {target.pk: target for target in project.targets.all()}
    timezero_to_season_name = project.timezero_to_season_name()

    logger.debug("_write_csv_score_data_for_project(): iterating")
    for (forecast_model_id, time_zero_id, location_id, target_id), score_id_value_grouper \
            in groupby(rows, key=lambda _: (_[0], _[1], _[2], _[3])):
        forecast_model = forecast_model_id_to_obj[forecast_model_id]
        timezero = timezero_id_to_obj[time_zero_id]
        location = location_id_to_obj[location_id]
        target = target_id_to_obj[target_id]
        # ex score_groups: [(1, 18, 1, 1, 1, 1.0), (1, 18, 1, 1, 2, 2.0)]  # multiple scores per group
        #                  [(1, 18, 1, 2, 2, 0.0)]                         # single score
        score_groups = list(score_id_value_grouper)
        score_id_to_value = {score_group[-2]: score_group[-1] for score_group in score_groups}
        score_values = [score_id_to_value[score.id] if score.id in score_id_to_value else None for score in scores]
        csv_writer.writerow([forecast_model.abbreviation if forecast_model.abbreviation else forecast_model.name,
                             timezero.timezero_date.strftime(YYYYMMDD_DATE_FORMAT), timezero_to_season_name[timezero],
                             location.name, target.name]
                            + score_values)
    logger.debug("_write_csv_score_data_for_project(): done")
