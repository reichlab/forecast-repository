import csv
from pathlib import Path

from django.contrib.auth.mixins import UserPassesTestMixin
from django.contrib.auth.models import User
from django.http import JsonResponse, HttpResponseForbidden, HttpResponse
from rest_framework import generics
from rest_framework.decorators import api_view, renderer_classes
from rest_framework.generics import get_object_or_404
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework_csv.renderers import CSVRenderer

from forecast_app.models import Project, ForecastModel, Forecast
from forecast_app.models.project import TRUTH_CSV_HEADER
from forecast_app.models.upload_file_job import UploadFileJob
from forecast_app.serializers import ProjectSerializer, UserSerializer, ForecastModelSerializer, ForecastSerializer, \
    TemplateSerializer, TruthSerializer, UploadFileJobSerializer
from utils.utilities import CDC_CSV_HEADER


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
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer


    # def get_serializer(self, *args, **kwargs):
    #     # (<QuerySet [(3, 'public project'), (4, 'private project')]>,) {'many': True}
    #     super_serializer = super().get_serializer(*args, **kwargs)
    #     print('yy', args, kwargs, type(super_serializer), super_serializer)
    #     return super_serializer


    # def get_serializer_class(self):
    #     # return super().get_serializer_class()
    #
    #     # project = self.get_object()
    #     # return project.is_user_ok_to_view(self.request.user)
    #
    #     print('xx', self.request.user)
    #     return ProjectSerializer
    #     # if xx:
    #     #     return ProjectSerializer
    #     # else:
    #     #     return ProjectSerializerMinimal


class ProjectDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        project = self.get_object()
        return project.is_user_ok_to_view(self.request.user)


class UserList(generics.ListCreateAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer


class UserDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception

    def test_func(self):  # return True if the current user can access the view
        detail_user = self.get_object()
        return self.request.user.is_superuser or (detail_user == self.request.user)


class UploadFileJobDetailView(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = UploadFileJob.objects.all()
    serializer_class = UploadFileJobSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        upload_file_job = self.get_object()
        return self.request.user.is_superuser or (upload_file_job.user == self.request.user)


class ForecastModelDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = ForecastModel.objects.all()
    serializer_class = ForecastModelSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        forecast_model = self.get_object()
        return forecast_model.project.is_user_ok_to_view(self.request.user)


class ForecastDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Forecast.objects.all()
    serializer_class = ForecastSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        forecast = self.get_object()
        return forecast.forecast_model.project.is_user_ok_to_view(self.request.user)


class TemplateDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Project.objects.all()
    serializer_class = TemplateSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        project = self.get_object()
        return project.is_user_ok_to_view(self.request.user)


class TruthDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Project.objects.all()
    serializer_class = TruthSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


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


#
# Template and forecast data-related views
#

@api_view(['GET'])
@renderer_classes((JSONRenderer, BrowsableAPIRenderer, CSVRenderer))
def template_data(request, project_pk):
    """
    :return: the Project's template data as JSON or CSV. note that the actual data is wrapped by metadata
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_ok_to_view(request.user):
        return HttpResponseForbidden()

    # dispatch based on requested format. I tried a number of things to get DRF to pass a 'format' param, but didn't
    # succeed. What worked was to install the https://github.com/mjumbewu/django-rest-framework-csv custom CSV renderer
    # per http://www.django-rest-framework.org/api-guide/renderers/#csv , and then decorate these two view-based
    # functions
    if ('format' in request.query_params) and (request.query_params['format'] == 'csv'):
        return csv_response_for_model_with_cdc_data(project)
    else:
        return json_response_for_model_with_cdc_data(request, project)


@api_view(['GET'])
@renderer_classes((JSONRenderer, BrowsableAPIRenderer, CSVRenderer))
def forecast_data(request, forecast_pk):
    """
    :return: the Project's template data as JSON or CSV. note that the actual data is wrapped by metadata
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    if not forecast.forecast_model.project.is_user_ok_to_view(request.user):
        return HttpResponseForbidden()

    # dispatch based on requested format. see note in template_data() re: getting this via a 'format' param
    if ('format' in request.query_params) and (request.query_params['format'] == 'csv'):
        return csv_response_for_model_with_cdc_data(forecast)
    else:
        return json_response_for_model_with_cdc_data(request, forecast)


def json_response_for_model_with_cdc_data(request, model_with_cdc_data):
    """
    :return: a JsonResponse for model_with_cdc_data
    """
    # note: I tried to use a rest_framework.response.Response, which is supposed to support pretty printing on the
    # client side via something like:
    #   curl -H 'Accept: application/json; indent=4' http://127.0.0.p1:8000/api/project/1/template_data/
    # but when I tried this, returned a delimited string instead of JSON:
    #   return Response(JSONRenderer().render(location_dicts))
    # via https://stackoverflow.com/questions/23195210/how-to-get-pretty-output-from-rest-framework-serializer
    from forecast_app.serializers import ProjectSerializer, ForecastSerializer  # avoid circular imports


    detail_serializer_class = ProjectSerializer if isinstance(model_with_cdc_data, Project) else ForecastSerializer
    detail_serializer = detail_serializer_class(model_with_cdc_data, context={'request': request})
    metadata_dict = detail_serializer.data
    location_dicts = model_with_cdc_data.get_location_dicts_download_format()
    response = JsonResponse({'metadata': metadata_dict,
                             'locations': location_dicts})  # defaults to 'content_type' 'application/json'
    response['Content-Disposition'] = 'attachment; filename="{csv_filename}.json"' \
        .format(csv_filename=model_with_cdc_data.csv_filename)
    return response


def csv_response_for_model_with_cdc_data(model_with_cdc_data):
    """
    Similar to json_response_for_model_with_cdc_data(), but returns a response with model_with_cdc_data's data formatted
    as CSV.
    """
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="{csv_filename}"' \
        .format(csv_filename=model_with_cdc_data.csv_filename)


    def transform_row(row):
        return row  # todo xx replace '', etc.


    writer = csv.writer(response)
    writer.writerow(CDC_CSV_HEADER)
    for row in model_with_cdc_data.get_data_rows(is_order_by_pk=True):
        writer.writerow(transform_row(row))

    return response


def csv_response_for_project_truth_data(project):
    """
    Similar to json_response_for_model_with_cdc_data(), but returns a response with project's truth data formatted as
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
    response['Content-Disposition'] = 'attachment; filename="{csv_filename}"'.format(csv_filename=str(csv_filename))


    def transform_row(row):
        return row  # todo xx replace '', etc.


    writer = csv.writer(response)
    writer.writerow(TRUTH_CSV_HEADER)
    for row in project.get_truth_data_rows():
        writer.writerow(transform_row(row))

    return response
