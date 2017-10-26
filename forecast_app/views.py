import os
from pathlib import Path

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.http import JsonResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import DetailView

from forecast_app.models import Project, ForecastModel, Forecast, TimeZero
from utils.utilities import mean_abs_error_rows_for_project


def index(request):
    projects = Project.objects.all()
    return render(
        request,
        'index.html',
        context={'projects': projects},
    )


def about(request):
    return render(request, 'about.html')


def project_visualizations(request, project_pk):
    """
    View function to render various visualizations for a particular project.
    """
    # todo xx pull season_start_year and location from somewhere, probably form elements on the page
    season_start_year = 2016
    location = 'US National'

    project = get_object_or_404(Project, pk=project_pk)
    mean_abs_error_rows = mean_abs_error_rows_for_project(project, season_start_year, location)
    return render(
        request,
        'project_visualizations.html',
        context={'project': project,
                 'season_start_year': season_start_year,
                 'location': location,
                 'mean_abs_error_rows': mean_abs_error_rows},
    )


class ProjectDetailView(DetailView):
    model = Project


class ForecastModelDetailView(DetailView):
    model = ForecastModel


    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        forecast_model = self.get_object()

        # pass a list of 2-tuples of time_zero/forecast pairs for this ForecastModel: (TimeZero, Forecast)
        timezero_forecast_pairs = []
        for time_zero in forecast_model.project.timezero_set.all().order_by('timezero_date'):
            timezero_forecast_pairs.append((time_zero, forecast_model.forecast_for_time_zero(time_zero)))
        context['timezero_forecast_pairs'] = timezero_forecast_pairs

        return context


class ForecastDetailView(DetailView):
    model = Forecast


def download_json_for_forecast(request, forecast_pk):
    """
    Returns a response containing a JSON file for a Forecast's data.

    :return: JSON version of the passed Forecast's data
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    location_target_dict = forecast.get_location_target_dict()
    response = JsonResponse(location_target_dict)
    response['Content-Disposition'] = 'attachment; filename="{csv_filename}.json"'.format(
        csv_filename=forecast.csv_filename)
    return response


# todo authorization
def delete_forecast(request, forecast_pk):
    """
    Deletes the passed Forecast.

    :return: redirect to the forecast's forecast_model detail page
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    forecast.delete()
    return redirect('forecastmodel-detail', pk=forecast.forecast_model.pk)


# todo authorization
def upload_forecast(request, forecast_model_pk, timezero_pk):
    """
    Uploads the passed data into a new Forecast.

    :return: redirect to the new forecast's detail page
    """
    forecast_model = get_object_or_404(ForecastModel, pk=forecast_model_pk)
    time_zero = get_object_or_404(TimeZero, pk=timezero_pk)

    if 'data_file' not in request.FILES:  # user submitted without specifying a file to upload
        return render(request, 'message.html',
                      context={'title': "No file selected to upload.",
                               'message': "Please go back and select one."})

    # todo memory, etc: https://stackoverflow.com/questions/3702465/how-to-copy-inmemoryuploadedfile-object-to-disk
    data_file = request.FILES['data_file']  # InMemoryUploadedFile
    file_name = data_file.name

    # error if data already exists for same time_zero and data_file.name
    existing_forecast_for_time_zero = forecast_model.forecast_for_time_zero(time_zero)
    if existing_forecast_for_time_zero and (existing_forecast_for_time_zero.csv_filename == file_name):
        return render(request, 'message.html',
                      context={'title': "A forecast already exists.",
                               'message': "time_zero={}, file_name='{}'. Please delete existing data and then "
                                          "upload again. You may need to refresh the page to see the delete "
                                          "button.".format(time_zero.timezero_date, file_name)})

    data = data_file.read()
    path = default_storage.save('tmp/somename.mp3', ContentFile(data))
    tmp_data_file = os.path.join(settings.MEDIA_ROOT, path)
    try:
        forecast_model.load_forecast(Path(tmp_data_file), time_zero, file_name)
        return redirect('forecastmodel-detail', pk=forecast_model.pk)
    except RuntimeError as rte:
        return render(request, 'message.html',
                      context={'title': "Got an error trying to load the data.",
                               'message': "The error was: &ldquo;<span class=\"bg-danger\">{}</span>&rdquo;. "
                                          "Please go back and select a valid file.".format(rte)})
