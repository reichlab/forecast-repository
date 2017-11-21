import os
from pathlib import Path

from django.conf import settings
from django.contrib.auth.models import User
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.http import JsonResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import DetailView

from forecast_app.models import Project, ForecastModel, Forecast, TimeZero
from utils.utilities import mean_abs_error_rows_for_project


def index(request):
    return render(
        request,
        'index.html',
        context={'users': User.objects.all(),
                 'projects': Project.objects.all()},
    )


def about(request):
    return render(request, 'about.html')


def template_detail(request, project_pk):
    """
    View function to render a preview of a Project's template.
    """
    project = get_object_or_404(Project, pk=project_pk)
    return render(
        request,
        'template_data_detail.html',
        context={'project': project})


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
                 'mean_abs_error_rows': mean_abs_error_rows})


class ProjectDetailView(DetailView):
    model = Project


class UserDetailView(DetailView):
    model = User

    # rename from the default 'user', which shadows the context var of that name that's always passed to templates:
    context_object_name = 'detail_user'


    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # pass a list of Projects. we have two cases: 1) projects owned by this user, and 2) projects where this user is
        # in model_owners. thus this list is of 2-tuples: (Project, user_role), where user_role is "Project Owner" or
        # "Model Owner"
        user = self.get_object()
        projects_and_roles = []
        owned_models = []
        for project in Project.objects.all():
            if project.owner == user:
                projects_and_roles.append((project, 'Project Owner'))
            elif user in project.model_owners.all():
                projects_and_roles.append((project, 'Model Owner'))
            for model in project.forecastmodel_set.all():
                if user == model.owner:
                    owned_models.append(model)
        context['projects_and_roles'] = sorted(projects_and_roles,
                                               key=lambda project_and_role: project_and_role[0].name)
        context['owned_models'] = owned_models

        return context


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


def download_json_for_model_with_cdc_data(request, model_with_cdc_data_pk, **kwargs):
    """
    Returns a response containing a JSON file for a ModelWithCDCData's (Project or Forecast) data.

    :param model_with_cdc_data_pk: pk of either a Project or Forecast - disambiguated by kwargs['type']
    :param kwargs: has a single 'type' key that's either 'project' or 'forecast', which determines what
        model_with_cdc_data_pk refers to
    :return: response for the JSON version of the passed ModelWithCDCData's data
    """
    if kwargs['type'] == 'project':
        model_with_cdc_data_class = Project
    elif kwargs['type'] == 'forecast':
        model_with_cdc_data_class = Forecast
    else:
        raise RuntimeError("invalid kwargs: {}".format(kwargs))
    model_with_cdc_data = get_object_or_404(model_with_cdc_data_class, pk=model_with_cdc_data_pk)
    location_target_dict = model_with_cdc_data.get_location_target_dict()
    response = JsonResponse(location_target_dict)
    response['Content-Disposition'] = 'attachment; filename="{csv_filename}.json"'.format(
        csv_filename=model_with_cdc_data.csv_filename)
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
