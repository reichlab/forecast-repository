import json
import os
from pathlib import Path

from django.conf import settings
from django.contrib.auth.models import User
from django.core.exceptions import PermissionDenied
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.http import JsonResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import DetailView

from forecast_app.forms import ProjectForm, ForecastModelForm
from forecast_app.models import Project, ForecastModel, Forecast, TimeZero
from forecast_app.models.project import PROJECT_OWNER_GROUP_NAME
from forecast_app.templatetags.auth_extras import has_group
from utils.make_example_projects import CDC_CONFIG_DICT
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


def documentation(request):
    return render(request, 'documentation.html')


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


#
# ---- CRUD-related form functions ----
#

def create_project(request, user_pk):
    """
    Shows a form to add a new Project for the passed User. Authorization: The logged-in user must be the same as the
    passed detail user, AND the logged-in user must be in the group PROJECT_OWNER_GROUP_NAME.
    """
    authenticated_user = request.user
    new_project_user = get_object_or_404(User, pk=user_pk)
    if (authenticated_user != new_project_user) or (not has_group(authenticated_user, PROJECT_OWNER_GROUP_NAME)):
        raise PermissionDenied("logged-in user was not the same as the new Project's user, or was not in the Project "
                               "Owner group. authenticated_user={}, new_project_user={}"
                               .format(authenticated_user, new_project_user))

    if request.method == 'POST':
        project_form = ProjectForm(request.POST)
        if project_form.is_valid():
            new_project = project_form.save(commit=False)
            new_project.owner = new_project_user  # force the owner to the current user
            new_project.save()
            # todo xx flash a temporary 'success' message
            return redirect('project-detail', pk=new_project.pk)

    else:  # GET
        project_form = ProjectForm(initial={'config_dict': json.dumps(CDC_CONFIG_DICT, sort_keys=True, indent=4)})

    return render(request, 'show_form.html',
                  context={'title': 'New Project',
                           'button_name': 'Create',
                           'form': project_form})


def edit_project(request, project_pk):
    """
    Shows a form to edit a Project's basic information. Authorization: The logged-in user must be the same as the
    Project's owner.
    """
    user = request.user
    project = get_object_or_404(Project, pk=project_pk)
    if user != project.owner:
        raise PermissionDenied("logged-in user was not the Project's owner. user={}, owner={}"
                               .format(user, project.owner))

    if request.method == 'POST':
        project_form = ProjectForm(request.POST, instance=project)
        if project_form.is_valid():
            project_form.save()
            # todo xx flash a temporary 'success' message
            return redirect('project-detail', pk=project.pk)

    else:  # GET
        project_form = ProjectForm(instance=project)

    return render(request, 'show_form.html',
                  context={'title': 'Edit Project',
                           'button_name': 'Save',
                           'form': project_form})


def delete_project(request, project_pk):
    """
    Does the actual deletion of the passed Project. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be the same as the Project's owner.
    """
    user = request.user
    project = get_object_or_404(Project, pk=project_pk)
    if user != project.owner:
        raise PermissionDenied("logged-in user was not the Project's owner. user={}, owner={}"
                               .format(user, project.owner))

    project.delete()
    # todo xx flash a temporary 'success' message
    return redirect('user-detail', pk=user.pk)


def create_model(request, project_pk):
    """
    Shows a form to add a new ForecastModel for the passed User. Authorization: The logged-in user must be in the
    Project's model_owners.
    """
    user = request.user
    project = get_object_or_404(Project, pk=project_pk)
    if user not in project.model_owners.all():
        raise PermissionDenied("logged-in user was not in the Project's model_owners. user={}, "
                               "Project.model_owners={}".format(user, project.model_owners))

    if request.method == 'POST':
        forecast_model_form = ForecastModelForm(request.POST)
        if forecast_model_form.is_valid():
            new_model = forecast_model_form.save(commit=False)
            new_model.owner = user  # force the owner to the current user
            new_model.project = project
            new_model.save()
            # todo xx flash a temporary 'success' message
            return redirect('model-detail', pk=new_model.pk)

    else:  # GET
        forecast_model_form = ForecastModelForm()

    return render(request, 'show_form.html',
                  context={'title': 'New Model',
                           'button_name': 'Create',
                           'form': forecast_model_form})


def edit_model(request, model_pk):
    """
    Shows a form to edit a ForecastModel. Authorization: The logged-in user must be the model's owner.
    """
    user = request.user
    forecast_model = get_object_or_404(ForecastModel, pk=model_pk)
    if user != forecast_model.owner:
        raise PermissionDenied("logged-in user was not the model's owner. user={}, forecast_model={}"
                               .format(user, forecast_model))

    if request.method == 'POST':
        forecast_model_form = ForecastModelForm(request.POST, instance=forecast_model)
        if forecast_model_form.is_valid():
            forecast_model_form.save()

            # todo xx flash a temporary 'success' message
            return redirect('model-detail', pk=forecast_model.pk)

    else:  # GET
        forecast_model_form = ForecastModelForm(instance=forecast_model)

    return render(request, 'show_form.html',
                  context={'title': 'Edit Project',
                           'button_name': 'Save',
                           'form': forecast_model_form})


def delete_model(request, model_pk):
    """
    Does the actual deletion of the ForecastModel. Authorization: The logged-in user must be the model's owner.
    """
    user = request.user
    forecast_model = get_object_or_404(ForecastModel, pk=model_pk)
    if user != forecast_model.owner:
        raise PermissionDenied("logged-in user was not the model's owner. user={}, forecast_model={}"
                               .format(user, forecast_model))

    forecast_model.delete()
    # todo xx flash a temporary 'success' message
    return redirect('user-detail', pk=user.pk)


#
# ---- Detail views ----
#

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
        context['PROJECT_OWNER_GROUP_NAME'] = PROJECT_OWNER_GROUP_NAME

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


#
# ---- Forecast upload/delete views ----
#

# todo authorization
def delete_forecast(request, forecast_pk):
    """
    Deletes the passed Forecast.

    :return: redirect to the forecast's forecast_model detail page
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    forecast.delete()
    return redirect('model-detail', pk=forecast.forecast_model.pk)


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
        return redirect('model-detail', pk=forecast_model.pk)
    except RuntimeError as rte:
        return render(request, 'message.html',
                      context={'title': "Got an error trying to load the data.",
                               'message': "The error was: &ldquo;<span class=\"bg-danger\">{}</span>&rdquo;. "
                                          "Please go back and select a valid file.".format(rte)})
