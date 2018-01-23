import os
from pathlib import Path

from PIL import Image, ImageDraw
from django.conf import settings
from django.contrib.auth.mixins import UserPassesTestMixin
from django.contrib.auth.models import User
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.forms import inlineformset_factory
from django.http import JsonResponse, HttpResponseForbidden, HttpResponse, HttpResponseBadRequest
from django.shortcuts import render, get_object_or_404, redirect
from django.urls import reverse
from django.views.generic import DetailView, ListView

from forecast_app.forms import ProjectForm, ForecastModelForm
from forecast_app.models import Project, ForecastModel, Forecast, TimeZero
from forecast_app.models.project import PROJECT_OWNER_GROUP_NAME, Target
from forecast_app.templatetags.auth_extras import has_group
from utils.utilities import mean_abs_error_rows_for_project


def index(request):
    # set project_sparkline_tuples
    project_sparkline_tuples = []
    for project in sorted(Project.objects.all(), key=lambda p: p.name):
        sparkline_url = None  # this default covers cases where no authorization or no data
        img_title = None  # ""
        if project.is_user_allowed_to_view(request.user):
            distribution_preview = project.get_distribution_preview()
            if distribution_preview:
                first_forecast, first_location, first_target = distribution_preview
                sparkline_url = "{reverse_url}?location={location}&target={target}".format(
                    reverse_url=reverse('forecast-sparkline', args=[str(first_forecast.pk)]),
                    location=first_location,
                    target=first_target)  # manually build query parameters
                # note: I tried urllib.parse.quote(sparkline_url), but it didn't work with the 'forecast-sparkline' url
                # (location and target were coming through as None). so we leave it for now
                img_title = "Model '{model_name}' > Forecast tz={tz_date}{maybe_dvdate} > Location '{location}' > " \
                            "Target '{target}'".format(
                    model_name=first_forecast.forecast_model.name,
                    tz_date=str(first_forecast.time_zero.timezero_date),
                    maybe_dvdate=', dvd=' + str(first_forecast.time_zero.data_version_date)
                    if first_forecast.time_zero.data_version_date else '',
                    location=first_location,
                    target=first_target)
        project_sparkline_tuples.append((project, sparkline_url, img_title))

    # done
    return render(
        request,
        'index.html',
        context={'users': User.objects.all(),
                 'project_sparkline_tuples': project_sparkline_tuples},
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
    if not project.is_user_allowed_to_view(request.user):
        return HttpResponseForbidden()

    return render(
        request,
        'template_data_detail.html',
        context={'project': project,
                 'ok_user_edit_project': ok_user_edit_project(request.user, project)})


def project_visualizations(request, project_pk):
    """
    View function to render various visualizations for a particular project.
    """
    # todo xx pull season_start_year and location from somewhere, probably form elements on the page
    season_start_year = 2016
    location = 'US National'

    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_allowed_to_view(request.user):
        return HttpResponseForbidden()

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
    Shows a form to add a new Project for the passed User. Authorization: The logged-in user must be a superuser, or the
    same as the passed OBO user AND the must be in the group PROJECT_OWNER_GROUP_NAME.

    :param: user_pk: the on-behalf-of user. may not be the same as the authenticated user
    """
    authenticated_user = request.user
    new_project_user = get_object_or_404(User, pk=user_pk)
    if not ok_user_create_project(new_project_user, authenticated_user):
        return HttpResponseForbidden()

    # set up Target and TimeZero formsets using a new (unsaved) Project
    from utils.make_cdc_flu_challenge_project import CDC_CONFIG_DICT  # avoid circular imports


    new_project = Project(owner=new_project_user,
                          config_dict=CDC_CONFIG_DICT)
    TargetInlineFormSet = inlineformset_factory(Project, Target, fields=('name', 'description'), extra=3)
    target_formset = TargetInlineFormSet(instance=new_project)

    TimeZeroInlineFormSet = inlineformset_factory(Project, TimeZero, fields=('timezero_date', 'data_version_date'),
                                                  extra=3)
    timezero_formset = TimeZeroInlineFormSet(instance=new_project)

    if request.method == 'POST':
        project_form = ProjectForm(request.POST, instance=new_project)
        target_formset = TargetInlineFormSet(request.POST, instance=new_project)
        timezero_formset = TimeZeroInlineFormSet(request.POST, instance=new_project)
        if project_form.is_valid() and target_formset.is_valid() and timezero_formset.is_valid():
            new_project = project_form.save(commit=False)
            new_project.owner = new_project_user  # force the owner to the current user
            new_project.save()
            project_form.save_m2m()

            target_formset.save()
            timezero_formset.save()

            # todo xx flash a temporary 'success' message
            return redirect('project-detail', pk=new_project.pk)

    else:  # GET
        project_form = ProjectForm(instance=new_project)

    return render(request, 'show_form.html',
                  context={'title': 'New Project',
                           'button_name': 'Create',
                           'form': project_form,
                           'target_formset': target_formset,
                           'timezero_formset': timezero_formset})


def edit_project(request, project_pk):
    """
    Shows a form to edit a Project's basic information. Authorization: The logged-in user must be a superuser or the
    Project's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not ok_user_edit_project(request.user, project):
        return HttpResponseForbidden()

    TargetInlineFormSet = inlineformset_factory(Project, Target, fields=('name', 'description'), extra=3)
    target_formset = TargetInlineFormSet(instance=project)

    TimeZeroInlineFormSet = inlineformset_factory(Project, TimeZero, fields=('timezero_date', 'data_version_date'),
                                                  extra=3)
    timezero_formset = TimeZeroInlineFormSet(instance=project)

    if request.method == 'POST':
        project_form = ProjectForm(request.POST, instance=project)
        target_formset = TargetInlineFormSet(request.POST, instance=project)
        timezero_formset = TimeZeroInlineFormSet(request.POST, instance=project)
        if project_form.is_valid() and target_formset.is_valid() and timezero_formset.is_valid():
            project_form.save()
            target_formset.save()
            timezero_formset.save()

            # todo xx flash a temporary 'success' message
            return redirect('project-detail', pk=project.pk)

    else:  # GET
        project_form = ProjectForm(instance=project)

    return render(request, 'show_form.html',
                  context={'title': 'Edit Project',
                           'button_name': 'Save',
                           'form': project_form,
                           'target_formset': target_formset,
                           'timezero_formset': timezero_formset})


def delete_project(request, project_pk):
    """
    Does the actual deletion of a Project. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be a superuser or the Project's owner.
    """
    user = request.user
    project = get_object_or_404(Project, pk=project_pk)
    if not ok_user_edit_project(request.user, project):
        return HttpResponseForbidden()

    project.delete()
    # todo xx flash a temporary 'success' message
    return redirect('user-detail', pk=user.pk)


def create_model(request, project_pk):
    """
    Shows a form to add a new ForecastModel for the passed User. Authorization: The logged-in user must be a superuser,
    or the Project's owner, or one if its model_owners.
    """
    user = request.user
    project = get_object_or_404(Project, pk=project_pk)
    if not ok_user_create_model(request.user, project):
        return HttpResponseForbidden()

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
    Shows a form to edit a ForecastModel. Authorization: The logged-in user must be a superuser, or the Project's owner,
    or the model's owner.
    """
    forecast_model = get_object_or_404(ForecastModel, pk=model_pk)
    if not ok_user_edit_model(request.user, forecast_model):
        return HttpResponseForbidden()

    if request.method == 'POST':
        forecast_model_form = ForecastModelForm(request.POST, instance=forecast_model)
        if forecast_model_form.is_valid():
            forecast_model_form.save()

            # todo xx flash a temporary 'success' message
            return redirect('model-detail', pk=forecast_model.pk)

    else:  # GET
        forecast_model_form = ForecastModelForm(instance=forecast_model)

    return render(request, 'show_form.html',
                  context={'title': 'Edit Model',
                           'button_name': 'Save',
                           'form': forecast_model_form})


def delete_model(request, model_pk):
    """
    Does the actual deletion of the ForecastModel. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be a superuser, or the Project's owner, or the model's owner.
    """
    user = request.user
    forecast_model = get_object_or_404(ForecastModel, pk=model_pk)
    if not ok_user_edit_model(request.user, forecast_model):
        return HttpResponseForbidden()

    forecast_model.delete()
    # todo xx flash a temporary 'success' message
    return redirect('user-detail', pk=user.pk)


#
# ---- List views ----
#

class UserListView(ListView):
    model = User


#
# ---- Detail views ----
#


class ProjectDetailView(UserPassesTestMixin, DetailView):
    """
    Authorization: private projects can only be accessed by the project's owner or any of its model_owners
    """
    model = Project
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        project = self.get_object()
        return project.is_user_allowed_to_view(self.request.user)


    def get_context_data(self, **kwargs):
        project = self.get_object()
        context = super().get_context_data(**kwargs)
        context['ok_user_edit_project'] = ok_user_edit_project(self.request.user, project)
        context['ok_user_create_model'] = ok_user_create_model(self.request.user, project)
        timezeros_to_num_forecasts = {
            timezero: sum(map(lambda x: 1 if x else 0, project.forecasts_for_timezero(timezero)))
            for timezero in project.timezeros.all()}
        context['timezeros_to_num_forecasts'] = timezeros_to_num_forecasts
        return context


def forecast_models_owned_by_user(user):
    """
    :param user: a User
    :return: searches all ForecastModels and returns those where the owner is user
    """
    owned_models = []
    for forecast_model in ForecastModel.objects.all():
        if forecast_model.owner == user:
            owned_models.append(forecast_model)
    return owned_models


def projects_and_roles_for_user(user):
    """
    :param user: a User
    :return: searches all projects and returns a list of 2-tuples of projects and roles that user is involved in,
        each of the form: (project, role), where role is either 'Project Owner' or 'Model Owner'
    """
    projects_and_roles = []
    for project in Project.objects.all():
        if project.owner == user:
            projects_and_roles.append((project, 'Project Owner'))
        elif user in project.model_owners.all():
            projects_and_roles.append((project, 'Model Owner'))
    return projects_and_roles


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
        projects_and_roles = projects_and_roles_for_user(user)
        owned_models = forecast_models_owned_by_user(user)
        context['projects_and_roles'] = sorted(projects_and_roles,
                                               key=lambda project_and_role: project_and_role[0].name)
        context['owned_models'] = owned_models
        context['PROJECT_OWNER_GROUP_NAME'] = PROJECT_OWNER_GROUP_NAME
        context['ok_user_create_project'] = ok_user_create_project(user, self.request.user)

        return context


def timezero_forecast_pairs_for_forecast_model(forecast_model):
    """
    :return: a list of 2-tuples of time_zero/forecast pairs for forecast_model. form: (TimeZero, Forecast)
    """
    timezero_forecast_pairs = []
    for time_zero in forecast_model.project.timezeros.all().order_by('timezero_date'):
        timezero_forecast_pairs.append((time_zero, forecast_model.forecast_for_time_zero(time_zero)))
    return timezero_forecast_pairs


class ForecastModelDetailView(UserPassesTestMixin, DetailView):
    model = ForecastModel
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        forecast_model = self.get_object()
        return forecast_model.project.is_user_allowed_to_view(self.request.user)


    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        forecast_model = self.get_object()
        context['timezero_forecast_pairs'] = timezero_forecast_pairs_for_forecast_model(forecast_model)
        context['ok_user_edit_model'] = ok_user_edit_model(self.request.user, forecast_model)

        return context


class ForecastDetailView(UserPassesTestMixin, DetailView):
    model = Forecast
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        forecast = self.get_object()
        return forecast.forecast_model.project.is_user_allowed_to_view(self.request.user)


#
# ---- download-related functions ----
#

def download_json_for_model_with_cdc_data(request, model_with_cdc_data_pk, **kwargs):
    """
    Returns a response containing a JSON file for a ModelWithCDCData's (Project or Forecast) data.

    :param model_with_cdc_data_pk: pk of either a Project or Forecast - disambiguated by kwargs['type']
    :param kwargs: has a single 'type' key that's either 'project' or 'forecast', which determines what
        model_with_cdc_data_pk refers to
    :return: response for the JSON version of the passed ModelWithCDCData's data
    """
    is_project = kwargs['type'] == 'project'
    if is_project:
        model_with_cdc_data_class = Project
    elif kwargs['type'] == 'forecast':
        model_with_cdc_data_class = Forecast
    else:
        raise RuntimeError("invalid kwargs: {}".format(kwargs))

    model_with_cdc_data = get_object_or_404(model_with_cdc_data_class, pk=model_with_cdc_data_pk)
    project = model_with_cdc_data if is_project else model_with_cdc_data.forecast_model.project
    if not project.is_user_allowed_to_view(request.user):
        return HttpResponseForbidden()

    from forecast_app.serializers import ProjectSerializer, ForecastSerializer  # avoid circular imports


    detail_serializer_class = ProjectSerializer if is_project else ForecastSerializer
    detail_serializer = detail_serializer_class(model_with_cdc_data, context={'request': request})
    detail_data = detail_serializer.data
    cdc_data = model_with_cdc_data.get_location_target_dict()
    response = JsonResponse({'metadata': detail_data,
                             'data': cdc_data})
    response['Content-Disposition'] = 'attachment; filename="{csv_filename}.json"' \
        .format(csv_filename=model_with_cdc_data.csv_filename)
    return response


def forecast_sparkline_bin_for_loc_and_target(request, forecast_pk):
    """
    :param request: A GET that must contain two query parameters: 'location': a valid location in forecast_pk's data,
        and 'target', a valid target ""
    :param forecast_pk
    :return: a small image that is a sparkline for the passed bin
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    project = forecast.forecast_model.project
    if not project.is_user_allowed_to_view(request.user):
        return HttpResponseForbidden()

    # validate query parameters
    location = request.GET['location'] if 'location' in request.GET else None
    target = request.GET['target'] if 'target' in request.GET else None
    if (not location) or (not target):
        return HttpResponseBadRequest("one or both of the two required query parameters was not passed. location={}, "
                                      "target={}".format(location, target))

    # validate location and target
    locations = project.get_locations()
    targets = project.get_targets(location)
    if (location not in locations) or (target not in targets):
        return HttpResponseBadRequest("invalid target or location for project. project={}, location={}, locations={}, "
                                      "target={}, targets={}".format(project, location, locations, target, targets))

    rescaled_vals_from_forecast = forecast.rescaled_bin_for_loc_and_target(location, target)

    # limit the length so the image is not too wide - 30 is magic. NB: first items may not be characteristic at all:
    image = plot_sparkline(rescaled_vals_from_forecast[:30])

    response = HttpResponse(content_type='image/png')
    image.save(response, 'png')
    return response


def plot_sparkline(normalized_values):
    """
    from: https://bitworking.org/news/2005/04/Sparklines_in_data_URIs_in_Python

    :param normalized_values: a list of numbers scaled to between 0 and 100
    :return a sparkline .png image for the passed data. Values greater than 95 are displayed in red, otherwise they are
        displayed in green
    """
    image = Image.new("RGB", (len(normalized_values) * 2, 15), 'white')
    draw = ImageDraw.Draw(image)
    for (r, i) in zip(normalized_values, range(0, len(normalized_values) * 2, 2)):
        color = (r > 50) and "red" or "gray"
        draw.line((i, image.size[1] - r / 10 - 4, i, (image.size[1] - r / 10)), fill=color)
    del draw
    return image


#
# ---- Project template upload/delete views ----
#

def delete_template(request, project_pk):
    """
    Does the actual deletion of a Forecast. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be a superuser, or the Project's owner, or the forecast's model's owner.

    :return: redirect to the forecast's forecast_model detail page
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not ok_user_edit_project(request.user, project):
        return HttpResponseForbidden()

    project.delete_template()
    return redirect('project-detail', pk=project_pk)


def upload_template(request, project_pk):
    """
    Uploads the passed data into a the project's template. Authorization: The logged-in user must be a superuser or the
    Project's owner.

    :return: redirect to the new forecast's detail page
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not ok_user_edit_project(request.user, project):
        return HttpResponseForbidden()

    if 'data_file' not in request.FILES:  # user submitted without specifying a file to upload
        return render(request, 'message.html',
                      context={'title': "No file selected to upload.",
                               'message': "Please go back and select one."})

    # error if there is already a template
    if project.is_template_loaded():
        return render(request, 'message.html',
                      context={'title': "Template already exists.",
                               'message': "The project already has a template. Please delete it and then upload again."})

    # todo memory, etc: https://stackoverflow.com/questions/3702465/how-to-copy-inmemoryuploadedfile-object-to-disk
    data_file = request.FILES['data_file']  # InMemoryUploadedFile
    file_name = data_file.name
    data = data_file.read()
    path = default_storage.save('tmp/temp.csv', ContentFile(data))  # todo xx use with TemporaryFile :-)
    tmp_data_file = os.path.join(settings.MEDIA_ROOT, path)
    try:
        project.load_template(Path(tmp_data_file), file_name)
        return redirect('template-data-detail', project_pk=project_pk)
    except RuntimeError as rte:
        return render(request, 'message.html',
                      context={'title': "Got an error trying to load the data.",
                               'message': "The error was: &ldquo;<span class=\"bg-danger\">{}</span>&rdquo;. "
                                          "Please go back and select a valid file.".format(rte)})


#
# ---- Forecast upload/delete views ----
#

def delete_forecast(request, forecast_pk):
    """
    Does the actual deletion of a Forecast. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be a superuser, or the Project's owner, or the forecast's model's owner.

    :return: redirect to the forecast's forecast_model detail page
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    is_allowed_to_delete = request.user.is_superuser or (request.user == forecast.forecast_model.project.owner) or \
                           (request.user == forecast.forecast_model.owner)
    if not is_allowed_to_delete:
        return HttpResponseForbidden()

    forecast_model_pk = forecast.forecast_model.pk  # in case can't access after delete() <- todo possible?
    forecast.delete()
    return redirect('model-detail', pk=forecast_model_pk)


def upload_forecast(request, forecast_model_pk, timezero_pk):
    """
    Uploads the passed data into a new Forecast. Authorization: The logged-in user must be a superuser, or the Project's
    owner, or the model's owner.

    :return: redirect to the new forecast's detail page
    """
    forecast_model = get_object_or_404(ForecastModel, pk=forecast_model_pk)
    time_zero = get_object_or_404(TimeZero, pk=timezero_pk)
    is_allowed_to_upload = request.user.is_superuser or (request.user == forecast_model.project.owner) or \
                           (request.user == forecast_model.owner)
    if not is_allowed_to_upload:
        return HttpResponseForbidden()

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
    path = default_storage.save('tmp/temp.csv', ContentFile(data))  # todo xx use with TemporaryFile :-)
    tmp_data_file = os.path.join(settings.MEDIA_ROOT, path)
    try:
        forecast_model.load_forecast(Path(tmp_data_file), time_zero, file_name)
        return redirect('model-detail', pk=forecast_model.pk)
    except RuntimeError as rte:
        return render(request, 'message.html',
                      context={'title': "Got an error trying to load the data.",
                               'message': "The error was: &ldquo;<span class=\"bg-danger\">{}</span>&rdquo;. "
                                          "Please go back and select a valid file.".format(rte)})


#
# ---- authorization utilities ----


def ok_user_create_project(new_project_user, authenticated_user):
    return authenticated_user.is_superuser or ((authenticated_user == new_project_user) and
                                               has_group(authenticated_user, PROJECT_OWNER_GROUP_NAME))


def ok_user_edit_project(user, project):
    # applies to delete too
    return user.is_superuser or (user == project.owner)


def ok_user_create_model(user, project):
    return user.is_superuser or (user == project.owner) or (user in project.model_owners.all())


def ok_user_edit_model(user, forecast_model):
    # applies to delete too
    return user.is_superuser or (user == forecast_model.project.owner) or (user == forecast_model.owner)
