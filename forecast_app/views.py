import enum
import json
import logging
from collections import defaultdict

import django
import django_rq
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError, ClientError, ConnectionClosedError
from django import db
from django.contrib import messages
from django.contrib.auth import update_session_auth_hash
from django.contrib.auth.mixins import UserPassesTestMixin
from django.contrib.auth.models import User
from django.db import connection, transaction, IntegrityError
from django.db.models import Count
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseForbidden
from django.shortcuts import render, get_object_or_404, redirect
from django.urls import reverse
from django.utils.text import get_valid_filename
from django.views.generic import DetailView, ListView
from rq.timeouts import JobTimeoutException

from forecast_app.forms import ProjectForm, ForecastModelForm, UserModelForm, UserPasswordChangeForm, QueryForm
from forecast_app.models import Project, ForecastModel, Forecast, TimeZero, Prediction, Unit, Target, BinDistribution, \
    NamedDistribution, PointPrediction, SampleDistribution, QuantileDistribution, ForecastMetaPrediction
from forecast_app.models.job import Job, JOB_TYPE_DELETE_FORECAST, JOB_TYPE_UPLOAD_TRUTH, \
    JOB_TYPE_UPLOAD_FORECAST, JOB_TYPE_QUERY_FORECAST, JOB_TYPE_QUERY_TRUTH
from forecast_repo.settings.base import S3_BUCKET_PREFIX, UPLOAD_FILE_QUEUE_NAME, DELETE_FORECAST_QUEUE_NAME, \
    MAX_NUM_QUERY_ROWS, MAX_UPLOAD_FILE_SIZE
from utils.forecast import PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS, data_rows_from_forecast, \
    is_forecast_metadata_available, forecast_metadata, forecast_metadata_counts_for_project
from utils.project import config_dict_from_project, create_project_from_json, group_targets, unit_rows_for_project, \
    models_summary_table_rows_for_project, target_rows_for_project, latest_forecast_ids_for_project
from utils.project_diff import project_config_diff, database_changes_for_project_config_diff, Change, \
    execute_project_config_diff, order_project_config_diff
from utils.project_queries import _forecasts_query_worker, _truth_query_worker
from utils.project_truth import is_truth_data_loaded, get_truth_data_preview, get_num_truth_rows, delete_truth_data, \
    first_truth_data_forecast, oracle_model_for_project
from utils.utilities import YYYY_MM_DD_DATE_FORMAT

logger = logging.getLogger(__name__)


def index(request):
    return render(request, 'index.html')


def robots_txt(request):
    # the robots.txt template contains a mix of absolute and relative paths. for simplicity we "hard-code" the absolute
    # ones rather than getting them via `reverse()`. we do get the relative paths (which all happen to be Project-
    # specific) via `reverse()`. this means this function needs to be pretty lightweight because it can be called
    # frequently by different bots
    disallow_urls = []  # relative URLs
    for project_id in Project.objects.all().values_list('id', flat=True):
        for project_url_name in ['project-explorer', 'project-config', 'truth-data-detail', 'project-visualizations']:
            disallow_urls.append(reverse(project_url_name, args=[str(project_id)]))  # relative URLs
    return render(request, 'robots.html', content_type="text/plain", context={'disallow_urls': disallow_urls})


def about(request):
    return render(request, 'about.html')


def projects(request):
    # we cache Project.last_update() to avoid duplicate calls. recall last_update can be None.
    # per https://stackoverflow.com/questions/19868767/how-do-i-sort-a-list-with-nones-last
    projects_last_updates = sorted([(project, project.last_update()) for project in Project.objects.all()
                                    if is_user_ok_view_project(request.user, project)],
                                   reverse=True, key=lambda _: (_[1] is not None, _[1]))

    # list of 5-tuples: (project, num_models, num_forecasts, num_rows_est, num_rows_exact):
    projects_info = [(project_last_update[0], *project_summary_info(project_last_update[0]))
                     for project_last_update in projects_last_updates]
    return render(
        request,
        'projects.html',
        context={'projects_info': projects_info,
                 'is_user_ok_create_project': is_user_ok_create_project(request.user),
                 'num_public_projects': len(Project.objects.filter(is_public=True)),
                 'num_private_projects': len(Project.objects.filter(is_public=False))})


def project_summary_info(project):
    """
    Helper for views showing project summary information like # models, # forecasts, and # rows.

    :param project: a Project
    :return a 4-tuple: (num_models, num_forecasts, num_rows_est, num_rows_exact). num_rows_exact is None if no exact
        count is available
    """
    # set num_rows_exact. note that ideally we would verify that every Forecast in all of project's models has a
    # ForecastMetaPrediction, but for simplicity we simply sum them all, which will be zero if none are present. this
    # case cannot be differentiated from the one where there are ForecastMetaPredictions but their counts are all zero,
    # but that seems unlikely
    num_rows_exact = sum([sum([fmp.point_count, fmp.named_count, fmp.bin_count, fmp.sample_count, fmp.quantile_count])
                          for fmp in ForecastMetaPrediction.objects.filter(forecast__forecast_model__project=project)])
    return (*project.get_summary_counts(), num_rows_exact)


#
# ---- admin-related view functions ----
#

def zadmin_jobs(request):
    if not is_user_ok_admin(request.user):
        return HttpResponseForbidden(render(request, '403.html').content)

    return render(
        request, 'zadmin_jobs.html',
        context={'jobs': Job.objects.select_related('user').all()})  # datatable does order by


def zadmin(request):
    if not is_user_ok_admin(request.user):
        return HttpResponseForbidden(render(request, '403.html').content)

    django_db_name = db.utils.settings.DATABASES['default']['NAME']
    projects_sort_pk = [(project, project.models.count()) for project in Project.objects.order_by('pk')]
    return render(
        request, 'zadmin.html',
        context={'django_db_name': django_db_name,
                 'django_conn': connection,
                 's3_bucket_prefix': S3_BUCKET_PREFIX,
                 'max_num_query_rows': MAX_NUM_QUERY_ROWS,
                 'max_upload_file_size': MAX_UPLOAD_FILE_SIZE,
                 'projects_sort_pk': projects_sort_pk})


def delete_jobs(request):
    if not is_user_ok_admin(request.user):
        return HttpResponseForbidden(render(request, '403.html').content)

    # NB: delete() runs in current thread. recall pre_delete() signal deletes corresponding cloud file (the uploaded
    # file)
    Job.objects.all().delete()
    messages.success(request, "Deleted all Jobs.")
    return redirect('zadmin')  # hard-coded. see note below re: redirect to same page


#
# ---- visualization-related view functions ----
#

def project_visualizations(request, project_pk):
    """
    View function to render various visualizations for a particular project.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    return render(request, 'message.html',
                  context={'title': f"Project visualizations for '{project.name}'",
                           'message': "Zoltar visualization is under construction."})


def project_explorer(request, project_pk):
    """
    View function to render various exploration tabs for a particular project.

    GET query parameters:
    - `tab`: controls which tab is shown. choices:
        <missing> (defaults to 'latest_units'), 'latest_units', 'latest_targets'
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    tab = request.GET.get('tab', 'latest_units')
    return render(
        request,
        'project_explorer.html',
        context={'project': project,

                 # model, newest_forecast_tz_date, newest_forecast_id, num_present_unit_names, present_unit_names,
                 # missing_unit_names:
                 'unit_rows': unit_rows_for_project(project) if tab == 'latest_units' else [],

                 # model, newest_forecast_tz_date, newest_forecast_id, target_group_name, target_group_count:
                 'target_rows': target_rows_for_project(project) if tab == 'latest_targets' else []})


def project_forecasts(request, project_pk):
    """
    View function to render a list of all forecasts in a particular project, along with a boolean heatmap showing which
    Forecasts are present for which TimeZeros, based on https://vega.github.io/vega-lite/ .

    GET query parameters:
    - `colorby`: controls which data field is used to color the vega-lite heatmap.
                 choices: <missing> (defaults to 'units'), 'rows', 'units', 'targets'
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    # create heatmap data
    encoding_color_field = {None: '# targets',  # default
                            'rows': '# rows',
                            'units': '# units',
                            'targets': '# targets'}[request.GET.get('colorby')]
    forecast_id_to_counts = forecast_metadata_counts_for_project(project)
    vega_lite_spec = _vega_lite_spec_for_project(project, forecast_id_to_counts, encoding_color_field)

    # create forecasts table data
    forecast_rows = []  # filled next
    rows_qs = Forecast.objects.filter(forecast_model__project=project, forecast_model__is_oracle=False) \
        .values_list('id', 'issue_date', 'created_at', 'forecast_model_id', 'forecast_model__abbreviation',
                     'time_zero__id', 'time_zero__timezero_date')  # datatable does order by
    for f_id, f_issue_date, f_created_at, fm_id, fm_abbrev, tz_id, tz_timezero_date in rows_qs:
        counts = forecast_id_to_counts[f_id]  # [None, None, None] if forecast_id is None (via defauldict)
        num_rows = sum(counts[0]) if counts[0] is not None else 0
        forecast_rows.append((reverse('forecast-detail', args=[f_id]), tz_timezero_date, f_issue_date, f_created_at,
                              reverse('model-detail', args=[fm_id]), fm_abbrev, num_rows))

    return render(request, 'project_forecasts.html',
                  context={'project': project,
                           'forecast_rows': forecast_rows,
                           'vega_lite_spec': json.dumps(vega_lite_spec, indent=4)})


def _vega_lite_spec_for_project(project, forecast_id_to_counts, encoding_color_field):
    """
    A `project_forecasts()` helper that returns a Vega-Lite spec dict for a heatmap of all forecasts in project.
    """
    fm_tz_ids_to_f_id = latest_forecast_ids_for_project(project, False)  # ones with latest issue_date
    tz_id_dates = project.timezeros.all().order_by('timezero_date').values_list('id', 'timezero_date')
    values = []
    for fm_id, fm_abbrev in project.models.all().order_by('abbreviation').values_list('id', 'abbreviation'):
        for tz_id, tz_tzdate in tz_id_dates:
            forecast_id = fm_tz_ids_to_f_id.get((fm_id, tz_id), None)
            counts = forecast_id_to_counts[forecast_id]  # [None, None, None] if forecast_id is None (via defauldict)
            if forecast_id is not None:
                # 'T00:00:00' is per [Tooltip dates are off by one](https://github.com/vega/vega-lite/issues/6883):
                values.append({'model': fm_abbrev,
                               'timezero': tz_tzdate.strftime(YYYY_MM_DD_DATE_FORMAT) + 'T00:00:00',
                               'forecast_url': reverse('forecast-detail', args=[str(forecast_id)]),  # relative URL
                               '# rows': sum(counts[0]) if counts[0] is not None else 0,
                               '# units': counts[1],
                               '# targets': counts[2]})

    vega_lite_spec = {
        '$schema': 'https://vega.github.io/schema/vega-lite/v4.json',
        'data': {'values': values},
        # 'actions': {'export': False, 'source': False, 'compiled': False, 'editor': False},  # nope
        # 'actions': None,  # nope
        # 'actions': False,  # nope
        'mark': {'type': 'rect'},
        'width': 'container',
        'config': {
            'view': {'step': 10},
            'axis': {'grid': False},
            'legend': {'titleOrient': 'right'},
        },
        'encoding': {
            'x': {
                'field': 'timezero',
                'timeUnit': 'yearmonthdate',
                'type': 'temporal',
                'title': None,
                'axis': {'orient': 'top', 'format': '%Y-%m-%d'},
            },
            'y': {
                'field': 'model',
                'type': 'nominal',
                'title': None,
            },
            'href': {'field': 'forecast_url'},
            'tooltip': [{'field': 'model'},
                        {'field': 'timezero', 'type': 'temporal', 'format': '%Y-%m-%d'},
                        {'field': 'forecast_url'},
                        {'field': '# rows'},
                        {'field': '# units'},
                        {'field': '# targets'}],
            'color': {
                'field': encoding_color_field,  # '# rows', '# units', or '# targets'
                'type': 'quantitative',
                # note: cannot combine the tooltip encoding with scale due to bug:
                # https://observablehq.com/@ijlyttle/vega-lite-tooltip-formatting-issues . o/w get Error: Invalid
                # specification above in tooltip encoding channel:
                # "scale": {'type': 'threshold', 'domain': [30, 70], 'scheme': 'blues'},  # 'viridis'
            },
        },
    }
    return vega_lite_spec


#
# ---- query functions ----
#

class QueryType(enum.Enum):
    """
    Types of queries that `query_project()` can handle.
    """
    FORECASTS = enum.auto()
    TRUTH = enum.auto()


def query_project(request, project_pk, query_type):
    """
    Shows a form allowing users to edit a JSON query and submit it to query forecasts or truth based on query_type.

    :param request: a Request
    :param project_pk: a Project.pk
    :param query_type: a QueryType enum value indicating the type of query to run
    """
    from forecast_app.api_views import _create_query_job  # avoid circular imports

    project = get_object_or_404(Project, pk=project_pk)
    if not (request.user.is_authenticated and is_user_ok_view_project(request.user, project)):
        return HttpResponseForbidden(render(request, '403.html').content)

    # create or process the form based on the method
    if not isinstance(query_type, QueryType):
        raise RuntimeError(f"query_project(): invalid query_type: {query_type!r} ({type(query_type)})")

    if request.method == 'POST':  # create and bind a form instance from the request
        form = QueryForm(project, query_type, data=request.POST)
        if form.is_valid():  # query is valid, so submit it and redirect to the new Job
            cleaned_query_data = form.cleaned_data['query']
            query_job_type = {QueryType.FORECASTS: JOB_TYPE_QUERY_FORECAST,
                              QueryType.TRUTH: JOB_TYPE_QUERY_TRUTH,
                              }[query_type]
            query_worker_fcn = {QueryType.FORECASTS: _forecasts_query_worker,
                                QueryType.TRUTH: _truth_query_worker,
                                }[query_type]
            query = json.loads(cleaned_query_data)
            job = _create_query_job(project_pk, query, query_job_type, query_worker_fcn, request)
            messages.success(request, f"Query has been submitted.")
            return redirect('job-detail', pk=job.pk)
    else:  # GET (or any other method): create the default form
        # which params to include in which query_type:
        #            forecasts? truth?
        # units:     v          v
        # targets:   v          v
        # timezeros: v          v
        # models:    v          x
        # types:     v          x
        # as_of:     v          x
        first_unit = project.units.first()
        first_target = project.targets.first()
        first_timezero = project.timezeros.first()
        default_query = {'units': [first_unit.name] if first_unit else [],
                         'targets': [first_target.name] if first_target else [],
                         'timezeros': [first_timezero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)]
                         if first_timezero else []}
        if query_type == QueryType.FORECASTS:
            first_model = project.models.first()
            default_query['models'] = [first_model.abbreviation] if first_model else []
        if query_type == QueryType.FORECASTS:
            default_query['types'] = ['point']
            first_forecast = Forecast.objects.filter(forecast_model__project=project).first()
            if first_forecast:
                default_query['as_of'] = first_forecast.issue_date.strftime(YYYY_MM_DD_DATE_FORMAT)
        form = QueryForm(project, query_type, initial={'query': json.dumps(default_query)})

    # render
    query_type_str = {QueryType.FORECASTS: 'forecast', QueryType.TRUTH: 'truth'}[query_type]
    return render(request, 'query_form.html',
                  context={'title': f"Edit {query_type_str} query",
                           'button_name': 'Submit',
                           'form': form,
                           'project': project,
                           'query_type_str': query_type_str})


#
# ---- download_project_config() functions ----
#

def download_project_config(request, project_pk):
    """
    View function that returns a response containing a JSON config file for project_pk.
    Authorization: The project is public, or the logged-in user is a superuser, the Project's owner, or the forecast's
        model's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    project_config = config_dict_from_project(project, request)
    filename = get_valid_filename(f'{project.name}-config.json')
    response = JsonResponse(project_config)  # defaults to 'content_type' 'application/json'
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response


#
# ---- CRUD-related form functions ----
#

def create_project_from_file(request):
    """
    Creates a project from a project config dict valid for create_project_from_json(). Authorization: Any logged-in
    user. Runs in the calling thread and therefore blocks.
    """
    if not is_user_ok_create_project(request.user):
        return HttpResponseForbidden(render(request, '403.html').content)

    is_error = validate_data_file(request)  # 'data_file' in request.FILES, data_file.size <= MAX_UPLOAD_FILE_SIZE
    if is_error:
        return is_error

    data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
    project_dict = json.load(data_file)
    try:
        new_project = create_project_from_json(project_dict, request.user)
        messages.success(request, f"Created project '{new_project.name}'")
        return redirect('project-detail', pk=new_project.pk)
    except Exception as ex:
        return render(request, 'message.html',
                      context={'title': "Error creating project from file.",
                               'message': f"There was an error uploading the file. The error was: "
                                          f"&ldquo;{ex!r}&rdquo;"})


def create_project_from_form(request):
    """
    Shows a form to add a new Project with the owner being request.user. Authorization: Any logged-in user. Runs in the
    calling thread and therefore blocks.

    :param user_pk: the on-behalf-of user. may not be the same as the authenticated user
    """
    if not is_user_ok_create_project(request.user):
        return HttpResponseForbidden(render(request, '403.html').content)

    new_project = Project(owner=request.user)
    if request.method == 'POST':
        project_form = ProjectForm(request.POST, instance=new_project)
        if project_form.is_valid():
            new_project = project_form.save(commit=False)
            new_project.owner = request.user  # force the owner to the current user
            new_project.save()
            project_form.save_m2m()
            messages.success(request, "Created project '{}'.".format(new_project.name))
            return redirect('project-detail', pk=new_project.pk)

    else:  # GET
        project_form = ProjectForm(instance=new_project)

    return render(request, 'show_form.html',
                  context={'title': 'New Project',
                           'button_name': 'Create',
                           'form': project_form})


def edit_project_from_form(request, project_pk):
    """
    Shows a form to edit a Project's basic information. Authorization: The logged-in user must be a superuser or the
    Project's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_edit_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    if request.method == 'POST':
        project_form = ProjectForm(request.POST, instance=project)
        if project_form.is_valid():
            project_form.save()
            messages.success(request, "Edited project '{}.'".format(project.name))
            return redirect('project-detail', pk=project.pk)
    else:  # GET
        project_form = ProjectForm(instance=project)

    return render(request, 'show_form.html',
                  context={'title': 'Edit Project',
                           'button_name': 'Save',
                           'form': project_form})


def edit_project_from_file_preview(request, project_pk):
    """
    Part 1/2 of editing a project via uploading a new configuration file, shows a report and confirmation form to edit a
    Project's configuration via the diffs with an uploaded file. Authorization: The logged-in user must be a superuser
    or the Project's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_edit_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)  # ?

    if request.method != 'POST':
        return HttpResponseBadRequest(f"only the POST method is supported")

    is_error = validate_data_file(request)  # 'data_file' in request.FILES, data_file.size <= MAX_UPLOAD_FILE_SIZE
    if is_error:
        return is_error

    data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
    current_config_dict = config_dict_from_project(project, request)
    new_config_dict = json.load(data_file)
    changes = order_project_config_diff(project_config_diff(current_config_dict, new_config_dict))
    database_changes = database_changes_for_project_config_diff(project, changes)

    # we serialize Changes so they can be passed to the template as a json string that is posted back to the server on
    # Submit for execute_project_config_diff()
    changes_json = json.dumps([change.serialize_to_dict() for change in changes])
    return render(request, 'project_diff_report.html',
                  context={'project': project,
                           'data_file': data_file,
                           'changes': changes,
                           'changes_json': changes_json,
                           'database_changes': database_changes})


def edit_project_from_file_execute(request, project_pk):
    """
    Part 2/2 of editing a project via uploading a new configuration file, executes
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_edit_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    if request.method != 'POST':
        return HttpResponseBadRequest(f"only the POST method is supported")

    changes_json = request.POST['changes_json']  # serialized Changes list from the project_diff_report.html form
    deserialized_change_dicts = json.loads(changes_json)
    changes = [Change.deserialize_dict(change_dict) for change_dict in deserialized_change_dicts]
    logger.debug(f"edit_project_from_file_execute(): executing project config diff... changes={changes}")

    try:
        execute_project_config_diff(project, changes)
        logger.debug(f"edit_project_from_file_execute(): done")
        messages.success(request, f"Successfully applied {len(changes)} change(s) to project '{project.name}'.")
        return redirect('project-detail', pk=project_pk)
    except Exception as ex:
        return render(request, 'message.html',
                      context={'title': "Got an error trying to execute changes.",
                               'message': f"The error was: &ldquo;<span class=\"bg-danger\">{ex!r}</span>&rdquo;"})


def delete_project(request, project_pk):
    """
    Does the actual deletion of a Project. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be a superuser or the Project's owner.
    """
    user = request.user
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_edit_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    # imported here so that tests can patch via mock:
    from utils.project import delete_project_iteratively

    project_name = project.name
    delete_project_iteratively(project)  # more memory-efficient. o/w fails on Heroku for large projects
    messages.success(request, "Deleted project '{}'.".format(project_name))
    return redirect('projects')


def edit_user(request, user_pk):
    """
    Shows a form to edit a User's basic information. Authorization: The logged-in user must be a superuser or the
    passed user_pk.
    """
    detail_user = get_object_or_404(User, pk=user_pk)  # user page being edited
    if not is_user_ok_edit_user(request.user, detail_user):
        return HttpResponseForbidden(render(request, '403.html').content)

    if request.method == 'POST':
        user_model_form = UserModelForm(request.POST, instance=detail_user)
        if user_model_form.is_valid():
            user_model_form.save()

            messages.success(request, "Edited user '{}'".format(detail_user))
            return redirect('user-detail', pk=detail_user.pk)

    else:  # GET
        user_model_form = UserModelForm(instance=detail_user)

    return render(request, 'show_form.html',
                  context={'title': 'Edit User',
                           'button_name': 'Save',
                           'form': user_model_form})


def change_password(request):
    """
    Shows a form allowing the user to set her password.
    """
    if not request.user.is_authenticated:  # any authenticated user can edit her password
        return HttpResponseForbidden(render(request, '403.html').content)

    if request.method == 'POST':
        password_form = UserPasswordChangeForm(request.user, request.POST)
        if password_form.is_valid():
            user = password_form.save()
            update_session_auth_hash(request, user)
            messages.success(request, "Your password was successfully updated!")
            return redirect('user-detail', pk=request.user.pk)
    else:  # GET
        password_form = UserPasswordChangeForm(request.user)

    return render(request, 'show_form.html',
                  context={'title': 'Change password',
                           'button_name': 'Change',
                           'form': password_form})


def create_model(request, project_pk):
    """
    Shows a form to add a new ForecastModel for the passed User. Authorization: The logged-in user must be a superuser,
    or the Project's owner, or one if its model_owners.
    """
    user = request.user
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_create_model(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    if request.method == 'POST':
        forecast_model_form = ForecastModelForm(request.POST)
        if forecast_model_form.is_valid():
            new_model = forecast_model_form.save(commit=False)
            new_model.owner = user  # force the owner to the current user
            new_model.project = project
            new_model.save()
            messages.success(request, "Created model '{}'".format(new_model))
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
    if not is_user_ok_edit_model(request.user, forecast_model):
        return HttpResponseForbidden(render(request, '403.html').content)

    if request.method == 'POST':
        forecast_model_form = ForecastModelForm(request.POST, instance=forecast_model)
        if forecast_model_form.is_valid():
            forecast_model_form.save()
            messages.success(request, "Edited model '{}'".format(forecast_model.name))
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
    if not is_user_ok_edit_model(request.user, forecast_model):
        return HttpResponseForbidden(render(request, '403.html').content)

    forecast_model_name = forecast_model.name
    forecast_model.delete()
    messages.success(request, "Deleted model '{}'.".format(forecast_model_name))
    return redirect('user-detail', pk=user.pk)


#
# ---- List views ----
#

class UserListView(UserPassesTestMixin, ListView):
    model = User

    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        # replaces: AccessMixin.handle_no_permission() raises PermissionDenied
        return HttpResponseForbidden(render(self.request, '403.html').content)

    def test_func(self):  # return True if the current user can access the view
        return is_user_ok_admin(self.request.user)

    def get_context_data(self, **kwargs):
        # collect user info
        user_projs_models = []  # 3-tuples: User, num_projs, num_models
        for user in self.get_queryset().all():  # slow naive approach
            num_projs = len(projects_and_roles_for_user(user))
            num_models = len(forecast_models_owned_by_user(user))
            user_projs_models.append((user, num_projs, num_models))

        context = super().get_context_data(**kwargs)
        context['user_projs_models'] = user_projs_models
        return context


#
# ---- Detail views ----
#

class ProjectDetailView(UserPassesTestMixin, DetailView):
    """
    Authorization: private projects can only be accessed by the project's owner or any of its model_owners
    """
    model = Project

    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        # replaces: AccessMixin.handle_no_permission() raises PermissionDenied
        return HttpResponseForbidden(render(self.request, '403.html').content)

    def test_func(self):  # return True if the current user can access the view
        project = self.get_object()
        return is_user_ok_view_project(self.request.user, project)

    def get_context_data(self, **kwargs):
        project = self.get_object()

        # set target_groups: change from dict to 2-tuples
        target_groups = group_targets(project.targets.all())  # group_name -> group_targets
        target_groups = sorted([(group_name, target_list) for group_name, target_list in target_groups.items()],
                               key=lambda _: _[0])  # [(group_name, group_targets), ...]

        context = super().get_context_data(**kwargs)
        context['models_rows'] = models_summary_table_rows_for_project(project)
        context['is_user_ok_edit_project'] = is_user_ok_edit_project(self.request.user, project)
        context['is_user_ok_create_model'] = is_user_ok_create_model(self.request.user, project)
        context['timezeros_num_forecasts'] = self.timezeros_num_forecasts(project)
        context['units'] = project.units.all()  # datatable does order by
        context['target_groups'] = target_groups
        context['num_targets'] = project.targets.count()
        context['num_truth_rows'] = get_num_truth_rows(project)
        context['is_truth_data_loaded'] = is_truth_data_loaded(project)
        context['first_truth_forecast'] = first_truth_data_forecast(project)
        context['project_summary_info'] = project_summary_info(project)
        return context

    @staticmethod
    def timezeros_num_forecasts(project):
        """
        :return: a list of 2-tuples that relates project's TimeZeros to # Forecasts: (time_zero, num_forecasts)
        """
        # annotate() is a GROUP BY. Count() arg doesn't matter. datatable does order by
        rows = Forecast.objects.filter(forecast_model__project=project, forecast_model__is_oracle=False) \
            .values('time_zero__id') \
            .annotate(tz_count=Count('id'))

        # initialization is a work-around for missing LEFT JOIN items:
        timezero_id_to_obj = {timezero.pk: timezero for timezero in project.timezeros.all()}
        tz_to_num_forecasts = {time_zero: 0 for time_zero in project.timezeros.all()}
        for row in rows:
            time_zero = timezero_id_to_obj[row['time_zero__id']]
            tz_to_num_forecasts[time_zero] = row['tz_count']
        return [(k, tz_to_num_forecasts[k])
                for k in sorted(tz_to_num_forecasts.keys(), key=lambda timezero: timezero.timezero_date)]


def forecast_models_owned_by_user(user):
    """
    :param user: a User
    :return: searches all ForecastModels and returns those where the owner is user
    """
    return ForecastModel.objects.filter(owner=user, is_oracle=False)


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


class UserDetailView(UserPassesTestMixin, DetailView):
    model = User

    # rename from the default 'user', which shadows the context var of that name that's always passed to templates:
    context_object_name = 'detail_user'

    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        # replaces: AccessMixin.handle_no_permission() raises PermissionDenied
        return HttpResponseForbidden(render(self.request, '403.html').content)

    def test_func(self):  # return True if the current user can access the view
        detail_user = self.get_object()
        return is_user_ok_edit_user(self.request.user, detail_user)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # pass a list of Projects. we have two cases: 1) projects owned by this user, and 2) projects where this user is
        # in model_owners. thus this list is of 2-tuples: (Project, user_role), where user_role is "Project Owner" or
        # "Model Owner"
        detail_user = self.get_object()
        projects_and_roles = projects_and_roles_for_user(detail_user)
        owned_models = forecast_models_owned_by_user(detail_user)
        context['is_user_ok_edit_user'] = is_user_ok_edit_user(self.request.user, detail_user)
        context['projects_and_roles'] = sorted(projects_and_roles,
                                               key=lambda project_and_role: project_and_role[0].name)
        context['owned_models'] = owned_models
        context['jobs'] = detail_user.jobs.all()  # datatable does order by
        return context


class ForecastModelDetailView(UserPassesTestMixin, DetailView):
    model = ForecastModel

    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        # replaces: AccessMixin.handle_no_permission() raises PermissionDenied
        return HttpResponseForbidden(render(self.request, '403.html').content)

    def test_func(self):  # return True if the current user can access the view
        forecast_model = self.get_object()
        return is_user_ok_view_project(self.request.user, forecast_model.project)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        forecast_model = self.get_object()

        # set timezero_forecast_pairs, a list of (timezero, forecast) 2-tuples for every TimeZero in the model's
        # project, with forecast=None for any that are missing. first we get all of the model's projects TimeZeros,
        # then get all of the model's forecasts, then do an in-memory "join" to get the missing ones
        tz_to_forecasts = defaultdict(list)  # TimeZero -> list of its Forecasts ("versions")
        for forecast in forecast_model.forecasts.select_related('time_zero').order_by('issue_date'):
            # order_by('issue_date') allows us to deterministically name versions by index
            tz_to_forecasts[forecast.time_zero].append(forecast)

        timezero_forecast_pairs = []  # TimeZero, Forecast, version_str
        for timezero in forecast_model.project.timezeros.all():  # datatable does order by
            if timezero in tz_to_forecasts:
                forecasts = tz_to_forecasts[timezero]
                for idx, forecast in enumerate(forecasts):
                    version_str = "" if len(forecasts) == 1 else f"{idx + 1} of {len(forecasts)}"
                    timezero_forecast_pairs.append((timezero, forecast, version_str))
            else:
                timezero_forecast_pairs.append((timezero, None, ""))

        context['timezero_forecast_pairs'] = timezero_forecast_pairs
        context['is_user_ok_edit_model'] = is_user_ok_edit_model(self.request.user, forecast_model)
        return context


class ForecastDetailView(UserPassesTestMixin, DetailView):
    model = Forecast

    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        # replaces: AccessMixin.handle_no_permission() raises PermissionDenied
        return HttpResponseForbidden(render(self.request, '403.html').content)

    def test_func(self):  # return True if the current user can access the view
        forecast = self.get_object()
        return is_user_ok_view_project(self.request.user, forecast.forecast_model.project)

    def get_context_data(self, **kwargs):
        forecast = self.get_object()

        # collect computed metadata
        is_metadata_available = is_forecast_metadata_available(forecast)
        pred_type_count_pairs, found_units, found_targets = self.forecast_metadata_cached() \
            if is_metadata_available else self.forecast_metadata_dynamic()

        # set target_groups: change from dict to 2-tuples
        target_groups = group_targets(found_targets)  # group_name -> group_targets
        target_groups = sorted([(group_name, target_list) for group_name, target_list in target_groups.items()],
                               key=lambda _: _[0])  # [(group_name, group_targets), ...]

        # create sorted found_targets by: 1) group_name, then by: 2) step_ahead_increment if is_step_ahead. o/w by name
        found_targets = []
        for group_name, targets in target_groups:  # already sorted by group_name
            found_targets.extend(sorted(targets, key=lambda
                target: target.step_ahead_increment if target.is_step_ahead else target.name))

        # set search_unit, search_target, and data_rows_* if a query requested
        search_unit, search_target, data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, \
        data_rows_sample = self.search_forecast()

        # determine my version_str - must examine all forecasts for my timezero, similar to
        # `views.ForecastModelDetailView.get_context_data()`.
        # order_by('issue_date') allows us to deterministically name versions by index
        forecast_version_ids = Forecast.objects \
            .filter(forecast_model=forecast.forecast_model, time_zero=forecast.time_zero) \
            .order_by('issue_date') \
            .values_list('id', flat=True)
        forecast_version_ids = list(forecast_version_ids)
        version_str = "" if len(forecast_version_ids) == 1 else \
            f"{forecast_version_ids.index(forecast.id) + 1} of {len(forecast_version_ids)}"

        # done
        context = super().get_context_data(**kwargs)
        context['is_metadata_available'] = is_metadata_available
        context['version_str'] = version_str
        context['pred_type_count_pairs'] = sorted(pred_type_count_pairs)
        context['found_units'] = sorted(found_units, key=lambda _: _.name)
        context['found_targets'] = found_targets
        context['target_groups'] = target_groups
        context['search_unit'] = search_unit
        context['search_target'] = search_target
        context['data_rows_bin'] = data_rows_bin
        context['data_rows_named'] = data_rows_named
        context['data_rows_point'] = data_rows_point
        context['data_rows_quantile'] = data_rows_quantile
        context['data_rows_sample'] = data_rows_sample
        return context

    def forecast_metadata_cached(self):
        """
        ForecastDetailView helper that returns cached forecast metadata, i.e., DOES use `forecast_metadata()`. Assumes
        `is_forecast_metadata_available(forecast)` is True, i.e., does not check whether metadata is present.

        :return: 3-tuple: (pred_type_count_pairs, found_units, found_targets for forecast), where pred_type_count_pairs
            is a 2-tuple: (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS, count)
        """
        forecast = self.get_object()
        forecast_meta_prediction, forecast_meta_unit_qs, forecast_meta_target_qs = forecast_metadata(forecast)
        pred_type_count_pairs = [
            (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution], forecast_meta_prediction.bin_count),
            (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[NamedDistribution], forecast_meta_prediction.named_count),
            (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction], forecast_meta_prediction.point_count),
            (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleDistribution], forecast_meta_prediction.sample_count),
            (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[QuantileDistribution], forecast_meta_prediction.quantile_count)]
        found_units = [forecast_meta_unit.unit for forecast_meta_unit
                       in forecast_meta_unit_qs.select_related('unit')]
        found_targets = [forecast_meta_target.target for forecast_meta_target
                         in forecast_meta_target_qs.select_related('target')]
        return pred_type_count_pairs, found_units, found_targets

    def forecast_metadata_dynamic(self):
        """
        ForecastDetailView helper that dynamically calculates forecast metadata, i.e., does NOT use
        `forecast_metadata()`.

        :return: 3-tuple: (pred_type_count_pairs, found_units, found_targets for forecast), where pred_type_count_pairs
            is a 2-tuple: (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS, count)
        """
        forecast = self.get_object()

        # set pred_type_count_pairs
        pred_type_count_pairs = [
            (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[concrete_prediction_class],
             concrete_prediction_class.objects.filter(forecast=forecast).count())
            for concrete_prediction_class in Prediction.concrete_subclasses()]

        # set found_units
        unit_id_to_obj = {unit.id: unit for unit in forecast.forecast_model.project.units.all()}
        found_unit_ids = set()
        for concrete_prediction_class in Prediction.concrete_subclasses():
            pred_class_units = concrete_prediction_class.objects \
                .filter(forecast=forecast) \
                .values_list('unit', flat=True) \
                .distinct()
            found_unit_ids.update(pred_class_units)
        found_units = [unit_id_to_obj[unit_id] for unit_id in found_unit_ids]

        # set found_targets
        target_id_to_object = {target.id: target for target in forecast.forecast_model.project.targets.all()}
        found_target_ids = set()
        for concrete_prediction_class in Prediction.concrete_subclasses():
            pred_class_targets = concrete_prediction_class.objects \
                .filter(forecast=forecast) \
                .values_list('target', flat=True) \
                .distinct()
            found_target_ids.update(pred_class_targets)
        found_targets = [target_id_to_object[target_id] for target_id in found_target_ids]

        # done
        return pred_type_count_pairs, found_units, found_targets

    def search_forecast(self):
        """
        `ForecastDetailView.get_context_data` helper, returns a 7-tuple based on the two search args in self.request
        ('unit' - a Unit.id and 'target' - a Target.id):
            (search_unit, search_target, data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample)
        If the passed args are valid, the first two items are None and the remainder are [].
        """
        search_unit_and_target = None  # 2-tuple if query was valid: (search_unit, search_target). set next
        search_unit_id = self.request.GET['unit'] if 'unit' in self.request.GET else None
        search_target_id = self.request.GET['target'] if 'target' in self.request.GET else None
        if ((search_unit_id is not None) and (search_target_id is None)) or \
                ((search_unit_id is None) and (search_target_id is not None)):
            messages.error(self.request, f"Both 'unit' and 'target' IDs must be passed to do a search, but only one "
                                         f"was. unit={search_unit_id!r}, target={search_target_id!r}")

        if (search_unit_id is not None) and (search_target_id is not None):
            # both were passed, so validate
            try:
                search_unit_id = int(search_unit_id)
                search_target_id = int(search_target_id)
                found_search_unit = Unit.objects.filter(id=search_unit_id).first()  # None o/w
                found_search_target = Target.objects.filter(id=search_target_id).first()  # ""
                if (not found_search_unit) or (not found_search_target):
                    messages.error(self.request, f"Both 'unit' and 'target' IDs were passed to the search, but one or "
                                                 f"both did not identify an actual object. unit={found_search_unit}, "
                                                 f"target={found_search_target}")
                else:
                    search_unit_and_target = (found_search_unit, found_search_target)  # yay!
            except ValueError:
                messages.error(self.request, f"Both 'unit' and 'target' IDs were passed to the search, but one or "
                                             f"both were not ints. unit={search_unit_id}, "
                                             f"target={search_target_id}")

        # do the actual query if valid search params
        data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample = [], [], [], [], []
        if search_unit_and_target:
            forecast = self.get_object()
            data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample = \
                data_rows_from_forecast(forecast, search_unit_and_target[0], search_unit_and_target[1])

        return search_unit_and_target[0] if search_unit_and_target else None, \
               search_unit_and_target[1] if search_unit_and_target else None, \
               data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample


class JobDetailView(UserPassesTestMixin, DetailView):
    model = Job

    context_object_name = 'job'

    def handle_no_permission(self):  # called by UserPassesTestMixin.dispatch()
        # replaces: AccessMixin.handle_no_permission() raises PermissionDenied
        return HttpResponseForbidden(render(self.request, '403.html').content)

    def test_func(self):  # return True if the current user can access the view
        job = self.get_object()
        return self.request.user.is_superuser or (job.user == self.request.user)

    def get_context_data(self, **kwargs):
        from utils.cloud_file import is_file_exists

        job = self.get_object()
        context = super().get_context_data(**kwargs)
        context['is_file_exists'] = is_file_exists(job)[0]  # is_exists, size
        return context


#
# ---- download-related functions ----
#

def download_forecast(request, forecast_pk):
    """
    Returns a response containing a JSON file for a Forecast's data.
    Authorization: The project is public, or the logged-in user is a superuser, the Project's owner, or the forecast's
        model's owner.

    :return: response for the JSON format of the passed Forecast's data
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    project = forecast.forecast_model.project
    if not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    from forecast_app.api_views import json_response_for_forecast  # avoid circular imports:

    return json_response_for_forecast(forecast, request)


def download_job_data_file(request, pk):
    """
    Returns a CSV file containing the data (if any) corresponding to the passed Job's pk.
    """
    from forecast_app.api_views import _download_job_data_request  # avoid circular imports
    from utils.cloud_file import is_file_exists

    job = get_object_or_404(Job, pk=pk)
    if not (request.user.is_superuser or (job.user == request.user)):
        return HttpResponseForbidden(render(request, '403.html').content)

    if not is_file_exists(job)[0]:  # is_exists, size
        return render(request, 'message.html',
                      context={'title': f"No data for job {job.pk}",
                               'message': f"The job {job.pk} has no associated data."})

    return _download_job_data_request(job)


#
# ---- Truth-related views ----
#

def truth_detail(request, project_pk):
    """
    View function to render a preview of a Project's truth data.
    Authorization: The logged-in user must be a superuser, or the Project's owner, or the forecast's model's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_view_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    return render(
        request,
        'truth_data_detail.html',
        context={'project': project,
                 'num_truth_rows': get_num_truth_rows(project),
                 'truth_data_preview': get_truth_data_preview(project),
                 'first_truth_forecast': first_truth_data_forecast(project),
                 'is_truth_data_loaded': is_truth_data_loaded(project),
                 'oracle_model': oracle_model_for_project(project),
                 'is_user_ok_edit_project': is_user_ok_edit_project(request.user, project)})


def delete_truth(request, project_pk):
    """
    Does the actual deletion of truth data. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be a superuser or the Project's owner.

    :return: redirect to the forecast's forecast_model detail page
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_edit_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    delete_truth_data(project)
    return redirect('project-detail', pk=project_pk)


def upload_truth(request, project_pk):
    """
    Uploads the passed data into a the project's truth, replacing any existing truth data.
    Authorization: The logged-in user must be a superuser or the Project's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_edit_project(request.user, project):
        return HttpResponseForbidden(render(request, '403.html').content)

    if is_truth_data_loaded(project):
        return render(request, 'message.html',
                      context={'title': "Truth data already loaded.",
                               'message': "The project already has truth data. Please delete it and then upload again."})

    is_error = validate_data_file(request)  # 'data_file' in request.FILES, data_file.size <= MAX_UPLOAD_FILE_SIZE
    if is_error:
        return is_error

    # upload to cloud and enqueue a job to process a new Job
    data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
    is_error, job = _upload_file(request.user, data_file, _upload_truth_worker,
                                 type=JOB_TYPE_UPLOAD_TRUTH,
                                 project_pk=project_pk)
    if is_error:
        return render(request, 'message.html',
                      context={'title': "Error uploading file.",
                               'message': f"There was an error uploading the file. The error was: "
                                          f"&ldquo;{is_error}&rdquo;"})

    messages.success(request, "Queued the truth file '{}' for uploading.".format(data_file.name))
    return redirect('job-detail', pk=job.pk)


def _upload_truth_worker(job_pk):
    """
    An _upload_file() enqueue() function that loads a truth file. Called by upload_truth().

    - Expected Job.input_json key(s): 'project_pk', 'filename'
    - Saves Job.output_json key(s): None

    :param job_pk: the Job's pk
    """
    # imported here so that tests can patch via mock:
    from forecast_app.models.job import job_cloud_file
    from utils.project_truth import load_truth_data

    try:
        with job_cloud_file(job_pk) as (job, cloud_file_fp):
            if 'project_pk' not in job.input_json:
                job.status = Job.FAILED
                job.failure_message = f"_upload_truth_worker(): error: missing 'project_pk'"
                job.save()
                logger.error(job.failure_message + f". job={job}")
                return
            elif 'filename' not in job.input_json:
                job.status = Job.FAILED
                job.failure_message = f"_upload_truth_worker(): error: missing 'filename'"
                job.save()
                logger.error(job.failure_message + f". job={job}")
                return

            project_pk = job.input_json['project_pk']
            project = Project.objects.filter(pk=project_pk).first()  # None if doesn't exist
            if not project:
                job.status = Job.FAILED
                job.failure_message = f"_upload_truth_worker(): no Project found for project_pk={project_pk}"
                job.save()
                logger.error(job.failure_message + f". job={job}")
                return

            filename = job.input_json['filename']
            load_truth_data(project, cloud_file_fp, file_name=filename)
            job.status = Job.SUCCESS
            job.save()
    except JobTimeoutException as jte:
        job.status = Job.TIMEOUT
        job.save()
        logger.error(f"_upload_truth_worker(): error: {jte!r}. job={job}")
        raise jte
    except Exception as ex:
        job.status = Job.FAILED
        job.failure_message = f"_upload_truth_worker(): error: {ex!r}"
        job.save()
        logger.error(job.failure_message + f". job={job}")


#
# ---- Forecast upload/delete views ----
#

def upload_forecast(request, forecast_model_pk, timezero_pk):
    """
    Uploads the passed data into a new Forecast. Authorization: The logged-in user must be a superuser, or the Project's
    owner, or the model's owner. The data file must be in the format supported by load_predictions_from_json_io_dict().

    :return: redirect to the new forecast's detail page
    """
    forecast_model = get_object_or_404(ForecastModel, pk=forecast_model_pk)
    time_zero = get_object_or_404(TimeZero, pk=timezero_pk)
    if not is_user_ok_upload_forecast(request, forecast_model):
        return HttpResponseForbidden(render(request, '403.html').content)

    is_error = validate_data_file(request)  # 'data_file' in request.FILES, data_file.size <= MAX_UPLOAD_FILE_SIZE
    if is_error:
        return is_error

    data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)

    # see note in `api_views.ForecastModelForecastList.post()` re: "check for existing forecast" ...
    # "by creating the new Forecast"
    try:
        new_forecast = Forecast.objects.create(forecast_model=forecast_model, time_zero=time_zero, notes='')
    except IntegrityError as ie:
        return render(request, 'message.html',
                      context={'title': "Error uploading file.",
                               'message': f"new forecast was not a unique version. "
                                          f"time_zero={time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)}, "
                                          f"issue_date=~{django.utils.timezone.now().date()}, "
                                          f"file_name='{data_file.name}', "
                                          f"forecast_model={forecast_model}. error={ie}"})

    # upload to cloud and enqueue a job to process a new Job
    is_error, job = _upload_file(request.user, data_file, _upload_forecast_worker, type=JOB_TYPE_UPLOAD_FORECAST,
                                 forecast_pk=new_forecast.pk)
    if is_error:
        return render(request, 'message.html',
                      context={'title': "Error uploading file.",
                               'message': f"There was an error uploading the file. The error was: "
                                          f"&ldquo;{is_error}&rdquo;"})

    messages.success(request, "Queued the forecast file '{}' for uploading.".format(data_file.name))
    return redirect('job-detail', pk=job.pk)


def _upload_forecast_worker(job_pk):
    """
    An _upload_file() enqueue() function that loads a forecast data file. Called by upload_forecast(). It is passed an
    empty Forecast's id to load into. Deletes that forecast if there were errors loading the data.

    - Expected Job.input_json key(s): 'forecast_pk', 'filename' - passed to _upload_file()
    - Saves Job.output_json key(s): 'forecast_pk' (passed through from input_json for API caller convenience)

    :param job_pk: the Job's pk
    """
    # imported here so that tests can patch via mock:
    from forecast_app.models.job import job_cloud_file
    from utils.forecast import load_predictions_from_json_io_dict, cache_forecast_metadata

    with job_cloud_file(job_pk) as (job, cloud_file_fp):
        if 'forecast_pk' not in job.input_json:
            job.status = Job.FAILED
            job.failure_message = f"_upload_forecast_worker(): error: missing 'forecast_pk'"
            job.save()
            logger.error(job.failure_message + f". job={job}")
            return
        elif 'filename' not in job.input_json:
            job.status = Job.FAILED
            job.failure_message = f"_upload_forecast_worker(): error: missing 'filename'"
            job.save()
            logger.error(job.failure_message + f". job={job}")
            return

        forecast_pk = job.input_json['forecast_pk']
        forecast = Forecast.objects.filter(pk=forecast_pk).first()  # None if doesn't exist
        if not forecast:
            logger.error(f"_upload_forecast_worker(): error: no Forecast found for forecast_pk={forecast_pk}. "
                         f"job={job}")
            return

        # set source here rather than in caller b/c we now have filename via `_upload_file()`
        forecast.source = job.input_json['filename']
        forecast.save()
        try:
            with transaction.atomic():
                logger.debug(f"_upload_forecast_worker(): 1/4 loading json_io_dict. forecast={forecast}. job={job}")
                notes = job.input_json.get('notes', '')
                json_io_dict = json.load(cloud_file_fp)

                logger.debug(f"_upload_forecast_worker(): 2/4 loading predictions. job={job}")
                load_predictions_from_json_io_dict(forecast, json_io_dict, False)  # transaction.atomic

                logger.debug(f"_upload_forecast_worker(): 3/4 caching metadata. job={job}")
                cache_forecast_metadata(forecast)  # transaction.atomic
                job.output_json = {'forecast_pk': forecast_pk}
                job.status = Job.SUCCESS
                job.save()
                logger.debug(f"_upload_forecast_worker(): 4/4 done. job={job}")
        except JobTimeoutException as jte:
            forecast.delete()
            job.status = Job.TIMEOUT
            job.save()
            logger.error(f"_upload_forecast_worker(): error: {jte!r}. job={job}")
            raise jte
        except Exception as ex:
            forecast.delete()
            job.status = Job.FAILED
            job.failure_message = f"_upload_forecast_worker(): error: {ex!r}"
            job.save()
            logger.error(job.failure_message + f". job={job}")


def delete_forecast(request, forecast_pk):
    """
    Enqueues the deletion of a Forecast, returning a Job for it. Assumes that confirmation has already been given by the
    caller.
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    if not is_user_ok_delete_forecast(request.user, forecast):
        return HttpResponseForbidden(render(request, '403.html').content)

    job = enqueue_delete_forecast(request.user, forecast)
    messages.success(request, f"Queued deleting the forecast: {forecast}.")
    return redirect('job-detail', pk=job.pk)


def enqueue_delete_forecast(user, forecast):
    job = Job.objects.create(user=user)  # status = PENDING
    job.input_json = {'type': JOB_TYPE_DELETE_FORECAST, 'forecast_pk': forecast.pk}
    job.save()

    queue = django_rq.get_queue(DELETE_FORECAST_QUEUE_NAME)
    queue.enqueue(_delete_forecast_worker, job.pk)
    job.status = Job.QUEUED
    job.save()

    return job


def _delete_forecast_worker(job_pk):
    """
    enqueue() helper function
    """
    job = get_object_or_404(Job, pk=job_pk)
    if 'forecast_pk' not in job.input_json:
        job.status = Job.FAILED
        job.failure_message = f"_delete_forecast_worker: did not find 'forecast_pk'"
        job.save()
        return

    forecast_pk = job.input_json['forecast_pk']
    forecast = Forecast.objects.filter(id=forecast_pk).first()
    if not forecast:
        job.status = Job.FAILED
        job.failure_message = f"_delete_forecast_worker: no Forecast with forecast_pk={forecast_pk}"
        job.save()
        return

    try:
        forecast.delete()
        job.status = Job.SUCCESS
        job.save()
    except JobTimeoutException as jte:
        job.status = Job.TIMEOUT
        job.save()
        logger.error(f"_delete_forecast_worker(): error: {jte!r}. job={job}")
    except Exception as ex:
        job.status = Job.FAILED
        job.failure_message = f"_delete_forecast_worker(): error: {ex!r}"
        job.save()
        logger.error(job.failure_message + f". job={job}")


#
# ---- Upload-related functions ----
#

# The following code supports the user's uploading arbitrary files to Zoltar for processing - forecast data files, for
# example. We implement this using a general view function named __upload_file(), which accepts two functions that
# are used to control how the uploaded file is processed. Doing it this way keeps that function general. Currently we
# use simple (but limited) pass-through uploading, rather than more efficient direct uploading, but this is a todo.
# See for more: https://devcenter.heroku.com/articles/s3#file-uploads .

def _upload_file(user, data_file, process_job_fcn, **kwargs):
    """
    Accepts a file uploaded to this app by the user. Creates a Job to track the job, saves data_file in cloud
    storage, then enqueues process_job_fcn to process the file by an RQ worker.

    :param user: the User from request.User
    :param data_file: the data file to use as found in request.FILES . it is an UploadedFile (e.g.,
        InMemoryUploadedFile or TemporaryUploadedFile)
    :param process_job_fcn: a function of one arg (job_pk) that is passed to
        django_rq.enqueue(). NB: It MUST use the job_cloud_file context to have access to the file that was
        uploaded to cloud, e.g.,
            with job_cloud_file() as cloud_file_fp: ...
        NB: If it needs to save job.output_json, make sure to call save(), e.g.,
            job.output_json = {'forecast_pk': new_forecast.pk}
            job.save()
    :param kwargs: saved in the new Job's input_json. it is recommended that 'type' be one of them, as found in JobType
    :return a 2-tuple: (is_error, job) where:
        - is_error: True if there was an error, and False o/w. If true, it is actually an error message to show the user
        - job the new Job instance if not is_error. None o/w
    """
    from utils.cloud_file import delete_file, upload_file

    # create the Job
    logger.debug(f"_upload_file(): Got data_file: name={data_file.name!r}, size={data_file.size}, "
                 f"content_type={data_file.content_type}")
    try:
        job = Job.objects.create(user=user)  # status = PENDING
        kwargs['filename'] = data_file.name
        job.input_json = kwargs
        job.save()
        logger.debug("_upload_file(): 1/3 Created the Job: {}".format(job))
    except Exception as ex:
        logger.debug("_upload_file(): Error creating the Job: {}".format(ex))
        return "Error creating the Job: {}".format(ex), None

    # upload the file to cloud storage
    try:
        upload_file(job, data_file)  # might raise S3 exception
        job.status = Job.CLOUD_FILE_UPLOADED
        job.save()
        logger.debug(f"_upload_file(): 2/3 Uploaded the file to cloud. job={job}")
    except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError) as aws_exc:
        job.status = Job.FAILED
        job.failure_message = f"_upload_file(): error: {aws_exc!r}"
        job.save()
        logger.error(job.failure_message + f". job={job}")
    except Exception as ex:
        job.status = Job.FAILED
        job.failure_message = f"_upload_file(): error: {ex}"
        job.save()
        logger.error(job.failure_message + f". job={job}")
        return "Error uploading file to cloud: {}. job={}".format(ex, job), None

    # enqueue a worker
    try:
        queue = django_rq.get_queue(UPLOAD_FILE_QUEUE_NAME)
        rq_job = queue.enqueue(process_job_fcn, job.pk, job_id=job.rq_job_id())
        job.status = Job.QUEUED
        job.save()
        logger.debug("_upload_file(): 3/3 Enqueued the job: {}. job={}".format(rq_job, job))
    except Exception as ex:
        job.status = Job.FAILED
        job.failure_message = f"_upload_file(): error: {ex}"
        job.save()
        try:
            delete_file(job)  # might raise S3 exception. NB: in current thread
        except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError) as aws_exc:
            message = f"_upload_file(): error: {aws_exc!r}. job={job}"
            logger.error(message)
            return message, None
        logger.debug(f"_upload_file(): error: {ex}. job={job}")
        return f"Error enqueuing the job: {ex}. job={job}", None

    logger.debug("_upload_file(): done")
    return False, job


def validate_data_file(request):
    """
    An upload_*() helper function that checks the file in request.

    :return is_error: True if there was an error, and False o/w. If true, it is actually a render()'d error message to
        return from the calling view function
    """
    if 'data_file' not in request.FILES:  # user submitted without specifying a file to upload
        return render(request, 'message.html',
                      context={'title': "Error uploading file.",
                               'message': "No file selected to upload. Please go back and select one."})

    data_file = request.FILES['data_file']
    if data_file.size > MAX_UPLOAD_FILE_SIZE:
        message = "File was too large to upload. size={}, max={}.".format(data_file.size, MAX_UPLOAD_FILE_SIZE)
        return render(request, 'message.html',
                      context={'title': "Error uploading file.",
                               'message': message})
    return None  # is_error


#
# ---- authorization utilities ----
#

def is_user_ok_admin(user):
    return user.is_superuser


def is_user_ok_edit_user(user, detail_user):
    return user.is_superuser or (detail_user == user)


def is_user_ok_create_project(user):
    return user.is_superuser or (user.is_authenticated and user.is_staff)


def is_user_ok_view_project(user, project):
    return user.is_superuser or project.is_public or (user == project.owner) or (user in project.model_owners.all())


def is_user_ok_edit_project(user, project):
    # applies to delete too
    return user.is_superuser or (user == project.owner)


def is_user_ok_create_model(user, project):
    return user.is_superuser or (user == project.owner) or (user in project.model_owners.all())


def is_user_ok_edit_model(user, forecast_model):
    # applies to delete too
    return user.is_superuser or (user == forecast_model.project.owner) or (user == forecast_model.owner)


def is_user_ok_delete_forecast(user, forecast):
    return user.is_superuser or (user == forecast.forecast_model.project.owner) or (
            user == forecast.forecast_model.owner)


def is_user_ok_upload_forecast(request, forecast_model):
    return request.user.is_superuser or (request.user == forecast_model.project.owner) or \
           (request.user == forecast_model.owner)
