import io
import json
import logging
import time

import django_rq
import redis
from django import db
from django.contrib import messages
from django.contrib.auth import update_session_auth_hash
from django.contrib.auth.mixins import UserPassesTestMixin
from django.contrib.auth.models import User
from django.core.exceptions import PermissionDenied
from django.db import connection, transaction
from django.db.models import Count
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render, get_object_or_404, redirect
from django.utils.text import get_valid_filename
from django.views.generic import DetailView, ListView

from forecast_app.forms import ProjectForm, ForecastModelForm, UserModelForm, UserPasswordChangeForm
from forecast_app.models import Project, ForecastModel, Forecast, TimeZero, ScoreValue, Score, ScoreLastUpdate, \
    Prediction, ModelScoreChange
from forecast_app.models.row_count_cache import enqueue_row_count_updates_all_projs
from forecast_app.models.score_csv_file_cache import enqueue_score_csv_file_cache_all_projs
from forecast_app.models.upload_file_job import UploadFileJob, upload_file_job_cloud_file
from forecast_repo.settings.base import S3_BUCKET_PREFIX, UPLOAD_FILE_QUEUE_NAME
from utils.cloud_file import delete_file, upload_file
from utils.flusight import flusight_location_to_data_dict
from utils.forecast import load_predictions_from_json_io_dict, PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS
from utils.mean_absolute_error import location_to_mean_abs_error_rows_for_project
from utils.project import config_dict_from_project, create_project_from_json, load_truth_data
from utils.project_diff import project_config_diff, database_changes_for_project_config_diff, Change, \
    execute_project_config_diff, order_project_config_diff


logger = logging.getLogger(__name__)


def index(request):
    return render(request, 'index.html')


def about(request):
    return render(request, 'about.html')


def projects(request):
    return render(
        request,
        'projects.html',
        context={'projects': Project.objects.order_by('name'),
                 'is_user_ok_create_project': is_user_ok_create_project(request.user)})


def documentation(request):
    return render(request, 'documentation.html')


#
# ---- admin-related view functions ----
#

def zadmin_upload_file_jobs(request):
    return render(
        request, 'zadmin_upload_file_jobs.html',
        context={'upload_file_jobs': UploadFileJob.objects.all().order_by('-updated_at')})


def zadmin_score_last_updates(request):
    Score.ensure_all_scores_exist()

    # build score_last_update_rows. NB: num_score_values_for_model() took a long time, so we removed it. o/w the page
    # timed out on Heroku. was: score_last_update.score.num_score_values_for_model(score_last_update.forecast_model)
    score_last_update_rows = []  # forecast_model, score, num_score_values, last_update
    for score_last_update in ScoreLastUpdate.objects \
            .order_by('score__name', 'forecast_model__project__name', 'forecast_model__name'):
        score_last_update_rows.append(
            (score_last_update.forecast_model,
             score_last_update.score,
             score_last_update.updated_at,
             score_last_update.forecast_model.score_change.changed_at > score_last_update.updated_at))

    return render(
        request, 'zadmin_score_last_updates.html',
        context={'score_last_update_rows': score_last_update_rows})


def zadmin_model_score_changes(request):
    model_score_changes = ModelScoreChange.objects.all().order_by('changed_at')
    return render(
        request, 'zadmin_model_score_changes.html',
        context={'model_score_changes': model_score_changes})


def zadmin(request):
    if not is_user_ok_admin(request.user):
        raise PermissionDenied

    Score.ensure_all_scores_exist()

    django_db_name = db.utils.settings.DATABASES['default']['NAME']

    projects_sort_pk = [(project, project.models.count()) for project in Project.objects.order_by('pk')]
    return render(
        request, 'zadmin.html',
        context={'django_db_name': django_db_name,
                 'django_conn': connection,
                 's3_bucket_prefix': S3_BUCKET_PREFIX,
                 'projects_sort_pk': projects_sort_pk,
                 'projects_sort_rcc_last_update': Project.objects.order_by('-row_count_cache__updated_at'),
                 'scores_sort_name': Score.objects.all().order_by('name'),
                 'scores_sort_pk': Score.objects.all().order_by('pk')})


def delete_upload_file_jobs(request):
    if not is_user_ok_admin(request.user):
        raise PermissionDenied

    # NB: delete() runs in current thread. recall pre_delete() signal deletes corresponding cloud file (the uploaded
    # file)
    UploadFileJob.objects.all().delete()
    messages.success(request, "Deleted all UploadFileJobs.")
    return redirect('zadmin')  # hard-coded. see note below re: redirect to same page


def clear_row_count_caches(request):
    """
    View function that resets all projects' RowCountCaches. Runs in the calling thread and therefore blocks. However,
    this operation is fast.
    """
    if not is_user_ok_admin(request.user):
        raise PermissionDenied

    for project in Project.objects.all():
        project.row_count_cache.row_count = None
        project.row_count_cache.save()

    messages.success(request, "All row count caches were cleared.")

    # redirect to same page. NB: many ways to do this, with limitations. some that I tried in Firefox include
    # `return HttpResponseRedirect(request.path_info)` -> "The page isn’t redirecting properly",
    # `return redirect('')` -> "Reverse for '' not found.", and others. This did work, but had a caveat
    # ("many users/browsers have the http_referer turned off"):
    # `return redirect(request.META['HTTP_REFERER'])`. in the end I decided to hard-code, knowing the referring page
    return redirect('zadmin')  # hard-coded


def update_row_count_caches(request):
    """
    View function that enqueues updates of all projects' RowCountCaches and then returns. Users are not notified when
    the updates are done, and so must refresh, etc. Note that we choose to enqueue each project's update separately,
    rather than a single enqueue that updates them all in a loop, b/c each one might take a while, and we're trying to
    limit each job's duration.
    """
    if not is_user_ok_admin(request.user):
        raise PermissionDenied

    try:
        enqueue_row_count_updates_all_projs()
        messages.success(request, "Scheduled updating row count caches for all projects.")
    except redis.exceptions.ConnectionError as ce:
        messages.warning(request, "Error updating row count caches: {}.".format(ce))
    return redirect('zadmin')  # hard-coded


def clear_score_csv_file_caches(request):
    """
    View function that resets all projects' ScoreCsvFileCaches. Runs in the calling thread and therefore blocks.
    However, this operation is relatively fast, but does depend on S3 access.
    """
    if not is_user_ok_admin(request.user):
        raise PermissionDenied

    for project in Project.objects.all():
        project.score_csv_file_cache.delete_score_csv_file_cache()

    messages.success(request, "All score csv file caches were cleared.")
    return redirect('zadmin')  # hard-coded


def update_score_csv_file_caches(request):
    """
    View function that enqueues updates of all projects' ScoreCsvFileCaches and then returns. Users are not notified
    when the updates are done, and so must refresh, etc. Note that we choose to enqueue each project's update
    separately, rather than a single enqueue that updates them all in a loop, b/c each one might take a while, and
    we're trying to limit each job's duration.
    """
    if not is_user_ok_admin(request.user):
        raise PermissionDenied

    try:
        enqueue_score_csv_file_cache_all_projs()
        messages.success(request, "Scheduled updating score csv file caches for all projects.")
    except redis.exceptions.ConnectionError as ce:
        messages.warning(request, "Error updating score csv file caches: {}.".format(ce))
    return redirect('zadmin')  # hard-coded


def update_all_scores(request, **kwargs):
    """
    View function that enqueues updates of all scores for all models in all projects, regardless of whether each model
    has changed since the last score update.

    :param kwargs: has a single 'is_only_changed' key that's either True or False. this is passed to
        Score.enqueue_update_scores_for_all_models(), which means this arg controls whether updates are enqueued for
        only changed models (if True) or all of them (False)
    """
    if not is_user_ok_admin(request.user):
        raise PermissionDenied

    try:
        is_only_changed = kwargs['is_only_changed']
        enqueued_score_model_pks = Score.enqueue_update_scores_for_all_models(is_only_changed=is_only_changed)
        messages.success(request, f"Scheduled {len(enqueued_score_model_pks)} score updates for all projects. "
                                  f"is_only_changed={is_only_changed}")
    except redis.exceptions.ConnectionError as ce:
        messages.warning(request, f"Error updating scores: {ce}.")
    return redirect('zadmin')  # hard-coded


#
# ---- visualization-related view functions ----
#

def project_visualizations(request, project_pk):
    """
    View function to render various visualizations for a particular project.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_ok_to_view(request.user):
        raise PermissionDenied

    seasons = project.seasons()
    season_name = _param_val_from_request(request, 'season_name', seasons)

    # None if no targets in project:
    logger.debug("project_visualizations(): 1/3 calling flusight_location_to_data_dict(): {}".format(project))
    location_to_flusight_data_dict = flusight_location_to_data_dict(project, season_name, request)

    time_interval_type_to_x_axis_label = {Project.WEEK_TIME_INTERVAL_TYPE: 'Epi week',
                                          Project.BIWEEK_TIME_INTERVAL_TYPE: 'Biweek',
                                          Project.MONTH_TIME_INTERVAL_TYPE: 'Month'}
    loc_tz_date_to_actual_vals = project.location_timezero_date_to_actual_vals(season_name)
    location_to_actual_points = _location_to_actual_points(loc_tz_date_to_actual_vals)
    logger.debug("project_visualizations(): 2/3 calling location_to_max_val(): {}".format(project))
    location_to_max_val = project.location_to_max_val(season_name, project.step_ahead_targets())

    # correct location_to_max_val to account for max actual values
    location_to_actual_max_val = _location_to_actual_max_val(loc_tz_date_to_actual_vals)  # might be None
    for location in location_to_max_val:
        if (location_to_max_val[location]) \
                and (location in location_to_actual_max_val) \
                and (location_to_actual_max_val[location]):
            location_to_max_val[location] = max(location_to_max_val[location], location_to_actual_max_val[location])

    location_names = sorted(project.locations.all().values_list('name', flat=True))
    logger.debug("project_visualizations(): 3/3 rendering: {}".format(project))
    return render(
        request,
        'project_visualizations.html',
        context={'project': project,
                 'location': location_names[0],
                 'locations': location_names,
                 'season_name': season_name,
                 'seasons': seasons,
                 'location_to_flusight_data_dict': json.dumps(location_to_flusight_data_dict),
                 'location_to_actual_points': json.dumps(location_to_actual_points),
                 'location_to_max_val': json.dumps(location_to_max_val),
                 'x_axis_label': time_interval_type_to_x_axis_label[project.time_interval_type],
                 'y_axis_label': project.visualization_y_label})


def _location_to_actual_points(loc_tz_date_to_actual_vals):
    """
    :return: view function that returns a dict mapping location to a list of actual values found in
        loc_tz_date_to_actual_vals, which is as returned by location_timezero_date_to_actual_vals(). it is what the D3
        component expects: "[a JavaScript] array of the same length as timePoints"
    """


    def actual_list_from_tz_date_to_actual_dict(tz_date_to_actual):
        return [tz_date_to_actual[tz_date][0] if isinstance(tz_date_to_actual[tz_date], list) else None
                for tz_date in sorted(tz_date_to_actual.keys())]


    location_to_actual_points = {location: actual_list_from_tz_date_to_actual_dict(tz_date_to_actual)
                                 for location, tz_date_to_actual in loc_tz_date_to_actual_vals.items()}
    return location_to_actual_points


def _location_to_actual_max_val(loc_tz_date_to_actual_vals):
    """
    :return: view function that returns a dict mapping each location to the maximum value found in
        loc_tz_date_to_actual_vals, which is as returned by location_timezero_date_to_actual_vals()
    """


    def max_from_tz_date_to_actual_dict(tz_date_to_actual):
        flat_values = [item for sublist in tz_date_to_actual.values() if sublist for item in sublist]
        return max(flat_values) if flat_values else None  # NB: None is arbitrary


    location_to_actual_max = {location: max_from_tz_date_to_actual_dict(tz_date_to_actual)
                              for location, tz_date_to_actual in loc_tz_date_to_actual_vals.items()}
    return location_to_actual_max


#
# ---- score utility functions ----
#

def clear_all_scores(request):
    if not is_user_ok_admin(request.user):
        raise PermissionDenied

    # NB: runs in current thread
    for score in Score.objects.all():
        score.clear()
    messages.success(request, "Cleared all Scores.")
    return redirect('zadmin')  # hard-coded. see note below re: redirect to same page


def delete_score_last_updates(request):
    if not is_user_ok_admin(request.user):
        raise PermissionDenied

    # NB: delete() runs in current thread
    ScoreLastUpdate.objects.all().delete()
    messages.success(request, "Deleted all ScoreLastUpdates.")
    return redirect('zadmin')  # hard-coded. see note below re: redirect to same page


#
# ---- scores function ----
#

def project_scores(request, project_pk):
    """
    View function to render various scores for a particular project.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_ok_to_view(request.user):
        raise PermissionDenied

    # NB: inner knowledge about the targets location_to_mean_abs_error_rows_for_project() uses:
    step_ahead_targets = project.step_ahead_targets()
    if not step_ahead_targets:
        return render(request, 'message.html',
                      context={'title': "Required targets not found",
                               'message': "The project does not have the required score-related targets."})

    seasons = project.seasons()
    season_name = _param_val_from_request(request, 'season_name', seasons)
    try:
        logger.debug("project_scores(): calling: location_to_mean_abs_error_rows_for_project(). project={}, "
                     "season_name={}".format(project, season_name))
        location_to_rows_and_mins = location_to_mean_abs_error_rows_for_project(project, season_name)
        is_all_locations_have_rows = location_to_rows_and_mins and all(location_to_rows_and_mins.values())
        logger.debug("project_scores(): done: location_to_mean_abs_error_rows_for_project()")
    except RuntimeError as rte:
        return render(request, 'message.html',
                      context={'title': "Got an error trying to calculate scores.",
                               'message': "The error was: &ldquo;<span class=\"bg-danger\">{}</span>&rdquo;".format(
                                   rte)})

    location_names = project.locations.all().order_by('name').values_list('name', flat=True)
    model_pk_to_name_and_url = {forecast_model.pk: [forecast_model.name, forecast_model.get_absolute_url()]
                                for forecast_model in project.models.all()}
    return render(
        request,
        'project_scores.html',
        context={'project': project,
                 'model_pk_to_name_and_url': model_pk_to_name_and_url,
                 'season_name': season_name,
                 'seasons': seasons,
                 'location': location_names[0],
                 'locations': location_names,
                 'is_all_locations_have_rows': is_all_locations_have_rows,
                 'location_to_rows_and_mins': json.dumps(location_to_rows_and_mins),  # converts None -> null
                 })


def _param_val_from_request(request, param_name, choices):
    """
    :return param_name's value from query parameters. else use last one in choices, or None if no choices
    """
    param_val = request.GET[param_name] if param_name in request.GET else None
    if param_val in choices:
        return param_val
    else:
        return choices[-1] if choices else None


#
# ---- score data functions ----
#

def project_score_data(request, project_pk):
    """
    View function that renders a summary of all Scores in the passed Project.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_ok_to_view(request.user):
        raise PermissionDenied

    # set model_score_count_rows. we order by model.name then score.name, which we do in two passes: 1) look up objects
    # for PKs, and 2) iterate over sort, including only the first model in the rows
    model_score_counts = []
    for forecast_model_id, score_id, count in _model_score_count_rows_for_project(project):
        forecast_model = ForecastModel.objects.filter(pk=forecast_model_id).first()
        score = Score.objects.filter(pk=score_id).first()
        model_score_counts.append([forecast_model, score, count])

    # score, num_score_values, last_update:
    score_summaries = [(score,
                        score.num_score_values_for_project(project),
                        score.last_update_for_project(project))
                       for score in sorted(Score.objects.all(), key=lambda score: score.name)]

    model_score_count_rows = []  # forecast_model, score, count
    for forecast_model, score, count in sorted(model_score_counts, key=lambda row: (row[0].name, row[1].name)):
        model_score_count_rows.append([forecast_model, score, count])

    return render(request, 'project_score_data.html',
                  context={'project': project,
                           'score_summaries': score_summaries,
                           'model_score_count_rows': model_score_count_rows,
                           })


def _model_score_count_rows_for_project(project):
    """
    :return list of rows summarizing score information for project
    """
    # todo xx use meta for column names
    sql = """
        SELECT fm.id, sv.score_id, count(fm.id)
        FROM {scorevalue_table_name} AS sv
                    LEFT JOIN {forecast_table_name} AS f ON sv.forecast_id = f.id
                    LEFT JOIN {forecastmodel_forecast_table_name} AS fm ON f.forecast_model_id = fm.id
        WHERE fm.project_id = %s
        GROUP BY sv.score_id, fm.id;
    """.format(scorevalue_table_name=ScoreValue._meta.db_table,
               forecast_table_name=Forecast._meta.db_table,
               forecastmodel_forecast_table_name=ForecastModel._meta.db_table)
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        return cursor.fetchall()


def download_project_scores(request, project_pk):
    """
    Returns a response containing a CSV file for a project_pk's scores.
    Authorization: The project is public, or the logged-in user is a superuser, the Project's owner, or the forecast's
        model's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_ok_to_view(request.user):
        raise PermissionDenied

    from forecast_app.api_views import csv_response_for_project_score_data  # avoid circular imports
    from forecast_app.api_views import csv_response_for_cached_project_score_data  # ""


    if project.score_csv_file_cache.is_file_exists():
        return csv_response_for_cached_project_score_data(project)
    else:
        return csv_response_for_project_score_data(project)


def download_project_config(request, project_pk):
    """
    View function that returns a response containing a JSON config file for project_pk.
    Authorization: The project is public, or the logged-in user is a superuser, the Project's owner, or the forecast's
        model's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_ok_to_view(request.user):
        raise PermissionDenied

    project_config = config_dict_from_project(project)
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
        raise PermissionDenied

    is_error = validate_data_file(request)  # 'data_file' in request.FILES, data_file.size <= MAX_UPLOAD_FILE_SIZE
    if is_error:
        return is_error

    data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
    project_dict = json.load(data_file)
    try:
        new_project = create_project_from_json(project_dict, request.user)
        messages.success(request, f"Created project '{new_project.name}'")
        return redirect('project-detail', pk=new_project.pk)
    except RuntimeError as re:
        return render(request, 'message.html',
                      context={'title': "Error creating project from file.",
                               'message': f"There was an error uploading the file. The error was: "
                                          f"&ldquo;<span class=\"bg-danger\">{re}</span>&rdquo;"})


def create_project_from_form(request):
    """
    Shows a form to add a new Project with the owner being request.user. Authorization: Any logged-in user. Runs in the
    calling thread and therefore blocks.

    :param user_pk: the on-behalf-of user. may not be the same as the authenticated user
    """
    if not is_user_ok_create_project(request.user):
        raise PermissionDenied

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
        raise PermissionDenied

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
        raise PermissionDenied

    if request.method != 'POST':
        return HttpResponseBadRequest(f"only the POST method is supported")

    is_error = validate_data_file(request)  # 'data_file' in request.FILES, data_file.size <= MAX_UPLOAD_FILE_SIZE
    if is_error:
        return is_error

    data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
    out_config_dict = config_dict_from_project(project)
    edit_config_dict = json.load(data_file)
    changes = order_project_config_diff(project_config_diff(out_config_dict, edit_config_dict))
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
        raise PermissionDenied

    if request.method != 'POST':
        return HttpResponseBadRequest(f"only the POST method is supported")

    changes_json = request.POST['changes_json']  # serialized Changes list from the project_diff_report.html form
    deserialized_change_dicts = json.loads(changes_json)
    changes = [Change.deserialize_dict(change_dict) for change_dict in deserialized_change_dicts]
    logger.debug(f"edit_project_from_file_execute(): executing project config diff... changes={changes}")
    execute_project_config_diff(project, changes)
    logger.debug(f"edit_project_from_file_execute(): done")
    messages.success(request, f"Successfully applied {len(changes)} change(s) to project '{project.name}'.")
    return redirect('project-detail', pk=project_pk)


def delete_project(request, project_pk):
    """
    Does the actual deletion of a Project. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be a superuser or the Project's owner.
    """
    user = request.user
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_edit_project(request.user, project):
        raise PermissionDenied

    # imported here so that test_delete_project_iteratively() can patch via mock:
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
    if not is_user_ok_edit_user(request, detail_user):
        raise PermissionDenied

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
        raise PermissionDenied

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
        raise PermissionDenied

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
        raise PermissionDenied

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
        raise PermissionDenied

    forecast_model_name = forecast_model.name
    forecast_model.delete()
    messages.success(request, "Deleted model '{}'.".format(forecast_model_name))
    return redirect('user-detail', pk=user.pk)


#
# ---- List views ----
#

class UserListView(ListView):
    model = User


    def get_context_data(self, **kwargs):
        # collect user info
        user_projs_models = []  # 3-tuples: User, num_projs, num_models
        for user in self.get_queryset().all():
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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        project = self.get_object()
        return project.is_user_ok_to_view(self.request.user)


    def get_context_data(self, **kwargs):
        project = self.get_object()

        context = super().get_context_data(**kwargs)
        context['is_user_ok_edit_project'] = is_user_ok_edit_project(self.request.user, project)
        context['is_user_ok_create_model'] = is_user_ok_create_model(self.request.user, project)
        context['timezeros_num_forecasts'] = self.timezeros_num_forecasts(project)
        context['locations'] = project.locations.all().order_by('name')
        context['targets'] = project.targets.all().order_by('name')
        return context


    @staticmethod
    def timezeros_num_forecasts(project):
        """
        :return: a list of tuples that relates project's TimeZeros to # Forecasts. sorted by time_zero
        """
        rows = Forecast.objects.filter(forecast_model__project=project) \
            .values('time_zero__id') \
            .annotate(tz_count=Count('id')) \
            .order_by('time_zero__timezero_date')  # NB: Count() param doesn't matter

        # initialization is a work-around for missing LEFT JOIN items:
        tz_to_num_forecasts = {time_zero: 0 for time_zero in project.timezeros.all()}
        for row in rows:
            time_zero = TimeZero.objects.get(pk=row['time_zero__id'])
            tz_to_num_forecasts[time_zero] = row['tz_count']
        return [(k, tz_to_num_forecasts[k])
                for k in sorted(tz_to_num_forecasts.keys(), key=lambda timezero: timezero.timezero_date)]


def forecast_models_owned_by_user(user):
    """
    :param user: a User
    :return: searches all ForecastModels and returns those where the owner is user
    """
    return ForecastModel.objects.filter(owner=user)


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
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)

    # rename from the default 'user', which shadows the context var of that name that's always passed to templates:
    context_object_name = 'detail_user'


    def test_func(self):  # return True if the current user can access the view
        detail_user = self.get_object()
        return is_user_ok_edit_user(self.request, detail_user)


    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # pass a list of Projects. we have two cases: 1) projects owned by this user, and 2) projects where this user is
        # in model_owners. thus this list is of 2-tuples: (Project, user_role), where user_role is "Project Owner" or
        # "Model Owner"
        detail_user = self.get_object()
        projects_and_roles = projects_and_roles_for_user(detail_user)
        owned_models = forecast_models_owned_by_user(detail_user)
        context['is_user_ok_edit_user'] = is_user_ok_edit_user(self.request, detail_user)
        context['projects_and_roles'] = sorted(projects_and_roles,
                                               key=lambda project_and_role: project_and_role[0].name)
        context['owned_models'] = owned_models
        context['upload_file_jobs'] = detail_user.upload_file_jobs.all().order_by('-updated_at')
        return context


def timezero_forecast_pairs_for_forecast_model(forecast_model):
    """
    :return: a list of 2-tuples of timezero/forecast pairs for forecast_model. form: (TimeZero, Forecast)
    """
    return [(timezero, forecast_model.forecast_for_time_zero(timezero))
            for timezero in forecast_model.project.timezeros.order_by('timezero_date')]


class ForecastModelDetailView(UserPassesTestMixin, DetailView):
    model = ForecastModel
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):  # return True if the current user can access the view
        forecast_model = self.get_object()
        return forecast_model.project.is_user_ok_to_view(self.request.user)


    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        forecast_model = self.get_object()
        context['timezero_forecast_pairs'] = timezero_forecast_pairs_for_forecast_model(forecast_model)
        context['is_user_ok_edit_model'] = is_user_ok_edit_model(self.request.user, forecast_model)
        return context


class ForecastDetailView(UserPassesTestMixin, DetailView):
    model = Forecast
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)


    def test_func(self):  # return True if the current user can access the view
        forecast = self.get_object()
        return forecast.forecast_model.project.is_user_ok_to_view(self.request.user)


    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        forecast = self.get_object()
        pred_type_count_pairs = [
            (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[concrete_prediction_class],
             concrete_prediction_class.objects.filter(forecast=forecast).count())
            for concrete_prediction_class in Prediction.concrete_subclasses()]
        context['pred_type_count_pairs'] = sorted(pred_type_count_pairs)
        return context


class UploadFileJobDetailView(UserPassesTestMixin, DetailView):
    model = UploadFileJob
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect)

    context_object_name = 'upload_file_job'


    def test_func(self):  # return True if the current user can access the view
        upload_file_job = self.get_object()
        return self.request.user.is_superuser or (upload_file_job.user == self.request.user)


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
    if not project.is_user_ok_to_view(request.user):
        raise PermissionDenied

    from forecast_app.api_views import json_response_for_forecast  # avoid circular imports:


    return json_response_for_forecast(request, forecast)


#
# ---- Truth-related views ----
#

def truth_detail(request, project_pk):
    """
    View function to render a preview of a Project's truth data.
    Authorization: The logged-in user must be a superuser, or the Project's owner, or the forecast's model's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_ok_to_view(request.user):
        raise PermissionDenied

    return render(
        request,
        'truth_data_detail.html',
        context={'project': project,
                 'is_user_ok_edit_project': is_user_ok_edit_project(request.user, project)})


def delete_truth(request, project_pk):
    """
    Does the actual deletion of truth data. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be a superuser or the Project's owner.

    :return: redirect to the forecast's forecast_model detail page
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_edit_project(request.user, project):
        raise PermissionDenied

    project.delete_truth_data()
    return redirect('project-detail', pk=project_pk)


def upload_truth(request, project_pk):
    """
    Uploads the passed data into a the project's truth.
    Authorization: The logged-in user must be a superuser or the Project's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not is_user_ok_edit_project(request.user, project):
        raise PermissionDenied

    if project.is_truth_data_loaded():
        return render(request, 'message.html',
                      context={'title': "Truth data already loaded.",
                               'message': "The project already has truth data. Please delete it and then upload again."})

    is_error = validate_data_file(request)  # 'data_file' in request.FILES, data_file.size <= MAX_UPLOAD_FILE_SIZE
    if is_error:
        return is_error

    # upload to cloud and enqueue a job to process a new UploadFileJob
    data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
    is_error, upload_file_job = _upload_file(request.user, data_file, process_upload_file_job__truth,
                                             project_pk=project_pk)
    if is_error:
        return render(request, 'message.html',
                      context={'title': "Error uploading file.",
                               'message': "There was an error uploading the file. The error was: "
                                          "&ldquo;<span class=\"bg-danger\">{}</span>&rdquo;".format(is_error)})

    messages.success(request, "Queued the truth file '{}' for uploading.".format(data_file.name))
    return redirect('upload-file-job-detail', pk=upload_file_job.pk)


def process_upload_file_job__truth(upload_file_job_pk):
    """
    An _upload_file() enqueue() function that loads a truth file. Called by upload_truth().

    - Expected UploadFileJob.input_json key(s): 'project_pk' - passed to _upload_file()
    - Saves UploadFileJob.output_json key(s): None

    :param upload_file_job_pk: the UploadFileJob's pk
    """
    with upload_file_job_cloud_file(upload_file_job_pk) as (upload_file_job, cloud_file_fp):
        project_pk = upload_file_job.input_json['project_pk']
        project = get_object_or_404(Project, pk=project_pk)
        load_truth_data(project, cloud_file_fp, file_name=upload_file_job.filename)


def download_truth(request, project_pk):
    """
    Returns a response containing a CSV file for a project_pk's data.
    Authorization: The project is public, or the logged-in user is a superuser, the Project's owner, or the forecast's
        model's owner.
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_ok_to_view(request.user):
        raise PermissionDenied

    from forecast_app.api_views import csv_response_for_project_truth_data  # avoid circular imports


    return csv_response_for_project_truth_data(project)


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
        raise PermissionDenied

    is_error = validate_data_file(request)  # 'data_file' in request.FILES, data_file.size <= MAX_UPLOAD_FILE_SIZE
    if is_error:
        return is_error

    data_file = request.FILES['data_file']  # UploadedFile (e.g., InMemoryUploadedFile or TemporaryUploadedFile)
    existing_forecast_for_time_zero = forecast_model.forecast_for_time_zero(time_zero)
    if existing_forecast_for_time_zero and (existing_forecast_for_time_zero.source == data_file.name):
        return render(request, 'message.html',
                      context={'title': "Error uploading file.",
                               'message': "A forecast already exists. time_zero={}, file_name='{}'. Please delete "
                                          "existing data and then upload again. You may need to refresh the page to "
                                          "see the delete button.".format(time_zero.timezero_date, data_file.name)})

    # upload to cloud and enqueue a job to process a new UploadFileJob
    is_error, upload_file_job = _upload_file(request.user, data_file, process_upload_file_job__forecast,
                                             forecast_model_pk=forecast_model_pk,
                                             timezero_pk=timezero_pk)
    if is_error:
        return render(request, 'message.html',
                      context={'title': "Error uploading file.",
                               'message': "There was an error uploading the file. The error was: "
                                          "&ldquo;<span class=\"bg-danger\">{}</span>&rdquo;".format(is_error)})

    messages.success(request, "Queued the forecast file '{}' for uploading.".format(data_file.name))
    return redirect('upload-file-job-detail', pk=upload_file_job.pk)


def process_upload_file_job__forecast(upload_file_job_pk):
    """
    An _upload_file() enqueue() function that loads a forecast data file. Called by upload_forecast().

    - Expected UploadFileJob.input_json key(s): 'forecast_model_pk', 'timezero_pk' - passed to _upload_file()
    - Saves UploadFileJob.output_json key(s): 'forecast_pk'

    :param upload_file_job_pk: the UploadFileJob's pk
    """
    with upload_file_job_cloud_file(upload_file_job_pk) as (upload_file_job, cloud_file_fp):
        forecast_model_pk = upload_file_job.input_json['forecast_model_pk']
        forecast_model = get_object_or_404(ForecastModel, pk=forecast_model_pk)
        timezero_pk = upload_file_job.input_json['timezero_pk']
        time_zero = get_object_or_404(TimeZero, pk=timezero_pk)
        logger.debug(f"process_upload_file_job__forecast(): upload_file_job={upload_file_job}, "
                     f"forecast_model={forecast_model}, time_zero={time_zero}")
        with transaction.atomic():
            logger.debug(f"process_upload_file_job__forecast(): creating Forecast")
            new_forecast = Forecast.objects.create(forecast_model=forecast_model, time_zero=time_zero,
                                                   source=upload_file_job.filename)
            json_io_dict = json.load(cloud_file_fp)
            logger.debug(f"process_upload_file_job__forecast(): loading predictions. "
                         f"#predictions={len(json_io_dict['predictions'])}")
            load_predictions_from_json_io_dict(new_forecast, json_io_dict, False)
            upload_file_job.output_json = {'forecast_pk': new_forecast.pk}
            upload_file_job.save()
            logger.debug(f"process_upload_file_job__forecast(): done")


def delete_forecast(request, forecast_pk):
    """
    Does the actual deletion of a Forecast. Assumes that confirmation has already been given by the caller.
    Authorization: The logged-in user must be a superuser, or the Project's owner, or the forecast's model's owner.

    :return: redirect to the forecast's forecast_model detail page
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    is_allowed_to_delete = forecast.is_user_ok_to_delete(request.user)
    if not is_allowed_to_delete:
        raise PermissionDenied

    forecast_model_pk = forecast.forecast_model.pk  # in case can't access after delete() <- todo possible?
    forecast.delete()
    messages.success(request, "Deleted the forecast.")
    return redirect('model-detail', pk=forecast_model_pk)


#
# ---- Upload-related functions ----
#

# The following code supports the user's uploading arbitrary files to Zoltar for processing - forecast data files, for
# example. We implement this using a general view function named __upload_file(), which accepts two functions that
# are used to control how the uploaded file is processed. Doing it this way keeps that function general. Currently we
# use simple (but limited) pass-through uploading, rather than more efficient direct uploading, but this is a todo.
# See for more: https://devcenter.heroku.com/articles/s3#file-uploads .


MAX_UPLOAD_FILE_SIZE = 5E+06


def _upload_file(user, data_file, process_upload_file_job_fcn, **kwargs):
    """
    Accepts a file uploaded to this app by the user. Creates a UploadFileJob to track the job, saves data_file in cloud
    storage, then enqueues process_upload_file_job_fcn to process the file by an RQ worker.

    :param user: the User from request.User
    :param data_file: the data file to use as found in request.FILES . it is an UploadedFile (e.g.,
        InMemoryUploadedFile or TemporaryUploadedFile)
    :param process_upload_file_job_fcn: a function of one arg (upload_file_job_pk) that is passed to
        django_rq.enqueue(). NB: It MUST use the upload_file_job_cloud_file context to have access to the file that was
        uploaded to cloud, e.g.,
            with upload_file_job_cloud_file() as cloud_file_fp: ...
        NB: If it needs to save upload_file_job.output_json, make sure to call save(), e.g.,
            upload_file_job.output_json = {'forecast_pk': new_forecast.pk}
            upload_file_job.save()
    :param kwargs: saved in the new UploadFileJob's input_json
    :return a 2-tuple: (is_error, upload_file_job) where:
        - is_error: True if there was an error, and False o/w. If true, it is actually an error message to show the user
        - upload_file_job the new UploadFileJob instance if not is_error. None o/w
    """
    # create the UploadFileJob
    logger.debug("_upload_file(): Got data_file: name={!r}, size={}, content_type={}"
                 .format(data_file.name, data_file.size, data_file.content_type))
    try:
        upload_file_job = UploadFileJob.objects.create(user=user, filename=data_file.name)  # status = PENDING
        upload_file_job.input_json = kwargs
        upload_file_job.save()
        logger.debug("_upload_file(): 1/3 Created the UploadFileJob: {}".format(upload_file_job))
    except Exception as exc:
        logger.debug("_upload_file(): Error creating the UploadFileJob: {}".format(exc))
        return "Error creating the UploadFileJob: {}".format(exc), None

    # upload the file to cloud storage
    try:
        upload_file(upload_file_job, data_file)
        upload_file_job.status = UploadFileJob.CLOUD_FILE_UPLOADED
        upload_file_job.save()
        logger.debug("_upload_file(): 2/3 Uploaded the file to cloud. upload_file_job={}".format(upload_file_job))
    except Exception as exc:
        failure_message = "_upload_file(): Error uploading file to cloud: {}. upload_file_job={}" \
            .format(exc, upload_file_job)
        upload_file_job.status = UploadFileJob.FAILED
        upload_file_job.failure_message = failure_message
        upload_file_job.save()
        logger.debug(failure_message)
        return "Error uploading file to cloud: {}. upload_file_job={}".format(exc, upload_file_job), None

    # enqueue a worker
    try:
        queue = django_rq.get_queue(UPLOAD_FILE_QUEUE_NAME)
        rq_job = queue.enqueue(process_upload_file_job_fcn, upload_file_job.pk, job_id=upload_file_job.rq_job_id())
        upload_file_job.status = UploadFileJob.QUEUED
        upload_file_job.save()
        logger.debug("_upload_file(): 3/3 Enqueued the job: {}. upload_file_job={}".format(rq_job, upload_file_job))
    except Exception as exc:
        failure_message = "_upload_file(): FAILED_ENQUEUE: Error enqueuing the job: {}. upload_file_job={}" \
            .format(exc, upload_file_job)
        upload_file_job.status = UploadFileJob.FAILED
        upload_file_job.failure_message = failure_message
        upload_file_job.save()
        delete_file(upload_file_job)  # NB: in current thread
        logger.debug(failure_message)
        return "Error enqueuing the job: {}. upload_file_job={}".format(exc, upload_file_job), None

    logger.debug("_upload_file(): done")
    return False, upload_file_job


def process_upload_file_job__noop(upload_file_job_pk):
    """
    A demonstration _upload_file() enqueue() function that does nothing. Expected UploadFileJob.input_json keys: none.

    :param upload_file_job_pk: the UploadFileJob's pk
    """
    logger.debug("process_upload_file_job__noop(): entered. upload_file_job_pk={}".format(upload_file_job_pk))
    with upload_file_job_cloud_file(upload_file_job_pk) as (upload_file_job, cloud_file_fp):
        # show that we can access the file's data
        file_size = cloud_file_fp.seek(0, io.SEEK_END)
        cloud_file_fp.seek(0)
        lines = cloud_file_fp.readlines()
        cloud_file_fp.seek(0)
        logger.debug(
            "process_upload_file_job__noop(): upload_file_job={}, cloud_file_fp={}.\n\t-> from cloud_file_fp: {}, {}, {}"
                .format(upload_file_job, cloud_file_fp, file_size, len(lines), repr(lines[0:2])))

        # simulate a long-running operation
        time.sleep(5)


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

def is_user_ok_create_project(user):
    """
    :return: True if user (a User instance) is allowed to create Projects.
    """
    return user.is_authenticated  # any logged-in user can create. recall AnonymousUser.is_authenticated returns False


def is_user_ok_admin(user):
    return user.is_superuser


def is_user_ok_edit_project(user, project):
    # applies to delete too
    return user.is_superuser or (user == project.owner)


def is_user_ok_create_model(user, project):
    return user.is_superuser or (user == project.owner) or (user in project.model_owners.all())


def is_user_ok_edit_model(user, forecast_model):
    # applies to delete too
    return user.is_superuser or (user == forecast_model.project.owner) or (user == forecast_model.owner)


def is_user_ok_edit_user(request, detail_user):
    return request.user.is_superuser or (detail_user == request.user)


def is_user_ok_upload_forecast(request, forecast_model):
    return request.user.is_superuser or (request.user == forecast_model.project.owner) or \
           (request.user == forecast_model.owner)
