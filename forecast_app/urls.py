from django.conf.urls import include
from django.urls import re_path

from . import views


# todo xx should probably terminate all of the following with '$'. should also think about necessity of trailing '/'


urlpatterns = [
    re_path(r'^$', views.index, name='index'),
    re_path(r'^robots.txt$', views.robots_txt, name='robots'),
    re_path(r'^about$', views.about, name='about'),
    re_path(r'^projects$', views.projects, name='projects'),

    re_path(r'^zadmin$', views.zadmin, name='zadmin'),
    re_path(r'^zadmin/jobs$', views.zadmin_jobs, name='zadmin-jobs'),
    re_path(r'^zadmin/jobs_viz$', views.zadmin_jobs_viz, name='zadmin-jobs-viz'),

    re_path(r'^accounts/', include('django.contrib.auth.urls')),  # requires trailing slash and no $
    re_path(r'^users$', views.UserListView.as_view(), name='user-list'),

    re_path(r'^project/(?P<pk>\d+)$', views.ProjectDetailView.as_view(), name='project-detail'),
    re_path(r'^project/(?P<project_pk>\d+)/forecasts', views.project_forecasts, name='project-forecasts'),
    re_path(r'^project/(?P<project_pk>\d+)/query_forecasts$', views.query_project,
            {'query_type': views.QueryType.FORECASTS}, name='query-forecasts'),
    re_path(r'^project/(?P<project_pk>\d+)/explorer', views.project_explorer, name='project-explorer'),
    re_path(r'^project/(?P<project_pk>\d+)/viz$', views.project_viz, name='project-viz'),
    re_path(r'^project/(?P<project_pk>\d+)/viz_options_edit$',
            views.project_viz_options_edit, name='project-viz-options-edit'),
    re_path(r'^project/(?P<project_pk>\d+)/viz_options_execute$',
            views.project_viz_options_execute, name='project-viz-options-execute'),
    re_path(r'^project/(?P<project_pk>\d+)/download_config$', views.download_project_config, name='project-config'),

    re_path(r'^project/(?P<project_pk>\d+)/truth$', views.truth_detail, name='truth-data-detail'),
    re_path(r'^project/(?P<project_pk>\d+)/truth/upload/$', views.upload_truth, name='upload-truth'),
    re_path(r'^project/(?P<project_pk>\d+)/query_truth$', views.query_project,
            {'query_type': views.QueryType.TRUTH}, name='query-truth'),

    re_path(r'^model/(?P<pk>\d+)$', views.ForecastModelDetailView.as_view(), name='model-detail'),

    re_path(r'^user/(?P<pk>\d+)$', views.UserDetailView.as_view(), name='user-detail'),

    re_path(r'^job/(?P<pk>\d+)$', views.JobDetailView.as_view(), name='job-detail'),
    re_path(r'^job/(?P<pk>\d+)/download$', views.download_job_data_file, name='download-job-data'),

    re_path(r'^forecast/(?P<pk>\d+)$', views.ForecastDetailView.as_view(), name='forecast-detail'),
    re_path(r'^forecast/(?P<forecast_pk>\d+)/download$', views.download_forecast, name='download-forecast'),


    # ---- CRUD-related form URLs ----

    # Project
    re_path(r'^project/create_proj_form/$', views.create_project_from_form, name='create-project-from-form'),
    re_path(r'^project/create_proj_file/$', views.create_project_from_file, name='create-project-from-file'),
    re_path(r'^project/(?P<project_pk>\d+)/edit_proj_from_form/$', views.edit_project_from_form,
            name='edit-project-from-form'),
    re_path(r'^project/(?P<project_pk>\d+)/edit_proj_file_preview/$', views.edit_project_from_file_preview,
            name='edit-project-from-file-preview'),
    re_path(r'^project/(?P<project_pk>\d+)/edit_proj_file_execute/$', views.edit_project_from_file_execute,
            name='edit-project-from-file-execute'),
    re_path(r'^project/(?P<project_pk>\d+)/delete/$', views.delete_project, name='delete-project'),
    re_path(r'^project/(?P<project_pk>\d+)/delete_project_truth_latest_batch/$',
            views.delete_project_truth_latest_batch,
            name='delete-project-latest-truth-batch'),

    # User
    re_path(r'^user/(?P<user_pk>\d+)/edit/$', views.edit_user, name='edit-user'),
    re_path(r'^change_password/$', views.change_password, name='change-password'),

    # ForecastModel
    re_path(r'^project/(?P<project_pk>\d+)/models/create/$', views.create_model, name='create-model'),
    re_path(r'^model/(?P<model_pk>\d+)/edit/$', views.edit_model, name='edit-model'),
    re_path(r'^model/(?P<model_pk>\d+)/delete/$', views.delete_model, name='delete-model'),

    # Forecast
    re_path(r'^forecast/(?P<forecast_pk>\d+)/delete$', views.delete_forecast, name='delete-forecast'),
    re_path(r'^forecast/(?P<forecast_model_pk>\d+)/upload/(?P<timezero_pk>\d+)$', views.upload_forecast,
            name='upload-forecast'),

]
