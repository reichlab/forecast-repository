from django.conf.urls import url, include

from . import views
# todo xx should probably terminate all of the following with '$'. should also think about necessity of trailing '/'


urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^robots.txt$', views.robots_txt, name='robots'),
    url(r'^about$', views.about, name='about'),
    url(r'^projects$', views.projects, name='projects'),

    url(r'^zadmin$', views.zadmin, name='zadmin'),
    url(r'^zadmin/jobs$', views.zadmin_jobs, name='zadmin-jobs'),

    url(r'^accounts/', include('django.contrib.auth.urls')),  # requires trailing slash and no $
    url(r'^users$', views.UserListView.as_view(), name='user-list'),

    url(r'^project/(?P<pk>\d+)$', views.ProjectDetailView.as_view(), name='project-detail'),
    url(r'^project/(?P<project_pk>\d+)/forecasts', views.project_forecasts, name='project-forecasts'),
    url(r'^project/(?P<project_pk>\d+)/query_forecasts$', views.query_project,
        {'query_type': views.QueryType.FORECASTS}, name='query-forecasts'),
    url(r'^project/(?P<project_pk>\d+)/explorer', views.project_explorer, name='project-explorer'),
    url(r'^project/(?P<project_pk>\d+)/viz', views.project_viz, name='project-viz'),
    url(r'^project/(?P<project_pk>\d+)/download_config$', views.download_project_config, name='project-config'),

    url(r'^project/(?P<project_pk>\d+)/truth$', views.truth_detail, name='truth-data-detail'),
    url(r'^project/(?P<project_pk>\d+)/truth/upload/$', views.upload_truth, name='upload-truth'),
    url(r'^project/(?P<project_pk>\d+)/query_truth$', views.query_project,
        {'query_type': views.QueryType.TRUTH}, name='query-truth'),

    url(r'^model/(?P<pk>\d+)$', views.ForecastModelDetailView.as_view(), name='model-detail'),

    url(r'^user/(?P<pk>\d+)$', views.UserDetailView.as_view(), name='user-detail'),

    url(r'^job/(?P<pk>\d+)$', views.JobDetailView.as_view(), name='job-detail'),
    url(r'^job/(?P<pk>\d+)/download$', views.download_job_data_file, name='download-job-data'),

    url(r'^forecast/(?P<pk>\d+)$', views.ForecastDetailView.as_view(), name='forecast-detail'),
    url(r'^forecast/(?P<forecast_pk>\d+)/download$', views.download_forecast, name='download-forecast'),


    # ---- CRUD-related form URLs ----

    # Project
    url(r'^project/create_proj_form/$', views.create_project_from_form, name='create-project-from-form'),
    url(r'^project/create_proj_file/$', views.create_project_from_file, name='create-project-from-file'),
    url(r'^project/(?P<project_pk>\d+)/edit_proj_from_form/$', views.edit_project_from_form,
        name='edit-project-from-form'),
    url(r'^project/(?P<project_pk>\d+)/edit_proj_file_preview/$', views.edit_project_from_file_preview,
        name='edit-project-from-file-preview'),
    url(r'^project/(?P<project_pk>\d+)/edit_proj_file_execute/$', views.edit_project_from_file_execute,
        name='edit-project-from-file-execute'),
    url(r'^project/(?P<project_pk>\d+)/delete/$', views.delete_project, name='delete-project'),
    url(r'^project/(?P<project_pk>\d+)/delete_project_truth_latest_batch/$', views.delete_project_truth_latest_batch,
        name='delete-project-latest-truth-batch'),

    # User
    url(r'^user/(?P<user_pk>\d+)/edit/$', views.edit_user, name='edit-user'),
    url(r'^change_password/$', views.change_password, name='change-password'),

    # ForecastModel
    url(r'^project/(?P<project_pk>\d+)/models/create/$', views.create_model, name='create-model'),
    url(r'^model/(?P<model_pk>\d+)/edit/$', views.edit_model, name='edit-model'),
    url(r'^model/(?P<model_pk>\d+)/delete/$', views.delete_model, name='delete-model'),

    # Forecast
    url(r'^forecast/(?P<forecast_pk>\d+)/delete$', views.delete_forecast, name='delete-forecast'),
    url(r'^forecast/(?P<forecast_model_pk>\d+)/upload/(?P<timezero_pk>\d+)$', views.upload_forecast,
        name='upload-forecast'),

]
