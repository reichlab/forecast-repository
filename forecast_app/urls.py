from django.conf.urls import url, include

from . import views


# todo xx should probably terminate all of the following with '$'. should also think about necessity of trailing '/'
urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^about$', views.about, name='about'),
    url(r'^docs', views.documentation, name='docs'),

    url(r'^zadmin$', views.zadmin, name='zadmin'),
    url(r'^zadmin/upload_file_jobs$', views.zadmin_upload_file_jobs, name='zadmin-upload-file-jobs'),
    url(r'^zadmin/score_last_updates$', views.zadmin_score_last_updates, name='zadmin-score-last-updates'),
    url(r'^empty_rq/$', views.empty_rq, name='empty-rq'),
    url(r'^clear_row_count_caches', views.clear_row_count_caches, name='clear-row-count-caches'),
    url(r'^update_row_count_caches', views.update_row_count_caches, name='update-row-count-caches'),
    url(r'^clear_score_csv_file_caches', views.clear_score_csv_file_caches, name='clear-score-csv-file-caches'),
    url(r'^update_score_csv_file_caches', views.update_score_csv_file_caches, name='update-score-csv-file-caches'),
    url(r'^delete_upload_file_jobs/$', views.delete_upload_file_jobs, name='delete-file-jobs'),

    url(r'^update_all_scores', views.update_all_scores, name='update-all-scores'),
    url(r'^clear_all_scores', views.clear_all_scores, name='clear-all-scores'),
    url(r'^delete_score_last_updates', views.delete_score_last_updates, name='delete-score-last-updates'),

    url(r'^accounts/', include('django.contrib.auth.urls')),
    url(r'^users/', views.UserListView.as_view(), name='user-list'),

    url(r'^project/(?P<pk>\d+)$', views.ProjectDetailView.as_view(), name='project-detail'),
    url(r'^project/(?P<project_pk>\d+)/visualizations$', views.project_visualizations, name='project-visualizations'),
    url(r'^project/(?P<project_pk>\d+)/scores', views.project_scores, name='project-scores'),
    url(r'^project/(?P<project_pk>\d+)/score_data', views.project_score_data, name='project-score-data'),
    url(r'^project/(?P<project_pk>\d+)/download_score_data', views.download_project_scores,
        name='download-project-scores'),
    url(r'^project/(?P<project_pk>\d+)/download_config', views.download_project_config, name='project-config'),

    url(r'^project/(?P<project_pk>\d+)/truth$', views.truth_detail, name='truth-data-detail'),
    url(r'^project/(?P<project_pk>\d+)/truth/delete$', views.delete_truth, name='delete-truth'),
    url(r'^project/(?P<project_pk>\d+)/truth/upload/$', views.upload_truth, name='upload-truth'),
    url(r'^project/(?P<project_pk>\d+)/truth/download', views.download_truth, name='download-truth'),

    url(r'^model/(?P<pk>\d+)$', views.ForecastModelDetailView.as_view(), name='model-detail'),

    url(r'^user/(?P<pk>\d+)$', views.UserDetailView.as_view(), name='user-detail'),

    url(r'^uploadfilejob/(?P<pk>\d+)$', views.UploadFileJobDetailView.as_view(), name='upload-file-job-detail'),

    url(r'^forecast/(?P<pk>\d+)$', views.ForecastDetailView.as_view(), name='forecast-detail'),
    url(r'^forecast/(?P<forecast_pk>\d+)/download', views.download_forecast, name='download-forecast'),


    # ---- CRUD-related form URLs ----

    # Project
    url(r'^project/create_form/$', views.create_project_from_form, name='create-project-from-form'),
    url(r'^project/create_file/$', views.create_project_from_file, name='create-project-from-file'),
    url(r'^project/(?P<project_pk>\d+)/edit/$', views.edit_project, name='edit-project'),
    url(r'^project/(?P<project_pk>\d+)/delete/$', views.delete_project, name='delete-project'),

    # User
    url(r'^user/(?P<user_pk>\d+)/edit/$', views.edit_user, name='edit-user'),

    # ForecastModel
    url(r'^project/(?P<project_pk>\d+)/models/create/$', views.create_model, name='create-model'),
    url(r'^model/(?P<model_pk>\d+)/edit/$', views.edit_model, name='edit-model'),
    url(r'^model/(?P<model_pk>\d+)/delete/$', views.delete_model, name='delete-model'),

    # Forecast
    url(r'^forecast/(?P<forecast_pk>\d+)/delete$', views.delete_forecast, name='delete-forecast'),
    url(r'^forecast/(?P<forecast_model_pk>\d+)/upload/(?P<timezero_pk>\d+)$', views.upload_forecast,
        name='upload-forecast'),

]
