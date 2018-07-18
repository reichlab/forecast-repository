from django.conf.urls import url, include

from . import views


urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^about$', views.about, name='about'),
    url(r'^docs', views.documentation, name='docs'),

    url(r'^zadmin', views.zadmin, name='zadmin'),
    url(r'^clear_row_count_caches', views.clear_row_count_caches, name='clear-row-count-caches'),
    url(r'^update_row_count_caches', views.update_row_count_caches, name='update-row-count-caches'),

    url(r'^accounts/', include('django.contrib.auth.urls')),
    url(r'^users/', views.UserListView.as_view(), name='user-list'),

    url(r'^project/(?P<pk>\d+)$', views.ProjectDetailView.as_view(), name='project-detail'),
    url(r'^project/(?P<project_pk>\d+)/visualizations$', views.project_visualizations, name='project-visualizations'),
    url(r'^project/(?P<project_pk>\d+)/scores', views.project_scores, name='project-scores'),

    url(r'^project/(?P<project_pk>\d+)/template$', views.template_detail, name='template-data-detail'),
    url(r'^project/(?P<project_pk>\d+)/template/delete$', views.delete_template, name='delete-template'),
    url(r'^project/(?P<project_pk>\d+)/template/upload/$', views.upload_template, name='upload-template'),
    url(r'^project/(?P<model_with_cdc_data_pk>\d+)/download', views.download_file_for_model_with_cdc_data,
        {'type': 'project'}, name='download-template'),

    url(r'^project/(?P<project_pk>\d+)/truth$', views.truth_detail, name='truth-data-detail'),
    url(r'^project/(?P<project_pk>\d+)/truth/delete$', views.delete_truth, name='delete-truth'),
    url(r'^project/(?P<project_pk>\d+)/truth/upload/$', views.upload_truth, name='upload-truth'),
    url(r'^project/(?P<project_pk>\d+)/truth/download', views.download_truth, name='download-truth'),

    url(r'^model/(?P<pk>\d+)$', views.ForecastModelDetailView.as_view(), name='model-detail'),

    url(r'^user/(?P<pk>\d+)$', views.UserDetailView.as_view(), name='user-detail'),

    url(r'^forecast/(?P<pk>\d+)$', views.ForecastDetailView.as_view(), name='forecast-detail'),
    url(r'^forecast/(?P<model_with_cdc_data_pk>\d+)/download', views.download_file_for_model_with_cdc_data,
        {'type': 'forecast'}, name='download-forecast'),
    url(r'^forecast/(?P<forecast_pk>\d+)/sparkline', views.forecast_sparkline_bin_for_loc_and_target,
        name='forecast-sparkline'),


    # ---- CRUD-related form URLs ----

    # Project
    url(r'^project/create/$', views.create_project, name='create-project'),
    url(r'^project/(?P<project_pk>\d+)/edit/$', views.edit_project, name='edit-project'),
    url(r'^project/(?P<project_pk>\d+)/delete/$', views.delete_project, name='delete-project'),

    # ForecastModel
    url(r'^project/(?P<project_pk>\d+)/models/create/$', views.create_model, name='create-model'),
    url(r'^model/(?P<model_pk>\d+)/edit/$', views.edit_model, name='edit-model'),
    url(r'^model/(?P<model_pk>\d+)/delete/$', views.delete_model, name='delete-model'),

    # Forecast
    url(r'^forecast/(?P<forecast_pk>\d+)/delete$', views.delete_forecast, name='delete-forecast'),
    url(r'^forecast/(?P<forecast_model_pk>\d+)/upload/(?P<timezero_pk>\d+)$', views.upload_forecast,
        name='upload-forecast'),

]
