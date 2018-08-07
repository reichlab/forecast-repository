from django.conf.urls import url

from forecast_app import api_views


urlpatterns = [
    url(r'^$', api_views.api_root, name='api-root'),
    url(r'^projects/$', api_views.ProjectList.as_view(), name='api-project-list'),
    url(r'^project/(?P<pk>\d+)/$', api_views.ProjectDetail.as_view(), name='api-project-detail'),

    url(r'^project/(?P<pk>\d+)/template/$', api_views.TemplateDetail.as_view(), name='api-template-detail'),
    url(r'^project/(?P<project_pk>\d+)/template_data/$', api_views.template_data, name='api-template-data'),

    url(r'^project/(?P<pk>\d+)/truth/$', api_views.TruthDetail.as_view(), name='api-truth-detail'),
    url(r'^project/(?P<project_pk>\d+)/truth_data/$', api_views.truth_data, name='api-truth-data'),

    url(r'^user/(?P<pk>\d+)/$', api_views.UserDetail.as_view(), name='api-user-detail'),
    url(r'^uploadfilejob/(?P<pk>\d+)/$', api_views.UploadFileJobDetailView.as_view(),
        name='api-upload-file-job-detail'),

    url(r'^model/(?P<pk>\d+)/$', api_views.ForecastModelDetail.as_view(), name='api-model-detail'),

    url(r'^forecast/(?P<pk>\d+)/$', api_views.ForecastDetail.as_view(), name='api-forecast-detail'),
    url(r'^forecast/(?P<forecast_pk>\d+)/data/$', api_views.forecast_data, name='api-forecast-data'),
]
