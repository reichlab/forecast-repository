from django.conf.urls import url

from forecast_app import api_views


urlpatterns = [
    url(r'^$', api_views.api_root, name='api-root'),

    url(r'^user/(?P<pk>\d+)/$', api_views.UserDetail.as_view(), name='api-user-detail'),

    url(r'^uploadfilejob/(?P<pk>\d+)/$', api_views.UploadFileJobDetailView.as_view(),
        name='api-upload-file-job-detail'),

    url(r'^projects/$', api_views.ProjectList.as_view(), name='api-project-list'),
    url(r'^project/(?P<pk>\d+)/$', api_views.ProjectDetail.as_view(), name='api-project-detail'),
    url(r'^project/(?P<pk>\d+)/timezeros/$', api_views.ProjectTimeZeroList.as_view(), name='api-timezero-list'),
    url(r'^project/(?P<pk>\d+)/truth/$', api_views.TruthDetail.as_view(), name='api-truth-detail'),
    url(r'^project/(?P<pk>\d+)/truth_data/$', api_views.truth_data, name='api-truth-data'),
    url(r'^project/(?P<pk>\d+)/score_data/$', api_views.score_data, name='api-score-data'),
    url(r'^project/(?P<pk>\d+)/models/$', api_views.ProjectForecastModelList.as_view(), name='api-model-list'),

    url(r'^model/(?P<pk>\d+)/$', api_views.ForecastModelDetail.as_view(), name='api-model-detail'),
    url(r'^model/(?P<pk>\d+)/forecasts/$', api_views.ForecastModelForecastList.as_view(), name='api-forecast-list'),

    url(r'^timezero/(?P<pk>\d+)/$', api_views.TimeZeroDetail.as_view(), name='api-timezero-detail'),

    url(r'^forecast/(?P<pk>\d+)/$', api_views.ForecastDetail.as_view(), name='api-forecast-detail'),
    url(r'^forecast/(?P<pk>\d+)/data/$', api_views.forecast_data, name='api-forecast-data'),
]
