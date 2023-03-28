from django.urls import re_path

from forecast_app import api_views


urlpatterns = [
    # root
    re_path(r'^$', api_views.api_root, name='api-root'),

    # projects list, and related objects' lists and details
    re_path(r'^projects/$', api_views.ProjectList.as_view(), name='api-project-list'),
    re_path(r'^project/(?P<pk>\d+)/$', api_views.ProjectDetail.as_view(), name='api-project-detail'),
    re_path(r'^project/(?P<pk>\d+)/units/$', api_views.ProjectUnitList.as_view(), name='api-unit-list'),
    re_path(r'^project/(?P<pk>\d+)/targets/$', api_views.ProjectTargetList.as_view(), name='api-target-list'),
    re_path(r'^project/(?P<pk>\d+)/timezeros/$', api_views.ProjectTimeZeroList.as_view(), name='api-timezero-list'),
    re_path(r'^project/(?P<pk>\d+)/models/$', api_views.ProjectForecastModelList.as_view(), name='api-model-list'),
    re_path(r'^project/(?P<pk>\d+)/truth/$', api_views.TruthDetail.as_view(), name='api-truth-detail'),
    re_path(r'^project/(?P<pk>\d+)/forecast_queries/$', api_views.query_forecasts_endpoint,
            name='api-forecast-queries'),
    re_path(r'^project/(?P<pk>\d+)/truth_queries/$', api_views.query_truth_endpoint, name='api-truth-queries'),
    re_path(r'^project/(?P<pk>\d+)/forecasts/$', api_views.download_latest_forecasts,
            name='api-project-latest-forecasts'),
    re_path(r'^project/(?P<pk>\d+)/viz-data/$', api_views.viz_data_api, name='api-viz-data'),
    re_path(r'^project/(?P<pk>\d+)/viz-human-ensemble-model/$', api_views.viz_human_ensemble_model_api,
            name='api-viz-human-ensemble-model'),

    # other object detail
    re_path(r'^user/(?P<pk>\d+)/$', api_views.UserDetail.as_view(), name='api-user-detail'),
    re_path(r'^unit/(?P<pk>\d+)/$', api_views.UnitDetail.as_view(), name='api-unit-detail'),
    re_path(r'^target/(?P<pk>\d+)/$', api_views.TargetDetail.as_view(), name='api-target-detail'),
    re_path(r'^timezero/(?P<pk>\d+)/$', api_views.TimeZeroDetail.as_view(), name='api-timezero-detail'),

    re_path(r'^job/(?P<pk>\d+)/$', api_views.JobDetailView.as_view(), name='api-job-detail'),
    re_path(r'^job/(?P<pk>\d+)/data/$', api_views.download_job_data, name='api-job-data-download'),

    re_path(r'^model/(?P<pk>\d+)/$', api_views.ForecastModelDetail.as_view(), name='api-model-detail'),
    re_path(r'^model/(?P<pk>\d+)/forecasts/$', api_views.ForecastModelForecastList.as_view(), name='api-forecast-list'),

    re_path(r'^forecast/(?P<pk>\d+)/$', api_views.ForecastDetail.as_view(), name='api-forecast-detail'),
    re_path(r'^forecast/(?P<pk>\d+)/data/$', api_views.forecast_data, name='api-forecast-data'),
]
