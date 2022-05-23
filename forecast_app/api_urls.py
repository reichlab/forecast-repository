from django.conf.urls import url

from forecast_app import api_views


urlpatterns = [
    # root
    url(r'^$', api_views.api_root, name='api-root'),

    # projects list, and related objects' lists and details
    url(r'^projects/$', api_views.ProjectList.as_view(), name='api-project-list'),
    url(r'^project/(?P<pk>\d+)/$', api_views.ProjectDetail.as_view(), name='api-project-detail'),
    url(r'^project/(?P<pk>\d+)/units/$', api_views.ProjectUnitList.as_view(), name='api-unit-list'),
    url(r'^project/(?P<pk>\d+)/targets/$', api_views.ProjectTargetList.as_view(), name='api-target-list'),
    url(r'^project/(?P<pk>\d+)/timezeros/$', api_views.ProjectTimeZeroList.as_view(), name='api-timezero-list'),
    url(r'^project/(?P<pk>\d+)/models/$', api_views.ProjectForecastModelList.as_view(), name='api-model-list'),
    url(r'^project/(?P<pk>\d+)/truth/$', api_views.TruthDetail.as_view(), name='api-truth-detail'),
    url(r'^project/(?P<pk>\d+)/forecast_queries/$', api_views.query_forecasts_endpoint, name='api-forecast-queries'),
    url(r'^project/(?P<pk>\d+)/truth_queries/$', api_views.query_truth_endpoint, name='api-truth-queries'),
    url(r'^project/(?P<pk>\d+)/forecasts/$', api_views.download_latest_forecasts, name='api-project-latest-forecasts'),

    # visualization
    url(r'^project/(?P<pk>\d+)/viz-units/$', api_views.viz_units_api, name='api-viz-units'),
    url(r'^project/(?P<pk>\d+)/viz-target-vars/$', api_views.viz_target_vars, name='api-viz-target-vars'),
    url(r'^project/(?P<pk>\d+)/viz-avail-ref-dates/$', api_views.viz_avail_ref_dates, name='api-viz-avail-ref-dates'),
    url(r'^project/(?P<pk>\d+)/viz-models/$', api_views.viz_models, name='api-viz-models'),
    url(r'^project/(?P<pk>\d+)/viz-data/$', api_views.viz_data_api, name='api-viz-data'),

    # other object detail
    url(r'^user/(?P<pk>\d+)/$', api_views.UserDetail.as_view(), name='api-user-detail'),
    url(r'^unit/(?P<pk>\d+)/$', api_views.UnitDetail.as_view(), name='api-unit-detail'),
    url(r'^target/(?P<pk>\d+)/$', api_views.TargetDetail.as_view(), name='api-target-detail'),
    url(r'^timezero/(?P<pk>\d+)/$', api_views.TimeZeroDetail.as_view(), name='api-timezero-detail'),

    url(r'^job/(?P<pk>\d+)/$', api_views.JobDetailView.as_view(), name='api-job-detail'),
    url(r'^job/(?P<pk>\d+)/data/$', api_views.download_job_data, name='api-job-data-download'),

    url(r'^model/(?P<pk>\d+)/$', api_views.ForecastModelDetail.as_view(), name='api-model-detail'),
    url(r'^model/(?P<pk>\d+)/forecasts/$', api_views.ForecastModelForecastList.as_view(), name='api-forecast-list'),

    url(r'^forecast/(?P<pk>\d+)/$', api_views.ForecastDetail.as_view(), name='api-forecast-detail'),
    url(r'^forecast/(?P<pk>\d+)/data/$', api_views.forecast_data, name='api-forecast-data'),
]
