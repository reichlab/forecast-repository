from django.conf import settings
from django.conf.urls import url, include
from django.conf.urls.static import static
from django.contrib import admin

from forecast_app import api_views


urlpatterns = [

    # our app
    url(r'', include('forecast_app.urls')),

    # admin interface - mainly for User management
    url(r'^admin/', admin.site.urls),

    # REST framework. todo xx move to forecast_app.api_urls.py, include as above under /api/
    url(r'^api/$', api_views.api_root, name='api-root'),
    url(r'^api/projects/$', api_views.ProjectList.as_view(), name='api-project-list'),
    url(r'^api/project/(?P<pk>\d+)/$', api_views.ProjectDetail.as_view(), name='api-project-detail'),
    url(r'^api/project/(?P<project_pk>\d+)/template_data/$', api_views.template_data, name='api-template-data'),
    url(r'^api/user/(?P<pk>\d+)/$', api_views.UserDetail.as_view(), name='api-user-detail'),
    url(r'^api/model/(?P<pk>\d+)/$', api_views.ForecastModelDetail.as_view(), name='api-model-detail'),
    url(r'^api/forecast/(?P<pk>\d+)/$', api_views.ForecastDetail.as_view(), name='api-forecast-detail'),
    url(r'^api/forecast/(?P<forecast_pk>\d+)/data/$', api_views.forecast_data, name='api-forecast-data'),
]

# use static() to add url mapping to serve static files during development (only)
urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
