from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^project/(?P<pk>\d+)$', views.ProjectDetailView.as_view(), name='project-detail'),
    url(r'^model/(?P<pk>\d+)$', views.ForecastModelDetailView.as_view(), name='forecastmodel-detail'),
    url(r'^forecast/(?P<pk>\d+)$', views.ForecastDetailView.as_view(), name='forecast-detail'),
]
