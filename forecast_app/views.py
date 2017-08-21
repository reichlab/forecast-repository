from django.shortcuts import render
from django.views.generic import DetailView

from forecast_app.models import Project, ForecastModel, Forecast


def index(request):
    projects = Project.objects.all()
    return render(
        request,
        'index.html',
        context={'projects': projects},
    )


class ProjectDetailView(DetailView):
    model = Project


class ForecastModelDetailView(DetailView):
    model = ForecastModel


class ForecastDetailView(DetailView):
    model = Forecast
