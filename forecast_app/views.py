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


def about(request):
    return render(request, 'about.html')


def project_visualizations(request, pk):
    """
    View function to render various visualizations for a particular project.

    :param request:
    :param pk:
    :return:
    """
    project = Project.objects.get(pk=pk)
    return render(
        request,
        'project_visualizations.html',
        context={'project': project},
    )


class ProjectDetailView(DetailView):
    model = Project


class ForecastModelDetailView(DetailView):
    model = ForecastModel


class ForecastDetailView(DetailView):
    model = Forecast
