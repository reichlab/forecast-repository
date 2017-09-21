from django.shortcuts import render
from django.views.generic import DetailView

from forecast_app.models import Project, ForecastModel, Forecast
from utils.utilities import mean_abs_error_rows_for_project


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
        # todo xx pull season_start_year and location from somewhere:
        context={'project': project,
                 'mean_abs_error_rows': mean_abs_error_rows_for_project(project, 2016, 'US National')},
    )


class ProjectDetailView(DetailView):
    model = Project


class ForecastModelDetailView(DetailView):
    model = ForecastModel


class ForecastDetailView(DetailView):
    model = Forecast
