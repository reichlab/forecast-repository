from django.shortcuts import render

from forecast_app.models import Project


def index(request):
    """
    View function for home page of site.
    """
    projects = Project.objects.all()
    return render(
        request,
        'index.html',
        context={'projects': projects},
    )
