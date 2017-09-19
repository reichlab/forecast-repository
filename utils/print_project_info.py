# set up django. must be done before loading models. requires: os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
import django
import os


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
django.setup()

from forecast_app.models import Project


def print_project_info(project):
    print('*', project, project.name, project.description, project.url, '.', project.config_dict)
    print('** Targets')
    for target in project.target_set.all():
        print('  ', target)

    print('** TimeZeros')
    for timezero in project.timezero_set.all():
        print('  ', timezero)

    print('** ForecastModels', project.forecastmodel_set.all())
    for forecast_model in project.forecastmodel_set.all():
        print('**', forecast_model)
        for forecast in forecast_model.forecast_set.order_by('time_zero'):
            print('  ', forecast)


for project in Project.objects.all():
    print_project_info(project)
