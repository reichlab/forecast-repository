# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Project


@click.command()
def print_project_info_app():
    projects = Project.objects.all()
    if len(projects) != 0:
        for project in projects:
            print_project_info(project)
    else:
        print("<No Projects>")


def print_project_info(project):
    print('*', project, repr(project.name), repr(project.description), repr(project.home_url), '.',
          repr(project.csv_filename), project.config_dict)
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


if __name__ == '__main__':
    print_project_info_app()
