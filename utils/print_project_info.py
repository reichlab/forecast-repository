# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from django.contrib.auth.models import User
from forecast_app.models import Project


@click.command()
@click.option('--print_details', is_flag=True, default=False)
def print_project_info_app(print_details):
    projects = Project.objects.all()
    click.echo("Users: {}".format(User.objects.all()))

    if len(projects) != 0:
        print("Found {} projects: {}".format(len(projects), projects))
        for project in projects:
            print_project_info(project, print_details)
    else:
        print("<No Projects>")


def print_project_info(project, print_details):
    print('*', project, repr(project.name), repr(project.csv_filename), project.owner, project.model_owners.all())
    if not print_details:
        return

    print('** Targets')
    for target in project.targets.all():
        print('  ', target)

    print('** TimeZeros')
    for timezero in project.timezeros.all():
        print('  ', timezero)

    print('** ForecastModels', project.models.all())
    for forecast_model in project.models.all():
        print('***', forecast_model)
        for forecast in forecast_model.forecasts.order_by('time_zero'):
            print('  ', forecast)


if __name__ == '__main__':
    print_project_info_app()
