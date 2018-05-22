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
    projects = sorted(Project.objects.all(), key=lambda p: p.name)
    click.echo("Users: {}".format(User.objects.all()))

    if len(projects) != 0:
        click.echo("Found {} projects: {}".format(len(projects), projects))
        for project in projects:
            print_project_info(project, print_details)
    else:
        click.echo("<No Projects>")


def print_project_info(project, print_details):
    click.echo("* {} {!r} {} {}. {}".format(project, project.csv_filename, project.owner, project.model_owners.all(),
                                          project.get_summary_counts()))
    click.echo("** Targets")
    for target in project.targets.all():
        click.echo("  {}".format(target))

    click.echo("** Locations")
    for location in sorted(project.get_locations()):
        click.echo("  {}".format(location))

    click.echo("** TimeZeros")
    for timezero in project.timezeros.all():
        click.echo("  {}".format(timezero))

    if not print_details:
        return

    click.echo("** ForecastModels {}".format(project.models.all()))
    for forecast_model in project.models.all():
        click.echo("*** {}".format(forecast_model))
        for forecast in forecast_model.forecasts.order_by('time_zero'):
            click.echo("  {}".format(forecast))


if __name__ == '__main__':
    print_project_info_app()
