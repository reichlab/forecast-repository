# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from django.contrib.auth.models import User
from forecast_app.models import Project


@click.command()
@click.argument('verbosity', type=click.Choice(['1', '2', '3']), default='1')
def main(verbosity):
    """
    :param verbosity: increasing from 1 (minimal verbosity) to 3 (maximal)
    """
    projects = Project.objects.order_by('name')
    click.echo("Users: {}".format(User.objects.all()))

    if len(projects) != 0:
        click.echo("Found {} projects: {}".format(len(projects), projects))
        for project in projects:
            print_project_info(project, int(verbosity))
    else:
        click.echo("<No Projects>")


def print_project_info(project, verbosity):
    click.echo("* {} {!r} {}. {} {}. {}".format(project, project.csv_filename, project.truth_data_qs().count(),
                                            project.owner, project.model_owners.all(), project.get_summary_counts()))
    if verbosity == 1:
        return

    click.echo("** Targets")
    for target in project.targets.all():
        click.echo("  {}".format(target))

    click.echo("** Locations")
    for location in sorted(project.get_locations()):
        click.echo("  {}".format(location))

    click.echo("** TimeZeros")
    for timezero in project.timezeros.all():
        click.echo("  {}".format(timezero))

    if verbosity == 2:
        return

    click.echo("** ForecastModels {}".format(project.models.all()))
    for forecast_model in project.models.all():
        click.echo("*** {}".format(forecast_model))
        for forecast in forecast_model.forecasts.order_by('time_zero'):
            click.echo("  {}".format(forecast))


if __name__ == '__main__':
    main()
