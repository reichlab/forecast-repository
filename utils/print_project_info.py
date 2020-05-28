# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from django.contrib.auth.models import User
from forecast_app.models import Project


@click.command()
@click.argument('verbosity', type=click.Choice(['1', '2', '3', '4']), default='1')
def main(verbosity):
    """
    :param verbosity: increasing from 1 (minimal verbosity) to 3 (maximal)
    """
    projects = Project.objects.order_by('name')
    click.echo(f"Users: {User.objects.all()}")

    if len(projects) != 0:
        click.echo(f"Found {len(projects)} projects: {projects}")
        for project in projects:
            print_project_info(project, int(verbosity))
    else:
        click.echo("<No Projects>")


def print_project_info(project, verbosity):
    # verbosity == 1
    click.echo(f"\n\n* {project}. truth: # rows={project.truth_data_qs().count()}, "
               f"updated_at={project.truth_updated_at}. owner={project.owner}, "
               f"model_owners={project.model_owners.all()}, (num_models, num_forecasts, num_rows): "
               f"{project.get_summary_counts()}")
    if verbosity == 1:
        return

    # verbosity == 2
    click.echo(f"\n** Targets ({project.targets.count()})")
    for target in project.targets.all():
        click.echo(f"- {target}")

    click.echo(f"\n** Units ({project.units.count()})")
    for unit in project.units.all().order_by('name'):
        click.echo(f"- {unit}")

    click.echo(f"\n** TimeZeros ({project.timezeros.count()})")
    for timezero in project.timezeros.all():
        click.echo(f"- {timezero}")

    if verbosity == 2:
        return

    # verbosity == 3
    click.echo(f"\n** ForecastModels ({project.models.count()})")
    for forecast_model in project.models.all():
        if verbosity == 3:
            click.echo(f"- {forecast_model}")
        else:
            click.echo(f"*** {forecast_model} ({forecast_model.forecasts.count()} forecasts)")
        if verbosity == 4:
            for forecast in forecast_model.forecasts.order_by('time_zero'):
                click.echo(f"- {forecast}: {forecast.get_num_rows()} rows")


if __name__ == '__main__':
    main()
