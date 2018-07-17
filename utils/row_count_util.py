import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Project


@click.group()
def cli():
    pass


@cli.command()
def print():
    """
    A subcommand that prints all projects' RowCountCaches.
    """
    click.echo("row count caches:")
    for project in Project.objects.all():
        click.echo("- {}: {} @ {}"
                   .format(project, project.row_count_cache.row_count, project.row_count_cache.last_update))
        project.row_count_cache.row_count = None


@cli.command()
def clear():
    """
    A subcommand that resets all projects' RowCountCaches.
    """
    click.echo("clearing all projects' RowCountCache")
    for project in Project.objects.all():
        click.echo("- clearing {}".format(project))
        project.row_count_cache.row_count = None
        project.row_count_cache.save()
    click.echo("clear done")


@cli.command()
def update():
    """
    A subcommand that updates all projects' RowCountCaches in the calling thread, and therefore blocks.
    """
    click.echo("updating all projects' RowCountCache")
    for project in Project.objects.all():
        click.echo("- updating {}...".format(project))
        num_forecast_rows = project.update_row_count_cache()
        click.echo("  -> {} rows".format(num_forecast_rows))
    click.echo("update done")


if __name__ == '__main__':
    cli()
