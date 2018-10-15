import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.views import enqueue_row_count_updates_all_projs

from forecast_app.models import Project


@click.group()
def cli():
    pass


@cli.command()
def print():
    """
    A subcommand that prints all projects' RowCountCaches in the calling thread, and therefore blocks.
    """
    click.echo("row count caches:")
    for project in Project.objects.all():
        click.echo("- {} | {} | {}"
                   .format(project, project.row_count_cache.row_count, project.row_count_cache.updated_at))


@cli.command()
def clear():
    """
    A subcommand that resets all projects' RowCountCaches in the calling thread, and therefore blocks.
    """
    click.echo("clearing all projects' row count caches")
    for project in Project.objects.all():
        click.echo("- clearing {}".format(project))
        project.row_count_cache.row_count = None
        project.row_count_cache.save()
    click.echo("clear done")


@cli.command()
def update():
    """
    A subcommand that enqueues updating all projects' RowCountCaches.
    """
    click.echo("enqueuing all projects' row count caches")
    enqueue_row_count_updates_all_projs()
    click.echo("enqueuing done")


if __name__ == '__main__':
    cli()
