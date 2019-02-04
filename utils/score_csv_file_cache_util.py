import logging

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.cloud_file import is_file_exists
from forecast_app.models import Project
from forecast_app.models.score_csv_file_cache import enqueue_score_csv_file_cache_all_projs, delete_score_csv_file_cache


logging.getLogger().setLevel(logging.INFO)


@click.group()
def cli():
    pass


@cli.command()
def print():
    """
    A subcommand that prints all projects' ScoreCsvFileCache. Runs in the calling thread and therefore blocks.
    """
    click.echo("ScoreCsvFileCaches:")
    for project in Project.objects.all():
        score_csv_file_cache = project.score_csv_file_cache
        click.echo("- {} | {} | {}".format(
            project, score_csv_file_cache.updated_at, is_file_exists(score_csv_file_cache)))


@cli.command()
def clear():
    """
    A subcommand that resets all projects' ScoreCsvFileCache. Runs in the calling thread and therefore blocks.
    """
    click.echo("clearing all projects' ScoreCsvFileCaches")
    for project in Project.objects.all():
        click.echo("- clearing {}".format(project))
        delete_score_csv_file_cache(project)
    click.echo("clear done")


@cli.command()
def update():
    """
    A subcommand that enqueues updating all projects' ScoreCsvFileCache.
    """
    click.echo("enqueuing all projects' ScoreCsvFileCaches")
    enqueue_score_csv_file_cache_all_projs()
    click.echo("enqueuing done")


if __name__ == '__main__':
    cli()
