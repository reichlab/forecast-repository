import logging

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_repo.settings.base import S3_BUCKET_PREFIX
from utils.cloud_file import is_file_exists
from forecast_app.models import Project
from forecast_app.models.score_csv_file_cache import enqueue_score_csv_file_cache_all_projs


logging.getLogger().setLevel(logging.INFO)


@click.group()
def cli():
    pass


@cli.command(name="print")
def print_caches():
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
    click.echo("clearing all projects' ScoreCsvFileCaches. S3_BUCKET_PREFIX={}".format(S3_BUCKET_PREFIX))
    for project in Project.objects.all():
        click.echo("- clearing {}".format(project))
        project.score_csv_file_cache.delete_score_csv_file_cache()
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
