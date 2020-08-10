import logging

import click
import django
import django_rq
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_repo.settings.base import S3_BUCKET_PREFIX, UPDATE_PROJECT_SCORE_CSV_FILE_CACHE_QUEUE_NAME
from forecast_app.models import Project
from forecast_app.models.score_csv_file_cache import _update_project_score_csv_file_cache_worker


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
        click.echo(f"- {project} | {score_csv_file_cache.updated_at} | {score_csv_file_cache.is_file_exists()}")


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
@click.option('--project-pk')
def update(project_pk):
    """
    A subcommand that enqueues updating one or all projects' ScoreCsvFileCache.

    :param project_pk: if a valid Project pk then only that project's models are updated. o/w defers to `model_pk` arg
    """
    projects = [get_object_or_404(Project, pk=project_pk)] if project_pk else Project.objects.all()
    click.echo("enqueuing' ScoreCsvFileCaches")
    for project in projects:
        click.echo(f"- {project}")
        queue = django_rq.get_queue(UPDATE_PROJECT_SCORE_CSV_FILE_CACHE_QUEUE_NAME)
        queue.enqueue(_update_project_score_csv_file_cache_worker, project.pk)
    click.echo("enqueuing done")


if __name__ == '__main__':
    cli()
