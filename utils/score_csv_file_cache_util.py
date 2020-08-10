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


logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command(name="print")
def print_caches():
    """
    A subcommand that prints all projects' ScoreCsvFileCache. Runs in the calling thread and therefore blocks.
    """
    logger.info("ScoreCsvFileCaches:")
    for project in Project.objects.all():
        score_csv_file_cache = project.score_csv_file_cache
        logger.info(f"- {project} | {score_csv_file_cache.updated_at} | {score_csv_file_cache.is_file_exists()}")


@cli.command()
def clear():
    """
    A subcommand that resets all projects' ScoreCsvFileCache. Runs in the calling thread and therefore blocks.
    """
    logger.info("clearing all projects' ScoreCsvFileCaches. S3_BUCKET_PREFIX={}".format(S3_BUCKET_PREFIX))
    for project in Project.objects.all():
        logger.info("- clearing {}".format(project))
        project.score_csv_file_cache.delete_score_csv_file_cache()
    logger.info("clear done")


@cli.command()
@click.option('--project-pk')
@click.option('--no-enqueue', is_flag=True, default=False)
def update(project_pk, no_enqueue):
    """
    A subcommand that enqueues updating one or all projects' ScoreCsvFileCache.

    :param project_pk: if a valid Project pk then only that project's models are updated. o/w defers to `model_pk` arg
    :param no_enqueue: controls whether the update will be immediate in the calling thread (blocks), or enqueued for RQ
    """
    projects = [get_object_or_404(Project, pk=project_pk)] if project_pk else Project.objects.all()
    logger.info("updating ScoreCsvFileCaches")
    for project in projects:
        if no_enqueue:
            logger.info(f"- updating (no enqueue): {project}")
            project.score_csv_file_cache.update_score_csv_file_cache()
        else:
            logger.info(f"- enqueuing: {project}")
            queue = django_rq.get_queue(UPDATE_PROJECT_SCORE_CSV_FILE_CACHE_QUEUE_NAME)
            queue.enqueue(_update_project_score_csv_file_cache_worker, project.pk)
    logger.info("update done")


if __name__ == '__main__':
    cli()
