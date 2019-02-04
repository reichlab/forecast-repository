import logging

import django_rq
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.shortcuts import get_object_or_404

from forecast_app.models import Project
from utils.utilities import basic_str


logger = logging.getLogger(__name__)


class ScoreCsvFileCache(models.Model):
    """
    Stores a cached value of a Project's score csv file, which can be a time-consuming operation.
    """

    project = models.OneToOneField(
        Project,
        related_name='score_csv_file_cache',
        on_delete=models.CASCADE,
        primary_key=True)

    # datetime at the last update. auto_now: automatically set the field to now every time the object is saved:
    updated_at = models.DateTimeField(auto_now=True)


    def __repr__(self):
        return str((self.pk, self.project, self.updated_at))


    def __str__(self):  # todo
        return basic_str(self)


#
# utility functions
#

def enqueue_score_csv_file_cache_all_projs():
    for project in Project.objects.all():
        django_rq.enqueue(_update_project_score_csv_file_cache, project.pk)


def _update_project_score_csv_file_cache(project_pk):
    """
    Enqueue helper function.
    """
    project = get_object_or_404(Project, pk=project_pk)
    update_score_csv_file_cache(project)


def delete_score_csv_file_cache(project):
    """
    Updates the ScoreCsvFileCache file related to project. Runs in the calling thread and therefore blocks.
    """
    # imported here so that test_score_csv_file_cache() can patch via mock:
    from utils.cloud_file import delete_file


    delete_file(project.score_csv_file_cache)
    project.score_csv_file_cache.save()  # updates updated_at


def update_score_csv_file_cache(project):
    """
    Updates the ScoreCsvFileCache file related to project. Runs in the calling thread and therefore blocks.
    """
    # imported here so that test_score_csv_file_cache() can patch via mock:
    from utils.cloud_file import upload_file

    # avoid circular imports. also, caused manage.py to hang:
    from forecast_app.api_views import csv_response_for_project_score_data


    score_csv_file_cache = project.score_csv_file_cache
    logger.debug("update_score_csv_file_cache(): deleting: {}".format(score_csv_file_cache))
    delete_score_csv_file_cache(project)

    response = csv_response_for_project_score_data(project)
    logger.debug("update_score_csv_file_cache(): uploading")
    upload_file(score_csv_file_cache, response.content)
    score_csv_file_cache.save()  # updates updated_at
    logger.debug("update_score_csv_file_cache(): done")


#
# post_save signal
#

# see elsewhere re: https://stackoverflow.com/questions/1652550/can-django-automatically-create-a-related-one-to-one-model
@receiver(post_save, sender=Project)
def create_project_score_csv_file_cache(sender, instance, created, **kwargs):
    if created:
        if not hasattr(instance, 'score_csv_file_cache'):
            ScoreCsvFileCache.objects.create(project=instance)
