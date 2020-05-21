import logging

import django_rq
from django.db import models
from django.db.models import IntegerField
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.shortcuts import get_object_or_404

from forecast_app.models import Project
from forecast_repo.settings.base import ROW_COUNT_UPDATE_QUEUE_NAME
from utils.utilities import basic_str


logger = logging.getLogger(__name__)


class RowCountCache(models.Model):
    """
    Stores a cached value of Project.get_num_forecast_rows_all_models(), which can be a time-consuming operation.
    """

    project = models.OneToOneField(Project, related_name='row_count_cache', on_delete=models.CASCADE, primary_key=True)

    # datetime at the last update. auto_now: automatically set the field to now every time the object is saved:
    updated_at = models.DateTimeField(auto_now=True)

    row_count = IntegerField(default=None, null=True)  # count at the last update. None -> has not be updated at all yet


    def __repr__(self):
        return str((self.pk, self.project, self.updated_at, self.row_count, self.updated_at))


    def __str__(self):  # todo
        return basic_str(self)


    def update_row_count_cache(self):
        """
        Updates the RowCountCache related to project. Assumes one exists - see note at create_project_caches().
        Blocks the current thread until done - which can take a while due to Project.get_num_forecast_rows_all_models() being a
        time-consuming operation. Does not need to be @transaction.atomic b/c we have only one transaction here. Note
        this does not preclude race conditions if called simultaneously by different threads. In that case, the most
        recent call wins, which is not terrible if we assume that one used the latest data.
        """
        logger.debug(f"update_row_count_cache(): calling: get_num_forecast_rows_all_models(). project={self.project}")
        num_forecast_rows = self.project.get_num_forecast_rows_all_models()
        self.row_count = num_forecast_rows  # recall last_update is auto_now
        self.save()
        logger.debug("update_row_count_cache(): done: {}. project={}".format(num_forecast_rows, self.project))


#
# utility functions
#

def enqueue_row_count_updates_all_projs():
    for project in Project.objects.all():
        queue = django_rq.get_queue(ROW_COUNT_UPDATE_QUEUE_NAME)
        queue.enqueue(_update_project_row_count_cache_worker, project.pk)


def _update_project_row_count_cache_worker(project_pk):
    """
    enqueue() helper function
    """
    project = get_object_or_404(Project, pk=project_pk)
    project.row_count_cache.update_row_count_cache()


#
# post_save signal
#

# https://stackoverflow.com/questions/1652550/can-django-automatically-create-a-related-one-to-one-model
# NB: because this is the only place a RowCountCache is created, it means existing projects will not have one added,
# which will cause problems b/c RowCountCache-related code assumes one exists
@receiver(post_save, sender=Project)
def create_project_row_count_cache(sender, instance, created, **kwargs):
    if created:
        if not hasattr(instance, 'row_count_cache'):
            RowCountCache.objects.create(project=instance)
