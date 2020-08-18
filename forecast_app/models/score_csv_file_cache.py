import csv
import io
import logging

import django_rq
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError, ClientError, ConnectionClosedError
from django.db import models, transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.shortcuts import get_object_or_404

from forecast_app.models import Project
from forecast_repo.settings.base import UPDATE_PROJECT_SCORE_CSV_FILE_CACHE_QUEUE_NAME
from utils.cloud_file import is_file_exists
from utils.utilities import basic_str


logger = logging.getLogger(__name__)


class ScoreCsvFileCache(models.Model):
    """
    Stores a cached value of a Project's score csv file, which can be a time-consuming operation.
    """

    project = models.OneToOneField(Project, related_name='score_csv_file_cache', on_delete=models.CASCADE,
                                   primary_key=True)

    # datetime at the last update. auto_now: automatically set the field to now every time the object is saved:
    updated_at = models.DateTimeField(auto_now=True)


    def __repr__(self):
        return str((self.pk, self.project, str(self.updated_at)))


    def __str__(self):  # todo
        return basic_str(self)


    def is_file_exists(self):
        """
        :return: convenience method for cloud_file.is_file_exists(). returns False if there was an S3 error
        """
        try:
            return is_file_exists(self)[0]  # might raise S3 exception
        except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError):
            return False


    def delete_score_csv_file_cache(self):
        """
        Updates the ScoreCsvFileCache file related to project. Runs in the calling thread and therefore blocks.
        """
        # imported here so that test_score_csv_file_cache() can patch via mock:
        from utils.cloud_file import delete_file


        try:
            delete_file(self)
        except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError) as aws_exc:
            logger.error(f"delete_score_csv_file_cache(): AWS error: {aws_exc!r}. ScoreCsvFileCache={self}")
        self.save()  # updates updated_at


    @transaction.atomic
    def update_score_csv_file_cache(self):
        """
        Updates me. Runs in the calling thread and therefore blocks.
        """
        # imported here so that test_score_csv_file_cache() can patch via mock:
        from utils.cloud_file import upload_file

        # avoid circular imports. also, caused manage.py to hang:
        from forecast_app.api_views import csv_rows_for_project_score_data


        logger.debug(f"update_score_csv_file_cache(): 1/4 getting csv response. {self}")
        rows = csv_rows_for_project_score_data(self.project)

        try:
            logger.debug(f"update_score_csv_file_cache(): 2/4 deleting. {self}")
            self.delete_score_csv_file_cache()

            # see note in `api_views._query_forecasts_worker()` re: "we need a BytesIO for upload_file()"
            logger.debug(f"update_score_csv_file_cache(): 3/4 uploading. {self}")
            with io.BytesIO() as bytes_io:
                text_io_wrapper = io.TextIOWrapper(bytes_io, 'utf-8', newline='')
                csv.writer(text_io_wrapper).writerows(rows)
                text_io_wrapper.flush()
                bytes_io.seek(0)
                upload_file(self, bytes_io)  # might raise S3 exception
            self.save()  # updates updated_at

            logger.debug(f"update_score_csv_file_cache(): 4/4 done. {self}")
        except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError) as aws_exc:
            logger.error(f"update_score_csv_file_cache(): AWS error: {aws_exc!r}. ScoreCsvFileCache={self}")
        except Exception as ex:
            logger.error(f"update_score_csv_file_cache(): error: {ex!r}. ScoreCsvFileCache={self}")


#
# utility functions
#

def enqueue_score_csv_file_cache_all_projs():
    for project in Project.objects.all():
        queue = django_rq.get_queue(UPDATE_PROJECT_SCORE_CSV_FILE_CACHE_QUEUE_NAME)
        queue.enqueue(_update_project_score_csv_file_cache_worker, project.pk)


def _update_project_score_csv_file_cache_worker(project_pk):
    """
    enqueue() helper function
    """
    project = get_object_or_404(Project, pk=project_pk)
    try:
        project.score_csv_file_cache.update_score_csv_file_cache()
    except Exception as ex:
        logger.error(f"_update_project_score_csv_file_cache_worker(): Job timeout: {ex!r}. project={project}")


#
# post_save signal
#

# see elsewhere re: https://stackoverflow.com/questions/1652550/can-django-automatically-create-a-related-one-to-one-model
@receiver(post_save, sender=Project)
def create_project_score_csv_file_cache(sender, instance, created, **kwargs):
    if created:
        if not hasattr(instance, 'score_csv_file_cache'):
            ScoreCsvFileCache.objects.create(project=instance)
