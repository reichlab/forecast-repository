import io
import logging
import tempfile
import traceback
from contextlib import contextmanager

import boto3
import django_rq
from django.contrib.auth.models import User
from django.db import models
from django.db.models import BooleanField
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from django.shortcuts import get_object_or_404
from jsonfield import JSONField

from forecast_repo.settings.base import S3_UPLOAD_BUCKET_NAME
from utils.utilities import basic_str


logger = logging.getLogger(__name__)


class UploadFileJob(models.Model):
    """
    Holds information about user file uploads. Accessed by worker jobs when processing those files.
    """

    PENDING = 0
    S3_FILE_UPLOADED = 1
    QUEUED = 2
    S3_FILE_DOWNLOADED = 3
    SUCCESS = 4

    STATUS_CHOICES = (
        (PENDING, 'PENDING'),
        (S3_FILE_UPLOADED, 'S3_FILE_UPLOADED'),
        (QUEUED, 'QUEUED'),
        (S3_FILE_DOWNLOADED, 'S3_FILE_DOWNLOADED'),
        (SUCCESS, 'SUCCESS'),
    )
    status = models.IntegerField(default=PENDING, choices=STATUS_CHOICES)

    # User who submitted the job:
    user = models.ForeignKey(User, related_name='upload_file_jobs', on_delete=models.SET_NULL, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)  # when this instance was created. basically the submit date

    updated_at = models.DateTimeField(auto_now=True)  # time of last save(). basically last time status changed

    is_failed = BooleanField(default=False)

    failure_message = models.CharField(max_length=2000)  # non-empty if is_failed

    filename = models.CharField(max_length=200)  # original name of the uploaded file

    # app-specific data passed to the UploadFileJob from the request. ex: 'model_pk':
    input_json = JSONField(null=True, blank=True)

    # app-specific results from a successful completion of the upload. ex: 'forecast_pk':
    output_json = JSONField(null=True, blank=True)


    def __repr__(self):
        return str((self.pk, self.user,
                    self.status_as_str(), self.filename,
                    self.is_failed, self.failure_message[:30],
                    self.created_at, self.updated_at,
                    self.input_json, self.output_json))


    def __str__(self):  # todo
        return basic_str(self)


    def status_as_str(self):
        for status_int, status_name in self.STATUS_CHOICES:
            if self.status == status_int:
                return status_name

        return None


    def elapsed_time(self):
        return self.updated_at - self.created_at


    #
    # S3 and RQ service-specific keys/ids
    #

    # todo should UploadFileJob know about RQ and S3 at all? maybe some kind of adapter to separate concerns

    def s3_key(self):
        """
        :return: the S3 key in S3_UPLOAD_BUCKET_NAME corresponding to me
        """
        return str(self.pk)


    def rq_job_id(self):
        """
        :return: the RQ job id corresponding to me
        """
        return str(self.pk)


    def cancel_rq_job(self):
        """
        Cancels the RQ job corresponding to me.
        """
        try:
            logger.debug("cancel_rq_job(): Started: {}".format(self))
            queue = django_rq.get_queue()  # name='default'
            job = queue.fetch_job(self.rq_job_id())
            job.cancel()  # NB: just removes it from the queue and won't will kill it if is already executing
            logger.debug("cancel_rq_job(): Done: {}".format(self))
        except Exception as exc:
            logger.debug("cancel_rq_job(): Failed: {}, {}".format(exc, self))


    def delete_s3_object(self):
        """
        Deletes the S3 object corresponding to me. note that we do not log delete failures in the instance. This is b/c
        failing to delete a temporary file is not a failure to process an uploaded file. Though it's not clear when
        delete would fail but everything preceding it would succeed...

        Apps can infer this condition by looking for non-deleted S3 objects whose status != SUCCESS .
        """
        try:
            logger.debug("delete_s3_object(): Started: {}".format(self))
            s3 = boto3.resource('s3')
            s3.Object(S3_UPLOAD_BUCKET_NAME, self.s3_key()).delete()
            logger.debug("delete_s3_object(): Done: {}".format(self))
        except Exception as exc:
            logger.debug("delete_s3_object(): Failed: {}, {}".format(exc, self))


#
# the context manager for use by django_rq.enqueue() calls by views._upload_file()
#

@contextmanager
def upload_file_job_s3_file(upload_file_job_pk):
    """
    A context manager for use by django_rq.enqueue() calls by views._upload_file().

    Does the following setup:
    - get the UploadFileJob for upload_file_job_pk
    - download the corresponding S3 object/file data into a temporary file, setting the UploadFileJob's status to
      S3_FILE_DOWNLOADED
    - pass the temporary file's fp to this context's caller
    - set the UploadFileJob's status to SUCCESS

    Does this cleanup:
    - delete the S3 object, regardless of success or failure

    :param upload_file_job_pk: PK of the corresponding UploadFileJob instance
    """
    # __enter__()
    upload_file_job = get_object_or_404(UploadFileJob, pk=upload_file_job_pk)
    logger.debug("upload_file_job_s3_file(): Started. upload_file_job={}".format(upload_file_job))
    with tempfile.TemporaryFile() as s3_file_fp:  # <class '_io.BufferedRandom'>
        try:
            logger.debug("upload_file_job_s3_file(): Downloading from S3: {}, {}. upload_file_job={}"
                         .format(S3_UPLOAD_BUCKET_NAME, upload_file_job.s3_key(), upload_file_job))
            s3 = boto3.client('s3')  # using client here instead of higher-level resource b/c want to save to a fp
            s3.download_fileobj(S3_UPLOAD_BUCKET_NAME, upload_file_job.s3_key(), s3_file_fp)
            s3_file_fp.seek(0)  # yes you have to do this!
            upload_file_job.status = UploadFileJob.S3_FILE_DOWNLOADED
            upload_file_job.save()

            # make the context call. we need TextIOWrapper ('a buffered text stream over a BufferedIOBase binary
            # stream') b/c s3_file_fp is a <class '_io.BufferedRandom'>. o/w csv ->
            # 'iterator should return strings, not bytes'
            logger.debug("upload_file_job_s3_file(): Calling context. upload_file_job={}".format(upload_file_job))
            s3_file_fp = io.TextIOWrapper(s3_file_fp, 'utf-8')
            yield upload_file_job, s3_file_fp

            # __exit__()
            upload_file_job.status = UploadFileJob.SUCCESS  # yay!
            upload_file_job.save()
            logger.debug("upload_file_job_s3_file(): Done. upload_file_job={}".format(upload_file_job))
        except Exception as exc:
            upload_file_job.is_failed = True
            upload_file_job.failure_message = "FAILED_PROCESS_FILE: exc={}, traceback={}" \
                .format(exc, traceback.format_exc())
            upload_file_job.save()
            logger.debug("upload_file_job_s3_file(): FAILED_PROCESS_FILE: Error: {}. upload_file_job={}"
                         .format(exc, upload_file_job))
        finally:
            upload_file_job.delete_s3_object()  # NB: in current thread


#
# set up a signal to try to delete an UploadFileJob's S3 object before deleting the UploadFileJob
#

@receiver(pre_delete, sender=UploadFileJob)
def delete_s3_obj_for_upload_file_job(sender, instance, using, **kwargs):
    instance.cancel_rq_job()  # in case it's still in the queue
    instance.delete_s3_object()
