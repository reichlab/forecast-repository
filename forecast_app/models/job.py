import io
import logging
import tempfile
from contextlib import contextmanager

import django_rq
from django.contrib.auth.models import User
from django.db import models
from django.db.models.signals import pre_delete, post_save
from django.dispatch import receiver
from django.shortcuts import get_object_or_404
from django.template import Template, Context
from jsonfield import JSONField

from forecast_repo.settings.base import UPLOAD_FILE_QUEUE_NAME
from utils.cloud_file import delete_file, download_file
from utils.utilities import basic_str


logger = logging.getLogger(__name__)


class Job(models.Model):
    """
    Holds information about user file uploads. Accessed by worker jobs when processing those files.
    """

    PENDING = 0
    CLOUD_FILE_UPLOADED = 1
    QUEUED = 2
    CLOUD_FILE_DOWNLOADED = 3
    SUCCESS = 4
    FAILED = 5

    STATUS_CHOICES = (
        (PENDING, 'PENDING'),
        (CLOUD_FILE_UPLOADED, 'CLOUD_FILE_UPLOADED'),
        (QUEUED, 'QUEUED'),
        (CLOUD_FILE_DOWNLOADED, 'CLOUD_FILE_DOWNLOADED'),
        (SUCCESS, 'SUCCESS'),
        (FAILED, 'FAILED'),
    )
    status = models.IntegerField(default=PENDING, choices=STATUS_CHOICES)

    # User who submitted the job:
    user = models.ForeignKey(User, related_name='jobs', on_delete=models.SET_NULL, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)  # when this instance was created. basically the submit date
    updated_at = models.DateTimeField(auto_now=True)  # time of last save(). basically last time status changed
    failure_message = models.TextField()  # non-empty message if status == FAILED
    filename = models.TextField()  # original name of the uploaded file

    # app-specific data passed to the Job from the request. ex: 'model_pk':
    input_json = JSONField(null=True, blank=True)

    # app-specific results from a successful completion of the upload. ex: 'forecast_pk':
    output_json = JSONField(null=True, blank=True)


    def __repr__(self):
        return str((self.pk, self.user,
                    self.status_as_str(), self.filename,
                    self.is_failed(), self.failure_message[:30],
                    self.created_at, self.updated_at,
                    self.input_json, self.output_json))


    def __str__(self):  # todo
        return basic_str(self)


    def is_failed(self):
        return self.status == Job.FAILED


    def status_as_str(self):
        return Job.status_int_as_str(self.status)


    def status_color(self):
        """
        Yes, this is mixing model and view code, but it makes it easy for templates to color-code status, so we fudge.

        :return: a color for my status - https://getbootstrap.com/docs/4.0/utilities/colors/
        """
        return {Job.PENDING: 'text-primary',
                Job.CLOUD_FILE_UPLOADED: 'text-secondary',
                Job.QUEUED: 'text-secondary',
                Job.CLOUD_FILE_DOWNLOADED: 'text-secondary',
                Job.SUCCESS: 'text-success',
                Job.FAILED: 'text-danger'}[self.status]


    @classmethod
    def status_int_as_str(cls, the_status_int):
        for status_int, status_name in cls.STATUS_CHOICES:
            if status_int == the_status_int:
                return status_name

        return '!?'


    def elapsed_time(self):
        return self.updated_at - self.created_at


    #
    # RQ service-specific functions
    #

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
            logger.debug(f"cancel_rq_job(): Started: {self}")
            queue = django_rq.get_queue(UPLOAD_FILE_QUEUE_NAME)
            job = queue.fetch_job(self.rq_job_id())
            job.cancel()  # NB: just removes it from the queue and won't will kill it if is already executing
            logger.debug(f"cancel_rq_job(): done: {self}")
        except Exception as ex:
            logger.debug(f"cancel_rq_job(): Failed: {ex}, {self}")


#
# the context manager for use by django_rq.enqueue() calls by views._upload_file()
#

@contextmanager
def job_cloud_file(job_pk):
    """
    A context manager for use by django_rq.enqueue() calls by views._upload_file().

    Does the following setup:
    - get the Job for job_pk
    - download the corresponding cloud file data into a temporary file, setting the Job's status to
      CLOUD_FILE_DOWNLOADED
    - pass the temporary file's fp to this context's caller
    - set the Job's status to SUCCESS

    Does this cleanup:
    - delete the cloud object, regardless of success or failure

    :param job_pk: PK of the corresponding Job instance
    """
    # __enter__()
    job = get_object_or_404(Job, pk=job_pk)
    logger.debug(f"job_cloud_file(): Started. job={job}")
    with tempfile.TemporaryFile() as cloud_file_fp:  # <class '_io.BufferedRandom'>
        try:
            logger.debug(f"job_cloud_file(): Downloading from cloud. job={job}")
            download_file(job, cloud_file_fp)
            cloud_file_fp.seek(0)  # yes you have to do this!
            job.status = Job.CLOUD_FILE_DOWNLOADED
            job.save()

            # make the context call. we need TextIOWrapper ('a buffered text stream over a BufferedIOBase binary
            # stream') b/c cloud_file_fp is a <class '_io.BufferedRandom'>. o/w csv ->
            # 'iterator should return strings, not bytes'
            logger.debug(f"job_cloud_file(): Calling context. job={job}")
            cloud_file_fp = io.TextIOWrapper(cloud_file_fp, 'utf-8')
            yield job, cloud_file_fp

            # __exit__()
            job.status = Job.SUCCESS  # yay!
            job.save()
            logger.debug(f"job_cloud_file(): Done. job={job}")
        except Exception as ex:
            job.status = Job.FAILED
            job.failure_message = f"Failed to process the file: '{ex.args[0]}'"
            job.save()
            logger.error(f"job_cloud_file(): FAILED_PROCESS_FILE: Error: {ex}. "
                         f"job={job}")
        finally:
            delete_file(job)  # NB: in current thread


#
# set up a signal to try to delete an Job's S3 object before deleting the Job
#

@receiver(pre_delete, sender=Job)
def delete_file_for_job(sender, instance, using, **kwargs):
    instance.cancel_rq_job()  # in case it's still in the queue
    delete_file(instance)


#
# set up a signal to try notifying the user of SUCCESS or FAILURE
#

@receiver(post_save, sender=Job)
def send_notification_for_job(sender, instance, using, **kwargs):
    # imported here so that test_email_notification() can patch via mock:
    from forecast_app.notifications import send_notification_email


    if instance.status == Job.FAILED:
        address, subject, message = address_subject_message_for_job(instance)
        send_notification_email(address, subject, message)


def address_subject_message_for_job(job):
    """
    An email notification helper function that constructs an email subject line and body for the passed job.

    :param job: an Job
    :return: email_address, subject, message
    """
    subject = "Job #{} result: {}".format(job.pk, job.status_as_str())
    message_template_str = """A <a href="zoltardata.com">Zoltar</a> user with your email address uploaded a file with this result:
<ul>
    <li>Job ID: {{job.pk}}</li>
    <li>Status: {{job.status_as_str}}</li>
    <li>User: {{job.user}}</li>
    <li>Filename: {% if job.filename %}{{ job.filename }}{% else %}(No filename){% endif %}</li>
    <li>Created_at: {{job.created_at}}</li>
    <li>Updated_at: {{job.updated_at}}</li>
    <li>Failure_message: {% if job.failure_message %}{{ job.failure_message }}{% else %}(No message){% endif %}</li>
    <li>Input_json: {{job.input_json}}</li>
    <li>Output_json: {{job.output_json}}</li>
</ul>

Thanks! -- Zoltar"""
    message_template = Template(message_template_str)
    message = message_template.render(Context({'job': job}))
    return job.user.email, subject, message
