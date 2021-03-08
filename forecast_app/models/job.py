import io
import logging
import tempfile
from contextlib import contextmanager

import django_rq
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError, ClientError, ConnectionClosedError
from django.contrib.auth.models import User
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.shortcuts import get_object_or_404
from django.template import Template, Context
from jsonfield import JSONField
from rq.timeouts import JobTimeoutException

from forecast_repo.settings.base import UPLOAD_FILE_QUEUE_NAME
from utils.cloud_file import delete_file
from utils.utilities import basic_str


logger = logging.getLogger(__name__)

#
# Job "types"
#
# Used by methods that create Jobs to save a 'type' key in Job.input_json. There is nothing formal about this; it's
# currently simply a convention for visual idenfication by people. We considered adding a Job.type, but we could not
# justify the overhead of having to change the class (and the resulting migration) every time new types are added (an
# unknown frequency, BTW). That is, informal means we can add new types without having to migrate. By convention the
# members are verbs.
#

JOB_TYPE_QUERY_FORECAST = 'QUERY_FORECAST'
JOB_TYPE_QUERY_TRUTH = 'JOB_TYPE_QUERY_TRUTH'
JOB_TYPE_DELETE_FORECAST = 'DELETE_FORECAST'
JOB_TYPE_UPLOAD_TRUTH = 'UPLOAD_TRUTH'
JOB_TYPE_UPLOAD_FORECAST = 'UPLOAD_FORECAST'


#
# Job
#

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
    TIMEOUT = 6

    STATUS_CHOICES = (
        (PENDING, 'PENDING'),
        (CLOUD_FILE_UPLOADED, 'CLOUD_FILE_UPLOADED'),
        (QUEUED, 'QUEUED'),
        (CLOUD_FILE_DOWNLOADED, 'CLOUD_FILE_DOWNLOADED'),
        (SUCCESS, 'SUCCESS'),
        (FAILED, 'FAILED'),
        (TIMEOUT, 'TIMEOUT'),
    )
    status = models.IntegerField(default=PENDING, choices=STATUS_CHOICES)

    # User who submitted the job:
    user = models.ForeignKey(User, related_name='jobs', on_delete=models.SET_NULL, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)  # when this instance was created. basically the submit date
    updated_at = models.DateTimeField(auto_now=True)  # time of last save(). basically last time status changed
    failure_message = models.TextField()  # non-empty message if status == FAILED

    # app-specific data passed to the Job from the request. ex: 'model_pk':
    input_json = JSONField(null=True, blank=True)

    # app-specific results from a successful completion of the upload. ex: 'forecast_pk':
    output_json = JSONField(null=True, blank=True)


    def __repr__(self):
        return str((self.pk, self.user, self.status_as_str(),
                    self.is_failed(), self.failure_message[:30],
                    str(self.created_at), str(self.updated_at),
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
                Job.FAILED: 'text-danger',
                Job.TIMEOUT: 'text-danger',
                }[self.status]


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
    A context manager for use by django_rq.enqueue() calls by views._upload_file(). It wraps the caller by first
    downloading the file corresponding to `job_pk` and then returning a fp to it. Cleans up by deleting the job's file.

    :param job_pk: PK of the corresponding Job instance
    """
    # imported here so that tests can patch via mock:
    from utils.cloud_file import download_file

    # __enter__()
    job = get_object_or_404(Job, pk=job_pk)
    logger.debug(f"job_cloud_file(): 1/4 Started. job={job}")
    with tempfile.TemporaryFile() as cloud_file_fp:  # <class '_io.BufferedRandom'>
        try:
            logger.debug(f"job_cloud_file(): 2/4 Downloading from cloud. job={job}")
            download_file(job, cloud_file_fp)
            cloud_file_fp.seek(0)  # yes you have to do this!
            job.status = Job.CLOUD_FILE_DOWNLOADED
            job.save()

            # make the context call. we need TextIOWrapper ('a buffered text stream over a BufferedIOBase binary
            # stream') b/c cloud_file_fp is a <class '_io.BufferedRandom'>. o/w csv ->
            # 'iterator should return strings, not bytes'
            logger.debug(f"job_cloud_file(): 3/4 Calling context. job={job}")
            cloud_file_fp = io.TextIOWrapper(cloud_file_fp, 'utf-8')
            yield job, cloud_file_fp

            # __exit__(). NB: does NOT do: `job.status = Job.SUCCESS` - that's left to the caller
            logger.debug(f"job_cloud_file(): Done. job={job}")
        except JobTimeoutException as jte:
            job.status = Job.TIMEOUT
            job.save()
            logger.error(f"job_cloud_file(): error: {jte!r}. job={job}")
            raise jte
        except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError) as aws_exc:
            job.status = Job.FAILED
            job.failure_message = f"job_cloud_file(): error: {aws_exc!r}"
            job.save()
            logger.error(job.failure_message + f". job={job}")
            raise aws_exc
        except Exception as ex:
            job.status = Job.FAILED
            job.failure_message = f"job_cloud_file(): error: {ex!r}"
            job.save()
            logger.error(job.failure_message + f". job={job}")
            raise ex
        finally:
            delete_file(job)  # NB: in current thread


#
# set up a signal to try notifying the user of SUCCESS or FAILURE
#

@receiver(post_save, sender=Job)
def send_notification_for_job(sender, instance, using, **kwargs):
    # imported here so that tests can patch via mock:
    from forecast_app.notifications import send_notification_email


    if instance.status == Job.FAILED:
        address_subject_message = address_subject_message_for_job(instance)
        if address_subject_message:
            address, subject, message = address_subject_message
            send_notification_email(address, subject, message)


def address_subject_message_for_job(job):
    """
    An email notification helper function that constructs an email subject line and body for the passed job.

    :param job: an Job
    :return: email_address, subject, message. return None if not possible to send due to no job.user, or no user email
    """
    if (not job.user) or (not job.user.email):
        return None

    subject = "Job #{} result: {}".format(job.pk, job.status_as_str())
    message_template_str = """A <a href="zoltardata.com">Zoltar</a> user with your email address uploaded a file with this result:
<ul>
    <li>Job ID: {{job.pk}}</li>
    <li>Status: {{job.status_as_str}}</li>
    <li>User: {{job.user}}</li>
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
