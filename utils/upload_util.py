import logging
from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()
from utils.cloud_file import upload_file
from django.contrib.auth.models import User

from forecast_app.models import Job


logger = logging.getLogger(__name__)


@click.command()
@click.argument('file', type=click.Path(file_okay=True, exists=True))
@click.argument('username', type=click.STRING, required=False)
def upload_file_app(file, username):
    """
    An app that uploads a file to S3, associating it with a new Job owned by `username`. Note that due to how
    `_download_job_data_request()` currently works, the downloaded file has the extension and content type "csv". Users
    may need to change the extension to match that of the original file.

    :param file: path (str) to an existing file
    :param username: valid user name (str)
    """
    file = Path(file)
    logger.debug(f"upload_file_app(): file={file}, username={username!r}")
    try:
        user = User.objects.get(username=username)  # raises if not found

        logger.debug(f"upload_file_app(): creating job. user={user}")
        job = Job.objects.create(user=user)  # status = PENDING
        job.input_json = {'type': 'UPLOAD_FILE', 'file': file.name}
        job.save()

        logger.debug(f"upload_file_app(): uploading file. job={job}")
        with open(file, 'rb') as fp:
            upload_file(job, fp)  # might raise S3 exception
            job.status = Job.SUCCESS
            job.save()

        logger.debug(f"upload_file_app(): done. job={job}")
    except Exception as exc:
        # todo if job: job.status = Job.FAILED
        logger.error(f"upload_file_app(): error. exc={exc!r}")


if __name__ == '__main__':
    upload_file_app()
