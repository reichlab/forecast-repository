from unittest.mock import patch

from botocore.exceptions import BotoCoreError
from django.test import TestCase
from rq.timeouts import JobTimeoutException

from forecast_app.models import Job
from forecast_app.models.job import job_cloud_file


class ForecastTestCase(TestCase):
    """
    """


    def test_job_cloud_file(self):
        # test that job_cloud_file() does not set job.status after yield
        job = Job.objects.create()
        job.status = Job.CLOUD_FILE_DOWNLOADED  # an arbitrary one that's not PENDING (default) or SUCCESS
        job.save()
        with patch('utils.cloud_file.download_file') as download_file_mock:
            # test when caller sets status - exposes bug where job_cloud_file() was setting to SUCCESS
            with job_cloud_file(job.pk) as (job, cloud_file_fp):
                job.status = Job.CLOUD_FILE_UPLOADED
                job.save()
            job.refresh_from_db()
            self.assertEqual(Job.CLOUD_FILE_UPLOADED, job.status)

            # test when __enter__() raises JobTimeoutException
            job.status = Job.CLOUD_FILE_DOWNLOADED
            job.save()
            download_file_mock.side_effect = JobTimeoutException('download_file_mock Exception')
            with self.assertRaises(JobTimeoutException):
                with job_cloud_file(job.pk) as (job, cloud_file_fp):
                    pass
            job.refresh_from_db()
            self.assertEqual(Job.TIMEOUT, job.status)

            # test when __enter__() raises BotoCoreError
            job.status = Job.CLOUD_FILE_DOWNLOADED
            job.save()
            download_file_mock.side_effect = BotoCoreError()
            with self.assertRaises(BotoCoreError):
                with job_cloud_file(job.pk) as (job, cloud_file_fp):
                    pass
            job.refresh_from_db()
            self.assertEqual(Job.FAILED, job.status)

            # test when __enter__() raises Exception
            job.status = Job.CLOUD_FILE_DOWNLOADED
            job.save()
            download_file_mock.side_effect = Exception('download_file_mock Exception')
            with self.assertRaises(Exception):
                with job_cloud_file(job.pk) as (job, cloud_file_fp):
                    pass
            job.refresh_from_db()
            self.assertEqual(Job.FAILED, job.status)
