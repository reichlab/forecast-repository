from unittest.mock import patch

from django.test import TestCase

from forecast_app.models.upload_file_job import UploadFileJob, address_subject_message_for_upload_file_job
from utils.make_cdc_flu_contests_project import get_or_create_super_po_mo_users


class NotificationTestCase(TestCase):
    """
    """


    def test_email_notification(self):
        # test that UploadFileJob.save() calls send_notification_email() for UploadFileJob.FAILED, but not any others
        # including UploadFileJob.SUCCESS
        _, _, mo_user, mo_user_password = get_or_create_super_po_mo_users(create_super=False)
        self.client.login(username=mo_user.username, password=mo_user_password)
        mo_user.email = "user@example.com"

        with patch('forecast_app.notifications.send_notification_email') as send_email_mock:
            # test UploadFileJob.PENDING
            UploadFileJob.objects.create(user=mo_user, status=UploadFileJob.PENDING)
            send_email_mock.assert_not_called()

            # test UploadFileJob.SUCCESS
            send_email_mock.reset_mock()
            UploadFileJob.objects.create(user=mo_user, status=UploadFileJob.SUCCESS)
            send_email_mock.assert_not_called()

            # test UploadFileJob.FAILED
            send_email_mock.reset_mock()
            upload_file_job = UploadFileJob.objects.create(user=mo_user, status=UploadFileJob.FAILED)
            address, subject, message = address_subject_message_for_upload_file_job(upload_file_job)
            send_email_mock.assert_called_once_with(address, subject, message)
