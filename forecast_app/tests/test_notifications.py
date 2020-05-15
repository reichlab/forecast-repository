from unittest.mock import patch

from django.test import TestCase

from forecast_app.models.job import Job, address_subject_message_for_job
from utils.utilities import get_or_create_super_po_mo_users


class NotificationTestCase(TestCase):
    """
    """


    def test_email_notification(self):
        # test that Job.save() calls send_notification_email() for Job.FAILED, but not any others
        # including Job.SUCCESS
        _, _, mo_user, mo_user_password = get_or_create_super_po_mo_users(is_create_super=False)
        self.client.login(username=mo_user.username, password=mo_user_password)
        mo_user.email = "user@example.com"

        with patch('forecast_app.notifications.send_notification_email') as send_email_mock:
            # test Job.PENDING
            Job.objects.create(user=mo_user, status=Job.PENDING)
            send_email_mock.assert_not_called()

            # test Job.SUCCESS
            send_email_mock.reset_mock()
            Job.objects.create(user=mo_user, status=Job.SUCCESS)
            send_email_mock.assert_not_called()

            # test Job.FAILED
            send_email_mock.reset_mock()
            job = Job.objects.create(user=mo_user, status=Job.FAILED)
            address, subject, message = address_subject_message_for_job(job)
            send_email_mock.assert_called_once_with(address, subject, message)
