import logging

from anymail.exceptions import AnymailError
from django.core.mail import send_mail


logger = logging.getLogger(__name__)


def send_notification_email(address, subject, message):
    """
    Sends an email per the passed args. Sends it immediately and in the current thread. In the future may enqueue
    the sending.
    """
    try:
        send_mail(subject, message, None, [address])  # from_email = DEFAULT_FROM_EMAIL
        logger.info("send_notification_email(): sent a message to: {}, subject: '{}'".format(address, subject))
    except AnymailError as ae:
        logger.error("send_notification_email(): error: {}".format(ae))
