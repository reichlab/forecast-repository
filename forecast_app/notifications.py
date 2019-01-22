def send_notification_email(address, subject, message):
    """
    Sends an email per the passed args. Sends it immediately and in the current thread. In the future may enqueue
    the sending.
    """
    try:
        pass  # todo xx actual email call
        print('xx sent email', address, subject, message)
    except Exception as exc:
        print('xx failed to send email', address, subject, message, exc)
