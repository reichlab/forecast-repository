import logging

import click
import django

# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

logger = logging.getLogger(__name__)


#
# ---- application----
#

@click.command()
@click.argument('level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']))
@click.argument('message', type=click.STRING, required=True)
def log_util_app(level, message):
    """
    Simple CLI that outputs a log message. Useful for logging messages/markers/milestones in Heroku logs to ease later
    retrieval.

    https://docs.python.org/3/howto/logging.html#logging-basic-tutorial

    :param level: log level to use. one of: DEBUG, INFO, WARNING, ERROR, CRITICAL
    :param message: the message to log
    """
    # set log_fcn
    log_fcn = None
    if level == 'DEBUG':
        log_fcn = logger.info
    elif level == 'INFO':
        log_fcn = logger.info
    elif level == 'WARNING':
        log_fcn = logger.warning
    elif level == 'ERROR':
        log_fcn = logger.error
    else:  # 'CRITICAL'
        log_fcn = logger.critical

    log_fcn(message)


#
# ---- main ----
#

if __name__ == '__main__':
    log_util_app()
