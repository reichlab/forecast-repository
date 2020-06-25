import datetime
import logging

import click
import django
from django.utils.timezone import now


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Job


logger = logging.getLogger(__name__)


#
# ---- application----
#

@click.command()
@click.argument('num_days', type=click.INT, required=True)
@click.option('--dry-run', is_flag=True, default=False)
def delete_old_jobs_app(num_days, dry_run):
    """
    List (and then delete) jobs older than X days.
    """
    all_jobs_qs = Job.objects
    old_jobs_qs = Job.objects.filter(updated_at__lt=now() - datetime.timedelta(days=num_days)).order_by('updated_at')
    logger.info(f"delete_old_jobs_app(): num_days={num_days}, dry_run={dry_run}. "
                f"# jobs={all_jobs_qs.count()}, # old={old_jobs_qs.count()}")
    if not dry_run:
        logger.info("delete_old_jobs_app(): deleting...")
        delete_result = old_jobs_qs.delete()
        logger.info(f'delete_old_jobs_app(): done. delete_result={delete_result}, # old={old_jobs_qs.count()}')
    else:
        logger.info("delete_old_jobs_app(): done (not deleting)")


#
# ---- main ----
#

if __name__ == '__main__':
    delete_old_jobs_app()
