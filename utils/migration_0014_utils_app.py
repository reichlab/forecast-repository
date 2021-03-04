import logging
from itertools import groupby

import click
import django
import django_rq


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.migration_0014_utils import _migrate_correctness_worker, is_different_old_new_json, \
    _grouped_version_rows, _migrate_forecast_worker

from forecast_app.models import Forecast, Project


logger = logging.getLogger(__name__)


#
# cli()
#

@click.group()
def cli():
    pass


#
# check_correctness()
#

@cli.command(name="check")
@click.option('--enqueue/--no-enqueue', default=False)
def check_correctness(enqueue):
    """
    Compares old and new json_io_dict outputs for every Forecast in the database. ERRORs if didn't match.

    :param enqueue: controls whether the update will be immediate in the calling thread (blocks), or enqueued for RQ
    """
    from forecast_repo.settings.base import DEFAULT_QUEUE_NAME  # avoid circular imports


    forecasts = Forecast.objects.all().order_by('created_at')
    if not enqueue:
        logger.info(f"checking {len(forecasts)} forecasts")
        for forecast in forecasts:
            is_different = is_different_old_new_json(forecast)
            if is_different:
                logger.error(f"old != new: {is_different}")
        logger.info(f"checking done")
    else:
        logger.info(f"enqueuing {len(forecasts)} forecasts")
        queue = django_rq.get_queue(DEFAULT_QUEUE_NAME)
        for forecast in forecasts:
            queue.enqueue(_migrate_correctness_worker, forecast.pk)
        logger.info(f"enqueuing done")


#
# enqueue_migrate_worker()
#

@cli.command(name="migrate")
def enqueue_migrate_worker():
    """
    CLI that enqueues migration of all forecasts in every project in the database. NB: there should be no non-migration
    jobs running! We order by created_at to simulate how the forecasts were created as close as possible. This becomes
    an issue with respect to versioning of the same timezero: If we load a newer version first and then an older
    version, the latter will possibly have fewer post-migrate rows due to duplications. B/c workers are running in
    parallel, we cannot guarantee order, though. But this is the best we came up with.
    """
    from forecast_repo.settings.base import DEFAULT_QUEUE_NAME  # avoid circular imports


    # forecasts = Forecast.objects.all().order_by('created_at')
    logger.info(f"enqueuing {Forecast.objects.count()} forecasts")  # somewhat expensive
    queue = django_rq.get_queue(DEFAULT_QUEUE_NAME)
    for project in Project.objects.all():  # iterate over projects b/c _grouped_version_rows() is by project
        logger.info(f"* {project}")
        # fm_id, tz_id, issue_date, f_id, f_source, f_created_at, rank:
        grouped_version_rows = _grouped_version_rows(project, False)  # is_versions_only
        for (fm_id, tz_id), grouper in groupby(grouped_version_rows, key=lambda _: (_[0], _[1])):
            logger.info(f"  {fm_id}, {tz_id}")
            # process forecast versions in created_at order (not the returned issue_date order) to simulate the original
            # upload order
            versions = sorted(grouper, key=lambda row: row[5])  # f_created_at
            queue.enqueue(_migrate_forecast_worker, [version[3] for version in versions])  # f_id
            for _, _, issue_date, f_id, source, created_at, rank in versions:
                logger.info(f"    {issue_date}, {f_id}, {source}, {created_at}, {rank}")
    logger.info(f"enqueuing done")


#
# ---- main ----
#

if __name__ == '__main__':
    cli()
