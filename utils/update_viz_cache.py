import logging
import timeit

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Project
from utils.visualization import viz_cache_avail_ref_dates, viz_cache_avail_ref_dates_delete


logger = logging.getLogger(__name__)


@click.command()
def update_viz_cache_app():
    """
    Updates VizAvailRefDatesCache for all projects.
    """
    for project in Project.objects.all():
        logger.info(f"update_viz_cache_app(): entered. project={project}")
        start_time = timeit.default_timer()
        viz_cache_avail_ref_dates_delete(project)
        viz_cache_avail_ref_dates(project)  # computes if cache miss
        logger.info(f"update_viz_cache_app(): done. delta_secs={timeit.default_timer() - start_time}")


if __name__ == '__main__':
    update_viz_cache_app()
