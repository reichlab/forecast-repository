import logging
import timeit

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.views import _viz_options_from_project
from forecast_app.models import Project
from utils.visualization import viz_cache_avail_ref_dates, viz_cache_avail_ref_dates_delete, viz_cache_data


logger = logging.getLogger(__name__)


@click.command()
def update_viz_cache_app():
    """
    Updates VizAvailRefDatesCache for all projects.
    """
    for project in Project.objects.all():
        logger.info(f"update_viz_cache_app(): entered. project={project}")
        start_time = timeit.default_timer()

        # delete and then cache `viz_available_reference_dates()` for initial page load. also speeds up below
        # `_viz_options_from_project()` call
        viz_cache_avail_ref_dates_delete(project)
        available_as_ofs = viz_cache_avail_ref_dates(project)

        # cache `viz_data()` truth and forecasts for strategic combinations of target_var, unit, and as_of
        for target_var, unit, as_of in _target_unit_as_of_combos_to_cache(project, available_as_ofs):
            if any(map(lambda _: _ == '', [target_var, unit, as_of])):
                continue

            for is_forecast in [False, True]:
                viz_cache_data(project, is_forecast, target_var, unit, as_of, force=True)

        # done
        logger.info(f"update_viz_cache_app(): done. delta_secs={timeit.default_timer() - start_time}")


def _target_unit_as_of_combos_to_cache(project, available_as_ofs):
    """
    :param project: a Project
    :param available_as_ofs: as returned by `viz_cache_avail_ref_dates()`
    :return: list of 3-tuples to pass to `viz_cache_data()`: (target_var, unit, as_of)
    """
    target_unit_as_ofs = set()

    # cache initial page load. NB: `_viz_options_from_project()` will be relatively fast due to above
    # `viz_cache_avail_ref_dates()` call
    viz_options = _viz_options_from_project(project)
    initial_target_var = viz_options['included_target_vars'][0]
    initial_unit = viz_options['initial_unit']
    initial_as_of = viz_options['initial_as_of']

    target_unit_as_ofs.add((initial_target_var, initial_unit, initial_as_of))

    # cache two "left arrows" (previous/older as_ofs, default unit and target)
    try:
        target_avail_as_ofs = available_as_ofs[initial_target_var]  # KeyError if missing
        initial_ref_date_idx = target_avail_as_ofs.index(initial_as_of)  # ValueError ""
        if (initial_ref_date_idx - 1) >= 0:  # adjacent older as_of
            as_of = target_avail_as_ofs[initial_ref_date_idx - 1]
            target_unit_as_ofs.add((initial_target_var, initial_unit, as_of))
        if (initial_ref_date_idx - 2) >= 0:  # as_of before that one
            as_of = target_avail_as_ofs[initial_ref_date_idx - 2]
            target_unit_as_ofs.add((initial_target_var, initial_unit, as_of))
    except (KeyError, ValueError) as error:
        pass

    # done
    return target_unit_as_ofs


if __name__ == '__main__':
    update_viz_cache_app()
