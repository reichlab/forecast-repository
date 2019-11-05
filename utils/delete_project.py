import logging
import resource

import click
import django
from django.db import transaction


logger = logging.getLogger(__name__)

# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Project


#
# ---- application----
#

@click.command()
@click.argument('name', type=click.STRING, required=True)
def delete_project_app(name):
    project = Project.objects.filter(name=name).first()  # None if doesn't exist
    if not project:
        logger.error(f"Project not found: '{name}'")
        return

    print(f"delete_project_app(): project={project}")
    # delete_project_single_call(project)
    delete_project_iteratively(project)
    print(f"\ndelete_project_app(): done!")


@transaction.atomic
def delete_project_iteratively(project):
    """
    Deletes the passed Project, but unlike `delete_project_single_call()`, does so by iterating over objects that refer
    to the project before deleting the project itself. This apparently reduces the memory usage enough to allow the
    below Heroku deletion.
    """
    print(f"\n* models and forecasts")
    for forecast_model in project.models.iterator():
        print(f"- {forecast_model.pk}")
        for forecast in forecast_model.forecasts.iterator():
            print(f"  = {forecast.pk}")
            forecast.delete()
        forecast_model.delete()

    print(f"\n* locations")
    for location in project.locations.iterator():
        print(f"- {location.pk}")
        location.delete()

    print(f"\n* targets")
    for target in project.targets.iterator():
        print(f"- {target.pk}")
        target.delete()

    print(f"\n* timezeros")
    for timezero in project.timezeros.iterator():
        print(f"- {timezero.pk}")
        timezero.delete()

    project.delete()  # deletes remaining references: RowCountCache, ScoreCsvFileCache


def delete_project_single_call(project):
    """
    Deletes the passed Project via a single `delete()` call. This is fine for small projects, but it soon uses a lot of
    memory as project size (# models and # forecasts) increases. On Heroku, deleting the 'CDC Flu challenge' project
    caused "Error R14 (Memory quota exceeded)" and "Error R15 (Memory quota vastly exceeded)" errors, leading to
    "Stopping process with SIGKILL".
    """
    logger.info(f"Deleting project: {project}")
    logger.debug(f"memory before: {sizeof_fmt(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)}")
    project.delete()
    logger.info(f"Delete: done")
    logger.debug(f"memory after: {sizeof_fmt(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)}")


#
# ---- humanize utility ----
#

# https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


#
# ---- main ----
#

if __name__ == '__main__':
    delete_project_app()
