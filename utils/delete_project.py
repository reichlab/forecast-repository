import logging
import resource

import click
import django


logger = logging.getLogger(__name__)

# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.project import delete_project_iteratively
from forecast_app.models import Project


#
# ---- application----
#

@click.command()
@click.argument('name', type=click.STRING, required=True)
def delete_project_app(name):
    project = Project.objects.filter(name=name).first()  # None if doesn't exist
    if not project:
        logger.error(f"delete_project_app(): error: Project not found: {name!r}")
        return

    print(f"delete_project_app(): project={project}")
    # delete_project_single_call(project)
    delete_project_iteratively(project)
    print(f"\ndelete_project_app(): done!")


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
