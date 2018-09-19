import logging
import resource

import click
import django


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
        logger.error("Project not found: '{}'".format(name))
        return

    logger.info("Deleting project: {}".format(project))
    logger.debug("memory before: {}".format(sizeof_fmt(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)))
    project.delete()
    logger.info("Delete: done")
    logger.debug("memory after: {}".format(sizeof_fmt(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)))


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
