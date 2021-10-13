import csv
import logging

import click
import django
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Project


logger = logging.getLogger(__name__)


@click.command()
def post_018_migrate_app():
    udpate_units_covid()
    udpate_units_docs()


def udpate_units_covid():
    # change the COVID-19 Forecasts project's Unit.names to be the official ones from locations.csv. (abbreviations are
    # already correct from the migration, which copied name -> abbreviation). we use the naive way: iterate through the
    # 3K+ locations in the csv file and set unit name - 1000s of queries
    project = get_object_or_404(Project, name='COVID-19 Forecasts')
    csv_file = '/Users/cornell/IdeaProjects/covid19-forecast-hub/data-locations/locations.csv'
    with open(csv_file, 'r') as fp:
        csv_reader = csv.reader(fp, delimiter=',')
        next(csv_reader)  # skip header
        for abbreviation, location, location_name, population in csv_reader:  # ex: "AL,01,Alabama,4903185.0"
            unit = project.units.get(name=location)
            unit.name = location_name
            unit.save()


def udpate_units_docs():
    # change the docs projects' units to match tests/projects/docs-project.json
    project = get_object_or_404(Project, name='Docs Example Project')
    for name, abbrev in [("location1", "loc1"),
                         ("location2", "loc2"),
                         ("location3", "loc3")]:
        unit = project.units.get(name=name)
        unit.abbreviation = abbrev
        unit.save()


if __name__ == '__main__':
    post_018_migrate_app()
