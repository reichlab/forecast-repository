import datetime
import json
import timeit
from pathlib import Path

import click
import django
# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
from django.db import transaction


django.setup()
from utils.forecast import load_predictions_from_json_io_dict, cache_forecast_metadata

from utils.project import create_project_from_json
from utils.project_truth import load_truth_data

from utils.cdc_io import load_cdc_csv_forecast_file, make_cdc_units_and_targets
from forecast_app.models import Project, TimeZero, ForecastModel, Forecast
from utils.utilities import get_or_create_super_po_mo_users


#
# ---- application----
#

MINIMAL_PROJECT_NAMES = ['public project', 'private project']


@click.command()
def make_minimal_projects_app():
    """
    App to populate the Heroku database with three small projects with a little data for browsing:

    1. a public CDC-based public_project using cdc-project.json
    2. a private CDC project
    3. a docs project using docs-project.json

    NB: requires DJANGO_SETTINGS_MODULE to be set.

    You might want to run this afterwards: $ python3 utils/fix_owners.py
    """
    click.echo("* started creating temp projects")

    for project_name in MINIMAL_PROJECT_NAMES:
        found_project = Project.objects.filter(name=project_name).first()
        if found_project:
            click.echo("* deleting previous project: {}".format(found_project))
            found_project.delete()

    po_user, _, mo_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=False)

    click.echo("* creating CDC projects")
    public_project = Project.objects.create(name=MINIMAL_PROJECT_NAMES[0], is_public=True)
    public_project.owner = po_user
    public_project.model_owners.add(mo_user)
    public_project.save()

    private_project = Project.objects.create(name=MINIMAL_PROJECT_NAMES[1], is_public=False)
    private_project.owner = po_user
    private_project.model_owners.add(mo_user)
    private_project.save()

    fill_cdc_project(public_project, mo_user, is_public=True)  # uses cdc-project.json
    fill_cdc_project(private_project, mo_user, is_public=False)  # ""

    click.echo("* creating Docs project")
    _make_docs_project(po_user)

    click.echo("* Done")


def fill_cdc_project(project, mo_user, is_public):
    project.description = "description"
    project.home_url = "http://example.com/"
    project.core_data = "http://example.com/"

    # make the Units and Targets via cdc-project.json (recall it has no timezeros)
    make_cdc_units_and_targets(project)

    # make two TimeZeros - one for ground truth, and one for the forecast's data:
    # EW1-KoTsarima-2017-01-17-small.csv -> pymmwr.date_to_mmwr_week(datetime.date(2017, 1, 17))  # EW01 2017
    #   -> {'year': 2017, 'week': 3, 'day': 3}
    time_zero1 = TimeZero.objects.create(project=project,
                                         timezero_date=datetime.date(2017, 1, 17),
                                         data_version_date=None)
    TimeZero.objects.create(project=project,
                            timezero_date=datetime.date(2017, 1, 24),
                            data_version_date=None)

    # load ground truth
    load_truth_data(project, Path('forecast_app/tests/truth_data/2017-01-17-truths.csv'), is_convert_na_none=True,
                    file_name='2017-01-17-truths.csv')

    # create the two models
    click.echo("creating ForecastModel")
    forecast_model1 = ForecastModel.objects.create(project=project,
                                                   name=f'Test ForecastModel1 ({"public" if is_public else "private"})',
                                                   abbreviation='model1_abbrev',
                                                   team_name='ForecastModel1 team',
                                                   description="a ForecastModel for testing",
                                                   home_url='http://example.com',
                                                   owner=mo_user)

    # load the forecasts using a small data file
    csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
    click.echo("* loading forecast into forecast_model={}, csv_file_path={}".format(forecast_model1, csv_file_path))
    start_time = timeit.default_timer()
    forecast1 = load_cdc_csv_forecast_file(2016, forecast_model1, csv_file_path, time_zero1)
    click.echo("  loaded forecast={}. {}".format(forecast1, timeit.default_timer() - start_time))

    ForecastModel.objects.create(project=project,
                                 name=f'Test ForecastModel2 ({"public" if is_public else "private"})',
                                 abbreviation='model2_abbrev',
                                 # team_name='ForecastModel2 team',  # leave default ('')
                                 description="a second ForecastModel for testing",
                                 home_url='http://example.com',
                                 owner=mo_user)


#
# _make_docs_project()
#

DOCS_PROJECT_NAME = "Docs Example Project"  # overrides the json file one


@transaction.atomic
def _make_docs_project(user):
    """
    Creates a project based on docs-project.json with forecasts from docs-predictions.json.
    """
    found_project = Project.objects.filter(name=DOCS_PROJECT_NAME).first()
    if found_project:
        click.echo("* deleting previous project: {}".format(found_project))
        found_project.delete()

    project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), user)  # atomic
    project.name = DOCS_PROJECT_NAME
    project.save()

    load_truth_data(project, Path('forecast_app/tests/truth_data/docs-ground-truth.csv'),
                    file_name='docs-ground-truth.csv')

    forecast_model = ForecastModel.objects.create(project=project, name='docs forecast model', abbreviation='docs_mod')
    time_zero = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 2)).first()
    forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                       time_zero=time_zero, notes="a small prediction file")
    with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
        json_io_dict_in = json.load(fp)
        load_predictions_from_json_io_dict(forecast, json_io_dict_in, is_validate_cats=False)  # atomic
        cache_forecast_metadata(forecast)  # atomic

    return project, time_zero, forecast_model, forecast


if __name__ == '__main__':
    make_minimal_projects_app()
