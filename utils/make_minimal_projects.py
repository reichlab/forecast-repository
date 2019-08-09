import datetime
import timeit
from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.cdc import load_cdc_csv_forecast_file
from forecast_app.models import Project, TimeZero, ForecastModel
from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, get_or_create_super_po_mo_users


#
# ---- application----
#

MINIMAL_PROJECT_NAMES = ['public project', 'private project']


@click.command()
def make_minimal_projects_app():
    """
    App to populate the Heroku database with fairly minimal data for simple browsing - one Project, two ForecastModels,
    one with one Forecast and the other with no Forecasts. NB: requires DJANGO_SETTINGS_MODULE to be set. Final
    projects:

    public_project (2016-2017_submission_template.csv)
        targets: target1
        time_zeros: time_zero1, time_zero2
        models:
            forecast_model1
                time_zero1: forecast1 (EW1-KoTsarima-2017-01-17.csv)
                time_zero2: not set
            forecast_model2
                time_zero1: not set
                time_zero2: not set

    private_project


    cd ~/IdeaProjects/forecast-repository/
    export PYTHONPATH=.
    pipenv shell
    python3 utils/fix_owners_app.py

    """
    click.echo("* started creating temp projects")

    for project_name in MINIMAL_PROJECT_NAMES:
        found_project = Project.objects.filter(name=project_name).first()
        if found_project:
            click.echo("* deleting previous project: {}".format(found_project))
            found_project.delete()

    po_user, _, mo_user, _ = get_or_create_super_po_mo_users(is_create_super=False)

    click.echo("* creating Projects")
    public_project = Project.objects.create(name=MINIMAL_PROJECT_NAMES[0], is_public=True)
    public_project.owner = po_user
    public_project.model_owners.add(mo_user)
    public_project.save()

    # create a TimeZero so that this truth file can be loaded:
    # public_project.load_truth_data(Path('forecast_app/tests/truth_data/truths-ok.csv'))
    TimeZero.objects.create(project=public_project, timezero_date=datetime.date(2017, 1, 1))

    private_project = Project.objects.create(name=MINIMAL_PROJECT_NAMES[1], is_public=False)
    private_project.owner = po_user
    private_project.model_owners.add(mo_user)
    private_project.save()

    for project in [public_project, private_project]:
        fill_project(project, mo_user)

    click.echo("* Done")


def fill_project(project, mo_user):
    # make the Locations and Targets
    make_cdc_locations_and_targets(project)

    # make a few TimeZeros that match the truth and forecast data
    # EW1-KoTsarima-2017-01-17-small.csv -> pymmwr.date_to_mmwr_week(datetime.date(2017, 1, 17))
    #   -> {'year': 2017, 'week': 3, 'day': 3}
    time_zero1 = TimeZero.objects.create(project=project,
                                         timezero_date=datetime.date(2017, 1, 17),
                                         data_version_date=None)
    TimeZero.objects.create(project=project,
                            timezero_date=datetime.date(2017, 1, 24),
                            data_version_date=None)

    # load the truth data. todo xx file_name arg:
    project.load_truth_data(Path('forecast_app/tests/truth_data/2017-01-17-truths.csv'))

    # create the models
    click.echo("creating ForecastModel")
    forecast_model1 = ForecastModel.objects.create(project=project,
                                                   name='Test ForecastModel1',
                                                   team_name='ForecastModel1 team',
                                                   description="a ForecastModel for testing",
                                                   home_url='http://example.com',
                                                   owner=mo_user)

    # load the forecasts using the small data file
    # csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')
    csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')
    click.echo("* loading forecast into forecast_model={}, csv_file_path={}".format(forecast_model1, csv_file_path))
    start_time = timeit.default_timer()
    forecast1 = load_cdc_csv_forecast_file(forecast_model1, csv_file_path, time_zero1)
    click.echo("  loaded forecast={}. {}".format(forecast1, timeit.default_timer() - start_time))

    ForecastModel.objects.create(project=project,
                                 name='Test ForecastModel2',
                                 # team_name='ForecastModel2 team',  # leave default ('')
                                 description="a second ForecastModel for testing",
                                 home_url='http://example.com',
                                 owner=mo_user)


if __name__ == '__main__':
    make_minimal_projects_app()
