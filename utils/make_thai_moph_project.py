import datetime
from pathlib import Path

import click
import django

from utils.make_cdc_flu_challenge_project import get_or_create_super_po_mo_users


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models.project import Target, TimeZero
from forecast_app.models import Project, ForecastModel


@click.command()
def make_thai_moph_project_app():
    """
    Deletes and creates a database with one project, one group, and two classes of users. Then loads models from the
    Impetus project. Note: The input files to this program are the output from a spamd export script located the
    dengue-data repo ( https://github.com/reichlab/dengue-data/blob/master/misc/cdc-csv-export.R ) and are committed to
    https://epimodeling.springloops.io/project/156725/svn/source/browse/-/trunk%2Farchives%2Fdengue-reports%2Fdata-summaries
    They currently must be processed (currently by hand) via these rough steps:

        1. download template
        2. correct template header from 'bin_end_not_incl' to 'bin_end_notincl'
        3. delete files where first date (data_version_date) was before 0525
        4. for files with duplicate second dates (timzeros), keep the one with the most recent first date (data_version_date)

    """
    click.echo("* started creating Thai MOPH project")

    project_name = 'Impetus Province Forecasts'
    data_dir = Path('/Users/cornell/IdeaProjects/moph-forecast-files')

    found_project = Project.objects.filter(name=project_name).first()
    if found_project:
        click.echo("* deleting previous project")
        found_project.delete()

    po_user, po_user_password, mo_user, po_user_password = get_or_create_super_po_mo_users(create_super=False)

    click.echo("* creating project")
    template_path = data_dir / 'thai-moph-forecasting-template.csv'
    project = make_thai_moph_project(project_name, template_path, data_dir)
    project.owner = po_user
    project.model_owners.add(mo_user)
    project.save()

    click.echo("* creating model. data_dir={}".format(data_dir))
    make_model(project, mo_user, data_dir)

    click.echo('* done!')


def make_thai_moph_project(project_name, template_path, data_dir):
    project = Project.objects.create(
        name=project_name,
        is_public=False,
        description="Impetus Project dengue forecasts for the 2017 season in Thailand",
        home_url='https://epimodeling.springloops.io/project/156725',
        core_data='https://github.com/reichlab/dengue-data')

    click.echo("  loading template")
    project.load_template(template_path)

    # create Targets
    click.echo("  creating targets")
    for target_name in ["1 biweek ahead", '2 biweek ahead', '3 biweek ahead', '4 biweek ahead', '5 biweek ahead']:
        Target.objects.create(project=project, name=target_name, description=target_name)

    # create TimeZeros from file names in data_dir. format (e.g., '20170506-r6object-20170504.cdc.csv'):
    #
    #   "[data_version_date]-r6object-[timezero].cdc.csv"
    #
    click.echo("  creating timezeros")
    for csv_file, first_date, second_date in csv_file_date_pairs_from_data_dir(data_dir):
        TimeZero.objects.create(project=project, timezero_date=str(second_date), data_version_date=str(first_date))

    # done
    return project


def csv_file_date_pairs_from_data_dir(data_dir):
    """
    :return a list of 3-tuples for each *.cdc.csv file in data_dir of the form (csv_file, first_date, second_date)
    """
    file_name_date_pairs = []
    for csv_file in data_dir.glob('*.cdc.csv'):  # '20170506-r6object-20170504.cdc.csv'
        first_date, second_date = date_pair_from_csv_file(csv_file)
        file_name_date_pairs.append((csv_file, first_date, second_date))
    return file_name_date_pairs


def date_pair_from_csv_file(csv_file):  # a Path
    """
    :param csv_file: a *.cdc.csv file, e.g., '20170506-r6object-20170504.cdc.csv'
    :return: a 2-tuple of datetime.dates in csv_file: (first_date, second_date)
    """
    prefix = csv_file.name.split('.cdc.csv')[0]
    first_date_str, second_date_str = prefix.split('-r6object-')  # format: 'YYYYMMDD'
    first_date = datetime.date(int(first_date_str[:4]), int(first_date_str[4:6]), int(first_date_str[6:]))
    second_date = datetime.date(int(second_date_str[:4]), int(second_date_str[4:6]), int(second_date_str[6:]))
    return first_date, second_date


def make_model(project, model_owner, data_dir):
    """
    Creates the gam-lag1-tops3 ForecastModel and its Forecast.
    """
    description = "A spatio-temporal forecasting model for province-level dengue hemorrhagic fever incidence in " \
                  "Thailand. The model is fit using the generalized additive model framework, with the number of " \
                  "cases in the previous biweek in the top three correlated provinces informing the current " \
                  "forecast. Forecasts at multiple horizons into the future are made by recursively applying the model."
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='gam-lag1-tops3',
        description=description,
        home_url='http://journals.plos.org/plosntds/article?id=10.1371/journal.pntd.0004761',
        aux_data_url=None)
    add_forecasts_to_model(forecast_model, data_dir)

    # done
    return project


def add_forecasts_to_model(forecast_model, data_dir):
    """
    Adds Forecast objects to forecast_model based on data_dir under data_dir. Recall data file naming
    scheme: 'EW<mmwr_week>-<team_name>-<sub_date_yyy_mm_dd>.csv'
    """
    for csv_file, first_date, second_date in csv_file_date_pairs_from_data_dir(data_dir):
        # format from above: "[data_version_date]-r6object-[timezero].cdc.csv"
        time_zero = forecast_model.time_zero_for_timezero_date(second_date)
        if not time_zero:
            raise RuntimeError("no time_zero found. csv_file={}, first_date={}".format(csv_file, second_date))

        click.echo('  adding forecast: csv_file={}, time_zero={}'.format(csv_file.name, time_zero))
        forecast_model.load_forecast(csv_file, time_zero)


if __name__ == '__main__':
    make_thai_moph_project_app()
