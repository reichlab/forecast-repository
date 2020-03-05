import datetime
import json
import re
import timeit
from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set


django.setup()

from utils.utilities import get_or_create_super_po_mo_users
from forecast_app.models.project import TimeZero
from forecast_app.models import Project, ForecastModel
from utils.project import create_project_from_json, _validate_and_create_units, _validate_and_create_targets, \
    delete_project_iteratively, load_truth_data
from utils.cdc import load_cdc_csv_forecast_file


#
# ---- application----
#

THAI_PROJECT_NAME = 'Impetus Province Forecasts'


@click.command()
@click.argument('data_dir', type=click.Path(file_okay=False, exists=True))
@click.argument('truths_csv_file', type=click.Path(file_okay=True, exists=True))
def make_thai_moph_project_app(data_dir, truths_csv_file):
    """
    Deletes and creates a database with one project, one group, and two classes of users. Hard-coded for 2017-2018
    season. Then loads models from the Impetus project. Note: The input files to this program are the output from a
    spamd export script located the dengue-data repo ( https://github.com/reichlab/dengue-data/blob/master/misc/cdc-csv-export.R )
    and are committed to https://epimodeling.springloops.io/project/156725/svn/source/browse/-/trunk%2Farchives%2Fdengue-reports%2Fdata-summaries
    They currently must be processed (currently by hand) via these rough steps:

        1. download template
        2. correct template header from 'bin_end_not_incl' to 'bin_end_notincl'
        3. delete files where first date (data_version_date) was before 0525
        4. for files with duplicate second dates (timezeros), keep the one with the most recent first date (data_version_date)
    """
    start_time = timeit.default_timer()
    data_dir = Path(data_dir)
    click.echo(f"* make_thai_moph_project_app(): data_dir={data_dir}, truths_csv_file={truths_csv_file}")

    project = Project.objects.filter(name=THAI_PROJECT_NAME).first()
    if project:
        click.echo("* Deleting existing project: {}".format(project))
        delete_project_iteratively(project)

    # create the Project (and Users if necessary), including loading the template and creating Targets
    po_user, _, mo_user, _ = get_or_create_super_po_mo_users(is_create_super=False)

    # !is_validate to bypass Impetus non-uniform bins: [0, 1), [1, 10), [10, 20), ..., [1990, 2000):
    project = create_project_from_json(Path('forecast_app/tests/projects/thai-project.json'), po_user)
    project.model_owners.add(mo_user)
    project.save()
    click.echo("* Created project: {}".format(project))

    # make the model
    forecast_model = make_model(project, mo_user)
    click.echo("* created model: {}".format(forecast_model))

    # create TimeZeros. NB: we skip existing TimeZeros in case we are loading new forecasts. for is_season_start and
    # season_name we use year transitions: the first 2017 we encounter -> start of that year, etc.
    seen_years = []  # indicates a year has been processed. used to determine season starts
    for cdc_csv_file, timezero_date, _, data_version_date in cdc_csv_components_from_data_dir(data_dir):
        timezero_year = timezero_date.year
        is_season_start = timezero_year not in seen_years
        if is_season_start:
            seen_years.append(timezero_year)

        found_time_zero = project.time_zero_for_timezero_date(timezero_date)
        if found_time_zero:
            click.echo(f"s (TimeZero exists)\t{cdc_csv_file}\t")  # 's' from load_cdc_csv_forecasts_from_dir()
            continue

        TimeZero.objects.create(project=project,
                                timezero_date=str(timezero_date),
                                data_version_date=str(data_version_date) if data_version_date else None,
                                is_season_start=(True if is_season_start else False),
                                season_name=(str(timezero_year) if is_season_start else None))
    click.echo("- created TimeZeros: {}".format(project.timezeros.all()))

    # load the truth
    click.echo("- loading truth values")
    load_truth_data(project, Path('utils/dengue-truth-table-script/truths.csv'), is_convert_na_none=True)

    # load data
    click.echo("* Loading forecasts")
    forecast_model = project.models.first()
    forecasts = load_cdc_csv_forecasts_from_dir(forecast_model, data_dir, None)  # season_start_year
    click.echo("- Loading forecasts: loaded {} forecast(s)".format(len(forecasts)))

    # done
    click.echo(f"* Done. time: {timeit.default_timer() - start_time}")


def make_model(project, model_owner):
    """
    Creates the gam_lag1_tops3 ForecastModel and its Forecast.
    """
    description = "A spatio-temporal forecasting model for province-level dengue hemorrhagic fever incidence in " \
                  "Thailand. The model is fit using the generalized additive model framework, with the number of " \
                  "cases in the previous biweek in the top three correlated provinces informing the current " \
                  "forecast. Forecasts at multiple horizons into the future are made by recursively applying the model."
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='gam_lag1_tops3',
        team_name='Impetus',
        description=description,
        home_url='http://journals.plos.org/plosntds/article?id=10.1371/journal.pntd.0004761',
        aux_data_url=None)

    # done
    return forecast_model


#
# ---- utilities ----
#

def create_thai_units_and_targets(project):
    with open(Path('forecast_app/tests/projects/thai-project.json')) as fp:
        project_dict = json.load(fp)
    _validate_and_create_units(project, project_dict)

    # !is_validate to bypass Impetus non-uniform bins: [0, 1), [1, 10), [10, 20), ..., [1990, 2000):
    _validate_and_create_targets(project, project_dict)


def load_cdc_csv_forecasts_from_dir(forecast_model, data_dir, season_start_year):
    """
    Adds Forecast objects to forecast_model using the cdc csv files under data_dir. Assumes TimeZeros match those in my
    Project. Skips files that have already been loaded. Skips files that cause load_forecast() to raise a RuntimeError.

    :param forecast_model: a ForecastModel to load the data into
    :param data_dir: Path of the directory that contains cdc csv files
    :param season_start_year: optional (used only if the files in data_dir have the date-related targets
        'Season onset' or 'Season peak week')
    :return list of loaded Forecasts
    """
    forecasts = []
    for cdc_csv_file, timezero_date, _, _ in cdc_csv_components_from_data_dir(data_dir):
        timezero = forecast_model.project.time_zero_for_timezero_date(timezero_date)
        if not timezero:
            click.echo("x (no TimeZero found)\t{}\t".format(cdc_csv_file.name))
            continue

        found_forecast_for_time_zero = forecast_model.forecast_for_time_zero(timezero)
        if found_forecast_for_time_zero:
            click.echo("s (found forecast)\t{}\t".format(cdc_csv_file.name))
            continue

        try:
            forecast = load_cdc_csv_forecast_file(season_start_year, forecast_model, cdc_csv_file, timezero)
            forecasts.append(forecast)
            click.echo("o\t{}\t".format(cdc_csv_file.name))
        except RuntimeError as rte:
            click.echo("f\t{}\t{}".format(cdc_csv_file.name, rte))
    if not forecasts:
        click.echo("Warning: no valid forecast files found in directory: {}".format(data_dir))
    return forecasts


def cdc_csv_components_from_data_dir(cdc_csv_dir):
    """
    A utility that helps process a directory containing cdc csv files in our zoltar file name convention - see
    ZOLTAR_CSV_FILENAME_RE_PAT.

    :return a list of 4-tuples for each *.cdc.csv file in cdc_csv_dir, with the last three in the form returned by
        cdc_csv_filename_components(): (cdc_csv_file, timezero_date, model_name, data_version_date). cdc_csv_file is a
        Path. the list is sorted by timezero_date. Returns [] if no
    """
    cdc_csv_components = []
    for cdc_csv_file in cdc_csv_dir.glob('*.' + CDC_CSV_FILENAME_EXTENSION):
        filename_components = cdc_csv_filename_components(cdc_csv_file.name)
        if not filename_components:
            continue

        timezero_date, model_name, data_version_date = filename_components
        cdc_csv_components.append((cdc_csv_file, timezero_date, model_name, data_version_date))
    return sorted(cdc_csv_components, key=lambda _: _[1])


def cdc_csv_filename_components(cdc_csv_filename):
    """
    :param cdc_csv_filename: a *.cdc.csv file name, e.g., '20170419-gam_lag1_tops3-20170516.cdc.csv'
    :return: a 3-tuple of components from cdc_csv_file: (timezero_date, model_name, data_version_date), where dates are
        datetime.date objects. data_version_date is None if not found in the file name. returns None if the file name
        is invalid, i.e., does not conform to our standard.
    """
    match = ZOLTAR_CSV_FILENAME_RE_PAT.match(cdc_csv_filename)
    if not match:
        return None

    # groups has our two cases: with and without data_version_date, e.g.,
    # ('2017', '04', '19', 'gam_lag1_tops3', '2017', '05', '16')
    # ('2017', '04', '19', 'gam_lag1_tops3', None, None, None)
    groups = match.groups()
    timezero_date = datetime.date(int(groups[0]), int(int(groups[1])), int(int(groups[2])))
    model_name = groups[3]
    data_version_date = datetime.date(int(groups[4]), int(int(groups[5])), int(int(groups[6]))) if groups[4] else None
    return timezero_date, model_name, data_version_date


#
# variables
#

CDC_CSV_FILENAME_EXTENSION = 'cdc.csv'

#
# The following defines this project's file naming standard, and defined in 'Forecast data file names' in
# documentation.html, e.g., '<time_zero>-<model_name>[-<data_version_date>].cdc.csv' . For example:
#
# - '20170419-gam_lag1_tops3-20170516.cdc.csv'
# - '20161023-KoTstable-20161109.cdc.csv'
# - '20170504-gam_lag1_tops3.cdc.csv'
#

ZOLTAR_CSV_FILENAME_RE_PAT = re.compile(r"""
^
(\d{4})(\d{2})(\d{2})    # time_zero YYYYMMDD
-                        # dash
([a-zA-Z0-9_]+)          # model_name
(?:                      # non-repeating group so that '-20170516' doesn't get included
  -                      # optional dash and dvd
  (\d{4})(\d{2})(\d{2})  # data_version_date YYYYMMDD
  )?                     #
\.cdc.csv$
""", re.VERBOSE)

#
# app
#

if __name__ == '__main__':
    make_thai_moph_project_app()
