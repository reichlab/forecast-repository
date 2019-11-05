import json
import timeit
from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set


django.setup()

from forecast_app.models.project import TimeZero
from forecast_app.models import Project, ForecastModel
from utils.project import create_project_from_json, validate_and_create_locations, validate_and_create_targets
from utils.make_cdc_flu_contests_project import get_or_create_super_po_mo_users
from utils.cdc import cdc_csv_components_from_data_dir, load_cdc_csv_forecasts_from_dir


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
        project.delete()

    # create the Project (and Users if necessary), including loading the template and creating Targets
    po_user, _, mo_user, _ = get_or_create_super_po_mo_users(is_create_super=False)

    # !is_validate to bypass Impetus non-uniform bins: [0, 1), [1, 10), [10, 20), ..., [1990, 2000):
    project = create_project_from_json(Path('forecast_app/tests/projects/thai-project.json'), po_user)
    project.model_owners.add(mo_user)
    project.save()
    click.echo("* Created project: {}".format(project))

    # make the model
    forecast_model = make_model(project, mo_user, data_dir)
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
    project.load_truth_data(Path('utils/dengue-truth-table-script/truths.csv'))

    # load data
    click.echo("* Loading forecasts")
    forecast_model = project.models.first()
    forecasts = load_cdc_csv_forecasts_from_dir(forecast_model, data_dir)
    click.echo("- Loading forecasts: loaded {} forecast(s)".format(len(forecasts)))

    # done
    click.echo(f"* Done. time: {timeit.default_timer() - start_time}")


def make_model(project, model_owner, data_dir):
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
# ---- test utilities ----
#

def create_thai_locations_and_targets(project):
    with open(Path('forecast_app/tests/projects/thai-project.json')) as fp:
        project_dict = json.load(fp)
    validate_and_create_locations(project, project_dict)

    # !is_validate to bypass Impetus non-uniform bins: [0, 1), [1, 10), [10, 20), ..., [1990, 2000):
    validate_and_create_targets(project, project_dict)


if __name__ == '__main__':
    make_thai_moph_project_app()
