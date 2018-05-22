import shutil
from pathlib import Path

import click
import django
import pymmwr


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set

django.setup()

from utils.cdc import epi_week_filename_components_2016_2017_flu_contest
from utils.utilities import cdc_csv_filename_components, YYYYMMDD_DATE_FORMAT


# This number is the internal reichlab standard: "We used week 30. I don't think this is a standardized concept outside
# of our lab though. We use separate concepts for a "season" and a "year". So, e.g. the "2016/2017 season" starts with
# EW30-2016 and ends with EW29-2017."
SEASON_START_EW_NUMBER = 30


@click.command()
@click.argument('cdc_data_parent_dir', type=click.Path(file_okay=False, exists=True))
def normalize_cdc_flu_challenge_filenames_app(cdc_data_parent_dir):
    """
    Accepts an input directory that contains subdirectories of files using the CDC file name convention as documented in
    epi_week_filename_components_2016_2017_flu_contest(), i.e.,

        'EW1-KoTstable-2017-01-17.csv'  # ew_week_number, team_name, submission_datetime

    and makes copies of the *.csv files, using new names following our system's naming scheme as documented in
    documentation.html,
    i.e.,

        '20170917-gam_lag1_tops3-20170919.cdc.csv'  # time_zero, model_name, data_version_date (latter is optional)

    Each cdc_data_parent_dir subdirectory represents a particular model's output. Writes the *.cdc.csv files to the
    input directory, OVERWRITING any that exist.

    Note that we exclude data_version_date. Due to our use of the https://github.com/reichlab/pymmwr/ library,
    time_zero dates (the first date component) are STARTING dates of the EW.
    """
    season_start_year = 2016  # hard-coded
    cdc_data_parent_dir = Path(cdc_data_parent_dir)
    click.echo("* normalize_cdc_flu_challenge_filenames_app(). season_start_year={}, cdc_data_parent_dir={}"
               .format(season_start_year, cdc_data_parent_dir))

    for model_dir in cdc_data_parent_dir.iterdir():
        if not model_dir.is_dir():
            continue

        click.echo("* model_dir: {}".format(model_dir))
        for csv_file in model_dir.glob('*.csv'):
            if cdc_csv_filename_components(csv_file.name):  # skip *.cdc.csv files
                continue

            filename_components = epi_week_filename_components_2016_2017_flu_contest(csv_file.name)
            if not filename_components:
                raise RuntimeError("CSV file name did not match expected. csv_file={}".format(csv_file))

            ew_week_number, team_name, submission_datetime = filename_components  # ex: 1, KoTstable, 2017-01-17
            mmwr_year = season_start_year if ew_week_number >= SEASON_START_EW_NUMBER else season_start_year + 1
            datetime_for_mmwr_week = pymmwr.mmwr_week_to_date(mmwr_year, ew_week_number)

            new_filename = '{time_zero}-{model_name}-{data_version_date}.cdc.csv' \
                .format(time_zero=datetime_for_mmwr_week.strftime(YYYYMMDD_DATE_FORMAT),
                        model_name=team_name,
                        data_version_date=submission_datetime.strftime(YYYYMMDD_DATE_FORMAT))
            out_file = model_dir / new_filename
            click.echo("  {}\t-> {}".format(csv_file.name, out_file.name))
            shutil.copy(str(csv_file), str(out_file))

    click.echo("* Done")


if __name__ == '__main__':
    normalize_cdc_flu_challenge_filenames_app()
