import shutil
from pathlib import Path

import click
import django
import pymmwr

from utils.cdc import epi_week_filename_components
from utils.mean_absolute_error import SEASON_START_EW_NUMBER


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set


django.setup()


@click.command()
@click.argument('cdc_data_dir', type=click.Path(file_okay=False, exists=True))
def normalize_cdc_flu_challenge_filenames_app(cdc_data_dir):
    """
    Accepts an input directory that contains files using the CDC file name convention as documented in
    epi_week_filename_components(), i.e.,

        'EW1-KoTstable-2017-01-17.csv'  # ew_week_number, team_name, submission_datetime

    and makes copies of the *.csv files, using new names following our system's naming scheme as documented in
    documentation.html,
    i.e.,

        '20170917-gam_lag1_tops3-20170919.cdc.csv'  # time_zero, model_name, data_version_date (latter is optional)

    Note that we exclude data_version_date. Due to our use of the https://github.com/reichlab/pymmwr/ library,
    time_zero dates (the first date component) are STARTING dates of the EW.

    Outputs to a new 'out' directory created under the input one, which is *overwritten* if it exists.
    """
    cdc_data_dir = Path(cdc_data_dir)
    click.echo("normalize_cdc_flu_challenge_filenames_app(). cdc_data_dir={}".format(cdc_data_dir))

    out_dir = cdc_data_dir / 'out'
    if out_dir.exists():
        shutil.rmtree(str(out_dir))  # danger: wipes it out, including all children
    out_dir.mkdir()
    click.echo("created out_dir={}".format(out_dir))

    season_start_year = 2016  # hard-coded
    for csv_file in cdc_data_dir.glob('*.csv'):
        filename_components = epi_week_filename_components(csv_file.name)
        if not filename_components:
            raise RuntimeError("CSV file name did not match expected. csv_file={}".format(csv_file))

        ew_week_number, team_name, submission_datetime = filename_components  # ex: 1, KoTstable, 2017-01-17
        mmwr_year = season_start_year if ew_week_number >= SEASON_START_EW_NUMBER else season_start_year + 1
        datetime_for_mmwr_week = pymmwr.mmwr_week_to_date(mmwr_year, int(ew_week_number))
        new_filename = '{time_zero}-{model_name}-{data_version_date}.cdc.csv' \
            .format(time_zero=datetime_for_mmwr_week.strftime('%Y%m%d'),
                    model_name=team_name,
                    data_version_date=submission_datetime.strftime('%Y%m%d'))
        out_file = out_dir / new_filename
        click.echo("{} -> {}".format(csv_file.name, out_file.name))
        shutil.copy(str(csv_file), str(out_file))


if __name__ == '__main__':
    normalize_cdc_flu_challenge_filenames_app()
