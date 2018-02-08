import datetime
import shutil
from pathlib import Path

import click
import django
import pymmwr


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set

django.setup()

from utils.cdc import ensemble_epi_week_filename_components
from utils.utilities import cdc_csv_filename_components


@click.command()
@click.argument('cdc_data_parent_dir', type=click.Path(file_okay=False, exists=True))
def normalize_cdc_flusight_ensemble_filenames_app(cdc_data_parent_dir):
    """
    Similar to normalize_cdc_flu_challenge_filenames_app.py, but instead uses ensemble_epi_week_filename_components()
    to extract the components. todo xx merge!

    To get a time_zero and data_version_date from an ensemble data file (e.g., 'EW01-2011-CU_EAKFC_SEIRS.csv'):

    - time_zero: convert the EW01-2011 pair to a date via pymmwr.mmwr_week_to_date()
    - data_version_date: The date is based on the "Specific guidelines for using data with revisions" section of
      https://github.com/FluSightNetwork/cdc-flusight-ensemble/blob/master/README.md :

        Retrospective component forecasts labeled "EWXX" are "due" (i.e. may only use data through) Monday 11:59pm of
        week XX+2.

      So, we add two EWs to the EW01-2011 pair and then use the Monday from that EW.
    """
    cdc_data_parent_dir = Path(cdc_data_parent_dir)
    click.echo("* normalize_cdc_flusight_ensemble_filenames_app(). cdc_data_parent_dir={}".format(cdc_data_parent_dir))

    for model_dir in cdc_data_parent_dir.iterdir():
        if not model_dir.is_dir():
            continue

        click.echo("* model_dir: {}".format(model_dir))
        for csv_file in model_dir.glob('*.csv'):
            if cdc_csv_filename_components(csv_file.name):  # skip *.cdc.csv files
                continue

            filename_components = ensemble_epi_week_filename_components(csv_file.name)
            if not filename_components:
                raise RuntimeError("CSV file name did not match expected. csv_file={}".format(csv_file))

            # set time_zero and data_version_date
            ew_week_number, ew_year, team_name = filename_components  # ex: 1, 2011, 'CU_EAKFC_SEIRS'
            time_zero = pymmwr.mmwr_week_to_date(ew_year, ew_week_number)
            future_yw_mmwr_dict = pymmwr.mmwr_week_with_delta(ew_year, ew_week_number, 2)  # add 2 EWs
            data_version_date_sunday = pymmwr.mmwr_week_to_date(future_yw_mmwr_dict['year'],
                                                                future_yw_mmwr_dict['week'])  # first day of EW (a Sun)
            data_version_date_monday = data_version_date_sunday + datetime.timedelta(days=1)  # Sunday + 1 = Monday

            new_filename = '{time_zero}-{model_name}-{data_version_date}.cdc.csv' \
                .format(time_zero=time_zero.strftime('%Y%m%d'),
                        model_name=team_name,
                        data_version_date=data_version_date_monday.strftime('%Y%m%d'))
            out_file = model_dir / new_filename
            click.echo("\t{}\t{}".format(csv_file.name, out_file.name))
            shutil.copy(str(csv_file), str(out_file))

    click.echo("* Done")


if __name__ == '__main__':
    normalize_cdc_flusight_ensemble_filenames_app()
