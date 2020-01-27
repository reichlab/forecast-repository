import json
from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.cdc import json_io_dict_from_cdc_csv_file


#
# ---- application----
#

@click.command()
@click.argument('forecast_csv_file', type=click.Path(file_okay=True, exists=True))
@click.argument('season_start_year', type=click.INT)
def convert_cdc_csv_to_json_app(forecast_csv_file, season_start_year):
    """
    App to convert files in the CDC CSV format to our "JSON IO dict" one. Saves into the same dir as the source.
    """
    forecast_csv_file = Path(forecast_csv_file)
    output_json_file = forecast_csv_file.with_suffix('.json')
    click.echo(f"* started converting {forecast_csv_file} -> {output_json_file}")
    with open(forecast_csv_file) as cdc_csv_fp, \
            open(output_json_file, 'w') as output_json_fp:
        json_io_dict = json_io_dict_from_cdc_csv_file(season_start_year, cdc_csv_fp)
        json.dump(json_io_dict, output_json_fp, indent=4)
    click.echo("* done")


if __name__ == '__main__':
    convert_cdc_csv_to_json_app()
