import click


#
# ---- application----
#

@click.command()
@click.argument('forecast_csv_file', type=click.Path(file_okay=True, exists=True))
def convert_cdc_csv_to_json_app(forecast_csv_file):
    """
    App to convert files in the CDC CSV format
    """
    click.echo("* started creating temp projects")
    with open(forecast_csv_file) as cdc_csv_file_fp:
        json_io_dict = json_io_dict_from_cdc_csv_file(new_forecast, cdc_csv_file_fp)
    click.echo("* Done")


if __name__ == '__main__':
    convert_cdc_csv_to_json_app()
