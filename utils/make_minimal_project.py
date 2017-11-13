import datetime
import timeit
from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Project, TimeZero, ForecastModel, Target
from utils.make_example_projects import CDC_CONFIG_DICT


@click.command()
def temp_app():
    """
    App to populate the Heroku database with minimal data for simple browsing - one Project, one ForecastModel, and
    one Forecast. NB: requires DJANGO_SETTINGS_MODULE to be set.

    cd ~/IdeaProjects/forecast-repository/
    export PYTHONPATH=.
    pipenv shell
    python3 utils/temp_app.py

    """
    try:
        old_project = Project.objects.get(name='Test Project')
        click.echo("deleting old_project={}".format(old_project))
        old_project.delete()
    except Project.DoesNotExist:
        pass

    click.echo("creating Project")
    project = Project.objects.create(name='Test Project',
                                     description="a Project for testing",
                                     url='http://example.com',
                                     core_data='http://example.com',
                                     config_dict=CDC_CONFIG_DICT)
    target = Target.objects.create(project=project, name="Test target", description="a Target for testing")
    tz_tomorrow = TimeZero.objects.create(project=project,
                                          timezero_date=str(datetime.date.today() + datetime.timedelta(days=1)),
                                          data_version_date=None)
    tz_today = TimeZero.objects.create(project=project,
                                       timezero_date=str(datetime.date.today()),
                                       data_version_date=None)

    template_path = Path('forecast_app/tests/2016-2017_submission_template.csv')
    click.echo("loading template into project={}, template_path={}".format(project, template_path))
    start_time = timeit.default_timer()
    project.load_template(template_path)
    click.echo("  loaded template: {}. {}".format(project.csv_filename, timeit.default_timer() - start_time))

    click.echo("creating ForecastModel")
    forecast_model = ForecastModel.objects.create(project=project,
                                                  name='Test ForecastModel',
                                                  description="a ForecastModel for testing",
                                                  url='http://example.com')

    csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')
    click.echo("loading forecast into forecast_model={}, csv_file_path={}".format(forecast_model, csv_file_path))
    start_time = timeit.default_timer()
    forecast = forecast_model.load_forecast(csv_file_path, tz_today)
    click.echo("  loaded forecast={}. {}".format(forecast, timeit.default_timer() - start_time))

    click.echo("done!")


if __name__ == '__main__':
    temp_app()
