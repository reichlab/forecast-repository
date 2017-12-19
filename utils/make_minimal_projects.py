import datetime
import timeit
from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Project, TimeZero, ForecastModel, Target
from utils.make_cdc_flu_challenge_project import get_or_create_super_po_mo_users


@click.command()
def make_minimal_projects_app():
    """
    App to populate the Heroku database with minimal data for simple browsing - one Project, one ForecastModel, and
    one Forecast. NB: requires DJANGO_SETTINGS_MODULE to be set.

    cd ~/IdeaProjects/forecast-repository/
    export PYTHONPATH=.
    pipenv shell
    python3 utils/temp_app.py

    """
    click.echo("* started creating temp projects")

    project_names = ['public project', 'private project']
    for project_name in project_names:
        found_project = Project.objects.filter(name=project_name).first()
        if found_project:
            click.echo("* deleting previous project: {}".format(found_project))
            found_project.delete()

    po_user, po_user_password, mo_user, po_user_password = get_or_create_super_po_mo_users(create_super=False)

    click.echo("* creating Projects")
    public_project = Project.objects.create(name=project_names[0], is_public=True)
    public_project.owner = po_user
    public_project.model_owners.add(mo_user)
    public_project.save()

    private_project = Project.objects.create(name=project_names[1], is_public=False)
    private_project.owner = po_user
    private_project.model_owners.add(mo_user)
    private_project.save()

    Target.objects.create(project=public_project, name="Test target", description="a Target for testing")
    tz_today = TimeZero.objects.create(project=public_project,
                                       timezero_date=str(datetime.date.today()),
                                       data_version_date=None)

    template_path = Path('forecast_app/tests/2016-2017_submission_template.csv')
    click.echo("* loading template into public_project={}, template_path={}".format(public_project, template_path))
    start_time = timeit.default_timer()
    public_project.load_template(template_path)
    click.echo("  loaded template: {}. {}".format(public_project.csv_filename, timeit.default_timer() - start_time))

    click.echo("creating ForecastModel")
    forecast_model = ForecastModel.objects.create(project=public_project,
                                                  name='Test ForecastModel',
                                                  description="a ForecastModel for testing",
                                                  home_url='http://example.com')

    csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')
    click.echo("* loading forecast into forecast_model={}, csv_file_path={}".format(forecast_model, csv_file_path))
    start_time = timeit.default_timer()
    forecast = forecast_model.load_forecast(csv_file_path, tz_today)
    click.echo("  loaded forecast={}. {}".format(forecast, timeit.default_timer() - start_time))

    click.echo("* done!")


if __name__ == '__main__':
    make_minimal_projects_app()
