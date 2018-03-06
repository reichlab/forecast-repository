import datetime
import timeit
from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Project, TimeZero, ForecastModel, Target
from utils.make_2016_2017_flu_contest_project import get_or_create_super_po_mo_users


@click.command()
def make_minimal_projects_app():
    """
    App to populate the Heroku database with fairly minimal data for simple browsing - one Project, two ForecastModels,
    one with one Forecast and the other with no Forecasts. NB: requires DJANGO_SETTINGS_MODULE to be set. Final
    projects:

    public_project (2016-2017_submission_template.csv)
        targets: target1
        time_zeros: time_zero1, time_zero2
        models:
            forecast_model1
                time_zero1: forecast1 (EW1-KoTsarima-2017-01-17.csv)
                time_zero2: not set
            forecast_model2
                time_zero1: not set
                time_zero2: not set

    private_project


    cd ~/IdeaProjects/forecast-repository/
    export PYTHONPATH=.
    pipenv shell
    python3 utils/fix_owners_app.py

    """
    click.echo("* started creating temp projects")

    project_names = ['public project', 'private project']
    for project_name in project_names:
        found_project = Project.objects.filter(name=project_name).first()
        if found_project:
            click.echo("* deleting previous project: {}".format(found_project))
            found_project.delete()

    po_user, _, mo_user, _ = get_or_create_super_po_mo_users(create_super=False)

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
    time_zero1 = TimeZero.objects.create(project=public_project,
                                         timezero_date=str(datetime.date.today() - datetime.timedelta(days=1)),
                                         data_version_date=None)
    time_zero2 = TimeZero.objects.create(project=public_project,
                                         timezero_date=str(datetime.date.today()),
                                         data_version_date=None)

    # template_path = Path('forecast_app/tests/2016-2017_submission_template.csv')
    template_path = Path('forecast_app/tests/2016-2017_submission_template-small.csv')
    click.echo("* loading template into public_project={}, template_path={}".format(public_project, template_path))
    start_time = timeit.default_timer()
    public_project.load_template(template_path)
    click.echo("  loaded template: {}. {}".format(public_project.csv_filename, timeit.default_timer() - start_time))

    click.echo("creating ForecastModel")
    forecast_model1 = ForecastModel.objects.create(project=public_project,
                                                   name='Test ForecastModel1',
                                                   description="a ForecastModel for testing",
                                                   home_url='http://example.com',
                                                   owner=mo_user)
    # csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')
    csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')
    click.echo("* loading forecast into forecast_model={}, csv_file_path={}".format(forecast_model1, csv_file_path))
    start_time = timeit.default_timer()
    forecast1 = forecast_model1.load_forecast(csv_file_path, time_zero1)
    click.echo("  loaded forecast={}. {}".format(forecast1, timeit.default_timer() - start_time))

    ForecastModel.objects.create(project=public_project,
                                 name='Test ForecastModel2',
                                 description="a second ForecastModel for testing",
                                 home_url='http://example.com',
                                 owner=mo_user)

    click.echo("* Done")


if __name__ == '__main__':
    make_minimal_projects_app()
