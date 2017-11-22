import os
import tempfile
from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()
from django.contrib.auth.models import Group, User
from forecast_app.models.project import PROJECT_OWNER_GROUP_NAME

from utils.mmwr_utils import end_date_2016_2017_for_mmwr_week
from forecast_app.models import Project, Target, TimeZero, ForecastModel, Forecast
from forecast_app.models.data import ProjectTemplateData, ForecastData


CDC_CONFIG_DICT = {
    "target_to_week_increment": {
        "1 wk ahead": 1,
        "2 wk ahead": 2,
        "3 wk ahead": 3,
        "4 wk ahead": 4
    },
    "location_to_delphi_region": {
        "US National": "nat",
        "HHS Region 1": "hhs1",
        "HHS Region 2": "hhs2",
        "HHS Region 3": "hhs3",
        "HHS Region 4": "hhs4",
        "HHS Region 5": "hhs5",
        "HHS Region 6": "hhs6",
        "HHS Region 7": "hhs7",
        "HHS Region 8": "hhs8",
        "HHS Region 9": "hhs9",
        "HHS Region 10": "hhs10"
    }
}


@click.command()
@click.option('--kot_data_dir', type=click.Path())
def make_example_projects_app(kot_data_dir):
    """
    Deletes and creates a database with one project, one group, and two classes of users. Then loads models from the
    CDC Flu challenge project.

    @:param: kot_data_dir is an optional location of files cloned from
        https://github.com/matthewcornell/split_kot_models_from_submissions . if not passed, the program will clone a
        copy to /tmp , like:

        $ cd /tmp
        $ git clone https://github.com/matthewcornell/split_kot_models_from_submissions.git

    """
    click.echo("* deleting database...")
    for model_class in [Project, Target, TimeZero, ForecastModel, Forecast, ProjectTemplateData, ForecastData]:
        model_class.objects.all().delete()

    click.echo("* (re)creating group and PO and MO users: {}".format(PROJECT_OWNER_GROUP_NAME))
    Group.objects.filter(name=PROJECT_OWNER_GROUP_NAME).delete()
    po_group = Group.objects.create(name=PROJECT_OWNER_GROUP_NAME)

    User.objects.filter(username='project_owner1').delete()
    po_user = User.objects.create_user(username='project_owner1', password='po1-asdf')

    User.objects.filter(username='model_owner1').delete()
    mo_user = User.objects.create_user(username='model_owner1', password='mo1-asdf')

    po_user.groups.add(po_group)

    click.echo("* creating CDC Flu challenge project...")
    project = make_cdc_flu_challenge_project(CDC_CONFIG_DICT)
    project.owner = po_user
    project.model_owners.add(mo_user)
    project.save()

    # download to kot_data_dir if necessary
    if kot_data_dir and os.path.exists(kot_data_dir):
        make_cdc_flu_challenge_models(project, mo_user, Path(kot_data_dir))
    elif kot_data_dir:
        raise RuntimeError("Passed kot_data_dir doesn't exist: {}".format(kot_data_dir))
    else:
        with tempfile.TemporaryDirectory() as tmp_clone_parent_dir:
            click.echo("* no kot_data_dir. cloning split_kot_models_from_submissions into {}"
                       .format(tmp_clone_parent_dir))
            os.system('git clone https://github.com/matthewcornell/split_kot_models_from_submissions.git {}'
                      .format(tmp_clone_parent_dir))
            make_cdc_flu_challenge_models(project, mo_user, Path(tmp_clone_parent_dir))

    click.echo('* done!')


def make_cdc_flu_challenge_project(config_dict):
    project = Project.objects.create(
        name='CDC Flu challenge (2016-2017)',
        description="Code, results, submissions, and method description for the 2016-2017 CDC flu contest submissions "
                    "based on ensembles.",
        url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        core_data='https://github.com/reichlab/2016-2017-flu-contest-ensembles/tree/master/inst/submissions',
        config_dict=config_dict)
    project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))  # todo xx move into repo
    week_ahead_descr = "One- to four-week ahead forecasts will be defined as the weighted ILINet percentage for the target week."
    for target_name, descr in (
            ('Season onset',
             "The onset of the season is defined as the MMWR surveillance week "
             "(http://wwwn.cdc.gov/nndss/script/downloads.aspx) when the percentage of visits for influenza-like illness (ILI) "
             "reported through ILINet reaches or exceeds the baseline value for three consecutive weeks (updated 2016-2017 "
             "ILINet baseline values for the US and each HHS region will be available at "
             "http://www.cdc.gov/flu/weekly/overview.htm the week of October 10, 2016). Forecasted 'onset' week values should "
             "be for the first week of that three week period."),
            ('Season peak week',
             "The peak week will be defined as the MMWR surveillance week that the weighted ILINet percentage is the highest "
             "for the 2016-2017 influenza season."),
            ('Season peak percentage',
             "The intensity will be defined as the highest numeric value that the weighted ILINet percentage reaches during " \
             "the 2016-2017 influenza season."),
            ('1 wk ahead', week_ahead_descr),
            ('2 wk ahead', week_ahead_descr),
            ('3 wk ahead', week_ahead_descr),
            ('4 wk ahead', week_ahead_descr)):
        Target.objects.create(project=project, name=target_name, description=descr)

    # create the project's TimeZeros. b/c this is a CDC project, timezero_dates are all MMWR Week ENDING Dates as listed in
    # MMWR_WEEK_TO_YEAR_TUPLE. note that the project has no data_version_dates
    for mmwr_week in list(range(43, 53)) + list(range(1, 19)):  # [43, ..., 52, 1, ..., 18] for 2016-2017
        TimeZero.objects.create(project=project,
                                timezero_date=str(end_date_2016_2017_for_mmwr_week(mmwr_week)),
                                data_version_date=None)

    # done
    return project


def make_cdc_flu_challenge_models(project, model_owner, kot_data_dir):
    """
    creates the four Kernel of Truth (KoT) ForecastModels and their Forecasts
    """
    click.echo("* creating CDC Flu challenge models. model_owner={}, kot_data_dir={}".format(model_owner, kot_data_dir))

    # KoT ensemble
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT ensemble',
        description="Team Kernel of Truth's ensemble model.",
        url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        auxiliary_data='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble')
    add_kot_forecasts_to_model(forecast_model, kot_data_dir, 'ensemble')

    # KoT Kernel Density Estimation (KDE)
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT KDE',
        description="Team Kernel of Truth's 'fixed' model using Kernel Density Estimation.",
        url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        auxiliary_data='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/kde')
    add_kot_forecasts_to_model(forecast_model, kot_data_dir, 'kde')

    # KoT Kernel Conditional Density Estimation (KCDE)
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT KCDE',
        description="Team Kernel of Truth's model combining Kernel Conditional Density Estimation (KCDE) and copulas.",
        url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        auxiliary_data='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/kcde')
    add_kot_forecasts_to_model(forecast_model, kot_data_dir, 'kcde')

    # KoT SARIMA
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT SARIMA',
        description="Team Kernel of Truth's SARIMA model.",
        url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        auxiliary_data='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/sarima')
    add_kot_forecasts_to_model(forecast_model, kot_data_dir, 'sarima')

    # done
    return project


def add_kot_forecasts_to_model(forecast_model, kot_data_dir, kot_model_dir_name):
    """
    Adds Forecast objects to forecast_model based on kot_model_dir_name under kot_data_dir. Recall data file naming
    scheme: 'EW<mmwr_week>-<team_name>-<sub_date_yyy_mm_dd>.csv'
    """
    click.echo('add_forecasts_to_model. forecast_model={}, kot_model_dir_name={}, kot_data_dir={}'
               .format(forecast_model, kot_model_dir_name, kot_data_dir))

    # Set KOT_DATA_DIR. We assume the KOT_DATA_DIR is set to the cloned location of
    # https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble e.g.,
    kot_model_dir = kot_data_dir / kot_model_dir_name
    if not Path(kot_model_dir).exists():
        raise RuntimeError("KOT_DATA_DIR does not exist: {}".format(kot_data_dir))

    for csv_file in [csv_file for csv_file in kot_model_dir.glob('*.csv')]:  # 'EW1-KoTstable-2017-01-17.csv'
        mmwr_week = csv_file.name.split('-')[0].split('EW')[1]  # re.split(r'^EW(\d*).*$', csv_file.name)[1]
        timezero_date = end_date_2016_2017_for_mmwr_week(int(mmwr_week))
        time_zero = forecast_model.time_zero_for_timezero_date_str(timezero_date)
        if not time_zero:
            raise RuntimeError("no time_zero found for timezero_date={}. csv_file={}, mmwr_week={}".format(
                timezero_date, csv_file, mmwr_week))

        click.echo('  csv_file={}'.format(csv_file))
        forecast_model.load_forecast(csv_file, time_zero)


if __name__ == '__main__':
    make_example_projects_app()
