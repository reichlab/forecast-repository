import itertools
from pathlib import Path

import click
import django
import pymmwr


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from django.contrib.auth.models import Group, User
from forecast_app.models.project import PROJECT_OWNER_GROUP_NAME

from forecast_app.models import Project, Target, TimeZero, ForecastModel


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
@click.argument('kot_data_dir', type=click.Path(file_okay=False, exists=True))
def make_cdc_flu_challenge_project_app(kot_data_dir):
    """
    Deletes and creates a database with one project, one group, and two classes of users. Then loads models from the
    CDC Flu challenge project. The data directory should be a cloned version of the following repo, which has then been
    normalized via normalize_cdc_flu_challenge_filenames_app.py :
    https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble

    @:param: kot_data_dir is a directory cloned from https://github.com/matthewcornell/split_kot_models_from_submissions
    """
    project_name = 'CDC Flu challenge (2016-2017)'
    found_project = Project.objects.filter(name=project_name).first()
    if found_project:
        click.echo("* deleting previous project")
        found_project.delete()

    po_user, po_user_password, mo_user, po_user_password = get_or_create_super_po_mo_users(create_super=False)

    click.echo("* creating CDC Flu challenge project...")
    project = make_cdc_flu_challenge_project(project_name, CDC_CONFIG_DICT)
    project.owner = po_user
    project.model_owners.add(mo_user)
    project.save()

    # make the models, first downloading kot_data_dir if necessary
    make_cdc_flu_challenge_models(project, mo_user, Path(kot_data_dir))
    click.echo('* done!')


def get_or_create_super_po_mo_users(create_super):
    """
    A utility that creates (as necessary) a group named PROJECT_OWNER_GROUP_NAME and three users - 'project_owner1' (a
    member of that group), 'model_owner1' (not a member), and a superuser

    :param create_super: boolean that controls whether a superuser is created. used only for testing b/c password is shown
    :return: a 4-tuple (if not create_super) or 6-tuple (if create_super) of Users and passwords:
        (superuser, superuser_password, po_user, po_user_password, mo_user, mo_user_password)
    """
    po_group = Group.objects.filter(name=PROJECT_OWNER_GROUP_NAME).first()
    if not po_group:
        click.echo("* creating PO group: {}".format(PROJECT_OWNER_GROUP_NAME))
        po_group = Group.objects.create(name=PROJECT_OWNER_GROUP_NAME)

    po_username = 'project_owner1'
    po_user_password = 'po1-asdf'
    po_user = User.objects.filter(username=po_username).first()
    if not po_user:
        click.echo("* creating PO user")
        po_user = User.objects.create_user(username=po_username, password=po_user_password)
        po_user.groups.add(po_group)

    mo_username = 'model_owner1'
    mo_user_password = 'mo1-asdf'
    mo_user = User.objects.filter(username=mo_username).first()
    if not mo_user:
        click.echo("* creating MO user")
        mo_user = User.objects.create_user(username=mo_username, password=mo_user_password)

    super_username = 'superuser1'
    superuser_password = 'su1-asdf'
    superuser = User.objects.filter(username=super_username).first()
    if create_super and not superuser:
        click.echo("* creating supersuser")
        superuser = User.objects.create_superuser(username=super_username, password=superuser_password,
                                                  email='test@example.com')

    return (superuser, superuser_password, po_user, po_user_password, mo_user, mo_user_password) if create_super \
        else (po_user, po_user_password, mo_user, mo_user_password)


def make_cdc_flu_challenge_project(project_name, config_dict):
    project = Project.objects.create(
        is_public=True,
        name=project_name,
        description="Code, results, submissions, and method description for the 2016-2017 CDC flu contest submissions "
                    "based on ensembles.",
        home_url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        logo_url='http://reichlab.io/assets/images/logo/nav-logo.png',
        core_data='https://github.com/reichlab/2016-2017-flu-contest-ensembles/tree/master/inst/submissions',
        config_dict=config_dict)

    click.echo("  loading template")
    project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))  # todo xx move into repo

    # create Targets
    week_ahead_descr = "One- to four-week ahead forecasts will be defined as the weighted ILINet percentage for the target week."
    for target_name, description in (
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
        Target.objects.create(project=project, name=target_name, description=description)

    # create TimeZeros
    yr_wk_2016 = list(zip(itertools.repeat(2016), range(43, 53)))
    yr_wk_2017 = list(zip(itertools.repeat(2017), range(1, 19)))
    for mmwr_year, mmwr_week in yr_wk_2016 + yr_wk_2017:
        TimeZero.objects.create(project=project,
                                timezero_date=str(pymmwr.mmwr_week_to_date(mmwr_year, mmwr_week)),
                                data_version_date=None)

    # done
    return project


def make_cdc_flu_challenge_models(project, model_owner, kot_data_dir):
    """
    creates the four Kernel of Truth (KoT) ForecastModels and their Forecasts
    """
    click.echo("* creating CDC Flu challenge models. model_owner={}, kot_data_dir={}".format(model_owner, kot_data_dir))

    # KoT ensemble
    click.echo("  ensemble")
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT ensemble',
        description="Team Kernel of Truth's ensemble model.",
        home_url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        aux_data_url='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble')
    forecast_model.load_forecasts_from_dir(kot_data_dir / 'ensemble',
                                           callback_fcn=lambda cdc_csv_file: click.echo("    {}".format(cdc_csv_file)))

    # KoT Kernel Density Estimation (KDE)
    click.echo("  kde")
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT KDE',
        description="Team Kernel of Truth's 'fixed' model using Kernel Density Estimation.",
        home_url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        aux_data_url='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/kde')
    forecast_model.load_forecasts_from_dir(kot_data_dir / 'kde',
                                           callback_fcn=lambda cdc_csv_file: click.echo("    {}".format(cdc_csv_file)))

    # KoT Kernel Conditional Density Estimation (KCDE)
    click.echo("  kcde")
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT KCDE',
        description="Team Kernel of Truth's model combining Kernel Conditional Density Estimation (KCDE) and copulas.",
        home_url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        aux_data_url='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/kcde')
    forecast_model.load_forecasts_from_dir(kot_data_dir / 'kcde',
                                           callback_fcn=lambda cdc_csv_file: click.echo("    {}".format(cdc_csv_file)))

    # KoT SARIMA
    click.echo("  sarima")
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT SARIMA',
        description="Team Kernel of Truth's SARIMA model.",
        home_url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        aux_data_url='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/sarima')
    forecast_model.load_forecasts_from_dir(kot_data_dir / 'sarima',
                                           callback_fcn=lambda cdc_csv_file: click.echo("    {}".format(cdc_csv_file)))

    # done
    return project


if __name__ == '__main__':
    make_cdc_flu_challenge_project_app()
