import itertools
import timeit
from pathlib import Path

import click
import django
import pymmwr


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, get_or_create_super_po_mo_users, CDC_CONFIG_DICT
from forecast_app.models import Project, TimeZero, ForecastModel


@click.command()
@click.argument('kot_data_dir', type=click.Path(file_okay=False, exists=True))
def make_2016_2017_flu_contest_project_app(kot_data_dir):
    """
    Deletes and creates a Project, along with one group, and two classes of users. Then loads models from the CDC Flu
    challenge project.

    :param: kot_data_dir: data directory should be a cloned version of the following repo, which has then been
    normalized via normalize_filenames_2016_2017_flu_contest.py :
    https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble

    :param: kot_data_dir: a directory cloned from
        https://github.com/matthewcornell/split_kot_models_from_submissions , which has then been normalized via
        normalize_filenames_2016_2017_flu_contest.py .
    """
    start_time = timeit.default_timer()
    project_name = 'CDC Flu challenge (2016-2017)'
    found_project = Project.objects.filter(name=project_name).first()
    if found_project:
        click.echo("* deleting previous project")
        found_project.delete()

    po_user, _, mo_user, _ = get_or_create_super_po_mo_users(create_super=False)

    click.echo("* creating CDC Flu challenge project...")
    project = make_cdc_flu_challenge_project(project_name, CDC_CONFIG_DICT)
    project.owner = po_user
    project.model_owners.add(mo_user)
    project.save()

    # make the models, first downloading kot_data_dir if necessary
    make_cdc_flu_challenge_models(project, mo_user, Path(kot_data_dir))

    # done
    click.echo("* Done. time: {}".format(timeit.default_timer() - start_time))


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
    make_cdc_locations_and_targets(project)
    project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))  # todo xx move into repo

    # create TimeZeros
    yr_wk_2016 = list(zip(itertools.repeat(2016), range(43, 53)))
    yr_wk_2017 = list(zip(itertools.repeat(2017), range(1, 19)))
    for idx, (mmwr_year, mmwr_week) in enumerate(yr_wk_2016 + yr_wk_2017):
        TimeZero.objects.create(project=project,
                                timezero_date=str(pymmwr.mmwr_week_to_date(mmwr_year, mmwr_week)),
                                data_version_date=None,
                                is_season_start=(True if idx == 0 else False),
                                season_name=('2016-2017' if idx == 0 else None))

    click.echo("  loading truth values")
    project.load_truth_data(Path('utils/ensemble-truth-table-script/truths-2016-2017-reichlab.csv'))

    # done
    return project


def make_cdc_flu_challenge_models(project, model_owner, kot_data_dir):
    """
    Creates the four Kernel of Truth (KoT) ForecastModels and their Forecasts.
    """
    click.echo("* creating CDC Flu challenge models. model_owner={}, kot_data_dir={}".format(model_owner, kot_data_dir))

    # KoT ensemble
    click.echo("** ensemble")
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT ensemble',
        description="Team Kernel of Truth's ensemble model.",
        home_url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        aux_data_url='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble')
    forecast_model.load_forecasts_from_dir(kot_data_dir / 'ensemble')

    # KoT Kernel Density Estimation (KDE)
    click.echo("** kde")
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT KDE',
        description="Team Kernel of Truth's 'fixed' model using Kernel Density Estimation.",
        home_url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        aux_data_url='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/kde')
    forecast_model.load_forecasts_from_dir(kot_data_dir / 'kde')

    # KoT Kernel Conditional Density Estimation (KCDE)
    click.echo("** kcde")
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT KCDE',
        description="Team Kernel of Truth's model combining Kernel Conditional Density Estimation (KCDE) and copulas.",
        home_url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        aux_data_url='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/kcde')
    forecast_model.load_forecasts_from_dir(kot_data_dir / 'kcde')

    # KoT SARIMA
    click.echo("** sarima")
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='KoT SARIMA',
        description="Team Kernel of Truth's SARIMA model.",
        home_url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
        aux_data_url='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/sarima')
    forecast_model.load_forecasts_from_dir(kot_data_dir / 'sarima')

    # done
    return project


if __name__ == '__main__':
    make_2016_2017_flu_contest_project_app()
