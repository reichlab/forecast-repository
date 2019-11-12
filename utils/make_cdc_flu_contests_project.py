import csv
import json
import logging
import timeit
from collections import defaultdict
from pathlib import Path

import click
import django
import pymmwr
import yaml
from django.template import Template, Context


logger = logging.getLogger(__name__)

# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import TimeZero, ForecastModel, Project
from django.contrib.auth.models import User
from utils.project import create_project_from_json, validate_and_create_locations, validate_and_create_targets, \
    delete_project_iteratively
from utils.normalize_filenames_2016_2017_flu_contest import SEASON_START_EW_NUMBER
from utils.cdc import cdc_csv_components_from_data_dir, cdc_csv_filename_components, first_model_subdirectory, \
    load_cdc_csv_forecasts_from_dir


#
# ---- application----
#

CDC_PROJECT_NAME = 'CDC Flu challenge'


@click.command()
@click.argument('component_models_dir_ensemble', type=click.Path(file_okay=False, exists=True))
@click.argument('truths_csv_file', type=click.Path(file_okay=True, exists=True))
def make_cdc_flu_contests_project_app(component_models_dir_ensemble, truths_csv_file):
    """
    This application creates a CDC flu contest project from the FluSightNetwork models project's component models:
    https://github.com/FluSightNetwork/cdc-flusight-ensemble/tree/master/model-forecasts/component-models . It uses
    truth data from a file in this project: utils/ensemble-truth-table-script/truths-2010-through-2018.csv (see the
    readme for details). Thus it depends on both of these inputs being up-to-date.

    Notes:
    - DELETES any existing project without prompting
    - uses the model's 'metadata.txt' file's 'model_name' to find an existing model, if any
    """
    start_time = timeit.default_timer()
    click.echo(f"* make_cdc_flu_contests_project_app(): component_models_dir_ensemble={component_models_dir_ensemble}, "
               f"truths_csv_file={truths_csv_file}")

    # create the project. error if already exists
    project = Project.objects.filter(name=CDC_PROJECT_NAME).first()  # None if doesn't exist
    if project:
        logger.warning(f"make_cdc_flu_contests_project_app(): found existing project. deleting project={project}")
        delete_project_iteratively(project)
    po_user, _, mo_user, _ = get_or_create_super_po_mo_users(is_create_super=False)

    logger.info("* Creating Project...")
    project = create_project_from_json(Path('forecast_app/tests/projects/cdc-project.json'), po_user)
    project.model_owners.add(mo_user)
    logger.info(f"- created Project: {project}")

    logger.info("* Creating TimeZeros...")
    component_models_dir_ensemble = Path(component_models_dir_ensemble)
    model_subdir = first_model_subdirectory(component_models_dir_ensemble)
    if not model_subdir:
        raise RuntimeError(f"first_model_subdirectory was None. component_models_dir_ensemble="
                           f"{component_models_dir_ensemble}. did you run "
                           f"normalize_filenames_cdc_flusight_ensemble.py?")

    timezeros = make_timezeros(project, [model_subdir])
    logger.info("- created {} TimeZeros: {}"
                .format(len(timezeros), sorted(timezeros, key=lambda time_zero: time_zero.timezero_date)))

    # load the truth data
    truth_file_path = Path(truths_csv_file)
    logger.info("* Loading truth values: {}".format(truth_file_path))
    project.load_truth_data(truth_file_path)
    logger.info("- loaded truth values")

    # create the models
    model_dirs_to_load = []
    for component_models_dir in [component_models_dir_ensemble]:
        model_dirs_to_load.extend(get_model_dirs_to_load(component_models_dir))

    click.echo("* Creating models. {} model_dirs_to_load={}"
               .format(len(model_dirs_to_load), [d.name for d in model_dirs_to_load]))
    models = make_cdc_flusight_ensemble_models(project, model_dirs_to_load, po_user)
    click.echo("- created {} model(s): {}".format(len(models), models))

    # load the forecasts
    click.echo("* Loading forecasts")
    model_name_to_forecasts = load_forecasts(project, model_dirs_to_load)
    click.echo("- Loading forecasts: loaded {} forecast(s)".format(sum(map(len, model_name_to_forecasts.values()))))

    # done!
    click.echo(f"* Done. time: {timeit.default_timer() - start_time}")


def make_timezeros(project, model_dirs):
    """
    Create TimeZeros for project based on model_dirs. Returns a list of them.

    :param model_dirs: a list of model directories such as returned by first_model_subdirectory()
    """
    timezeros = []
    season_start_years = []  # helps track season transitions
    for model_dir in model_dirs:
        for cdc_csv_file, timezero_date, _, data_version_date in cdc_csv_components_from_data_dir(model_dir):
            if not is_cdc_file_ew43_through_ew18(cdc_csv_file):
                logger.info(
                    "s (not in range)\t{}\t".format(cdc_csv_file.name))  # 's' from load_cdc_csv_forecasts_from_dir()
                continue

            # NB: we skip existing TimeZeros in case we are loading new forecasts
            found_time_zero = project.time_zero_for_timezero_date(timezero_date)
            if found_time_zero:
                logger.info(
                    "s (TimeZero exists)\t{}\t".format(cdc_csv_file.name))  # 's' from load_cdc_csv_forecasts_from_dir()
                continue

            season_start_year = season_start_year_for_date(timezero_date)
            is_new_season = season_start_year not in season_start_years
            new_season_name = '{}-{}'.format(season_start_year, season_start_year + 1) if is_new_season else None
            timezeros.append(TimeZero.objects.create(project=project,
                                                     timezero_date=timezero_date,
                                                     data_version_date=data_version_date,
                                                     is_season_start=(True if is_new_season else False),
                                                     season_name=(new_season_name if is_new_season else None)))
            if is_new_season:
                season_start_years.append(season_start_year)
    return timezeros


def get_model_dirs_to_load(component_models_dir):
    """
    :return: list of Paths under component_models_dir that are listed in model-id-map.csv
    """
    model_dirs_to_load = []
    with open(str(component_models_dir / 'model-id-map.csv')) as model_id_map_csv_fp:
        csv_reader = csv.reader(model_id_map_csv_fp, delimiter=',')
        next(csv_reader)  # skip header
        for model_id, model_dir, complete in csv_reader:
            model_dirs_to_load.append(component_models_dir / model_dir)
    return sorted(model_dirs_to_load, key=lambda model_dir: model_dir.name)


def make_cdc_flusight_ensemble_models(project, model_dirs_to_load, model_owner):
    """
    Loads forecast data for models in model_dirs_to_load, with model_owner as the owner for all of them.
    """
    models = []
    for model_dir in model_dirs_to_load:
        if not model_dir.is_dir():
            click.echo("Warning: model_dir was not a directory: {}".format(model_dir))
            continue

        # get model name and description from metadata.txt
        metadata_dict = metadata_dict_for_file(model_dir / 'metadata.txt')
        model_name = metadata_dict['model_name']
        team_name = metadata_dict['team_name']
        found_model = ForecastModel.objects.filter(name=model_name).first()
        if found_model:
            click.echo("Warning: using existing model with same name: {}".format(found_model))
            continue

        # build description
        description_template_str = """<em>Team name</em>: {{ team_name }}.
    <em>Team members</em>: {{ team_members }}.
    <em>Data source(s)</em>: {% if data_source1 %}{{ data_source1 }}{% if data_source2 %}, {{ data_source2 }}{% endif %}{% else %}None specified{% endif %}.
    <em>Methods</em>: {{ methods }}
    """
        description_template = Template(description_template_str)
        description = description_template.render(
            Context({'team_name': team_name,
                     'team_members': metadata_dict['team_members'],
                     'data_source1': metadata_dict['data_source1'] if 'data_source1' in metadata_dict else None,
                     'data_source2': metadata_dict['data_source2'] if 'data_source2' in metadata_dict else None,
                     'methods': metadata_dict['methods'],
                     }))
        home_url = 'https://github.com/FluSightNetwork/cdc-flusight-ensemble/tree/master/model-forecasts/component-models' \
                   + '/' + model_dir.name
        forecast_model = ForecastModel.objects.create(owner=model_owner, project=project, name=model_name,
                                                      team_name=team_name, description=description,
                                                      home_url=home_url)
        models.append(forecast_model)
    return models


def load_forecasts(project, model_dirs_to_load):
    """
    Loads forecast data for models in model_dirs_to_load. Assumes model names in each directory's metadata.txt matches
    those in project, as done by make_cdc_flusight_ensemble_models().

    :return model_name_to_forecasts, which maps model_name -> list of its Forecasts
    """
    model_name_to_forecasts = defaultdict(list)
    for idx, model_dir in enumerate(model_dirs_to_load):
        if not model_dir.is_dir():
            click.echo("Warning: model_dir was not a directory: {}".format(model_dir))
            continue

        click.echo("** {}/{}: {}".format(idx, len(model_dirs_to_load) - 1, model_dir))
        metadata_dict = metadata_dict_for_file(model_dir / 'metadata.txt')
        model_name = metadata_dict['model_name']
        forecast_model = project.models.filter(name=model_name).first()
        if not forecast_model:
            raise RuntimeError("couldn't find model named '{}' in project {}".format(model_name, project))

        forecasts = load_cdc_csv_forecasts_from_dir(forecast_model, model_dir,
                                                    is_load_file=is_cdc_file_ew43_through_ew18)
        model_name_to_forecasts[model_name].extend(forecasts)

    return model_name_to_forecasts


#
# metadata.txt utilities
#

def metadata_dict_for_file(metadata_file):
    with open(metadata_file) as metadata_fp:
        metadata_dict = yaml.safe_load(metadata_fp)
    return metadata_dict


#
# ---- User utilities ----
#

def get_or_create_super_po_mo_users(is_create_super):
    """
    A utility that creates (as necessary) three users - 'project_owner1', 'model_owner1', and a superuser. Should
    probably only be used for testing.

    :param is_create_super: boolean that controls whether a superuser is created. used only for testing b/c password is
        shown
    :return: a 4-tuple (if not create_super) or 6-tuple (if create_super) of Users and passwords:
        (superuser, superuser_password, po_user, po_user_password, mo_user, mo_user_password)
    """
    po_username = 'project_owner1'
    po_user_password = 'po1-asdf'
    po_user = User.objects.filter(username=po_username).first()
    if not po_user:
        logger.info("* creating PO user")
        po_user = User.objects.create_user(username=po_username, password=po_user_password)

    mo_username = 'model_owner1'
    mo_user_password = 'mo1-asdf'
    mo_user = User.objects.filter(username=mo_username).first()
    if not mo_user:
        logger.info("* creating MO user")
        mo_user = User.objects.create_user(username=mo_username, password=mo_user_password)

    super_username = 'superuser1'
    superuser_password = 'su1-asdf'
    superuser = User.objects.filter(username=super_username).first()
    if is_create_super and not superuser:
        logger.info("* creating supersuser")
        superuser = User.objects.create_superuser(username=super_username, password=superuser_password,
                                                  email='test@example.com')

    return (superuser, superuser_password, po_user, po_user_password, mo_user, mo_user_password) if is_create_super \
        else (po_user, po_user_password, mo_user, mo_user_password)


#
# ---- test utilities ----
#

def make_cdc_locations_and_targets(project):
    """
    Creates CDC standard Targets for project.
    """
    with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
        project_dict = json.load(fp)
    validate_and_create_locations(project, project_dict)
    validate_and_create_targets(project, project_dict)


#
# ---- CDC EW utilities ----
#

def is_cdc_file_ew43_through_ew18(cdc_csv_file):
    # only accept EW43 through EW18 per: "Following CDC guidelines from 2017/2018 season, using scores from
    # files from each season labeled EW43 through EW18 (i.e. files outside that range will not be considered)"
    time_zero, _, _ = cdc_csv_filename_components(cdc_csv_file.name)
    ywd_mmwr_dict = pymmwr.date_to_mmwr_week(time_zero)
    mmwr_week = ywd_mmwr_dict['week']
    return (mmwr_week <= 18) or (mmwr_week >= 43)


def season_start_year_for_date(date):
    """
    example seasons:
    - 2015/2016: EW30-2015 through EW29-2016
    - 2016/2017: EW30-2016 through EW29-2017
    - 2017/2018: EW30-2017 through EW29-2018

    rule:
    - EW01 through EW29: the previous year
    - EW30 through EW52/EW53: the current year

    :param date:
    :return: the season start year that date is in, based on SEASON_START_EW_NUMBER
    """
    ywd_mmwr_dict = pymmwr.date_to_mmwr_week(date)
    mmwr_year = ywd_mmwr_dict['year']
    mmwr_week = ywd_mmwr_dict['week']
    return mmwr_year - 1 if mmwr_week < SEASON_START_EW_NUMBER else mmwr_year


#
# ---- main ----
#

if __name__ == '__main__':
    make_cdc_flu_contests_project_app()
