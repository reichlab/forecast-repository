import csv
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

from django.contrib.auth.models import User
from forecast_app.models import Project, Target, TimeZero, ForecastModel
from utils.cdc import CDC_CONFIG_DICT
from utils.normalize_filenames_2016_2017_flu_contest import SEASON_START_EW_NUMBER
from utils.utilities import cdc_csv_components_from_data_dir, cdc_csv_filename_components, first_model_subdirectory


#
# ---- application----
#

@click.command()
@click.argument('kot_data_dir', type=click.Path(file_okay=False, exists=True))  # 2016/17
@click.argument('component_models_dir_2017', type=click.Path(file_okay=False, exists=True))  # 2017/18
@click.argument('component_models_dir_ensemble', type=click.Path(file_okay=False, exists=True))  # ensemble
def make_cdc_flu_contests_project_app(kot_data_dir, component_models_dir_2017, component_models_dir_ensemble):
    """
    This application creates a single large CDC flu contest project from three model directories:
    - 2016-2017 Reichlab submission - https://github.com/reichlab/2016-2017-flu-contest-ensembles
      (Note: This app actually runs on split_kot_models_from_submissions, which is a processed version that implements
       Zoltar's naming and layout scheme for model directories and file names.)
    - 2017-2018 Reichlab submission - https://github.com/reichlab/2017-2018-cdc-flu-contest
    - FluSightNetwork models - https://github.com/FluSightNetwork/cdc-flusight-ensemble

    Notes:
    - DELETES any existing project without prompting
    - uses the model's 'metadata.txt' file's 'model_name' to find an existing model, if any
    - takes b/w 30 and 90 minutes to run (sqlite3 vs. postgres)
    """
    start_time = timeit.default_timer()

    # create the project. error if already exists
    project_name = 'CDC Flu challenge'
    project = Project.objects.filter(name=project_name).first()  # None if doesn't exist
    if project:
        logger.warning("Found existing project. deleting project={}".format(project))
        project.delete()

    # make and fill the Project, Targets, and TimeZeros
    kot_data_dir = Path(kot_data_dir)
    component_models_dir_2017 = Path(component_models_dir_2017)
    component_models_dir_ensemble = Path(component_models_dir_ensemble)
    po_user, _, mo_user, _ = get_or_create_super_po_mo_users(create_super=False)

    logger.info("* Creating Project...")
    project = make_project(project_name, po_user, mo_user)
    logger.info("- created Project: {}".format(project))

    logger.info("* Creating Targets...")
    targets = make_cdc_targets(project)
    logger.info("- created {} Targets: {}".format(len(targets), targets))

    logger.info("* Creating TimeZeros...")
    time_zeros = make_timezeros(project, [first_model_subdirectory(kot_data_dir),
                                          first_model_subdirectory(component_models_dir_2017),
                                          first_model_subdirectory(component_models_dir_ensemble)])
    logger.info("- created {} TimeZeros: {}"
                .format(len(time_zeros), sorted(time_zeros, key=lambda time_zero: time_zero.timezero_date)))

    # load the template
    template = Path('forecast_app/tests/2016-2017_submission_template-plus-bin-53.csv')
    logger.info("* Loading template...: {}".format(template))
    project.load_template(template)
    logger.info("- Loaded template")

    # load the truth data
    truth_file_path = Path('utils/ensemble-truth-table-script/truths-2010-through-2017.csv')
    logger.info("* Loading truth values: {}".format(truth_file_path))
    project.load_truth_data(truth_file_path)
    logger.info("- loaded truth values")

    # create the models
    model_dirs_to_load = []
    for component_models_dir in [kot_data_dir, component_models_dir_2017, component_models_dir_ensemble]:
        model_dirs_to_load.extend(get_model_dirs_to_load(component_models_dir))

    click.echo("* Creating models. model_dirs_to_load={}".format([d.name for d in model_dirs_to_load]))
    models = make_cdc_flusight_ensemble_models(project, model_dirs_to_load, po_user)
    click.echo("- created {} model(s): {}".format(len(models), models))

    # load the forecasts
    click.echo("* Loading forecasts")
    model_name_to_forecasts = load_forecasts(project, model_dirs_to_load, template)
    click.echo("- Loading forecasts: loaded {} forecast(s)".format(sum(map(len, model_name_to_forecasts.values()))))

    # done!
    logger.info("* Done! time: {}".format(timeit.default_timer() - start_time))


def make_project(project_name, po_user, mo_user):
    project_description = "Guidelines and forecasts for a collaborative U.S. influenza forecasting project."
    home_url = 'http://example.com'  # todo
    logo_url = 'http://reichlab.io/assets/images/logo/nav-logo.png'
    core_data = 'http://example.com'  # todo
    project = Project.objects.create(
        owner=po_user,
        is_public=True,
        name=project_name,
        description=project_description,
        home_url=home_url,
        logo_url=logo_url,
        core_data=core_data,
        config_dict=CDC_CONFIG_DICT)
    project.model_owners.add(mo_user)
    project.save()
    return project


def make_cdc_targets(project):
    """
    Creates CDC standard Targets for project. Returns a list of them.
    """
    targets = []
    week_ahead_descr = "One- to four-week ahead forecasts will be defined as the weighted ILINet percentage for the target week."
    for target_name, description, is_step_ahead, step_ahead_increment in (
            ('Season onset',
             "The onset of the season is defined as the MMWR surveillance week "
             "(http://wwwn.cdc.gov/nndss/script/downloads.aspx) when the percentage of visits for influenza-like illness (ILI) "
             "reported through ILINet reaches or exceeds the baseline value for three consecutive weeks (updated 2016-2017 "
             "ILINet baseline values for the US and each HHS region will be available at "
             "http://www.cdc.gov/flu/weekly/overview.htm the week of October 10, 2016). Forecasted 'onset' week values should "
             "be for the first week of that three week period.",
             False, 0),
            ('Season peak week',
             "The peak week will be defined as the MMWR surveillance week that the weighted ILINet percentage is the highest "
             "for the 2016-2017 influenza season.",
             False, 0),
            ('Season peak percentage',
             "The intensity will be defined as the highest numeric value that the weighted ILINet percentage reaches during " \
             "the 2016-2017 influenza season.",
             False, 0),
            ('1 wk ahead', week_ahead_descr, True, 1),
            ('2 wk ahead', week_ahead_descr, True, 2),
            ('3 wk ahead', week_ahead_descr, True, 3),
            ('4 wk ahead', week_ahead_descr, True, 4)):
        targets.append(Target.objects.create(project=project, name=target_name, description=description,
                                             is_step_ahead=is_step_ahead, step_ahead_increment=step_ahead_increment))
    return targets


def make_timezeros(project, model_dirs):
    """
    Create TimeZeros for project based on model_dirs. Returns a list of them.

    :param model_dirs: a list of model directories such as returned by first_model_subdirectory()
    """
    time_zeros = []
    season_start_years = []  # helps track season transitions
    for model_dir in model_dirs:
        for cdc_csv_file, timezero_date, _, data_version_date in cdc_csv_components_from_data_dir(model_dir):
            if not is_cdc_file_ew43_through_ew18(cdc_csv_file):
                logger.info("s (not in range)\t{}\t".format(cdc_csv_file.name))  # 's' from load_forecasts_from_dir()
                continue

            # NB: we skip existing TimeZeros in case we are loading new forecasts
            found_time_zero = project.time_zero_for_timezero_date(timezero_date)
            if found_time_zero:
                logger.info("s (TimeZero exists)\t{}\t".format(cdc_csv_file.name))  # 's' from load_forecasts_from_dir()
                continue

            season_start_year = season_start_year_for_date(timezero_date)
            is_new_season = season_start_year not in season_start_years
            new_season_name = '{}-{}'.format(season_start_year, season_start_year + 1) if is_new_season else None
            time_zeros.append(TimeZero.objects.create(project=project,
                                                      timezero_date=timezero_date,
                                                      data_version_date=data_version_date,
                                                      is_season_start=(True if is_new_season else False),
                                                      season_name=(new_season_name if is_new_season else None)))
            if is_new_season:
                season_start_years.append(season_start_year)
    return time_zeros


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
            Context({'team_name': metadata_dict['team_name'],
                     'team_members': metadata_dict['team_members'],
                     'data_source1': metadata_dict['data_source1'] if 'data_source1' in metadata_dict else None,
                     'data_source2': metadata_dict['data_source2'] if 'data_source2' in metadata_dict else None,
                     'methods': metadata_dict['methods'],
                     }))

        home_url = 'https://github.com/FluSightNetwork/cdc-flusight-ensemble/tree/master/model-forecasts/component-models' \
                   + '/' + model_dir.name
        forecast_model = ForecastModel.objects.create(owner=model_owner, project=project, name=model_name,
                                                      description=description, home_url=home_url)
        models.append(forecast_model)
    return models


def load_forecasts(project, model_dirs_to_load, template):
    """
    Loads forecast data for models in model_dirs_to_load. Assumes model names in each directory's metadata.txt matches
    those in project, as done by make_cdc_flusight_ensemble_models(). see above note re: the two templates.

    :return model_name_to_forecasts, which maps model_name -> list of its Forecasts
    """
    model_name_to_forecasts = defaultdict(list)
    for idx, model_dir in enumerate(model_dirs_to_load):
        if not model_dir.is_dir():
            click.echo("Warning: model_dir was not a directory: {}".format(model_dir))
            continue

        click.echo("** {}/{}: {}".format(idx, len(model_dirs_to_load), model_dir))
        metadata_dict = metadata_dict_for_file(model_dir / 'metadata.txt')
        model_name = metadata_dict['model_name']
        forecast_model = project.models.filter(name=model_name).first()
        if not forecast_model:
            raise RuntimeError("Couldn't find model named '{}' in project {}".format(model_name, project))


        def forecast_bin_map_fcn(forecast_bin):
            # handle the cases of 52,1 and 53,1 -> changing them to 52,53 and 53,54 respectively
            # (52.0, 1.0, 0.0881763527054108)
            bin_start_incl, bin_end_notincl, value = forecast_bin
            if ((bin_start_incl == 52) or (bin_start_incl == 53)) and (bin_end_notincl == 1):
                bin_end_notincl = bin_start_incl + 1
            return bin_start_incl, bin_end_notincl, value


        forecasts = forecast_model.load_forecasts_from_dir(
            model_dir,
            is_load_file=is_cdc_file_ew43_through_ew18,
            forecast_bin_map_fcn=forecast_bin_map_fcn)
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

def get_or_create_super_po_mo_users(create_super):
    """
    A utility that creates (as necessary) three users - 'project_owner1', 'model_owner1', and a superuser. Should
    probably only be used for testing.

    :param create_super: boolean that controls whether a superuser is created. used only for testing b/c password is
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
    if create_super and not superuser:
        logger.info("* creating supersuser")
        superuser = User.objects.create_superuser(username=super_username, password=superuser_password,
                                                  email='test@example.com')

    return (superuser, superuser_password, po_user, po_user_password, mo_user, mo_user_password) if create_super \
        else (po_user, po_user_password, mo_user, mo_user_password)


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