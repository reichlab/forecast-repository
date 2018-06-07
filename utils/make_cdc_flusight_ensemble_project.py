import csv
import timeit
from collections import defaultdict
from pathlib import Path

import click
import django
import pymmwr
import yaml
from django.template import Template, Context

# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
from utils.normalize_filenames_2016_2017_flu_contest import SEASON_START_EW_NUMBER


django.setup()

from utils.utilities import cdc_csv_components_from_data_dir, cdc_csv_filename_components
from forecast_app.models import Project, ForecastModel, TimeZero
from utils.make_2016_2017_flu_contest_project import get_or_create_super_po_mo_users, create_cdc_targets
from utils.cdc import CDC_CONFIG_DICT


@click.command()
@click.argument('component_models_dir', type=click.Path(file_okay=False, exists=True))
@click.option('--make_project', is_flag=True, default=False)
@click.option('--load_data', is_flag=True, default=False)
def make_cdc_flusight_ensemble_project_app(component_models_dir, make_project, load_data):
    """
    Manages creating a Project for the https://github.com/FluSightNetwork/cdc-flusight-ensemble project and loading its
    models, based on the two flags.

    If make_project: Creates the Project (deleting existing if exists!), user group, and two classes of users, along
    with creating (but not necessarily loading data for) the Models found in component_models_dir, using the
    metadata.txt files.

    If load_data: Loads data from the models in component_models_dir. Errors if make_project was not done previously.

    :param: component_models_dir: a directory cloned from
        https://github.com/FluSightNetwork/cdc-flusight-ensemble/tree/master/model-forecasts/component-models , which
        has then been normalized via normalize_filenames_cdc_flusight_ensemble.py .
    """
    project_name = 'CDC FluSight ensemble'
    project_description = "Guidelines and forecasts for a collaborative U.S. influenza forecasting project. " \
                          "http://flusightnetwork.io/"
    home_url = 'https://github.com/FluSightNetwork/cdc-flusight-ensemble'
    core_data = 'https://github.com/FluSightNetwork/cdc-flusight-ensemble/tree/master/model-forecasts/component-models'
    _make_cdc_flusight_project(component_models_dir, make_project, load_data, project_name, project_description,
                               home_url, None, core_data,
                               Path('utils/ensemble-truth-table-script/truths-2010-through-2017.csv'))


def _make_cdc_flusight_project(component_models_dir, make_project, load_data, project_name, project_description,
                               home_url, logo_url, core_data, truth_file_path):
    start_time = timeit.default_timer()
    component_models_dir = Path(component_models_dir)
    model_dirs_to_load = get_model_dirs_to_load(component_models_dir)
    click.echo("* _make_cdc_flusight_project(): component_models_dir={}, make_project={}, load_data={}, project_name={}"
               "\n\t({}) model_dirs_to_load={}"
               .format(component_models_dir, make_project, load_data, project_name, len(model_dirs_to_load),
                       [d.name for d in model_dirs_to_load]))

    # create the project if necessary
    project = Project.objects.filter(name=project_name).first()  # None if doesn't exist
    template_52 = Path('forecast_app/tests/2016-2017_submission_template.csv')  # todo xx move into repo
    template_53 = Path('forecast_app/tests/2016-2017_submission_template-plus-bin-53.csv')  # ""
    if make_project:
        if project:
            click.echo("* Deleting existing project: {}".format(project))
            project.delete()

        # create the Project (and Users if necessary)
        po_user, _, mo_user, _ = get_or_create_super_po_mo_users(create_super=False)
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
        click.echo("* Created project: {}".format(project))

        # load the template. NB: this project is different from others in that there are two templates that apply, based
        # on the season/year: some have 53 days, which means the template being validated against for that year must
        # have a bin for week 53. we handle this using two templates:
        #
        # - 2016-2017_submission_template.csv: last bin is 52,53
        # - 2016-2017_submission_template-plus-bin-53.csv: "" 53,54
        #
        # because projects can only have one template, we arbitrarily choose the former. HOWEVER, this means future
        # forecast validation will fail if it's for a year with 53 days. for reference, we use
        # pymmwr.mmwr_weeks_in_year() determine the number of weeks in a year
        click.echo("- loading template")
        project.load_template(template_52)

        targets = create_cdc_targets(project)
        click.echo("- created {} Targets: {}".format(len(targets), targets))

        click.echo("* Creating models")
        models = make_cdc_flusight_ensemble_models(project, model_dirs_to_load, po_user)
        click.echo("- created {} model(s): {}".format(len(models), models))
    elif not project:  # not make_project, but couldn't find existing
        raise RuntimeError("Could not find existing project named '{}'".format(project_name))

    # create TimeZeros. we use an arbitrary model's *.cdc.csv files to get them (all models should have same ones,
    # which is checked during forecast validation later). NB: we skip existing TimeZeros in case we are loading new
    # forecasts
    click.echo("* Creating TimeZeros")
    time_zeros = create_timezeros(project, first_subdirectory(component_models_dir))  # assumes no non-model subdirs
    click.echo("- created {} TimeZeros: {}".format(len(time_zeros), time_zeros))

    click.echo("- loading truth values: {}".format(truth_file_path))
    project.load_truth_data(truth_file_path)

    # load data if necessary
    if load_data:
        click.echo("* Loading forecasts")
        model_name_to_forecasts = load_cdc_flusight_ensemble_forecasts(project, model_dirs_to_load,
                                                                       template_52, template_53)
        click.echo("- Loading forecasts: loaded {} forecast(s)".format(sum(map(len, model_name_to_forecasts.values()))))

    # done
    click.echo("* Done. time: {}".format(timeit.default_timer() - start_time))
    return project if make_project else None


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


def first_subdirectory(directory):
    for subdir in directory.iterdir():
        if subdir.is_dir():
            return subdir

    return None


def is_cdc_file_ew43_through_ew18(cdc_csv_file):
    # only accept EW43 through EW18 per: "Following CDC guidelines from 2017/2018 season, using scores from
    # files from each season labeled EW43 through EW18 (i.e. files outside that range will not be considered)"
    time_zero, _, _ = cdc_csv_filename_components(cdc_csv_file.name)
    ywd_mmwr_dict = pymmwr.date_to_mmwr_week(time_zero)
    mmwr_week = ywd_mmwr_dict['week']
    return (mmwr_week <= 18) or (mmwr_week >= 43)


def create_timezeros(project, model_dir):
    """
    Create TimeZeros for project based on model_dir. Returns a list of them.
    """
    time_zeros = []
    season_start_years = []  # helps track season transitions
    for cdc_csv_file, timezero_date, _, data_version_date in cdc_csv_components_from_data_dir(model_dir):
        if not is_cdc_file_ew43_through_ew18(cdc_csv_file):
            click.echo("s (not in range)\t{}\t".format(cdc_csv_file.name))  # 's' from load_forecasts_from_dir()
            continue

        # NB: we skip existing TimeZeros in case we are loading new forecasts
        found_time_zero = project.time_zero_for_timezero_date(timezero_date)
        if found_time_zero:
            click.echo("s (TimeZero exists)\t{}\t".format(cdc_csv_file.name))  # 's' from load_forecasts_from_dir()
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


def metadata_dict_for_file(metadata_file):
    with open(metadata_file) as metadata_fp:
        metadata_dict = yaml.safe_load(metadata_fp)
    return metadata_dict


def load_cdc_flusight_ensemble_forecasts(project, model_dirs_to_load, template_52, template_53):
    """
    Loads forecast data for models in model_dirs_to_load. Assumes model names in each directory's metadata.txt matches
    those in project, as done by make_cdc_flusight_ensemble_models(). see above note re: the two templates.
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


        def time_zero_to_template(time_zero):
            season_start_year = season_start_year_for_date(time_zero.timezero_date)
            return {52: template_52, 53: template_53}[pymmwr.mmwr_weeks_in_year(season_start_year)]


        def forecast_bin_map(forecast_bin):
            # handle the cases of 52,1 and 53,1 -> changing them to 52,53 and 53,54 respectively
            # (52.0, 1.0, 0.0881763527054108)
            bin_start_incl, bin_end_notincl, value = forecast_bin
            if ((bin_start_incl == 52) or (bin_start_incl == 53)) and (bin_end_notincl == 1):
                bin_end_notincl = bin_start_incl + 1
            return bin_start_incl, bin_end_notincl, value


        forecasts = forecast_model.load_forecasts_from_dir(
            model_dir,
            time_zero_to_template=time_zero_to_template,
            is_load_file=is_cdc_file_ew43_through_ew18,
            forecast_bin_map=forecast_bin_map)
        model_name_to_forecasts[model_name].extend(forecasts)

    return model_name_to_forecasts


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


if __name__ == '__main__':
    make_cdc_flusight_ensemble_project_app()
