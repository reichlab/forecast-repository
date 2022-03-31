import itertools
import json
import logging
import shutil
from collections import defaultdict
from pathlib import Path

import click
import django
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.utilities import YYYY_MM_DD_DATE_FORMAT
from forecast_app.models.target import reference_date_type_for_id
from utils.project_queries import query_forecasts_for_project
from utils.visualization import viz_key_for_target, viz_target_variables, viz_units, viz_available_reference_dates, \
    viz_model_names, viz_targets, _viz_truth_for_target_unit_ref_date
from forecast_app.models import Project


logger = logging.getLogger(__name__)

# logging.getLogger().setLevel(logging.ERROR)  # remove project_queries DEBUG output
logging.getLogger().setLevel(logging.INFO)  # remove project_queries DEBUG output


@click.command()
@click.argument('project_name', type=click.STRING, required=True)
@click.argument('output_dir', type=click.Path(file_okay=False, exists=True))
def visualization_app(project_name, output_dir):
    """
    https://github.com/reichlab/Covid-19-Hub-Vizualization integration prototype app that generates JSON files that are
    compatible with that project's static file setup. Usage:
    1. Run this app to generate the JSON files in output_dir and then replace the static files with them, first copying
       Covid-19-Hub-Vizualization/assets/analytics.js and Covid-19-Hub-Vizualization/static/analytics.js to the
       corresponding new dirs.
    2. Edit Covid-19-Hub-Vizualization/nuxt.config.js: in the 'nuxt-forecast-viz' object, replace 'death' with
       'incident_deaths' (both the string and vars).
    3. Rebuild the static site and then run local dev mode to see results.

    :param project_name:
    :param output_dir:
    :return:
    """
    project = get_object_or_404(Project, name=project_name)
    logger.info(f"starting. project={project}, output_dir={output_dir}")

    # build the dir structure
    output_dir = Path(output_dir)
    app_output_dir = output_dir / 'zoltar_viz'  # top level dir

    assets_dir = app_output_dir / 'assets'
    static_dir = app_output_dir / 'static'
    data_dir = static_dir / 'data'
    forecasts_dir = data_dir / 'forecasts'
    truth_dir = data_dir / 'truth'
    logger.info(f"creating dirs. app_output_dir={app_output_dir}")

    if app_output_dir.exists():
        logger.info(f"deleting app_output_dir={app_output_dir}")
        shutil.rmtree(app_output_dir)  # dangerous!

    assets_dir.mkdir(parents=True)
    forecasts_dir.mkdir(parents=True)
    truth_dir.mkdir(parents=True)

    # create non-data files
    locations_file = assets_dir / 'locations.json'
    with open(locations_file, 'w') as fp:
        logger.info(f"writing: {locations_file}")
        viz_units_dicts = viz_units(project)
        json.dump(viz_units_dicts, fp, indent=4)

    target_vars_file = assets_dir / 'target_variables.json'
    with open(target_vars_file, 'w') as fp:
        logger.info(f"writing: {target_vars_file}")
        json.dump(viz_target_variables(project), fp, indent=4)

    available_as_ofs_file = data_dir / 'available_as_ofs.json'
    with open(available_as_ofs_file, 'w') as fp:
        logger.info(f"writing: {available_as_ofs_file}")
        json.dump(viz_available_reference_dates(project), fp, indent=4)

    models_file = data_dir / 'models.json'
    with open(models_file, 'w') as fp:
        logger.info(f"writing: {models_file}")
        viz_models_list = viz_model_names(project)  # todo xx list special one(s) first
        json.dump(sorted(viz_models_list), fp, indent=4)

    # targets
    targets = viz_targets(project)
    targets = sorted(targets, key=lambda _: _.name)
    target_key_to_targets = defaultdict(list)
    for target in targets:
        target_key_to_targets[viz_key_for_target(target)].append(target)

    # units
    units = [project.units.filter(abbreviation=unit_dict['value']).first() for unit_dict in viz_units_dicts]

    # compute reference_dates and target_end_date, stored as dicts for fast lookup of CSV rows
    ref_date_to_target_tzs = defaultdict(list)  # ref_date -> (target_name, timezero_date): datetime.date -> (str, str)
    target_tz_to_target_end_date = {}  # (target_name, timezero_date) -> target_end_date: (str, str) -> str
    for target, timezero in itertools.product(targets, project.timezeros.all().order_by('timezero_date')):
        rdt = reference_date_type_for_id(target.reference_date_type)
        reference_date, target_end_date = rdt.calc_fcn(target, timezero)
        target_timezero_tuple = (target.name, timezero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT))
        ref_date_to_target_tzs[reference_date].append(target_timezero_tuple)
        target_tz_to_target_end_date[target_timezero_tuple] = target_end_date.strftime(YYYY_MM_DD_DATE_FORMAT)

    # query forecast and truth data for all combinations of target_key, unit, and reference_date. to avoid step ahead
    # duplicates for truth (which is a bit complicated), we only query for the 1 step ahead target for a particular
    # reference_date
    logger.info(f"creating files")
    for target_key, unit, ref_date in itertools.product(sorted(target_key_to_targets),
                                                        sorted(units, key=lambda _: _.abbreviation),
                                                        sorted(ref_date_to_target_tzs)):
        logger.info(f"* {target_key!r}, {unit.abbreviation!r}, {ref_date!r}")

        # save the data!
        _query_and_save_truth_data_to_json(project, truth_dir, target_key_to_targets, target_tz_to_target_end_date,
                                           target_key, unit, ref_date)
        _query_and_save_forecast_data_to_json(project, forecasts_dir, target_key_to_targets,
                                              target_tz_to_target_end_date, ref_date_to_target_tzs, viz_models_list,
                                              target_key, unit, ref_date)

    logger.info(f"done!")


#
# _query_and_save_truth_data_to_json()
#

def _query_and_save_truth_data_to_json(project, truth_dir, target_key_to_targets, target_tz_to_target_end_date,
                                       target_key, unit, ref_date):
    one_step_ahead_targets = [target for target in target_key_to_targets[target_key] if target.numeric_horizon == 1]
    if len(one_step_ahead_targets) != 1:
        logger.error(f"could not find exactly one one-step-ahead target. "
                     f"one_step_ahead_targets={one_step_ahead_targets}")
        return

    one_step_ahead_target = one_step_ahead_targets[0]
    date_y_pairs = set()  # 2-tuples as returned by _viz_truth_for_target_unit_ref_date()
    dates, ys = _viz_truth_for_target_unit_ref_date(project, target_tz_to_target_end_date, one_step_ahead_target,
                                                    unit.abbreviation, ref_date)  # datetime.date, *
    if not dates:  # if dates = [] then ys = [] too
        logger.warning(f"  x {target_key!r}, {unit.abbreviation!r}, {ref_date!r}: {one_step_ahead_target.name!r}")
        return  # no truth data

    date_y_pairs.update(zip(dates, ys))
    logger.info(f"  v {target_key!r}, {unit.abbreviation!r}, {ref_date!r}: {one_step_ahead_target.name!r}: "
                f"{len(dates), len(ys)}")
    if not date_y_pairs:
        return

    # save truth data as JSON, sorting first
    json_dates, json_ys = zip(*sorted(date_y_pairs, key=lambda _: _[0]))
    viz_dict = {'date': json_dates, 'y': json_ys}
    filename = f"{target_key}_{unit.abbreviation}_{ref_date}.json"
    truth_file = truth_dir / filename
    with open(truth_file, 'w') as fp:
        logger.info(f"writing: {truth_file}")
        json.dump(viz_dict, fp, indent=4)


#
# _query_and_save_forecast_data_to_json()
#

def _query_and_save_forecast_data_to_json(project, forecasts_dir, target_key_to_targets, target_tz_to_target_end_date,
                                          ref_date_to_target_tzs, models, target_key, unit, ref_date):
    # timezeros. these are limited to those with reference_dates (for any passed target) matching passed ref_date
    if ref_date not in ref_date_to_target_tzs:
        logger.error(f"ref_date not found in ref_date_to_target_tzs: {ref_date}")
        return

    timezeros = sorted(list(set([timezero for target, timezero in ref_date_to_target_tzs[ref_date]])))

    # query forecasts
    query = {'models': models,
             'units': [unit.abbreviation],
             'targets': [target.name for target in target_key_to_targets[target_key]],
             'timezeros': timezeros,
             'types': ['quantile']}  # NB: no point, just quantile
    rows = list(query_forecasts_for_project(project, query))  # list for generator
    rows.pop(0)  # header

    if not rows:
        logger.warning(f"query returned no rows")
        return

    # build and save viz_dict via a nested groupby() to fill viz_dict. recall query csv output columns:
    #  model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
    rows.sort(key=lambda _: (_[0], _[1], _[4]))  # sort for groupby(): model, timezero, target
    viz_dict = defaultdict(lambda: defaultdict(list))  # dict for JSON output. filled next
    for model, tz_target_grouper in itertools.groupby(rows, key=lambda _: _[0]):
        for (timezero, target), quantile_grouper in itertools.groupby(tz_target_grouper, key=lambda _: (_[1], _[4])):
            target_end_date = target_tz_to_target_end_date[(target, timezero)]  # YYYY_MM_DD_DATE_FORMAT

            if target_end_date in viz_dict[model]['target_end_date']:  # todo xx correct? think!
                logger.error(f"target_end_date already in viz_dict: {target_end_date!r}")
                continue

            viz_dict[model]['target_end_date'].append(target_end_date)
            for _, _, _, _, _, _, value, _, _, _, quantile, _, _, _, _ in quantile_grouper:
                quantile_key = f"q{quantile}"  # e.g., 'q0.025'
                if quantile_key not in ["q0.025", "q0.25", "q0.5", "q0.75", "q0.975"]:
                    continue  # viz only wants five quantiles. todo xx generalize?

                viz_dict[model][quantile_key].append(value)

    # save forecast data as JSON, sorting first
    filename = f"{target_key}_{unit.abbreviation}_{ref_date}.json"
    forecast_file = forecasts_dir / filename
    with open(forecast_file, 'w') as fp:
        logger.info(f"writing: {forecast_file}")
        json.dump(viz_dict, fp, indent=4)


#
# main
#

if __name__ == '__main__':
    visualization_app()
