import datetime
import json
import logging
from collections import defaultdict
from itertools import groupby
from pathlib import Path
from string import digits, ascii_letters, punctuation

from django.db import connection
from django.db import transaction

from forecast_app.models import Project, Unit, Target, Forecast, ForecastModel, ForecastMetaUnit, ForecastMetaTarget
from forecast_app.models.project import TimeZero
from forecast_app.models.target import reference_date_type_for_name, reference_date_type_for_id
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


logger = logging.getLogger(__name__)


#
# delete_project_iteratively()
#

@transaction.atomic
def delete_project_iteratively(project):
    """
    An alternative to Project.delete(), deletes the passed Project, but unlike that function, does so by iterating over
    objects that refer to the project before deleting the project itproject. This apparently reduces the memory usage
    enough to allow the below Heroku deletion. See [Deleting projects on Heroku production fails](https://github.com/reichlab/forecast-repository/issues/91).
    """
    logger.info(f"* delete_project_iteratively(): deleting models and forecasts")
    for forecast_model in project.models.iterator():
        logger.info(f"- {forecast_model.pk}")
        # order by to avoid RuntimeError: you cannot delete a forecast that has any newer versions
        for forecast in forecast_model.forecasts.order_by('-issued_at').iterator():
            logger.info(f"  = {forecast.pk}")
            forecast.delete()
        forecast_model.delete()

    logger.info(f"delete_project_iteratively(): deleting units")
    for unit in project.units.iterator():
        logger.info(f"- {unit.pk}")
        unit.delete()

    logger.info(f"delete_project_iteratively(): deleting targets")
    for target in project.targets.iterator():
        logger.info(f"- {target.pk}")
        target.delete()

    logger.info(f"delete_project_iteratively(): deleting timezeros")
    for timezero in project.timezeros.iterator():
        logger.info(f"- {timezero.pk}")
        timezero.delete()

    logger.info(f"delete_project_iteratively(): deleting remainder")
    project.delete()
    logger.info(f"delete_project_iteratively(): done")


#
# config_dict_from_project()
#

def config_dict_from_project(project, request):
    """
    The twin of `create_project_from_json()`, returns a configuration dict for project as passed to that function.

    :param project: a Project
    :param request: required for TargetSerializer's 'id' field
    """
    from forecast_app.serializers import UnitSerializer, TimeZeroSerializer  # avoid circular imports


    unit_serializer_multi = UnitSerializer(project.units, many=True, context={'request': request})
    tz_serializer_multi = TimeZeroSerializer(project.timezeros, many=True, context={'request': request})
    return {'name': project.name, 'is_public': project.is_public, 'description': project.description,
            'home_url': project.home_url, 'logo_url': project.logo_url, 'core_data': project.core_data,
            'time_interval_type': project.time_interval_type_as_str(),
            'visualization_y_label': project.visualization_y_label,
            'units': [dict(_) for _ in unit_serializer_multi.data],  # replace OrderedDicts
            'targets': [_target_dict_for_target(target, request) for target in project.targets.all()],
            'timezeros': [dict(_) for _ in tz_serializer_multi.data]}  # replace OrderedDicts


def _target_dict_for_target(target, request):
    # request is required for TargetSerializer's 'id' field
    from forecast_app.serializers import TargetSerializer  # avoid circular imports


    if target.type is None:
        raise RuntimeError(f"target has no type: {target}")

    serializer = TargetSerializer(target, context={'request': request})
    return serializer.data


#
# create_project_from_json()
#

@transaction.atomic
def create_project_from_json(proj_config_file_path_or_dict, owner, is_validate_only=False):
    """
    Top-level function that creates a Project based on the json configuration file at json_file_path. Errors if one with
    that name already exists. Does not set Project.model_owners, create TimeZeros, load truth data, create Models, or
    load forecasts.

    :param proj_config_file_path_or_dict: either a Path to project config json file OR a dict as loaded from a file.
        See https://docs.zoltardata.com/fileformats/#project-creation-configuration-json for details and
        docs-project.json for an example.
    :param owner: the new Project's owner (a User). used only if not is_validate_only
    :param is_validate_only: controls whether objects are actually created (is_validate_only=False), or whether only
        validation is done but no creation (is_validate_only=True)
    :return: the new Project
    """
    logger.info(f"* create_project_from_json(): started. proj_config_file_path_or_dict="
                f"{proj_config_file_path_or_dict}, owner={owner}, is_validate_only={is_validate_only}")
    if isinstance(proj_config_file_path_or_dict, dict):
        project_dict = proj_config_file_path_or_dict
    elif isinstance(proj_config_file_path_or_dict, Path):
        with open(proj_config_file_path_or_dict) as fp:
            try:
                project_dict = json.load(fp)
            except Exception as ex:
                raise RuntimeError(f"error loading json file. file={proj_config_file_path_or_dict}, ex={ex}")
    else:
        raise RuntimeError(f"proj_config_file_path_or_dict was neither a dict nor a Path. "
                           f"type={type(proj_config_file_path_or_dict).__name__}")  # is blank w/o __name__. unsure why

    # validate required fields
    all_keys = set(project_dict.keys())
    tested_keys = all_keys - {'logo_url'}  # optional keys
    field_name_to_type = {'name': str, 'is_public': bool, 'description': str, 'home_url': str, 'core_data': str,
                          'time_interval_type': str, 'visualization_y_label': str, 'units': list, 'targets': list,
                          'timezeros': list}
    expected_keys = set(field_name_to_type.keys())
    if tested_keys != expected_keys:
        raise RuntimeError(f"Wrong keys in project_dict. difference={expected_keys ^ all_keys}. "
                           f"expected={expected_keys}, actual={all_keys}")

    # validate optional field
    if ('logo_url' in project_dict) and (project_dict['logo_url'] is not None) and \
            (not isinstance(project_dict['logo_url'], str)):
        raise RuntimeError(f"top level field type was not {str}. field_name='logo_url', "
                           f"value={project_dict['logo_url']!r}, type={type(project_dict['logo_url'])}")

    # validate field types
    for field_name, field_type in field_name_to_type.items():
        if not isinstance(project_dict[field_name], field_type):
            raise RuntimeError(f"top level field type was not {field_type}. field_name={field_name!r}, "
                               f"value={project_dict[field_name]!r}, type={type(project_dict[field_name])}")

    if is_validate_only:
        project = None
        logger.info(f"- no created Project")
    else:
        # error if project already exists
        name = project_dict['name']
        project = Project.objects.filter(name=name).first()  # None if doesn't exist
        if project:
            raise RuntimeError(f"found existing project. name={name}, project={project}")

        project = _create_project(project_dict, owner)
        logger.info(f"- created Project: {project}")

    units = _validate_and_create_units(project, project_dict, is_validate_only)
    logger.info(f"- created {len(units)} Units: {units}")

    targets = _validate_and_create_targets(project, project_dict, is_validate_only)
    logger.info(f"- created {len(targets)} Targets: {targets}")

    timezeros = _validate_and_create_timezeros(project, project_dict, is_validate_only)
    logger.info(f"- created {len(timezeros)} TimeZeros: {timezeros}")

    logger.info(f"* create_project_from_json(): done!")
    return project


def _is_valid_unit_target_name_or_cat(name):
    # https://stackoverflow.com/questions/57062794/how-to-check-if-a-string-has-any-special-characters/57062899
    valid_chars = digits + ascii_letters + punctuation + ' '
    return not set(name).difference(valid_chars)


def _validate_and_create_units(project, project_dict, is_validate_only=False):
    units = []  # returned instances
    for unit_dict in project_dict['units']:
        if 'name' not in unit_dict:
            raise RuntimeError(f"unit_dict had no 'name' field. units={project_dict['units']}")

        unit_name = unit_dict['name']
        if (not isinstance(unit_name, str)) or (not _is_valid_unit_target_name_or_cat(unit_name)):
            raise RuntimeError(f"invalid unit name: {unit_name!r}")

        if 'abbreviation' not in unit_dict:
            raise RuntimeError(f"unit_dict had no 'abbreviation' field. units={project_dict['units']}")

        unit_abbrev = unit_dict['abbreviation']
        if (not isinstance(unit_abbrev, str)) or (not _is_valid_unit_target_name_or_cat(unit_abbrev)):
            raise RuntimeError(f"invalid unit abbreviation: {unit_abbrev!r}")

        # valid
        if not is_validate_only:
            # create the Unit, first checking for an existing one
            existing_unit = project.units.filter(name=unit_name).first()
            if existing_unit:
                raise RuntimeError(f"found existing Unit for name={unit_name}")

            units.append(Unit.objects.create(project=project, name=unit_name, abbreviation=unit_abbrev))
    return units


def _validate_and_create_timezeros(project, project_dict, is_validate_only=False):
    from forecast_app.api_views import validate_and_create_timezero  # avoid circular imports


    timezeros = [validate_and_create_timezero(project, timezero_config, is_validate_only)
                 for timezero_config in project_dict['timezeros']]
    return timezeros if not is_validate_only else []


def _validate_and_create_targets(project, project_dict, is_validate_only=False):
    targets = []
    for target_dict in project_dict['targets']:
        # raises RuntimeError if invalid:
        target_type_int, reference_date_type_int = _validate_target_dict(target_dict)
        if is_validate_only:
            continue

        # valid! create the Target and then supporting 'list' instances: TargetCat, TargetLwr, and TargetRange. atomic
        # so that Targets succeed only if others do too
        with transaction.atomic():
            model_init = {'project': project,
                          'type': target_type_int,
                          'name': target_dict['name'],
                          'description': target_dict['description'],
                          'outcome_variable': target_dict['outcome_variable'],
                          'is_step_ahead': target_dict['is_step_ahead'],
                          }  # required keys

            # add is_step_ahead
            if target_dict['is_step_ahead']:
                model_init['numeric_horizon'] = target_dict['numeric_horizon']
                model_init['reference_date_type'] = reference_date_type_int

            # instantiate the new Target, first checking for an existing one
            existing_target = project.targets.filter(name=target_dict['name']).first()
            if existing_target:
                raise RuntimeError(f"found existing Target for name={target_dict['name']}")

            target = Target.objects.create(**model_init)
            targets.append(target)

            # create two TargetRanges
            if ('range' in target_dict) and target_dict['range']:
                target.set_range(target_dict['range'][0], target_dict['range'][1])

            # create TargetCats and TargetLwrs
            if ('cats' in target_dict) and target_dict['cats']:
                # extra_lwr implements this relationship: "if `range` had been specified as [0, 100] in addition to the
                # above `cats`, then the final bin would be [2.2, 100]."
                extra_lwr = max(target_dict['range']) if ('range' in target_dict) and target_dict['range'] else None
                target.set_cats(target_dict['cats'], extra_lwr)
            if target.type == Target.BINARY_TARGET_TYPE:
                # add the two implicit boolean cats
                target.set_cats([False, True])
    return targets


def _validate_target_dict(target_dict):
    """
    Validates target_dict
    :param target_dict:
    :return: 2-tuple: (target_type_int, reference_date_type_int_or_None). latter is None if not is_step_ahead
    :raises RuntimeError: if target_dict is invalid
    """
    type_name_to_int = {type_name: type_int for type_int, type_name in Target.TYPE_CHOICES}

    # check for keys required by all target types. optional keys are tested below
    all_keys = set(target_dict.keys())
    tested_keys = all_keys - {'id', 'url', 'numeric_horizon', 'reference_date_type', 'range', 'cats'}  # optional keys
    field_name_to_type = {'name': str, 'description': str, 'type': str, 'outcome_variable': str, 'is_step_ahead': bool}
    expected_keys = set(field_name_to_type.keys())

    if tested_keys != expected_keys:
        raise RuntimeError(f"Wrong required keys in target_dict. difference={expected_keys ^ tested_keys}. "
                           f"expected_keys={expected_keys}, tested_keys={tested_keys}. target_dict={target_dict}")

    # validate required keys' field types
    for field_name, field_type in field_name_to_type.items():
        if not isinstance(target_dict[field_name], field_type):
            raise RuntimeError(f"field type was not {field_type}. field_name={field_name!r}, "
                               f"value={target_dict[field_name]!r}, type={type(target_dict[field_name])}")

    # validate name
    target_name = target_dict['name']
    if not _is_valid_unit_target_name_or_cat(target_name):
        raise RuntimeError(f"illegal target name: {target_name!r}")

    # validate type
    type_name = target_dict['type']
    valid_target_types = type_name_to_int.keys()
    if type_name not in valid_target_types:
        raise RuntimeError(f"Invalid type_name={type_name}. valid_target_types={valid_target_types} . "
                           f"target_dict={target_dict}")

    # validate is_step_ahead. field default if not passed is None
    if target_dict['is_step_ahead'] is None:
        raise RuntimeError(f"is_step_ahead not found but is required")

    # check for numeric_horizon and reference_date_type required if is_step_ahead
    if target_dict['is_step_ahead'] and (
            (('numeric_horizon' not in target_dict) or (target_dict['numeric_horizon'] is None)) or
            (('reference_date_type' not in target_dict)) or (target_dict['reference_date_type'] is None)):
        raise RuntimeError(f"`numeric_horizon` or `reference_date_type` not found but is required when "
                           f"`is_step_ahead` is passed. target_dict={target_dict}")

    # validate reference_date_type if is_step_ahead
    reference_date_type = target_dict['reference_date_type'] if target_dict['is_step_ahead'] else None
    ref_date_type_int = reference_date_type_for_name(reference_date_type).id if reference_date_type else None
    if target_dict['is_step_ahead'] and (ref_date_type_int is None):
        valid_ref_date_types = [rdt_name for rdt_id, rdt_name in Target.REF_DATE_TYPE_CHOICES]
        raise RuntimeError(f"invalid reference_date_type={reference_date_type}. "
                           f"valid_ref_date_types={valid_ref_date_types} . target_dict={target_dict}")

    # check required, optional, and invalid keys by target type. 2 cases: 'range', 'cats'
    type_int = type_name_to_int[type_name]

    # 1) test optional 'range'. three cases a-c follow

    # 1a) required but not passed: []: no need to validate

    # 1b) optional: ok to pass or not pass: ['continuous', 'discrete']: no need to validate

    # 1c) invalid but passed: ['nominal', 'binary', 'date']
    if ('range' in all_keys) and (type_int in [Target.NOMINAL_TARGET_TYPE, Target.BINARY_TARGET_TYPE,
                                               Target.DATE_TARGET_TYPE]):
        raise RuntimeError(f"'range' passed but is invalid for type_name={type_name}")

    # 2) test optional 'cats'. three cases a-c follow

    # 2a) required but not passed: ['nominal', 'date']
    if ('cats' not in all_keys) and (type_int in [Target.NOMINAL_TARGET_TYPE, Target.DATE_TARGET_TYPE]):
        raise RuntimeError(f"'cats' not passed but is required for type_name='{type_name}'")

    # 2b) optional: ok to pass or not pass: ['continuous', 'discrete']: no need to validate

    # 2c) invalid but passed: ['binary']
    if ('cats' in all_keys) and (type_int == Target.BINARY_TARGET_TYPE):
        raise RuntimeError(f"'cats' passed but is invalid for type_name={type_name}")

    # validate 'range' if passed. values can be either ints or floats, and must match the target's data type
    data_types = Target.data_types_for_target_type(type_int)  # python types. recall the first is the preferred one
    if 'range' in target_dict:
        for range_str in target_dict['range']:
            try:
                data_types[0](range_str)  # try parsing as an int or float
            except ValueError as ve:
                raise RuntimeError(f"range type did not match data_types. range_str={range_str!r}, "
                                   f"data_types={data_types}, error: {ve}")

        if len(target_dict['range']) != 2:
            raise RuntimeError(f"range did not contain exactly two items: {target_dict['range']}")

    # validate 'cats' if passed. values can strings, ints, or floats, and must match the target's data type. strings
    # can be either dates in YYYY_MM_DD_DATE_FORMAT form or just plain strings.
    if 'cats' in target_dict:
        for cat_str in target_dict['cats']:
            try:
                if type_int == Target.DATE_TARGET_TYPE:
                    datetime.datetime.strptime(cat_str, YYYY_MM_DD_DATE_FORMAT).date()  # try parsing as a date
                else:
                    parsed_cat = data_types[0](cat_str)  # try parsing as a string, int, or float
                    if isinstance(parsed_cat, str) and not _is_valid_unit_target_name_or_cat(parsed_cat):
                        raise RuntimeError(f"illegal cat value: {parsed_cat!r}")
            except ValueError as ve:
                raise RuntimeError(f"could not convert cat to data_type. cat_str={cat_str!r}, "
                                   f"data_type={data_types[0]}, error: {ve}")

    # test range-cat relationships
    if ('cats' in target_dict) and ('range' in target_dict):
        cats = [data_types[0](cat_str) for cat_str in target_dict['cats']]
        the_range = [data_types[0](range_str) for range_str in target_dict['range']]
        if cats and (min(cats) != min(the_range)):
            raise RuntimeError(f"the minimum cat ({min(cats)}) did not equal the range's lower bound "
                               f"({min(the_range)})")

        if cats and (max(cats) >= max(the_range)):
            raise RuntimeError(f"the maximum cat ({max(cats)}) was not less than the range's upper bound "
                               f"({max(the_range)})")

    return type_int, ref_date_type_int


def _create_project(project_dict, owner):
    # validate time_interval_type - one of: 'week', 'biweek', or 'month'
    time_interval_type_input = project_dict['time_interval_type'].lower()
    time_interval_type = None
    for db_value, human_readable_value in Project.TIME_INTERVAL_TYPE_CHOICES:
        if human_readable_value.lower() == time_interval_type_input:
            time_interval_type = db_value

    if time_interval_type is None:
        time_interval_type_choices = [choice[1] for choice in Project.TIME_INTERVAL_TYPE_CHOICES]
        raise RuntimeError(f"invalid 'time_interval_type': {time_interval_type_input}. must be one of: "
                           f"{time_interval_type_choices}")

    project = Project.objects.create(
        owner=owner,
        is_public=project_dict['is_public'],
        name=project_dict['name'],
        time_interval_type=time_interval_type,
        visualization_y_label=(project_dict['visualization_y_label']),
        description=project_dict['description'],
        home_url=project_dict['home_url'],  # required
        logo_url=project_dict['logo_url'] if 'logo_url' in project_dict else None,
        core_data=project_dict['core_data'] if 'core_data' in project_dict else None,
    )
    project.save()
    return project


#
# group_targets()
#

def group_targets(targets):
    """
    A utility for the `forecast_app.views.ProjectDetailView` class that groups related targets in `targets`. Only groups
    is_step_ahead targets, treating others as their own group. There are two pieces:

    1. how to decide grouped Targets:
      is_step_ahead=False: each Target is its own group
      is_step_ahead=True: group Targets using the 2-tuple: (Target.reference_date_type, Target.outcome_variable)

    2. how to calculate each group_name (`_group_name_for_target()`):
      is_step_ahead=False: Target.outcome_variable
      is_step_ahead=True: Target.reference_date_type.abbreviation + " ahead " + Target.outcome_variable

    :param targets: list of Targets from the same Project
    :return: a dict that maps group_name -> group_targets
    """
    group_name_to_targets = defaultdict(list)  # maps: group_name -> target_list
    for target in targets:
        group_name = _group_name_for_target(target) if target.is_step_ahead else target.name
        group_name_to_targets[group_name].append(target)
    return group_name_to_targets


def _group_name_for_target(target):
    """
    :param target: a Target where is_step_ahead = True
    :return: group name as documented in `group_targets()`
    """
    if (not target.is_step_ahead) or (target.reference_date_type is None):
        return target.outcome_variable
    else:
        return reference_date_type_for_id(target.reference_date_type).abbreviation + " ahead " + target.outcome_variable


def targets_for_group_name(project, group_name):
    """
    The inverse of `group_targets()`.

    :param project: the Project whose targets are to be checked against `group_name`
    :param group_name: as returned by `group_targets()` for Targets in Project
    :return: list of Targets in `project` matching `group_name`
    """
    return [target for target in project.targets.all() if _group_name_for_target(target) == group_name]


#
# models_summary_table_rows_for_project()
#

def models_summary_table_rows_for_project(project):
    """
    :return: a list of rows suitable for rendering as a table. returns a 6-tuple for each model in `project`:
        [forecast_model, num_forecasts, oldest_forecast_tz_date, newest_forecast_tz_date,
         newest_forecast_id, newest_forecast_created_at]

        NB: the dates and datetime are either objects OR strings depending on the database (postgres: objects,
        sqlite3: strings)
    """
    # this query has three steps: 1) a CTE that groups forecast by model, calculating for each: number of forecasts, and
    # min and max timezero_dates. 2) a CTE that joins that with forecasts and then groups to get forecasts corresponding
    # to max timezero_dates, resulting in separate rows per forecast version (i.e., per issued_at), from which we group
    # to get max issued_at. 3) a join on that with forecasts to get the actual forecast IDs corresponding to the max
    # issued_ats. note that this query does not return forecast IDs (with max issued_ats) for min timezero_dates,
    # which means we cannot link to them, only to the newest forecasts.
    #
    # final columns (one row/forecast model):
    # - fm_id: ForecastModel.id
    # - f_count: total number of forecasts in the model
    # - min_time_zero_date, max_time_zero_date: min and max TimeZero.timezero_date in the model
    # - f_id, f_created_at: Forecast.id and created_at for the forecast matching max_time_zero_date and the max
    #                       issued_at for that
    sql = f"""
        WITH
            fm_min_max_tzs AS (
                SELECT f.forecast_model_id   AS fm_id,
                       COUNT(*)              AS f_count,
                       MIN(tz.timezero_date) AS min_time_zero_date,
                       MAX(tz.timezero_date) AS max_time_zero_date
                FROM {Forecast._meta.db_table} AS f
                         JOIN {TimeZero._meta.db_table} AS tz ON f.time_zero_id = tz.id
                         JOIN {ForecastModel._meta.db_table} AS fm ON f.forecast_model_id = fm.id
                WHERE fm.project_id = %s AND NOT fm.is_oracle
                GROUP BY f.forecast_model_id),
            fm_max_issued_ats AS (
                SELECT fm_min_max_tzs.fm_id              AS fm_id,
                       fm_min_max_tzs.f_count            AS f_count,
                       fm_min_max_tzs.min_time_zero_date AS min_time_zero_date,
                       fm_min_max_tzs.max_time_zero_date AS max_time_zero_date,
                       MAX(f.issued_at)                  AS max_issued_at
                FROM fm_min_max_tzs
                         JOIN {TimeZero._meta.db_table} AS tz ON tz.timezero_date = fm_min_max_tzs.max_time_zero_date
                         JOIN {Forecast._meta.db_table} AS f
                              ON f.forecast_model_id = fm_min_max_tzs.fm_id
                                  AND f.time_zero_id = tz.id
                GROUP BY fm_min_max_tzs.fm_id,
                         fm_min_max_tzs.f_count,
                         fm_min_max_tzs.min_time_zero_date,
                         fm_min_max_tzs.max_time_zero_date)
        SELECT fm_max_issued_ats.fm_id               AS fm_id,
               fm_max_issued_ats.f_count             AS f_count,
               fm_max_issued_ats.min_time_zero_date  AS min_time_zero_date,
               fm_max_issued_ats.max_time_zero_date  AS max_time_zero_date,
               f.id                                  AS f_id,
               f.created_at                          AS f_created_at
        FROM fm_max_issued_ats
                 JOIN {TimeZero._meta.db_table} AS tz ON tz.timezero_date = fm_max_issued_ats.max_time_zero_date
                 JOIN {Forecast._meta.db_table} AS f
                      ON f.forecast_model_id = fm_max_issued_ats.fm_id
                          AND f.time_zero_id = tz.id
                          AND f.issued_at = fm_max_issued_ats.max_issued_at;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        rows = cursor.fetchall()

        # add model IDs with no forecasts (omitted by query)
        missing_model_ids = project.models.filter(is_oracle=False) \
            .exclude(id__in=[row[0] for row in rows]) \
            .values_list('id', flat=True)
        for missing_model_id in missing_model_ids:
            rows.append((missing_model_id, 0, None, None, None, None, None))  # caller/view handles Nones

        forecast_model_id_to_obj = {forecast_model.pk: forecast_model for forecast_model in project.models.all()}

        # replace forecast_model_ids (row[0]) with objects
        rows = [(forecast_model_id_to_obj[row[0]], row[1], row[2], row[3], row[4], row[5]) for row in rows]
        return rows


#
# unit_rows_for_project()
#

SUMMARY_STRING_MAX_NAMES = 7


def summary_string_for_names(names, num_names, summary_postfix):
    """
    Helper that shortens a list of strings based on length.

    :param names: a list of strings
    :param num_names: number of strings to compare to
    :return:
    """
    if len(names) == num_names:
        return '(all)'
    elif len(names) > SUMMARY_STRING_MAX_NAMES:
        return f'({len(names)} {summary_postfix})'
    else:
        return ', '.join(sorted(names))


def unit_rows_for_project(project):
    """
    A utility for the `forecast_app.views.project_explorer()` function. Returns a a list of lists that's used to
    generate that function's table rows, which look something like:

    +-----------------------+------------------------------+-------------+------------+------------+-------------------------|
    | Abbreviation          | Team                         | # Forecasts | Oldest     | Newest     | Upload time             |
    +-----------------------+------------------------------+-------------+------------+------------+-------------------------|
    | YYG-ParamSearch       | Youyang Gu (YYG)             | 156         | 2020-04-13 | 2020-09-15 | 2020-09-16 08:28:26 EDT |
    | CMU-TimeSeries        | Carnegie Mellon Delphi Group |   9         | 2020-07-20 | 2020-09-14 | 2020-09-14 20:53:37 EDT |
    | CovidAnalytics-DELPHI | CovidAnalytics at MIT        |  23         | 2020-04-22 | 2020-09-14 | 2020-09-14 21:27:30 EDT |
    |  ...                                                                                                                   |
    +-----------------------+------------------------------+-------------+------------+------------+-------------------------|

    :param project: a Project
    :return: a list of 6-tuples for each model in `project`:
        (model, newest_forecast_tz_date, newest_forecast_id, num_present_unit_names, present_unit_names,
         missing_unit_names). the last two are summarized via summary_string_for_names()
    """
    # model, newest_forecast_tz_date, newest_forecast_id, present_unit_names, missing_unit_names:
    # add count column, and replace sets with strings, truncating if too long
    # add num_present_unit_names and change: (present_unit_names, missing_unit_names) to: summaries. -> becomes:
    # (model, newest_forecast_tz_date, newest_forecast_id, num_present_unit_names, present_unit_names,
    #  missing_unit_names):
    num_units = project.units.count()
    unit_rows = [(model, newest_forecast_tz_date, newest_forecast_id,
                  len(present_unit_names), summary_string_for_names(present_unit_names, num_units, 'units'),
                  summary_string_for_names(missing_unit_names, num_units, 'units'))
                 for model, newest_forecast_tz_date, newest_forecast_id, present_unit_names, missing_unit_names
                 in _project_explorer_unit_rows(project)]
    return unit_rows


def _project_explorer_unit_rows(project):
    """
    :param project: a Project
    :return: list of 5-tuples of the form:
        (model, newest_forecast_tz_date, newest_forecast_id, present_unit_names, missing_unit_names)
    """
    # get newest forecast into 3-tuples from models_summary_table_rows_for_project()
    models_rows = [(forecast_model, newest_forecast_tz_date, newest_forecast_id)
                   for forecast_model, _, _, newest_forecast_tz_date, newest_forecast_id, _
                   in models_summary_table_rows_for_project(project)]

    # get corresponding unique Unit IDs for newest_forecast_ids
    forecast_ids = [newest_forecast_id for _, _, newest_forecast_id in models_rows if newest_forecast_id is not None]
    forecast_id_to_unit_id_set = _forecast_ids_to_present_unit_or_target_id_sets(forecast_ids, True)

    # combine into 5-tuple: (model, newest_forecast_tz_date, newest_forecast_id, present_unit_names, missing_unit_names)
    unit_id_to_obj = {unit.id: unit for unit in project.units.all()}
    all_unit_ids = set(unit_id_to_obj.keys())
    rows = []  # return value
    for model, newest_forecast_tz_date, newest_forecast_id in models_rows:
        present_unit_ids = forecast_id_to_unit_id_set.get(newest_forecast_id, set())
        missing_unit_ids = all_unit_ids - present_unit_ids
        rows.append((model, newest_forecast_tz_date, newest_forecast_id,
                     {unit_id_to_obj[_].name for _ in present_unit_ids},
                     {unit_id_to_obj[_].name for _ in missing_unit_ids}))

    return rows


def _forecast_ids_to_present_unit_or_target_id_sets(forecast_ids, is_unit):
    """
    :param forecast_ids: a list of Forecast IDs
    :param is_unit: True if should return Unit information. returns Target information o/w
    :return: a dict mapping each forecast_id to a set of either its Unit or Targets IDs, based on is_unit:
        {forecast_id -> set(unit_or_target_ids)}
    """
    if not forecast_ids:
        return {}

    forecast_id_to_unit_id_set = {}
    if is_unit:
        forecast_meta_unit_or_target_qs = ForecastMetaUnit.objects \
            .filter(forecast__id__in=forecast_ids) \
            .order_by('forecast__id', 'unit__id') \
            .values_list('forecast__id', 'unit__id')  # ordered so we can groupby()
    else:
        forecast_meta_unit_or_target_qs = ForecastMetaTarget.objects \
            .filter(forecast__id__in=forecast_ids) \
            .order_by('forecast__id', 'target__id') \
            .values_list('forecast__id', 'target__id')  # ordered so we can groupby()
    for forecast_id, unit_or_target_id_grouper in groupby(forecast_meta_unit_or_target_qs, key=lambda _: _[0]):
        forecast_id_to_unit_id_set[forecast_id] = {unit_id for forecast_id, unit_id in unit_or_target_id_grouper}

    return forecast_id_to_unit_id_set


#
# target_rows_for_project()
#

def target_rows_for_project(project):
    """
    A utility for the `forecast_app.views.project_explorer()` function. Returns a list of lists that's used to generate
    that function's table rows, which look something like:

    +-----------------------+---------------+--------------------+-----------+
    | model                 | forecast date | target group       | # targets |
    +-----------------------+---------------+--------------------+-----------+
    | YYG-ParamSearch       | 2020-09-15    | wk ahead cum death | 6         |  # target group 1/2 for this model
    | YYG-ParamSearch       | 2020-09-15    | wk ahead inc death | 6         |  # "" 2/2 ""
    | CMU-TimeSeries        | 2020-09-14    | wk ahead inc case  | 4         |
    | CMU-TimeSeries        | 2020-09-14    | wk ahead inc death | 4         |
    | CovidAnalytics-DELPHI | 2020-09-14    | wk ahead cum death | 7         |
    | CovidAnalytics-DELPHI | 2020-09-14    | wk ahead inc case  | 7         |
    |  ...                                                                   |
    +-----------------------+---------------+--------------------+-----------+

    :param project: a Project
    :return: a list of lists: G lists for each model in `project` where G is the number of target groups that that
        model's latest forecast has predictions for. each list is a 5-tuple of the form:
            [model, newest_forecast_tz_date, newest_forecast_id, target_group_name, target_group_count]
        where target_group is as returned by `group_targets()`.
    """
    # get newest forecast into 3-tuples from models_summary_table_rows_for_project()
    models_rows = [(forecast_model, newest_forecast_tz_date, newest_forecast_id)
                   for forecast_model, _, _, newest_forecast_tz_date, newest_forecast_id, _
                   in models_summary_table_rows_for_project(project)]

    # get corresponding unique Target IDs for newest_forecast_ids
    forecast_ids = [newest_forecast_id for _, _, newest_forecast_id in models_rows if newest_forecast_id is not None]
    forecast_id_to_target_id_set = _forecast_ids_to_present_unit_or_target_id_sets(forecast_ids, False)

    # build target_rows
    target_rows = []  # return value
    target_id_to_object = {target.id: target for target in project.targets.all()}
    for forecast_model, newest_forecast_tz_date, newest_forecast_id in models_rows:
        newest_forecast_target_ids = forecast_id_to_target_id_set.get(newest_forecast_id, [])
        newest_forecast_targets = [target_id_to_object[target_id] for target_id in newest_forecast_target_ids]
        if newest_forecast_targets:
            # for target_group_name, targets in group_targets(newest_forecast_targets).items():
            target_groups = group_targets(newest_forecast_targets)
            for target_group_name in sorted(target_groups):
                target_rows.append((forecast_model, newest_forecast_tz_date, newest_forecast_id,
                                    target_group_name, len(target_groups[target_group_name])))
        else:  # model has no forecasts so add a place-holder for it
            target_rows.append((forecast_model, '', '', '', 0))

    # done
    return target_rows


#
# latest_forecast_ids_for_project() - currently only used by _vega_lite_spec_for_project()
#

def latest_forecast_ids_for_project(project, is_only_f_id, model_ids=None, timezero_ids=None):
    """
    A multi-purpose utility that returns the latest forecast IDs for all forecasts in project by honoring
    `Forecast.issued_at`. Args customize filtering and return value.

    :param project: a Project
    :param is_only_f_id: boolean that controls the return value: True: return a list of the latest forecast IDs.
        False: Return a dict that maps (forecast_model_id, timezero_id) 2-tuples to the latest forecast's forecast_id
    :param model_ids: optional list of ForecastModel IDs to filter by. None means include all models
    :param timezero_ids: "" Timezero IDs "". None means include all TimeZeros
    :param as_of: optional date string in YYYY_MM_DD_DATE_FORMAT used for filter based on `Forecast.issued_at`. (note
        that both postgres and sqlite3 support that literal format)
    """
    # build up the query based on args
    select_ids = "f.id AS f_id" if is_only_f_id else "fm_tz_max_issued_ats.fm_id AS fm_id, fm_tz_max_issued_ats.tz_id AS tz_id, f.id AS f_id"
    and_model_ids = f"AND fm.id IN ({', '.join(map(str, model_ids))})" if model_ids else ""
    and_timezero_ids = f"AND f.time_zero_id IN ({', '.join(map(str, timezero_ids))})" if timezero_ids else ""
    sql = f"""
        WITH fm_tz_max_issued_ats AS (
            SELECT f.forecast_model_id AS fm_id,
                   f.time_zero_id      AS tz_id,
                   MAX(f.issued_at)    AS max_issued_at
            FROM {Forecast._meta.db_table} AS f
                     JOIN {TimeZero._meta.db_table} AS tz ON f.time_zero_id = tz.id
                     JOIN {ForecastModel._meta.db_table} AS fm ON f.forecast_model_id = fm.id
            WHERE fm.project_id = %s AND NOT fm.is_oracle  {and_model_ids}  {and_timezero_ids}
            GROUP BY f.forecast_model_id, f.time_zero_id
        )
        SELECT {select_ids}
        FROM fm_tz_max_issued_ats
                 JOIN {Forecast._meta.db_table} AS f
                      ON f.forecast_model_id = fm_tz_max_issued_ats.fm_id
                          AND f.time_zero_id = fm_tz_max_issued_ats.tz_id
                          AND f.issued_at = fm_tz_max_issued_ats.max_issued_at;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        rows = cursor.fetchall()

    return [row[0] for row in rows] if is_only_f_id else {(fm_id, tz_id): f_id for fm_id, tz_id, f_id, in rows}


#
# latest_forecast_cols_for_project()
#

def latest_forecast_cols_for_project(project, is_incl_fm_id=True, is_incl_tz_id=True, is_incl_issued_at=True,
                                     is_incl_created_at=True, is_incl_source=True, is_incl_notes=True):
    """
    Simpler variation of `latest_forecast_ids_for_project()` that uses window functions and returns a list of requested
    fields. Returns information about all of the latest forecasts in `project`.

    :param is_incl_*: booleans indicating which Forecast columns to include: 'forecast_model_id', 'time_zero_id',
        'issued_at', 'created_at', 'source', and 'notes'. NB: 'forecast_id' is always included
    :param project: a Project
    :return: a list of N+1-tuples (depends on is_incl_*):
        (forecast_id, [forecast_model_id], [time_zero_id], [issued_at], [created_at], [source], [notes])
    """
    col_name_to_is_include = {
        'id': True,
        'forecast_model_id': is_incl_fm_id,
        'time_zero_id': is_incl_tz_id,
        'issued_at': is_incl_issued_at,
        'created_at': is_incl_created_at,
        'source': is_incl_source,
        'notes': is_incl_notes,
    }
    with_select_cols = [f"f.{col_name} AS f_{col_name}"
                        for col_name, is_incl_col in col_name_to_is_include.items()
                        if is_incl_col]
    outer_select_cols = [f"f_{col_name}" for col_name, is_incl_col in col_name_to_is_include.items() if is_incl_col]
    sql = f"""
        WITH ranked_rows AS (
            SELECT {', '.join(with_select_cols)},
                   RANK() OVER (
                       PARTITION BY fm.id, f.time_zero_id
                       ORDER BY f.issued_at DESC) AS rownum
            FROM {Forecast._meta.db_table} AS f
                     JOIN {ForecastModel._meta.db_table} AS fm on f.forecast_model_id = fm.id
            WHERE fm.project_id = %s
              AND NOT fm.is_oracle
        )
        SELECT {', '.join(outer_select_cols)}
        FROM ranked_rows
        WHERE rownum = 1;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        # return batched_rows(cursor)  # todo xx cursor closed error
        return cursor.fetchall()
