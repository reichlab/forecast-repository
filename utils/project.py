import json
import logging
from datetime import datetime

from django.db import transaction

from forecast_app.models import Project, Location, Target
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


logger = logging.getLogger(__name__)


#
# delete_project_iteratively()
#

@transaction.atomic
def delete_project_iteratively(project):
    """
    An alternative to Project.delete(), deletes the passed Project, but unlike that function, does so by iterating over
    objects that refer to the project before deleting the project itself. This apparently reduces the memory usage
    enough to allow the below Heroku deletion. See [Deleting projects on Heroku production fails](https://github.com/reichlab/forecast-repository/issues/91).
    """
    logger.info(f"* delete_project_iteratively(): deleting models and forecasts")
    for forecast_model in project.models.iterator():
        logger.info(f"- {forecast_model.pk}")
        for forecast in forecast_model.forecasts.iterator():
            logger.info(f"  = {forecast.pk}")
            forecast.delete()
        forecast_model.delete()

    logger.info(f"delete_project_iteratively(): deleting locations")
    for location in project.locations.iterator():
        logger.info(f"- {location.pk}")
        location.delete()

    logger.info(f"delete_project_iteratively(): deleting targets")
    for target in project.targets.iterator():
        logger.info(f"- {target.pk}")
        target.delete()

    logger.info(f"delete_project_iteratively(): deleting timezeros")
    for timezero in project.timezeros.iterator():
        logger.info(f"- {timezero.pk}")
        timezero.delete()

    logger.info(f"delete_project_iteratively(): deleting remainder")
    project.delete()  # deletes remaining references: RowCountCache, ScoreCsvFileCache
    logger.info(f"delete_project_iteratively(): done")


#
# config_dict_from_project()
#

def config_dict_from_project(project):
    """
    The twin of `create_project_from_json()`, returns a configuration dict for project as passed to that function.
    """
    # todo xx integrate with API serialization!
    return {'name': project.name, 'is_public': project.is_public, 'description': project.description,
            'home_url': project.home_url, 'logo_url': project.logo_url, 'core_data': project.core_data,
            'time_interval_type': project.time_interval_type_as_str(),
            'visualization_y_label': project.visualization_y_label,
            'locations': [{'name': location.name} for location in project.locations.all()],
            'targets': _target_dicts_for_project_config(project),
            'timezeros': [{'timezero_date': timezero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),
                           'data_version_date':
                               timezero.data_version_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                               if timezero.data_version_date else None,
                           'is_season_start': timezero.is_season_start,
                           'season_name': timezero.season_name}
                          for timezero in project.timezeros.all()]}


# todo xx integrate with API serialization!
def _target_dicts_for_project_config(project):
    target_dicts = []
    for target in project.targets.all():
        data_type = Target.data_type(target.type)
        type_int_to_name = {type_int: type_name for type_int, type_name in Target.TARGET_TYPE_CHOICES}

        # start with required fields
        target_dict = {'name': target.name,
                       'description': target.description,
                       'type': type_int_to_name[target.type],  # TARGET_TYPE_CHOICES
                       'is_step_ahead': target.is_step_ahead}

        # add optional fields, including 'list' ones. rather than basing whether they are available on target type (for
        # example, 'continuous' targets /might/ have a range), we check for whether the optional field (including 'list'
        # ones) is present. the exception is step_ahead_increment, for which we check is_step_ahead

        # is_step_ahead
        if target.is_step_ahead:
            target_dict['step_ahead_increment'] = target.step_ahead_increment

        # unit (target.unit)
        if target.unit is not None:
            target_dict['unit'] = target.unit

        # range (target.ranges)
        target_ranges_qs = target.ranges  # target.value_i, target.value_f
        if target_ranges_qs.count() != 0:  # s/b exactly 2
            target_ranges = target_ranges_qs.values_list('value_i', flat=True) \
                if data_type == Target.INTEGER_DATA_TYPE \
                else target_ranges_qs.values_list('value_f', flat=True)
            target_ranges = sorted(target_ranges)
            target_dict['range'] = [target_ranges[0], target_ranges[1]]

        # cats (target.cats)
        target_cats_qs = target.cats  # target.cat_i, target.cat_f, target.cat_t, target.cat_d
        if target_cats_qs.count() != 0:
            if data_type == Target.INTEGER_DATA_TYPE:
                target_cats = target_cats_qs.values_list('cat_i', flat=True)
            elif data_type == Target.FLOAT_DATA_TYPE:
                target_cats = target_cats_qs.values_list('cat_f', flat=True)
            elif data_type == Target.TEXT_DATA_TYPE:
                target_cats = target_cats_qs.values_list('cat_t', flat=True)
            elif data_type == Target.DATE_DATA_TYPE:
                target_cats = [cat_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                               for cat_date in target_cats_qs.values_list('cat_d', flat=True)]
            else:
                raise RuntimeError(f"invalid data_type={data_type} ({type_int_to_name[target.type]})")
            target_dict['cats'] = sorted(target_cats)
        target_dicts.append(target_dict)
    return target_dicts


#
# create_project_from_json()
#

@transaction.atomic
def create_project_from_json(proj_config_file_path_or_dict, owner):
    """
    Top-level function that creates a Project based on the json configuration file at json_file_path. Errors if one with
    that name already exists. Does not set Project.model_owners, create TimeZeros, load truth data, create Models, or
    load forecasts.

    :param proj_config_file_path_or_dict: either a Path to project config json file OR a dict as loaded from a file.
        See https://docs.zoltardata.com/fileformats/#project-creation-configuration-json for details, and
        cdc-project.json for an example.
    :param owner: the new Project's owner (a User)
    :param is_validate: True if the input json should be validated. passed in case a project requires less stringent
        validation
    :return: the new Project
    """
    logger.info(f"* create_project_from_json(): started. proj_config_file_path_or_dict="
                f"{proj_config_file_path_or_dict}, owner={owner}")
    if isinstance(proj_config_file_path_or_dict, dict):
        project_dict = proj_config_file_path_or_dict
    else:
        with open(proj_config_file_path_or_dict) as fp:
            project_dict = json.load(fp)

    # validate project_dict
    actual_keys = set(project_dict.keys())
    expected_keys = {'name', 'is_public', 'description', 'home_url', 'logo_url', 'core_data', 'time_interval_type',
                     'visualization_y_label', 'locations', 'targets', 'timezeros'}
    if actual_keys != expected_keys:
        raise RuntimeError(f"Wrong keys in project_dict. difference={expected_keys ^ actual_keys}. "
                           f"expected={expected_keys}, actual={actual_keys}")

    # error if project already exists
    name = project_dict['name']
    project = Project.objects.filter(name=name).first()  # None if doesn't exist
    if project:
        raise RuntimeError(f"found existing project. name={name}, project={project}")

    project = create_project(project_dict, owner)
    logger.info(f"- created Project: {project}")

    locations = validate_and_create_locations(project, project_dict)
    logger.info(f"- created {len(locations)} Locations: {locations}")

    targets = validate_and_create_targets(project, project_dict)
    logger.info(f"- created {len(targets)} Targets: {targets}")

    timezeros = validate_and_create_timezeros(project, project_dict)
    logger.info(f"- created {len(timezeros)} TimeZeros: {timezeros}")

    logger.info(f"* create_project_from_json(): done!")
    return project


def validate_and_create_locations(project, project_dict):
    try:
        return [Location.objects.create(project=project, name=location_dict['name'])
                for location_dict in project_dict['locations']]
    except KeyError:
        raise RuntimeError(f"one of the location_dicts had no 'name' field. locations={project_dict['locations']}")


def validate_and_create_timezeros(project, project_dict):
    from forecast_app.api_views import validate_and_create_timezero  # avoid circular imports


    return [validate_and_create_timezero(project, timezero_config) for timezero_config in project_dict['timezeros']]


def validate_and_create_targets(project, project_dict):
    targets = []
    for target_dict in project_dict['targets']:
        # check for keys required by all target types. optional keys are tested below
        all_keys = set(target_dict.keys())
        tested_keys = all_keys - {'unit', 'step_ahead_increment', 'range', 'cats'}  # optional keys
        expected_keys = {'name', 'description', 'type', 'is_step_ahead'}
        if tested_keys != expected_keys:
            raise RuntimeError(f"Wrong required keys in target_dict. difference={expected_keys ^ tested_keys}. "
                               f"expected_keys={expected_keys}, tested_keys={tested_keys}. target_dict={target_dict}")

        # validate type
        type_name = target_dict['type']
        valid_target_types = [type_name for type_int, type_name in Target.TARGET_TYPE_CHOICES]
        if type_name not in valid_target_types:
            raise RuntimeError(f"Invalid type_name={type_name}. valid_target_types={valid_target_types} . "
                               f"target_dict={target_dict}")

        # check for step_ahead_increment required if is_step_ahead
        if target_dict['is_step_ahead'] and ('step_ahead_increment' not in target_dict):
            raise RuntimeError(f"step_ahead_increment not found but is required when is_step_ahead is passed. "
                               f"target_dict={target_dict}")

        # check required, optional, and invalid keys by target type. 3 cases: 'unit', 'range', 'cats'
        type_name_to_type_int = {type_name: type_int for type_int, type_name in Target.TARGET_TYPE_CHOICES}
        type_int = type_name_to_type_int[type_name]

        # 1) test optional 'unit'. three cases a-c follow
        # 1a) required but not passed: ['continuous', 'discrete', 'date']
        if ('unit' not in all_keys) and \
                (type_int in [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE, Target.DATE_TARGET_TYPE]):
            raise RuntimeError(f"'unit' not passed but is required for type_name={type_name}")

        # 1b) optional: ok to pass or not pass: []: no need to validate

        # 1c) invalid but passed: ['nominal', 'binary', 'compositional']
        if ('unit' in all_keys) and \
                (type_int in [Target.NOMINAL_TARGET_TYPE, Target.BINARY_TARGET_TYPE, Target.COMPOSITIONAL_TARGET_TYPE]):
            raise RuntimeError(f"'unit' passed but is invalid for type_name={type_name}")

        # 2) test optional 'range'. three cases a-c follow
        # 2a) required but not passed: []: no need to validate

        # 2b) optional: ok to pass or not pass: ['continuous', 'discrete']: no need to validate

        # 2c) invalid but passed: ['nominal', 'binary', 'date', 'compositional']
        if ('range' in all_keys) and (
                type_int in [Target.NOMINAL_TARGET_TYPE, Target.BINARY_TARGET_TYPE, Target.DATE_TARGET_TYPE,
                             Target.COMPOSITIONAL_TARGET_TYPE]):
            raise RuntimeError(f"'range' passed but is invalid for type_name={type_name}")

        # 3) test optional 'cats'. three cases a-c follow
        # 3a) required but not passed: ['nominal', 'date', 'compositional']
        if ('cats' not in all_keys) and \
                (type_int in [Target.NOMINAL_TARGET_TYPE, Target.DATE_TARGET_TYPE, Target.COMPOSITIONAL_TARGET_TYPE]):
            raise RuntimeError(f"'cats' not passed but is required for type_name={type_name}")

        # 3b) optional: ok to pass or not pass: ['continuous', 'discrete']: no need to validate

        # 3c) invalid but passed: ['binary']
        if ('cats' in all_keys) and (type_int == Target.BINARY_TARGET_TYPE):
            raise RuntimeError(f"'cats' passed but is invalid for type_name={type_name}")

        # validate 'range' if passed
        data_type = Target.data_type(type_int)  # python type
        range_parsed = []  # set next if valid
        if 'range' in target_dict:
            for range_str in target_dict['range']:
                try:
                    range_parsed.append(data_type(range_str))
                except ValueError as ve:
                    raise RuntimeError(f"range type did not match data_type. range_str={range_str!r}, "
                                       f"data_type={data_type}, error: {ve}")

            if len(target_dict['range']) != 2:
                raise RuntimeError(f"range did not contain exactly two items: {target_dict['range']}")

        # validate 'cats' if passed. data type can be any of Target.*_DATA_TYPE: bool, datetime.date, float, int, or
        # str. all functions but datetime.date can be called directly on the passed cat. we handle datetime.date
        # separately
        cats_parsed = []  # set next if valid
        if 'cats' in target_dict:
            for cat_str in target_dict['cats']:
                try:
                    if type_int == Target.DATE_TARGET_TYPE:
                        cats_parsed.append(datetime.strptime(cat_str, YYYY_MM_DD_DATE_FORMAT).date())
                    else:
                        cats_parsed.append(data_type(cat_str))
                except ValueError as ve:
                    raise RuntimeError(f"could not convert cat to data_type. cat_str={cat_str!r}, "
                                       f"data_type={data_type}, error: {ve}")

        # valid! create the Target and then supporting 'list' instances: TargetCat, TargetLwr, TargetDate,
        # and TargetRange. atomic so that Targets succeed only if others do too
        with transaction.atomic():
            model_init = {'project': project,
                          'type': type_name_to_type_int[type_name],
                          'name': target_dict['name'],
                          'description': target_dict['description'],
                          'is_step_ahead': target_dict['is_step_ahead']}
            if target_dict['is_step_ahead']:
                model_init['step_ahead_increment'] = target_dict['step_ahead_increment']
            if 'unit' in target_dict:
                model_init['unit'] = target_dict['unit']
            target = Target.objects.create(**model_init)
            targets.append(target)

            # create TargetRange, and TargetCat and TargetLwr (via set_cats()) instances
            if ('range' in target_dict) and range_parsed:  # create two TargetRanges
                target.set_range(range_parsed[0], range_parsed[1])
            if ('cats' in target_dict) and cats_parsed:  # create TargetCats and TargetLwrs
                target.set_cats(cats_parsed)
    return targets


def create_project(project_dict, owner):
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
