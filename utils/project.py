import json
import logging

from django.db import transaction

from forecast_app.models import Project, Location, Target
from utils.forecast import PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS


logger = logging.getLogger(__name__)


#
# create_project_from_json()
#

@transaction.atomic
def create_project_from_json(json_file_path, owner):
    """
    Creates a Project based on the json configuration file at json_file_path. Errors if one with that name already
    exists. Does not set Project.model_owners, create TimeZeros, load truth data, create Models, or load forecasts.

    :param json_file_path: a Path to the json spec file. see cdc-project.json for an example
    :param owner: the new Project's owner (a User)
    :return: the new Project
    """
    logger.info(f"* create_project_from_json(): started. json_file_path={json_file_path}, owner={owner}")
    with open(json_file_path) as fp:
        project_dict = json.load(fp)

    # error if project already exists
    name = project_dict['name']
    project = Project.objects.filter(name=name).first()  # None if doesn't exist
    if project:
        raise RuntimeError(f"found existing project. name={name}, project={project}")

    project = create_project(project_dict, owner)
    logger.info(f"- created Project: {project}")

    locations = create_locations(project, project_dict)
    logger.info(f"- created {len(locations)} Locations: {locations}")

    targets = create_targets(project, project_dict)
    logger.info(f"- created {len(targets)} Targets: {targets}")

    logger.info(f"* create_project_from_json(): done!")
    return project


def create_locations(project, project_dict):
    return [Location.objects.create(project=project, name=location_dict['name'])
            for location_dict in project_dict['locations']]


def create_targets(project, project_dict):
    targets = []
    for target_dict in project_dict['targets']:
        # validate point_value_type - one of: 'integer', 'float', or 'text'
        point_value_type_input = target_dict['point_value_type'].lower()
        point_value_type = None
        for db_value, human_readable_value in Target.POINT_VALUE_TYPE_CHOICES:
            if human_readable_value.lower() == point_value_type_input:
                point_value_type = db_value

        if point_value_type is None:
            point_value_type_choices = [choice[1] for choice in Target.POINT_VALUE_TYPE_CHOICES]
            raise RuntimeError(f"invalid 'point_value_type': {point_value_type_input}. must be one of: "
                               f"{point_value_type_choices}")

        # translate 'prediction_types' to a convenience dict that is passed Target's constructor via **
        prediction_ok_types_dict = {}
        prediction_type_to_field_name = {'BinCat': 'ok_bincat_distribution',
                                         'BinLwr': 'ok_binlwr_distribution',
                                         'Binary': 'ok_binary_distribution',
                                         'Named': 'ok_named_distribution',
                                         'Point': 'ok_point_prediction',
                                         'Sample': 'ok_sample_distribution',
                                         'SampleCat': 'ok_samplecat_distribution', }
        for prediction_type in target_dict['prediction_types']:
            if prediction_type not in PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS.values():
                prediction_type_choices = [choice[1] for choice in PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS]
                raise RuntimeError(f"invalid prediction_type: {prediction_type}. must be one of: "
                                   f"{prediction_type_choices}")

            prediction_ok_types_dict[prediction_type_to_field_name[prediction_type]] = True

        # todo xx "lwr": [0, 0.1, 0.2, ..., 13]

        target = Target.objects.create(project=project, name=target_dict['name'],
                                       description=target_dict['description'], unit=target_dict['unit'],
                                       is_date=target_dict['is_date'], is_step_ahead=target_dict['is_step_ahead'],
                                       step_ahead_increment=target_dict['step_ahead_increment'],
                                       point_value_type=point_value_type, **prediction_ok_types_dict)
        targets.append(target)
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

    visualization_y_label = project_dict['visualization_y_label']
    # todo xx replace: config_dict=CDC_CONFIG_DICT with: visualization_y_label = project_dict['visualization_y_label']

    project = Project.objects.create(
        owner=owner,
        is_public=project_dict['is_public'],
        name=project_dict['name'],
        time_interval_type=time_interval_type,
        description=project_dict['description'],
        home_url=project_dict['home_url'],  # required
        logo_url=project_dict['logo_url'] if 'logo_url' in project_dict else None,
        core_data=project_dict['core_data'] if 'core_data' in project_dict else None,
    )
    project.save()
    return project
