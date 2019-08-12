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
def create_project_from_json(proj_config_file_path_or_dict, owner):
    """
    Creates a Project based on the json configuration file at json_file_path. Errors if one with that name already
    exists. Does not set Project.model_owners, create TimeZeros, load truth data, create Models, or load forecasts.

    :param proj_config_file_path_or_dict: either a Path to project config json file OR a dict as loaded from a file.
        see cdc-project.json for an example
    :param owner: the new Project's owner (a User)
    :return: the new Project
    """

    # https://stackoverflow.com/questions/1661262/check-if-object-is-file-like-in-python
    # if isinstance(truth_file_path_or_fp, io.IOBase):
    #     self._load_truth_data(truth_file_path_or_fp)
    # else:
    #     with open(str(truth_file_path_or_fp)) as cdc_csv_file_fp:
    #         self._load_truth_data(cdc_csv_file_fp)

    logger.info(f"* create_project_from_json(): started. proj_config_file_path_or_dict="
                f"{proj_config_file_path_or_dict}, owner={owner}")
    if isinstance(proj_config_file_path_or_dict, dict):
        project_dict = proj_config_file_path_or_dict
    else:
        with open(proj_config_file_path_or_dict) as fp:
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

    targets = validate_and_create_targets(project, project_dict)
    logger.info(f"- created {len(targets)} Targets: {targets}")

    logger.info(f"* create_project_from_json(): done!")
    return project


def create_locations(project, project_dict):
    return [Location.objects.create(project=project, name=location_dict['name'])
            for location_dict in project_dict['locations']]


def validate_and_create_targets(project, project_dict):
    targets = []
    for target_dict in project_dict['targets']:
        # validate point_value_type and convert to db choice - one of: 'integer', 'float', or 'text'
        point_value_type_input = target_dict['point_value_type'].lower()
        point_value_type = None
        for db_value, human_readable_value in Target.POINT_VALUE_TYPE_CHOICES:
            if human_readable_value.lower() == point_value_type_input:
                point_value_type = db_value

        if point_value_type is None:
            point_value_type_choices = [choice[1] for choice in Target.POINT_VALUE_TYPE_CHOICES]
            raise RuntimeError(f"invalid 'point_value_type': {point_value_type_input}. must be one of: "
                               f"{point_value_type_choices}")

        # validate and translate 'prediction_types' to a convenience dict that is passed Target's constructor via **
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

            if (prediction_type == 'BinLwr') and (('lwr' not in target_dict) or not target_dict['lwr']):
                raise RuntimeError(f"required lwr entry is missing for BinLwr prediction type")

            prediction_ok_types_dict[prediction_type_to_field_name[prediction_type]] = True

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
