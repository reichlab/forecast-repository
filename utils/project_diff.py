import logging
from enum import IntEnum
from itertools import groupby

from django.db import transaction

from forecast_app.models import Unit, Target, PredictionElement
from forecast_app.models.project import TimeZero
from forecast_app.models.target import reference_date_type_for_name
from utils.project import create_project_from_json, _validate_and_create_units, _validate_and_create_targets, \
    _validate_and_create_timezeros
from utils.project_truth import truth_data_qs
from utils.utilities import basic_str


logger = logging.getLogger(__name__)


#
# project_config_diff()
#

# valid 'object_type' values:
class ObjectType(IntEnum):  # IntEnum so tests can sort
    PROJECT = 0  # object_pk=project.name
    UNIT = 1  # object_pk=unit.abbreviation
    TARGET = 2  # object_pk=target.name
    TIMEZERO = 3  # object_pk=timezero.timezero_date formatted as YYYY_MM_DD_DATE_FORMAT


# valid 'change_type' values:
class ChangeType(IntEnum):  # IntEnum so tests can sort
    OBJ_ADDED = 0  # added an object of type object_type to a Project. object_dict=the new object's contents. field_name is unused
    OBJ_REMOVED = 1  # removed "" from a project. field_name and object_dict are unused
    FIELD_EDITED = 2  # edited the field of an object of object_type, setting it to object_dict[field_name]. field_name is unused
    FIELD_ADDED = 3  # added ""
    FIELD_REMOVED = 4  # removed "". object_dict is unused


class Change:
    """
    Represents a change to a dict as returned by config_dict_from_project().

    'field_name' and 'object_dict' usage by 'change_type':
    +---------------+------------+--------------------------+
    | change_type   | field_name | object_dict              |
    +---------------+------------+--------------------------+
    | OBJ_ADDED     | n/a        | new object's contents    | # ala create_project_from_json()
    | OBJ_REMOVED   | n/a        | n/a                      |
    | FIELD_EDITED  | field name | edited object's contents |
    | FIELD_ADDED   | field name | added object's contents  |
    | FIELD_REMOVED | field name | n/a                      |
    +---------------+------------+--------------------------+

    Note: Due to __hash__()'s ignoring of object_dict, be careful not to change object_dict after the fact. This
    implementation is a quick-and-dirty way to allow using Change objects as dict keys, or in sets.
    """


    def __init__(self, object_type: ObjectType, object_pk: str, change_type: ChangeType,
                 field_name: str, object_dict: dict):
        super().__init__()
        self.object_type = ObjectType(object_type)  # we cast b/c inputs might be plain ints
        self.object_pk = object_pk
        self.change_type = ChangeType(change_type)  # ""
        self.field_name = field_name
        self.object_dict = object_dict


    def __repr__(self):
        return f"Change(ObjectType.{self.object_type.name}, {self.object_pk!r}, ChangeType.{self.change_type.name}, " \
               f"{self.field_name!r}, {self.object_dict})"


    def __str__(self):  # todo
        return basic_str(self)


    # https://stackoverflow.com/questions/2909106/whats-a-correct-and-good-way-to-implement-hash
    def __key(self):
        return (self.object_type, self.object_pk, self.change_type, self.field_name)  # NB: we ignore self.object_dict


    def __hash__(self):
        return hash(self.__key())


    #   # NB: this would ignore self.object_dict:
    # def __eq__(self, other):
    #     if isinstance(other, Change):
    #         return self.__key() == other.__key()
    #     return NotImplemented


    def __eq__(self, other):
        if not isinstance(other, Change):
            return NotImplemented

        return (self.object_type == other.object_type) and (self.object_pk == other.object_pk) \
               and (self.change_type == other.change_type) and (self.field_name == other.field_name) \
               and (self.object_dict == other.object_dict)


    def serialize_to_dict(self):
        """
        A poor man's JSON serializer. Should probably be done cleanly, e.g., via subcassing
        https://docs.python.org/3.6/library/json.html#json.JSONEncoder (via
        https://stackoverflow.com/questions/3768895/how-to-make-a-class-json-serializable ).

        :return: a dict containing my content, suitable for json.dumps()
        """
        return {'object_type': int(self.object_type),
                'object_pk': self.object_pk,
                'change_type': int(self.change_type),
                'field_name': self.field_name,
                'object_dict': self.object_dict}


    @classmethod
    def deserialize_dict(cls, serialized_change_dict):
        """
        Sister to serialize_to_dict().

        :param serialized_change_dict: as returned by serialize_to_dict()
        :return: a Change from serialized_change
        """
        return Change(serialized_change_dict['object_type'],
                      serialized_change_dict['object_pk'],
                      serialized_change_dict['change_type'],
                      serialized_change_dict['field_name'],
                      serialized_change_dict['object_dict'])


def project_config_diff(config_dict_1, config_dict_2):
    """
    Analyzes and returns the differences between the two project configuration dicts, specifically the changes that were
    made to config_dict_1 to result in config_dict_2. Here are the kinds of diffs:

    Project edits:
    - editable fields: 'name', 'is_public', 'description', 'home_url', 'logo_url', 'core_data'
    - 'units': add, remove
    - 'timezeros': add, remove, edit
    - 'targets': add, remove, edit

    Unit edits: fields:
    - 'abbreviation': the Unit's pk. therefore editing this field effectively removes the existing Unit and adds a new
        one to replace it
    - editable fields: 'name'

    TimeZero edits: fields:
    - 'timezero_date': the TimeZero's pk. therefore editing this field effectively removes the existing TimeZero and
        adds a new one to replace it
    - editable fields: 'data_version_date', 'is_season_start', 'season_name'

    Target edits: fields:
    - 'name': the Target's pk. therefore editing this field effectively removes the existing Target and adds a new one
        to replace it
    - editable fields: 'description', 'is_step_ahead', 'numeric_horizon', 'outcome_variable'
    - 'type': Target type cannot be edited because it might invalidate existing forecasts. therefore editing this field
        effectively removes the existing Target and adds a new one to replace it
    - 'range': similarly cannot be edited due to possible forecast invalidation
    - 'cats': ""

    :param config_dict_1: as returned by config_dict_from_project(). treated as the "from" dict
    :param config_dict_2: "". treated as the "to" dict
    :return: a list of Change objects that 'move' the state of config_dict_1 to config_dict_2. aka a
        "project config diff". list order is non-deterministic
    """
    changes = []  # return value. filled next. a list of Changes

    # validate inputs (ensures expected fields are present)
    create_project_from_json(config_dict_1, None, is_validate_only=True)
    create_project_from_json(config_dict_2, None, is_validate_only=True)

    # check project field edits
    for field_name in ['name', 'is_public', 'description', 'home_url', 'logo_url', 'core_data']:
        if config_dict_1[field_name] != config_dict_2[field_name]:
            changes.append(Change(ObjectType.PROJECT, None, ChangeType.FIELD_EDITED, field_name, config_dict_2))

    # check for units added or removed
    unit_abbrevs_1 = {unit_dict['abbreviation'] for unit_dict in config_dict_1['units']}
    unit_abbrevs_2 = {unit_dict['abbreviation'] for unit_dict in config_dict_2['units']}
    removed_unit_abbrevs = unit_abbrevs_1 - unit_abbrevs_2
    added_unit_abbrevs = unit_abbrevs_2 - unit_abbrevs_1
    changes.extend([Change(ObjectType.UNIT, abbrev, ChangeType.OBJ_REMOVED, None, None)
                    for abbrev in removed_unit_abbrevs])
    changes.extend([Change(ObjectType.UNIT, unit_dict['abbreviation'], ChangeType.OBJ_ADDED, None, unit_dict)
                    for unit_dict in config_dict_2['units'] if unit_dict['abbreviation'] in added_unit_abbrevs])

    # check for unit field edits
    unit_abbrev_1_to_dict = {unit_dict['abbreviation']: unit_dict for unit_dict in config_dict_1['units']}
    unit_abbrev_2_to_dict = {unit_dict['abbreviation']: unit_dict for unit_dict in config_dict_2['units']}
    for unit_abbrev in unit_abbrevs_1 & unit_abbrevs_2:
        for field_name in ['name']:
            if (field_name in unit_abbrev_1_to_dict[unit_abbrev]) and \
                    (field_name in unit_abbrev_2_to_dict[unit_abbrev]) and \
                    (unit_abbrev_2_to_dict[unit_abbrev][field_name] != '') and \
                    (unit_abbrev_1_to_dict[unit_abbrev][field_name] != unit_abbrev_2_to_dict[unit_abbrev][field_name]):
                # field_name edited
                changes.append(Change(ObjectType.UNIT, unit_abbrev, ChangeType.FIELD_EDITED, field_name,
                                      unit_abbrev_2_to_dict[unit_abbrev]))  # use 2nd dict in case other changes

    # check for timezeros added or removed
    timezero_dates_1 = {timezero_dict['timezero_date'] for timezero_dict in config_dict_1['timezeros']}
    timezero_dates_2 = {timezero_dict['timezero_date'] for timezero_dict in config_dict_2['timezeros']}
    removed_tz_dates = timezero_dates_1 - timezero_dates_2
    added_tz_dates = timezero_dates_2 - timezero_dates_1
    changes.extend([Change(ObjectType.TIMEZERO, name, ChangeType.OBJ_REMOVED, None, None) for name in removed_tz_dates])
    changes.extend([Change(ObjectType.TIMEZERO, tz_dict['timezero_date'], ChangeType.OBJ_ADDED, None, tz_dict)
                    for tz_dict in config_dict_2['timezeros'] if tz_dict['timezero_date'] in added_tz_dates])

    # check for timezero field edits
    tz_date_1_to_dict = {timezero_dict['timezero_date']: timezero_dict for timezero_dict in config_dict_1['timezeros']}
    tz_date_2_to_dict = {timezero_dict['timezero_date']: timezero_dict for timezero_dict in config_dict_2['timezeros']}
    for timezero_date in timezero_dates_1 & timezero_dates_2:  # timezero_dates_both
        for field_name in ['data_version_date', 'is_season_start', 'season_name']:  # season_name is only optional field
            if (field_name in tz_date_1_to_dict[timezero_date]) and \
                    (field_name not in tz_date_2_to_dict[timezero_date]):
                # field_name removed
                changes.append(Change(ObjectType.TIMEZERO, timezero_date, ChangeType.FIELD_REMOVED, field_name, None))
            elif (field_name not in tz_date_1_to_dict[timezero_date]) and \
                    (field_name in tz_date_2_to_dict[timezero_date]):
                # field_name added
                changes.append(Change(ObjectType.TIMEZERO, timezero_date, ChangeType.FIELD_ADDED, field_name,
                                      tz_date_2_to_dict[timezero_date]))
            # this test for `!= ''` matches this one below: "NB: here we convert '' to None to avoid errors like"
            elif (field_name in tz_date_1_to_dict[timezero_date]) and \
                    (field_name in tz_date_2_to_dict[timezero_date]) and \
                    (tz_date_2_to_dict[timezero_date][field_name] != '') and \
                    (tz_date_1_to_dict[timezero_date][field_name] != tz_date_2_to_dict[timezero_date][field_name]):
                # field_name edited
                changes.append(Change(ObjectType.TIMEZERO, timezero_date, ChangeType.FIELD_EDITED, field_name,
                                      tz_date_2_to_dict[timezero_date]))  # use 2nd dict in case other changes

    # check for targets added or removed
    target_names_1 = {target_dict['name'] for target_dict in config_dict_1['targets']}
    target_names_2 = {target_dict['name'] for target_dict in config_dict_2['targets']}
    removed_target_names = target_names_1 - target_names_2
    added_target_names = target_names_2 - target_names_1
    changes.extend([Change(ObjectType.TARGET, name, ChangeType.OBJ_REMOVED, None, None)
                    for name in removed_target_names])
    changes.extend([Change(ObjectType.TARGET, target_dict['name'], ChangeType.OBJ_ADDED, None, target_dict)
                    for target_dict in config_dict_2['targets'] if target_dict['name'] in added_target_names])

    # check for target field edits. as noted above, editing some fields imply entire target replacement (remove and then
    # add)
    targ_name_1_to_dict = {target_dict['name']: target_dict for target_dict in config_dict_1['targets']}
    targ_name_2_to_dict = {target_dict['name']: target_dict for target_dict in config_dict_2['targets']}
    editable_fields = ['description', 'outcome_variable', 'is_step_ahead', 'numeric_horizon', 'reference_date_type']
    non_editable_fields = ['type', 'range', 'cats']
    for target_name in target_names_1 & target_names_2:  # target_names_both
        for field_name in editable_fields + non_editable_fields:
            if (field_name in targ_name_1_to_dict[target_name]) and \
                    (field_name not in targ_name_2_to_dict[target_name]):
                # field_name removed
                if field_name in non_editable_fields:
                    changes.append(Change(ObjectType.TARGET, target_name, ChangeType.OBJ_REMOVED, None, None))
                    changes.append(Change(ObjectType.TARGET, target_name, ChangeType.OBJ_ADDED, None,
                                          targ_name_2_to_dict[target_name]))  # use 2nd dict in case other changes
                else:
                    changes.append(Change(ObjectType.TARGET, target_name, ChangeType.FIELD_REMOVED, field_name, None))
            elif (field_name not in targ_name_1_to_dict[target_name]) and \
                    (field_name in targ_name_2_to_dict[target_name]):
                # field_name added
                if field_name in non_editable_fields:
                    changes.append(Change(ObjectType.TARGET, target_name, ChangeType.OBJ_REMOVED, None, None))
                    changes.append(Change(ObjectType.TARGET, target_name, ChangeType.OBJ_ADDED, None,
                                          targ_name_2_to_dict[target_name]))  # use 2nd dict in case other changes
                else:
                    changes.append(Change(ObjectType.TARGET, target_name, ChangeType.FIELD_ADDED, field_name,
                                          targ_name_2_to_dict[target_name]))
            elif (field_name in targ_name_1_to_dict[target_name]) and \
                    (field_name in targ_name_2_to_dict[target_name]) and \
                    (targ_name_1_to_dict[target_name][field_name] != targ_name_2_to_dict[target_name][field_name]):
                # field_name edited
                if field_name in non_editable_fields:
                    changes.append(Change(ObjectType.TARGET, target_name, ChangeType.OBJ_REMOVED, None, None))
                    changes.append(Change(ObjectType.TARGET, target_name, ChangeType.OBJ_ADDED, None,
                                          targ_name_2_to_dict[target_name]))  # use 2nd dict in case other changes
                else:
                    changes.append(Change(ObjectType.TARGET, target_name, ChangeType.FIELD_EDITED, field_name,
                                          targ_name_2_to_dict[target_name]))

    # done
    return changes


#
# order_project_config_diff()
#

def order_project_config_diff(changes):
    """
    Cleans and orders changes to be executed:

    - remove wasted activity: ChangeType.FIELD_EDITED on a ChangeType.OBJ_REMOVED
    - order: ChangeType.OBJ_REMOVED before ChangeType.OBJ_ADDED
    - convenience sorting: (object_type, object_pk)

    :param changes: list of Changes as returned by project_config_diff()
    :return: list of ordered and cleaned changes
    """
    changes = sorted(list(changes), key=lambda change: (change.object_type, change.object_pk))  # sorted for groupby()
    cleaned_changes = []  # return value. filled next
    for (object_type, object_pk), change_grouper \
            in groupby(changes, key=lambda change: (change.object_type, change.object_pk)):
        # collect the changes, omitting duplicates. note that we only check change_type and not object_dict, i.e., we
        # assume object_dict is the same for every item in the group
        group_changes = []
        for change in change_grouper:
            if change not in group_changes:
                group_changes.append(change)

        # remove ChangeType.FIELD_EDITED on a ChangeType.OBJ_REMOVED
        change_types = {change.change_type for change in group_changes}
        if (ChangeType.OBJ_REMOVED in change_types) and ((ChangeType.FIELD_EDITED in change_types) or
                                                         (ChangeType.FIELD_ADDED in change_types) or
                                                         (ChangeType.FIELD_REMOVED in change_types)):
            group_changes = [change for change in group_changes
                             if change.change_type in [ChangeType.OBJ_ADDED, ChangeType.OBJ_REMOVED]]

        # do convenience sorting
        group_changes = sorted(group_changes, key=lambda _: (_.object_type, _.object_pk))

        # order by: OBJ_REMOVED, OBJ_ADDED, FIELD_REMOVED, FIELD_EDITED, FIELD_ADDED (first two are important)
        group_changes.sort(key=lambda change: change.change_type, reverse=True)
        cleaned_changes.extend(group_changes)

    return cleaned_changes


#
# database_changes_for_project_config_diff()
#

def database_changes_for_project_config_diff(project, changes):
    """
    Analyzes impact of `changes` on project with respect to deleted rows. The only impactful one is
    ChangeType.OBJ_REMOVED.

    :param project: a Project whose data is being analyzed for changes
    :param changes: list of Changes as returned by project_config_diff()
    :return: a list of 3-tuples: (change, num_pred_eles, num_truth)
    """
    pred_ele_qs = PredictionElement.objects \
        .filter(forecast__forecast_model__project=project,
                forecast__forecast_model__is_oracle=False)
    pred_ele_truth_qs = truth_data_qs(project)
    database_changes = []  # return value. filled next
    for change in order_project_config_diff(changes):
        if (change.object_type == ObjectType.PROJECT) or (change.change_type != ChangeType.OBJ_REMOVED):
            continue

        if change.object_type == ObjectType.UNIT:  # removing a Unit
            unit = object_for_change(project, change, [])  # raises
            num_points = pred_ele_qs.filter(unit=unit).count()
            num_truth = pred_ele_truth_qs.filter(unit=unit).count()
        elif change.object_type == ObjectType.TARGET:  # removing a Target
            target = object_for_change(project, change, [])  # raises
            num_points = pred_ele_qs.filter(target=target).count()
            num_truth = pred_ele_truth_qs.filter(target=target).count()
        else:  # change.object_type == ObjectType.TIMEZERO:  # removing a TimeZero
            timezero = object_for_change(project, change, [])  # raises
            num_points = pred_ele_qs.filter(forecast__time_zero=timezero).count()
            num_truth = pred_ele_truth_qs.filter(forecast__time_zero=timezero).count()
        if num_points:
            database_changes.append((change, num_points, num_truth))
    return database_changes


#
# execute_project_config_diff()
#

@transaction.atomic
def execute_project_config_diff(project, changes):
    """
    Executes the passed Changes list by making the corresponding database changes to project.

    :param project: the Project that's being modified
    :param changes: list of Changes as returned by project_config_diff()
    """
    objects_to_save = []
    for change in order_project_config_diff(changes):
        if change.change_type == ChangeType.OBJ_ADDED:
            if change.object_type == ObjectType.UNIT:
                _validate_and_create_units(project, {'units': [change.object_dict]})
            elif change.object_type == ObjectType.TARGET:
                _validate_and_create_targets(project, {'targets': [change.object_dict]})
            elif change.object_type == ObjectType.TIMEZERO:
                _validate_and_create_timezeros(project, {'timezeros': [change.object_dict]})
        elif change.change_type == ChangeType.OBJ_REMOVED:
            the_obj = object_for_change(project, change, objects_to_save)  # Project/Unit/Target/TimeZero. raises
            the_obj.delete()
        elif (change.change_type == ChangeType.FIELD_EDITED) or (change.change_type == ChangeType.FIELD_ADDED) \
                or (change.change_type == ChangeType.FIELD_REMOVED):
            the_obj = object_for_change(project, change, objects_to_save)  # Project/Unit/Target/TimeZero. raises
            # NB: here we convert '' to None to avoid errors like when setting a timezero's data_version_date to '',
            # say when users incorrectly pass '' instead of null in a project config JSON file
            attr_value = None if (change.change_type == ChangeType.FIELD_REMOVED) \
                                 or (change.object_dict[change.field_name] == '') \
                else change.object_dict[change.field_name]

            # handle the special case of 'reference_date_type', which needs conversion from str to int
            if (change.change_type == ChangeType.FIELD_EDITED) and (change.field_name == 'reference_date_type'):
                attr_value = reference_date_type_for_name(change.object_dict[change.field_name]).id

            setattr(the_obj, change.field_name, attr_value)
            # NB: do not save here b/c multiple FIELD_* changes might be required together to be valid, e.g., when
            # changing Target.is_step_ahead to False, one must remove Target.numeric_horizon (i.e., set it to None)
            objects_to_save.append(the_obj)
    for object_to_save in objects_to_save:
        try:
            object_to_save.save()
        except Exception as ex:
            message = f"execute_project_config_diff(): error trying to save: ex={ex}, object_to_save={object_to_save}"
            logger.error(message)
            raise RuntimeError(message)


def object_for_change(project, change, objects_to_save):
    """
    :param project: a Project
    :param change: a Change
    :param objects_to_save: a list of objects as returned by this function. used to reuse already-loaded objects rather
        than reloading the from the database and thus wiping out any unsaved in-memory changes
    :return: the first object that matches change's object_type and object_pk
    :raises: RuntimeError: if not found
    """
    if change.object_type == ObjectType.PROJECT:
        found_object = project
    elif change.object_type == ObjectType.UNIT:
        found_objects_to_save = [the_obj for the_obj in objects_to_save
                                 if (type(the_obj) == Unit) and (the_obj.abbreviation == change.object_pk)]
        found_object = found_objects_to_save[0] if found_objects_to_save \
            else project.units.filter(abbreviation=change.object_pk).first()
    elif change.object_type == ObjectType.TARGET:
        found_objects_to_save = [the_obj for the_obj in objects_to_save
                                 if (type(the_obj) == Target) and (the_obj.name == change.object_pk)]
        found_object = found_objects_to_save[0] if found_objects_to_save \
            else project.targets.filter(name=change.object_pk).first()
    elif change.object_type == ObjectType.TIMEZERO:
        # queries work b/c # str is Date.isoformat(), the default for models.DateField
        found_objects_to_save = [the_obj for the_obj in objects_to_save
                                 if (type(the_obj) == TimeZero) and (the_obj.timezero_date == change.object_pk)]
        found_object = found_objects_to_save[0] if found_objects_to_save \
            else project.timezeros.filter(timezero_date=change.object_pk).first()
    else:
        raise RuntimeError(f"invalid object_type={change.object_type}")

    if found_object:
        return found_object

    raise RuntimeError(f"could not find object. change={change}")
