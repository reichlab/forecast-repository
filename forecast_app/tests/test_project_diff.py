import copy
import datetime
import json
import logging
from pathlib import Path

from django.test import TestCase
from rest_framework.test import APIRequestFactory

from forecast_app.tests.test_scores import _update_scores_for_all_projects
from utils.make_minimal_projects import create_docs_project
from utils.project import config_dict_from_project
from utils.project_diff import project_config_diff, Change, order_project_config_diff, execute_project_config_diff, \
    database_changes_for_project_config_diff, ObjectType, ChangeType
from utils.utilities import YYYY_MM_DD_DATE_FORMAT, get_or_create_super_po_mo_users


logging.getLogger().setLevel(logging.ERROR)


class ProjectDiffTestCase(TestCase):
    """
    """


    def test_project_config_diff(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_docs_project(po_user)  # docs-project.json, docs-ground-truth.csv, docs-predictions.json
        # note: using APIRequestFactory was the only way I could find to pass a request object. o/w you get:
        #   AssertionError: `HyperlinkedIdentityField` requires the request in the serializer context.
        request = APIRequestFactory().request()
        current_config_dict = config_dict_from_project(project, request)

        # project fields: edit
        edit_config_dict = copy.deepcopy(current_config_dict)
        edit_config_dict['name'] = 'new name'
        self.assertEqual([Change(ObjectType.PROJECT, None, ChangeType.FIELD_EDITED, 'name', edit_config_dict)],
                         project_config_diff(current_config_dict, edit_config_dict))

        # project fields: edit
        fields_new_values = [('name', 'new name'), ('is_public', False), ('description', 'new descr'),
                             ('home_url', 'new home_url'), ('logo_url', 'new logo_url'),
                             ('core_data', 'new core_data'), ('time_interval_type', 'Biweek'),
                             ('visualization_y_label', 'new visualization_y_label')]
        edit_config_dict = copy.deepcopy(current_config_dict)
        for field_name, new_value in fields_new_values:
            edit_config_dict[field_name] = new_value
        exp_changes = [Change(ObjectType.PROJECT, None, ChangeType.FIELD_EDITED, field_name, edit_config_dict) for
                       field_name, new_value in fields_new_values]
        act_changes = project_config_diff(current_config_dict, edit_config_dict)
        self.assertEqual(sorted(exp_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)),
                         sorted(act_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)))

        # project units: remove 'unit3', add 'unit4'
        edit_config_dict = copy.deepcopy(current_config_dict)
        edit_config_dict['units'][2]['name'] = 'location4'  # 'unit3'
        exp_changes = [Change(ObjectType.UNIT, 'location3', ChangeType.OBJ_REMOVED, None, None),
                       Change(ObjectType.UNIT, 'location4', ChangeType.OBJ_ADDED, None,
                              edit_config_dict['units'][2])]
        act_changes = project_config_diff(current_config_dict, edit_config_dict)
        self.assertEqual(sorted(exp_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)),
                         sorted(act_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)))

        # project timezeros: remove '2011-10-02', add '2011-10-22', edit '2011-10-09' fields
        edit_config_dict = copy.deepcopy(current_config_dict)
        edit_config_dict['timezeros'][0]['timezero_date'] = '2011-10-22'  # was '2011-10-02'
        edit_config_dict['timezeros'][1]['data_version_date'] = '2011-10-19'  # '2011-10-09'
        edit_config_dict['timezeros'][1]['is_season_start'] = True  # false
        edit_config_dict['timezeros'][1]['season_name'] = 'season name'  # null
        exp_changes = [Change(ObjectType.TIMEZERO, '2011-10-02', ChangeType.OBJ_REMOVED, None, None),
                       Change(ObjectType.TIMEZERO, '2011-10-22', ChangeType.OBJ_ADDED, None,
                              edit_config_dict['timezeros'][0]),
                       Change(ObjectType.TIMEZERO, '2011-10-09', ChangeType.FIELD_EDITED, 'data_version_date',
                              edit_config_dict['timezeros'][1]),
                       Change(ObjectType.TIMEZERO, '2011-10-09', ChangeType.FIELD_EDITED, 'is_season_start',
                              edit_config_dict['timezeros'][1]),
                       Change(ObjectType.TIMEZERO, '2011-10-09', ChangeType.FIELD_ADDED, 'season_name',
                              edit_config_dict['timezeros'][1])]
        act_changes = project_config_diff(current_config_dict, edit_config_dict)
        self.assertEqual(sorted(exp_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)),
                         sorted(act_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)))

        # project targets: remove 'pct next week', add 'pct next week 2', edit 'cases next week' and 'Season peak week'
        # fields
        edit_config_dict = copy.deepcopy(current_config_dict)
        edit_config_dict['targets'][0]['name'] = 'pct next week 2'  # was 'pct next week'
        edit_config_dict['targets'][1]['description'] = 'new descr'  # 'cases next week'
        edit_config_dict['targets'][1]['is_step_ahead'] = False
        del (edit_config_dict['targets'][1]['step_ahead_increment'])
        edit_config_dict['targets'][4]['description'] = 'new descr 2'  # 'Season peak week'
        edit_config_dict['targets'][4]['is_step_ahead'] = True
        edit_config_dict['targets'][4]['step_ahead_increment'] = 2
        edit_config_dict['targets'][4]['unit'] = 'biweek'
        exp_changes = [Change(ObjectType.TARGET, 'pct next week', ChangeType.OBJ_REMOVED, None, None),
                       Change(ObjectType.TARGET, 'pct next week 2', ChangeType.OBJ_ADDED, None,
                              edit_config_dict['targets'][0]),
                       Change(ObjectType.TARGET, 'cases next week', ChangeType.FIELD_REMOVED, 'step_ahead_increment',
                              None),
                       Change(ObjectType.TARGET, 'cases next week', ChangeType.FIELD_EDITED, 'description',
                              edit_config_dict['targets'][1]),
                       Change(ObjectType.TARGET, 'cases next week', ChangeType.FIELD_EDITED, 'is_step_ahead',
                              edit_config_dict['targets'][1]),
                       Change(ObjectType.TARGET, 'Season peak week', ChangeType.FIELD_ADDED, 'step_ahead_increment',
                              edit_config_dict['targets'][4]),
                       Change(ObjectType.TARGET, 'Season peak week', ChangeType.FIELD_EDITED, 'description',
                              edit_config_dict['targets'][4]),
                       Change(ObjectType.TARGET, 'Season peak week', ChangeType.FIELD_EDITED, 'is_step_ahead',
                              edit_config_dict['targets'][4]),
                       Change(ObjectType.TARGET, 'Season peak week', ChangeType.FIELD_EDITED, 'unit',
                              edit_config_dict['targets'][4])]
        act_changes = project_config_diff(current_config_dict, edit_config_dict)
        self.assertEqual(sorted(exp_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)),
                         sorted(act_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)))

        # project targets: edit 'pct next week' 'type' (non-editable) and 'description' (editable) fields
        edit_config_dict = copy.deepcopy(current_config_dict)
        edit_config_dict['targets'][0]['type'] = 'discrete'  # 'pct next week'
        edit_config_dict['targets'][0]['description'] = 'new descr'
        exp_changes = [Change(ObjectType.TARGET, 'pct next week', ChangeType.OBJ_REMOVED, None, None),
                       Change(ObjectType.TARGET, 'pct next week', ChangeType.OBJ_ADDED, None,
                              edit_config_dict['targets'][0]),
                       Change(ObjectType.TARGET, 'pct next week', ChangeType.FIELD_EDITED, 'description',
                              edit_config_dict['targets'][0])]
        act_changes = project_config_diff(current_config_dict, edit_config_dict)
        self.assertEqual(sorted(exp_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)),
                         sorted(act_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)))

        # project targets: edit 'cases next week': remove 'range' (non-editable)
        edit_config_dict = copy.deepcopy(current_config_dict)
        del (edit_config_dict['targets'][1]['range'])  # 'cases next week
        exp_changes = [Change(ObjectType.TARGET, 'cases next week', ChangeType.OBJ_REMOVED, None, None),
                       Change(ObjectType.TARGET, 'cases next week', ChangeType.OBJ_ADDED, None,
                              edit_config_dict['targets'][1])]
        act_changes = project_config_diff(current_config_dict, edit_config_dict)
        self.assertEqual(sorted(exp_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)),
                         sorted(act_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)))

        # project targets: edit 'season severity': edit 'cats' (non-editable)
        edit_config_dict = copy.deepcopy(current_config_dict)
        edit_config_dict['targets'][2]['cats'] = edit_config_dict['targets'][2]['cats'] + ['cat 2']  # 'season severity'
        exp_changes = [Change(ObjectType.TARGET, 'season severity', ChangeType.OBJ_REMOVED, None, None),
                       Change(ObjectType.TARGET, 'season severity', ChangeType.OBJ_ADDED, None,
                              edit_config_dict['targets'][2])]
        act_changes = project_config_diff(current_config_dict, edit_config_dict)
        self.assertEqual(sorted(exp_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)),
                         sorted(act_changes, key=lambda _: (_.object_type, _.object_pk, _.change_type)))


    def test_order_project_config_diff(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_docs_project(po_user)  # docs-project.json, docs-ground-truth.csv, docs-predictions.json
        _update_scores_for_all_projects()

        # note: using APIRequestFactory was the only way I could find to pass a request object. o/w you get:
        #   AssertionError: `HyperlinkedIdentityField` requires the request in the serializer context.
        request = APIRequestFactory().request()
        out_config_dict = config_dict_from_project(project, request)
        edit_config_dict = copy.deepcopy(out_config_dict)
        _make_some_changes(edit_config_dict)
        changes = project_config_diff(out_config_dict, edit_config_dict)
        # removes one wasted activity ('pct next week', ChangeType.FIELD_EDITED) that is wasted b/c that target is being
        # ChangeType.OBJ_REMOVED:
        ordered_changes = order_project_config_diff(changes)
        self.assertEqual(13, len(changes))  # contains two duplicate and one wasted change
        self.assertEqual(10, len(ordered_changes))


    def test_database_changes_for_project_config_diff(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_docs_project(po_user)  # docs-project.json, docs-ground-truth.csv, docs-predictions.json
        _update_scores_for_all_projects()

        # note: using APIRequestFactory was the only way I could find to pass a request object. o/w you get:
        #   AssertionError: `HyperlinkedIdentityField` requires the request in the serializer context.
        request = APIRequestFactory().request()
        out_config_dict = config_dict_from_project(project, request)
        edit_config_dict = copy.deepcopy(out_config_dict)
        _make_some_changes(edit_config_dict)

        changes = project_config_diff(out_config_dict, edit_config_dict)
        self.assertEqual(  # change, num_points, num_named, num_bins, num_samples, num_truth
            [(Change(ObjectType.UNIT, 'location3', ChangeType.OBJ_REMOVED, None, None), 3, 0, 2, 10, 0),
             (Change(ObjectType.TARGET, 'pct next week', ChangeType.OBJ_REMOVED, None, None), 3, 1, 3, 5, 3),
             (Change(ObjectType.TIMEZERO, '2011-10-02', ChangeType.OBJ_REMOVED, None, None), 11, 2, 16, 23, 5)],
            database_changes_for_project_config_diff(project, changes))


    def test_execute_project_config_diff(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_docs_project(po_user)  # docs-project.json, docs-ground-truth.csv, docs-predictions.json
        _update_scores_for_all_projects()

        # make some changes
        # note: using APIRequestFactory was the only way I could find to pass a request object. o/w you get:
        #   AssertionError: `HyperlinkedIdentityField` requires the request in the serializer context.
        request = APIRequestFactory().request()
        out_config_dict = config_dict_from_project(project, request)
        edit_config_dict = copy.deepcopy(out_config_dict)
        _make_some_changes(edit_config_dict)

        changes = project_config_diff(out_config_dict, edit_config_dict)
        execute_project_config_diff(project, changes)
        self._do_make_some_changes_tests(project)


    def test_diff_from_file(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_docs_project(po_user)  # docs-project.json, docs-ground-truth.csv, docs-predictions.json
        # note: using APIRequestFactory was the only way I could find to pass a request object. o/w you get:
        #   AssertionError: `HyperlinkedIdentityField` requires the request in the serializer context.
        request = APIRequestFactory().request()
        out_config_dict = config_dict_from_project(project, request)

        # this json file makes the same changes as _make_some_changes():
        with open(Path('forecast_app/tests/project_diff/docs-project-edited.json')) as fp:
            edited_config_dict = json.load(fp)
        changes = project_config_diff(out_config_dict, edited_config_dict)

        # # print a little report
        # print(f"* Analyzed {len(changes)} changes. Results:")
        # for change, num_points, num_named, num_bins, num_samples, num_truth in \
        #         database_changes_for_project_config_diff(project, changes):
        #     print(f"- {change.change_type.name} on {change.object_type.name} {change.object_pk!r} will delete:\n"
        #           f"  = {num_points} point predictions\n"
        #           f"  = {num_named} named predictions\n"
        #           f"  = {num_bins} bin predictions\n"
        #           f"  = {num_samples} samples\n"
        #           f"  = {num_truth} truth rows")

        # same tests as test_execute_project_config_diff():
        execute_project_config_diff(project, changes)
        self._do_make_some_changes_tests(project)


    def test_serialize_change_list(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_docs_project(po_user)  # docs-project.json, docs-ground-truth.csv, docs-predictions.json
        _update_scores_for_all_projects()

        # make some changes
        # note: using APIRequestFactory was the only way I could find to pass a request object. o/w you get:
        #   AssertionError: `HyperlinkedIdentityField` requires the request in the serializer context.
        request = APIRequestFactory().request()
        out_config_dict = config_dict_from_project(project, request)
        edit_config_dict = copy.deepcopy(out_config_dict)
        _make_some_changes(edit_config_dict)

        # test round-trip for one Change
        changes = sorted(project_config_diff(out_config_dict, edit_config_dict),
                         key=lambda _: (_.object_type, _.object_pk, _.change_type))
        exp_dict = {'object_type': ObjectType.PROJECT,
                    'object_pk': None,
                    'change_type': ChangeType.FIELD_EDITED,
                    'field_name': 'name',
                    'object_dict': edit_config_dict}
        act_dict = changes[0].serialize_to_dict()
        self.assertEqual(exp_dict, act_dict)
        self.assertEqual(changes[0], Change.deserialize_dict(exp_dict))

        # test serialize_to_dict() for all changes
        exp_dicts = [
            {'object_type': ObjectType.PROJECT, 'object_pk': None, 'change_type': ChangeType.FIELD_EDITED,
             'field_name': 'name', 'object_dict': edit_config_dict},
            {'object_type': ObjectType.UNIT, 'object_pk': 'location3', 'change_type': ChangeType.OBJ_REMOVED,
             'field_name': None, 'object_dict': None},
            {'object_type': ObjectType.UNIT, 'object_pk': 'location4', 'change_type': ChangeType.OBJ_ADDED,
             'field_name': None, 'object_dict': {'name': 'location4'}},
            {'object_type': ObjectType.TARGET, 'object_pk': 'cases next week', 'change_type': ChangeType.FIELD_EDITED,
             'field_name': 'is_step_ahead',
             'object_dict': {'name': 'cases next week', 'type': 'discrete',
                             'description': 'A forecasted integer number of cases for a future week.',
                             'is_step_ahead': False, 'unit': 'cases', 'range': [0, 100000], 'cats': [0, 2, 50]}},
            {'object_type': ObjectType.TARGET, 'object_pk': 'cases next week', 'change_type': ChangeType.FIELD_REMOVED,
             'field_name': 'step_ahead_increment', 'object_dict': None},
            {'object_type': ObjectType.TARGET, 'object_pk': 'pct next week', 'change_type': ChangeType.OBJ_ADDED,
             'field_name': None,
             'object_dict': {'name': 'pct next week', 'type': 'discrete', 'description': 'new descr',
                             'is_step_ahead': True, 'step_ahead_increment': 1, 'unit': 'percent', 'range': [0, 100],
                             'cats': [0, 1, 1, 2, 2, 3, 3, 5, 10, 50]}},
            {'object_type': ObjectType.TARGET, 'object_pk': 'pct next week', 'change_type': ChangeType.OBJ_ADDED,
             'field_name': None,
             'object_dict': {'type': 'discrete', 'name': 'pct next week', 'description': 'new descr',
                             'is_step_ahead': True, 'step_ahead_increment': 1, 'unit': 'percent', 'range': [0, 100],
                             'cats': [0, 1, 1, 2, 2, 3, 3, 5, 10, 50]}},
            {'object_type': ObjectType.TARGET, 'object_pk': 'pct next week', 'change_type': ChangeType.OBJ_REMOVED,
             'field_name': None, 'object_dict': None},
            {'object_type': ObjectType.TARGET, 'object_pk': 'pct next week', 'change_type': ChangeType.OBJ_REMOVED,
             'field_name': None, 'object_dict': None},
            {'object_type': ObjectType.TARGET, 'object_pk': 'pct next week', 'change_type': ChangeType.FIELD_EDITED,
             'field_name': 'description',
             'object_dict': {'name': 'pct next week', 'type': 'discrete', 'description': 'new descr',
                             'is_step_ahead': True, 'step_ahead_increment': 1, 'unit': 'percent', 'range': [0, 100],
                             'cats': [0, 1, 1, 2, 2, 3, 3, 5, 10, 50]}},
            {'object_type': ObjectType.TIMEZERO, 'object_pk': '2011-10-02', 'change_type': ChangeType.OBJ_REMOVED,
             'field_name': None, 'object_dict': None},
            {'object_type': ObjectType.TIMEZERO, 'object_pk': '2011-10-09', 'change_type': ChangeType.FIELD_EDITED,
             'field_name': 'data_version_date',
             'object_dict': {'timezero_date': '2011-10-09', 'data_version_date': '2011-10-19',
                             'is_season_start': False}},
            {'object_type': ObjectType.TIMEZERO, 'object_pk': '2011-10-22', 'change_type': ChangeType.OBJ_ADDED,
             'field_name': None,
             'object_dict': {'timezero_date': '2011-10-22', 'data_version_date': None, 'is_season_start': True,
                             'season_name': '2011-2012'}}
        ]
        act_dicts = [change.serialize_to_dict() for change in changes]
        for act_dict in act_dicts:  # remove 'id' and 'url' fields from TargetSerializer to ease testing
            if act_dict['object_dict']:
                if 'id' in act_dict['object_dict']:  # deleted in previous iteration?
                    del act_dict['object_dict']['id']
                    del act_dict['object_dict']['url']
        self.assertEqual(exp_dicts, act_dicts)

        # test round-trip for all changes
        for change in changes:
            serialized_change_dict = change.serialize_to_dict()
            deserialized_change = Change.deserialize_dict(serialized_change_dict)
            self.assertEqual(change, deserialized_change)


    def _do_make_some_changes_tests(self, project):
        # Change(ObjectType.PROJECT, None, ChangeType.FIELD_EDITED, 'name', {'name': 'new project name', ...}]})
        self.assertEqual('new project name', project.name)

        # Change(ObjectType.UNIT, 'unit3', ChangeType.OBJ_REMOVED, None, None)
        self.assertEqual(0, project.units.filter(name='location3').count())

        # Change(ObjectType.UNIT, 'unit4', ChangeType.OBJ_ADDED, None, {'name': 'unit4'})
        self.assertEqual(1, project.units.filter(name='location4').count())

        # Change(ObjectType.TIMEZERO, '2011-10-02', ChangeType.OBJ_REMOVED, None, None)
        # NB: queries work b/c # str is Date.isoformat(), the default for models.DateField
        self.assertEqual(0, project.timezeros.filter(timezero_date='2011-10-02').count())

        #  Change(ObjectType.TIMEZERO, '2011-10-22', ChangeType.OBJ_ADDED, None, {'timezero_date': '2011-10-22', ...})
        self.assertEqual(1, project.timezeros.filter(timezero_date='2011-10-22').count())

        # Change(ObjectType.TIMEZERO, '2011-10-09', ChangeType.FIELD_EDITED, 'data_version_date', {'timezero_date': '2011-10-09', ...})
        self.assertEqual(datetime.datetime.strptime('2011-10-19', YYYY_MM_DD_DATE_FORMAT).date(),
                         project.timezeros.filter(timezero_date='2011-10-09').first().data_version_date)

        # Change(ObjectType.TARGET, 'pct next week', ChangeType.FIELD_EDITED, 'description', {'type': 'discrete', 'name': 'pct next week', ...})
        # not tested for b/c wasted -> removed

        #  Change(ObjectType.TARGET, 'pct next week', ChangeType.OBJ_REMOVED, None, None)
        #  Change(ObjectType.TARGET, 'pct next week', ChangeType.OBJ_ADDED, None, {'type': 'discrete', 'name': 'pct next week', ...})
        self.assertEqual(1, project.targets.filter(name='pct next week').count())

        # Change(ObjectType.TARGET, 'cases next week', ChangeType.FIELD_EDITED, 'is_step_ahead', {'type': 'discrete', 'name': 'cases next week', ...})
        self.assertFalse(project.targets.filter(name='cases next week').first().is_step_ahead)

        # Change(ObjectType.TARGET, 'cases next week', ChangeType.FIELD_REMOVED, 'step_ahead_increment', None)
        self.assertIsNone(project.targets.filter(name='cases next week').first().step_ahead_increment)


def _make_some_changes(edit_config_dict):
    # makes a useful variety of changes to edit_config_dict for testing
    edit_config_dict['name'] = 'new project name'  # edit project 'name'
    edit_config_dict['units'][2]['name'] = 'location4'  # 'location3': remove and replace w/'location4'
    edit_config_dict['timezeros'][0]['timezero_date'] = '2011-10-22'  # '2011-10-02': remove and replace w/'2011-10-22'
    edit_config_dict['timezeros'][1]['data_version_date'] = '2011-10-19'  # '2011-10-09': edit 'data_version_date'
    edit_config_dict['targets'][0]['type'] = 'discrete'  # 'pct next week': remove 'pct next week' and add back in
    edit_config_dict['targets'][0]['range'] = [int(_) for _ in
                                               edit_config_dict['targets'][0]['range']]  # o/w type mismatch
    edit_config_dict['targets'][0]['cats'] = [int(_) for _ in edit_config_dict['targets'][0]['cats']]  # ""
    edit_config_dict['targets'][0]['description'] = 'new descr'  # edit 'description' on removed object
    edit_config_dict['targets'][1]['is_step_ahead'] = False  # 'cases next week': edit 'is_step_ahead'
    del (edit_config_dict['targets'][1]['step_ahead_increment'])  # delete 'step_ahead_increment'

    # resulting Changes. notes:
    # - 'pct next week': duplicate OBJ_REMOVED and OBJ_ADDED
    # - 'pct next week': wasted FIELD_EDITED and OBJ_REMOVED
    #
    # [Change(ObjectType.PROJECT,  None,              ChangeType.FIELD_EDITED,  'name',                 {'name': 'new project name', ...}]}),
    #  Change(ObjectType.UNIT, 'unit3',       ChangeType.OBJ_REMOVED,    None,                  None),
    #  Change(ObjectType.UNIT, 'unit4',       ChangeType.OBJ_ADDED,      None,                  {'name': 'unit4'}),
    #  Change(ObjectType.TIMEZERO, '2011-10-02',      ChangeType.OBJ_REMOVED,    None,                  None),
    #  Change(ObjectType.TIMEZERO, '2011-10-22',      ChangeType.OBJ_ADDED,      None,                  {'timezero_date': '2011-10-22', ...}),
    #  Change(ObjectType.TIMEZERO, '2011-10-09',      ChangeType.FIELD_EDITED,  'data_version_date',    {'timezero_date': '2011-10-09', ...}),
    #  Change(ObjectType.TARGET,   'pct next week',   ChangeType.FIELD_EDITED,  'description',          {'type': 'discrete', 'name': 'pct next week', ...}),
    #  Change(ObjectType.TARGET,   'pct next week',   ChangeType.OBJ_REMOVED,    None,                  None),
    #  Change(ObjectType.TARGET,   'pct next week',   ChangeType.OBJ_ADDED,      None,                  {'type': 'discrete', 'name': 'pct next week', ...}),
    #  Change(ObjectType.TARGET,   'pct next week',   ChangeType.OBJ_REMOVED,    None,                  None),
    #  Change(ObjectType.TARGET,   'pct next week',   ChangeType.OBJ_ADDED,      None,                  {'type': 'discrete', 'name': 'pct next week', ...}),
    #  Change(ObjectType.TARGET,   'cases next week', ChangeType.FIELD_EDITED,  'is_step_ahead',        {'type': 'discrete', 'name': 'cases next week', ...}),
    #  Change(ObjectType.TARGET,   'cases next week', ChangeType.FIELD_REMOVED, 'step_ahead_increment', None)]
