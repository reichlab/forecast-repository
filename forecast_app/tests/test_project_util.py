import datetime
import json
import logging
from pathlib import Path

from django.db import connection
from django.test import TestCase
from rest_framework.test import APIRequestFactory

from forecast_app.models import Project, Target, ForecastModel, TimeZero, Forecast
from utils.forecast import load_predictions_from_json_io_dict, cache_forecast_metadata
from utils.make_minimal_projects import _make_docs_project
from utils.project import create_project_from_json, config_dict_from_project, _target_dict_for_target, group_targets, \
    unit_rows_for_project, models_summary_table_rows_for_project, target_rows_for_project, targets_for_group_name
from utils.utilities import get_or_create_super_po_mo_users


logging.getLogger().setLevel(logging.ERROR)


class ProjectUtilTestCase(TestCase):
    """
    """


    def test_config_dict_from_project(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            input_project_dict = json.load(fp)
        project = create_project_from_json(input_project_dict, po_user)
        output_project_config = config_dict_from_project(project, APIRequestFactory().request())

        # remove 'id' and 'url' fields from TargetSerializer to ease testing:
        for target_dict in output_project_config['targets']:
            del target_dict['id']
            del target_dict['url']
        for target_dict in output_project_config['timezeros']:  # "" TimeZeroSerializer
            del target_dict['id']
            del target_dict['url']
        for target_dict in output_project_config['units']:  # "" UnitSerializer
            del target_dict['id']
            del target_dict['url']

        # account for non-determinism of output
        input_project_dict['units'].sort(key=lambda _: _['name'])
        input_project_dict['targets'].sort(key=lambda _: _['name'])
        input_project_dict['timezeros'].sort(key=lambda _: _['timezero_date'])
        output_project_config['units'].sort(key=lambda _: _['name'])
        output_project_config['targets'].sort(key=lambda _: _['name'])
        output_project_config['timezeros'].sort(key=lambda _: _['timezero_date'])

        self.assertEqual(input_project_dict, output_project_config)


    def test_create_project_from_json(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            timezero_config = {'timezero_date': '2017-12-01',
                               'data_version_date': None,
                               'is_season_start': True,
                               'season_name': 'tis the season'}
            project_dict['timezeros'] = [timezero_config]
        project = create_project_from_json(project_dict, po_user)

        # spot-check some fields
        self.assertEqual(po_user, project.owner)
        self.assertTrue(project.is_public)
        self.assertEqual('CDC Flu challenge', project.name)
        self.assertEqual(Project.WEEK_TIME_INTERVAL_TYPE, project.time_interval_type)
        self.assertEqual('Weighted ILI (%)', project.visualization_y_label)

        self.assertEqual(11, project.units.count())
        self.assertEqual(7, project.targets.count())

        # spot-check a Unit
        unit = project.units.filter(name='US National').first()
        self.assertIsNotNone(unit)

        # spot-check a Target
        target = project.targets.filter(name='1 wk ahead').first()
        self.assertEqual(Target.CONTINUOUS_TARGET_TYPE, target.type)
        self.assertEqual('ILI percent', target.outcome_variable)
        self.assertTrue(target.is_step_ahead)
        self.assertEqual(1, target.numeric_horizon)

        # check the TimeZero
        time_zero = project.timezeros.first()
        self.assertIsNotNone(time_zero)
        self.assertEqual(datetime.date(2017, 12, 1), time_zero.timezero_date)
        self.assertIsNone(time_zero.data_version_date)
        self.assertEqual(timezero_config['is_season_start'], time_zero.is_season_start)
        self.assertEqual(timezero_config['season_name'], time_zero.season_name)

        # test existing project
        project.delete()
        with open('forecast_app/tests/projects/cdc-project.json') as fp:
            cdc_project_json = json.load(fp)

        create_project_from_json(Path('forecast_app/tests/projects/cdc-project.json'), po_user)
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(cdc_project_json, po_user)
        self.assertIn("found existing project", str(context.exception))


    def test_create_project_from_json_bad_proj_config_file_path_or_dict_arg(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            timezero_config = {'timezero_date': '2017-12-01',
                               'data_version_date': None,
                               'is_season_start': True,
                               'season_name': 'tis the season'}
            project_dict['timezeros'] = [timezero_config]

        # note: blue sky args (dict or Path) are checked elsewhere
        bad_arg_exp_error = [([1, 2], 'proj_config_file_path_or_dict was neither a dict nor a Path'),
                             ('hi there', 'proj_config_file_path_or_dict was neither a dict nor a Path'),
                             (Path('forecast_app/tests/truth_data/truths-ok.csv'), 'error loading json file')]
        for bad_arg, exp_error in bad_arg_exp_error:
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(bad_arg, po_user)
            self.assertIn(exp_error, str(context.exception))


    def test_create_project_from_json_project_validation(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)

        # note: owner permissions tested by test_views_and_rest_api.py

        # test top level required fields: missing or wrong type
        for field_name in ['name', 'is_public', 'description', 'home_url', 'core_data', 'time_interval_type',
                           'visualization_y_label', 'units', 'targets', 'timezeros']:
            orig_field_value = project_dict[field_name]
            with self.assertRaisesRegex(RuntimeError, "Wrong keys in project_dict"):
                del (project_dict[field_name])
                create_project_from_json(project_dict, po_user)

            project_dict[field_name] = {}  # dict - not a str, boolean, or list
            with self.assertRaisesRegex(RuntimeError, "top level field type was not"):
                create_project_from_json(project_dict, po_user)

            project_dict[field_name] = orig_field_value  # reset to valid

        # test units
        project_dict['units'] = [{}]  # no 'name' or 'abbreviation'
        with self.assertRaisesRegex(RuntimeError, "unit_dict had no 'name' field"):
            create_project_from_json(project_dict, po_user)

        project_dict['units'] = [{'name': 'a name'}]  # no 'abbreviation'
        with self.assertRaisesRegex(RuntimeError, "unit_dict had no 'abbreviation' field"):
            create_project_from_json(project_dict, po_user)

        project_dict['units'] = [{'name': [], 'abbreviation': []}]  # 'name' and 'abbreviation' not a str
        with self.assertRaisesRegex(RuntimeError, "invalid unit name"):
            create_project_from_json(project_dict, po_user)

        project_dict['units'] = [{'name': 'a name', 'abbreviation': []}]  # 'abbreviation' not a str
        with self.assertRaisesRegex(RuntimeError, "invalid unit abbreviation"):
            create_project_from_json(project_dict, po_user)

        # note: targets tested in test_create_project_from_json_target_required_fields() and
        # test_create_project_from_json_target_optional_fields()

        # test timezero fields: missing or wrong type
        project_dict['units'] = [{"name": "HHS Region 1", 'abbreviation': "HHS Region 1"}]  # reset to valid
        timezero_config = {'timezero_date': '2017-12-01',
                           'data_version_date': None,
                           'is_season_start': False}
        project_dict['timezeros'] = [timezero_config]
        for field_name in ['timezero_date', 'data_version_date', 'is_season_start']:  # required fields
            orig_field_value = timezero_config[field_name]
            with self.assertRaisesRegex(RuntimeError, "Wrong keys in 'timezero_config"):
                del (timezero_config[field_name])
                create_project_from_json(project_dict, po_user)

            timezero_config[field_name] = {}  # dict - not a date str or boolean
            with self.assertRaisesRegex(RuntimeError, "invalid field"):
                create_project_from_json(project_dict, po_user)

            timezero_config[field_name] = orig_field_value  # reset to valid

        # test optional 'season_name' field
        timezero_config['is_season_start'] = True
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("season_name not found but is required when 'is_season_start'", str(context.exception))

        timezero_config['season_name'] = {}  # dict - not a string
        with self.assertRaisesRegex(RuntimeError, "invalid field"):
            create_project_from_json(project_dict, po_user)

        timezero_config['season_name'] = 'tis the season'  # reset to valid

        # test time_interval_type
        project_time_interval_type = project_dict['time_interval_type']
        project_dict['time_interval_type'] = "not 'week', 'biweek', or 'month'"
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("invalid 'time_interval_type'", str(context.exception))
        project_dict['time_interval_type'] = project_time_interval_type  # reset to valid

        # test existing project
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
            create_project_from_json(project_dict, po_user)
        self.assertIn('found existing project', str(context.exception))


    def test_create_project_from_json_target_types(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)

        # test valid types. no 'type':
        minimal_target_dict = {'name': 'n', 'description': 'd', 'outcome_variable': 'v', 'is_step_ahead': False}
        target_type_int_to_required_keys_and_values = {
            Target.CONTINUOUS_TARGET_TYPE: [('outcome_variable', 'month')],  # 'range' optional
            Target.DISCRETE_TARGET_TYPE: [('outcome_variable', 'month')],  # 'range' optional
            Target.NOMINAL_TARGET_TYPE: [('cats', ['a', 'b'])],
            Target.BINARY_TARGET_TYPE: [],  # binary has no required keys
            Target.DATE_TARGET_TYPE: [('outcome_variable', 'month'), ('cats', ['2019-12-15', '2019-12-22'])]}
        type_int_to_name = {type_int: type_name for type_int, type_name in Target.TYPE_CHOICES}
        for type_int, required_keys_and_values in target_type_int_to_required_keys_and_values.items():
            test_target_dict = dict(minimal_target_dict)  # shallow copy
            project_dict['targets'] = [test_target_dict]
            test_target_dict['type'] = type_int_to_name[type_int]
            for required_key, value in required_keys_and_values:
                test_target_dict[required_key] = value

            project = create_project_from_json(project_dict, po_user)
            project.delete()

        # test invalid type
        Project.objects.filter(name=project_dict['name']).delete()
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict['type'] = 'invalid type'
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("Invalid type_name", str(context.exception))


    def test_create_project_from_json_target_required_fields(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]

        # test required fields: missing or wrong type. optional keys are tested below
        for field_name in ['name', 'description', 'type', 'is_step_ahead']:  # required
            orig_field_value = first_target_dict[field_name]
            with self.assertRaisesRegex(RuntimeError, "Wrong required keys in target_dict"):
                del (first_target_dict[field_name])
                create_project_from_json(project_dict, po_user)

            first_target_dict[field_name] = {}  # dict - not a str or boolean
            with self.assertRaisesRegex(RuntimeError, "field type was not"):
                create_project_from_json(project_dict, po_user)

            first_target_dict[field_name] = orig_field_value  # reset to valid


    def test_create_project_from_json_target_optional_fields(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]

        # test optional 'numeric_horizon': required only if 'is_step_ahead'
        first_target_dict['is_step_ahead'] = True  # was False w/no 'numeric_horizon'
        with self.assertRaisesRegex(RuntimeError, "`numeric_horizon` or `reference_date_type` not found but is required"
                                                  " when `is_step_ahead` is passed"):
            create_project_from_json(project_dict, po_user)

        # test optional fields, based on type:
        # 1) test optional 'range'. three cases a-c follow
        # 1a) required but not passed: []: no need to validate
        # 1b) optional: ok to pass or not pass: ['continuous', 'discrete']: no need to validate

        # 2c) invalid but passed: ['nominal', 'binary', 'date']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict['range'] = [1, 2]
        for target_type in ['nominal', 'binary', 'date']:
            first_target_dict['type'] = target_type
            first_target_dict['outcome_variable'] = 'biweek'
            with self.assertRaisesRegex(RuntimeError, "'range' passed but is invalid for type_name"):
                create_project_from_json(project_dict, po_user)

        # 2) test optional 'cats'. three cases a-c follow
        # 2a) required but not passed: ['nominal', 'date']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict.pop('cats', None)
        first_target_dict.pop('outcome_variable', None)
        for target_type in ['nominal', 'date']:
            first_target_dict['type'] = target_type
            first_target_dict['outcome_variable'] = 'biweek'
            with self.assertRaisesRegex(RuntimeError, "'cats' not passed but is required for type_name"):
                create_project_from_json(project_dict, po_user)

        # 2b) optional: ok to pass or not pass: ['continuous', 'discrete']: no need to validate

        # 2c) invalid but passed: ['binary']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict['cats'] = ['a', 'b']
        for target_type in ['binary']:
            first_target_dict['type'] = target_type
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'cats' passed but is invalid for type_name={target_type}", str(context.exception))


    def test_create_project_from_json_duplicate_timezero(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            project_dict['timezeros'].append(project_dict['timezeros'][0])

        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("found existing TimeZero for timezero_date", str(context.exception))


    def test_create_project_from_json_duplicate_unit(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            project_dict['units'].append(project_dict['units'][0])

        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("found existing Unit for name", str(context.exception))


    def test_create_project_from_json_invalid_unit_target_name(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            orig_unit_name = project_dict['units'][0]['name']
            orig_target_name = project_dict['targets'][0]['name']

        bad_names = ('bad\nname', 'bad\tname', 'bad\u00072name')  # last: Bell :-)
        for bad_name in bad_names:
            project_dict['units'][0]['name'] = bad_name
            with self.assertRaisesRegex(RuntimeError, 'invalid unit name'):
                create_project_from_json(project_dict, po_user)
        project_dict['units'][0]['name'] = orig_unit_name

        for bad_name in bad_names:
            project_dict['targets'][0]['name'] = bad_name
            with self.assertRaisesRegex(RuntimeError, 'illegal target name'):
                create_project_from_json(project_dict, po_user)
        project_dict['targets'][0]['name'] = orig_target_name


    def test_create_project_from_json_illegal_cat(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            baseline_target_dict = project_dict['targets'][2]  # "above baseline"

        bad_names = ('bad\nname', 'bad\tname', 'bad\u00072name')  # last: Bell :-)
        for bad_name in bad_names:
            baseline_target_dict['cats'] = bad_name
            with self.assertRaisesRegex(RuntimeError, 'illegal cat value'):
                create_project_from_json(project_dict, po_user)


    def test_create_project_from_json_duplicate_target(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            project_dict['targets'].append(project_dict['targets'][0])

        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("found existing Target for name", str(context.exception))


    def test_create_project_from_json_target_range_format(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            pct_next_week_target_dict = [target_dict for target_dict in project_dict['targets']
                                         if target_dict['name'] == 'pct next week'][0]
            project_dict['targets'] = [pct_next_week_target_dict]

        # loaded range is valid format: test that an error is not raised
        try:
            project = create_project_from_json(project_dict, po_user)
            project.delete()
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        # break range by setting to invalid format: test that an error is raised
        range_list = ["not float", True]  # not floats
        pct_next_week_target_dict['range'] = range_list
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("range type did not match data_type", str(context.exception))

        # test exactly two items
        range_list = [1.0, 2.2, 3.3]  # 3, not 2
        pct_next_week_target_dict['range'] = range_list
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("range did not contain exactly two items", str(context.exception))


    def test_create_project_from_json_target_cats_format(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            pct_next_week_target_dict = [target_dict for target_dict in project_dict['targets']
                                         if target_dict['name'] == 'pct next week'][0]
            project_dict['targets'] = [pct_next_week_target_dict]

        # loaded cats is valid format: test that an error is not raised
        try:
            project = create_project_from_json(project_dict, po_user)
            project.delete()
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        # break cats by setting to invalid format: test that an error is raised
        cats = ["not float", True, {}]  # not floats
        pct_next_week_target_dict['cats'] = cats
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("could not convert cat to data_type. cat_str='not float'", str(context.exception))


    def test_create_project_from_json_target_dates_format(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            season_peak_week_target_dict = [target_dict for target_dict in project_dict['targets']
                                            if target_dict['name'] == 'Season peak week'][0]
            project_dict['targets'] = [season_peak_week_target_dict]

        # loaded dates are in valid 'yyyy-mm-dd' format: test that an error is not raised
        try:
            project = create_project_from_json(project_dict, po_user)
            project.delete()
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        # break dates by setting to invalid 'yyyymmdd' format: test that an error is raised
        season_peak_week_target_dict['cats'] = ['2019-12-15', '20191222']
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("could not convert cat to data_type. cat_str='20191222'", str(context.exception))


    def test_create_project_from_json_cats_lws_ranges_created(self):
        # verify that 'list' TargetCat, TargetLwr, and TargetRange instances are created.
        # docs-project.json contains examples of all five target types
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
        project = create_project_from_json(project_dict, po_user)

        # test 'pct next week' target. continuous, with range and cats (w/lwrs)
        target = project.targets.filter(name='pct next week').first()
        self.assertEqual(Target.CONTINUOUS_TARGET_TYPE, target.type)
        self.assertEqual('percentage positive tests', target.outcome_variable)
        self.assertTrue(target.is_step_ahead)
        self.assertEqual(1, target.numeric_horizon)

        ranges = target.ranges.all().order_by('value_f')
        self.assertEqual(2, len(ranges))
        self.assertEqual([(None, 0.0), (None, 100.0)],
                         list(ranges.values_list('value_i', 'value_f')))

        cats = target.cats.all().order_by('cat_f')
        self.assertEqual(10, len(cats))
        self.assertEqual([(None, 0.0, None, None, None), (None, 1.0, None, None, None), (None, 1.1, None, None, None),
                          (None, 2.0, None, None, None), (None, 2.2, None, None, None), (None, 3.0, None, None, None),
                          (None, 3.3, None, None, None), (None, 5.0, None, None, None), (None, 10.0, None, None, None),
                          (None, 50.0, None, None, None)],
                         list(cats.values_list('cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')))

        lwrs = target.lwrs.all().order_by('lwr', 'upper')
        self.assertEqual(11, len(lwrs))
        self.assertEqual([(0.0, 1.0), (1.0, 1.1), (1.1, 2.0), (2.0, 2.2), (2.2, 3.0), (3.0, 3.3), (3.3, 5.0),
                          (5.0, 10.0), (10.0, 50.0), (50.0, 100.0), (100.0, float('inf'))],
                         list(lwrs.values_list('lwr', 'upper')))

        # test 'cases next week' target. discrete, with range
        target = project.targets.filter(name='cases next week').first()
        ranges = target.ranges.all().order_by('value_i')
        self.assertEqual(2, len(ranges))
        self.assertEqual([(0, None), (100000, None)],
                         list(ranges.values_list('value_i', 'value_f')))

        # test 'season severity' target. nominal, with cats
        target = project.targets.filter(name='season severity').first()
        cats = target.cats.all().order_by('cat_t')
        self.assertEqual(4, len(cats))
        self.assertEqual([(None, None, 'high', None, None),
                          (None, None, 'mild', None, None),
                          (None, None, 'moderate', None, None),
                          (None, None, 'severe', None, None)],
                         list(cats.values_list('cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')))

        # test 'above baseline' target. binary, with two implicit boolean cats created behind-the-scenes
        target = project.targets.filter(name='above baseline').first()
        cats = target.cats.all().order_by('cat_b')
        self.assertEqual(2, len(cats))
        self.assertEqual([(None, None, None, None, False),
                          (None, None, None, None, True)],
                         list(cats.values_list('cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')))

        # test 'Season peak week' target. date, with dates as cats
        target = project.targets.filter(name='Season peak week').first()
        dates = target.cats.all().order_by('cat_d')  # date
        self.assertEqual(4, len(dates))
        self.assertEqual([datetime.date(2019, 12, 15), datetime.date(2019, 12, 22),
                          datetime.date(2019, 12, 29), datetime.date(2020, 1, 5)],
                         list(dates.values_list('cat_d', flat=True)))


    def test_target_round_trip_target_dict(self):
        # test round trip: target_dict -> Target -> target_dict
        # 1. target_dict -> Target
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
        input_target_dicts = project_dict['targets']
        # does _validate_and_create_targets() -> model_init = {...}  # required keys:
        project = create_project_from_json(project_dict, po_user)

        # 2. Target -> target_dict
        # does target_dict() = {...}  # required keys:
        output_target_dicts = [_target_dict_for_target(target, APIRequestFactory().request()) for target in
                               project.targets.all()]

        # 3. they should be equal
        for target_dict in output_target_dicts:  # remove 'id' and 'url' fields from TargetSerializer to ease testing
            del target_dict['id']
            del target_dict['url']
        self.assertEqual(sorted(input_target_dicts, key=lambda _: _['name']),
                         sorted(output_target_dicts, key=lambda _: _['name']))


    def test_group_targets(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)

        # case: target names with numeric_horizon at start of name
        project = create_project_from_json(Path('forecast_app/tests/projects/COVID-19_Forecasts-config.json'), po_user)
        grouped_targets = group_targets(project.targets.all())
        # group 1: "day ahead cumulative deaths"         | 0 day ahead cum death, 1 day ahead cum death, ..., 130 day ahead cum death
        # group 2: "day ahead incident deaths"           | 0, 1, ..., 130
        # group 3: "day ahead incident hospitalizations" | 0, 1, ..., 130
        # group 4: "week ahead cumulative deaths"        | 1 wk ahead cum death, 2 wk ahead cum death, ..., 20 wk ahead cum death
        # group 5: "week ahead incident deaths"          | 1, 2, ..., 20
        # group 6: "week ahead incident cases"           | 1 wk ahead inc case, 2 wk ahead inc case, ..., 8 wk ahead inc case
        self.assertEqual(6, len(grouped_targets))
        self.assertEqual({'week ahead incident deaths', 'day ahead incident hospitalizations',
                          'day ahead cumulative deaths', 'week ahead cumulative deaths', 'week ahead incident cases',
                          'day ahead incident deaths'},
                         set(grouped_targets.keys()))
        self.assertEqual(131, len(grouped_targets['day ahead incident hospitalizations']))
        self.assertEqual(131, len(grouped_targets['day ahead incident deaths']))
        self.assertEqual(131, len(grouped_targets['day ahead cumulative deaths']))
        self.assertEqual(20, len(grouped_targets['week ahead incident deaths']))
        self.assertEqual(20, len(grouped_targets['week ahead cumulative deaths']))
        self.assertEqual(8, len(grouped_targets['week ahead incident cases']))

        # test targets_for_group_name(group_name)
        self.assertEqual(grouped_targets['wk ahead inc case'], targets_for_group_name(project, 'wk ahead inc case'))

        # case: mix of target names with numeric_horizon at start of name, and others
        project = create_project_from_json(Path('forecast_app/tests/projects/cdc-project.json'), po_user)
        grouped_targets = group_targets(project.targets.all())
        # group 1: "Season onset"
        # group 2: "Season peak week"
        # group 3: "Season peak percentage"
        # group 4: "x wk ahead" | 1 wk ahead, 2 wk ahead, 3 wk ahead, 4 wk ahead
        self.assertEqual(4, len(grouped_targets))
        self.assertEqual({'Season onset', 'Season peak week', 'Season peak percentage', 'week ahead ILI percent'},
                         set(grouped_targets.keys()))
        self.assertEqual(4, len(grouped_targets['week ahead ILI percent']))

        # case: target names with numeric_horizon inside the name (i.e., not at start)
        project = Project.objects.create()
        for numeric_horizon in range(2):
            target_init = {'project': project, 'name': f'wk {numeric_horizon} ahead',
                           'type': Target.CONTINUOUS_TARGET_TYPE, 'outcome_variable': 'cases', 'is_step_ahead': True,
                           'numeric_horizon': numeric_horizon,
                           'reference_date_type': Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT}
            Target.objects.create(**target_init)
        grouped_targets = group_targets(project.targets.all())
        # group 1: 'wk x ahead'
        self.assertEqual(1, len(grouped_targets))
        self.assertEqual({'week ahead cases'}, set(grouped_targets.keys()))
        self.assertEqual(2, len(grouped_targets['week ahead cases']))

        # case: targets with no word boundaries
        project = create_project_from_json(Path('forecast_app/tests/projects/thai-project.json'), po_user)
        grouped_targets = group_targets(project.targets.all())
        # group 1: "x_biweek_ahead" | 1_biweek_ahead, 2_biweek_ahead, 3_biweek_ahead, 4_biweek_ahead, 5_biweek_ahead
        self.assertEqual(1, len(grouped_targets))
        self.assertEqual({'biweek ahead cases'}, set(grouped_targets.keys()))
        self.assertEqual(5, len(grouped_targets['biweek ahead cases']))


    def test_models_summary_table_rows_for_project(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)

        # test with just one forecast - oldest and newest forecast is the same. a 7-tuple:
        #   [forecast_model, num_forecasts, oldest_forecast_tz_date, newest_forecast_tz_date, oldest_forecast_id,
        #    newest_forecast_id, newest_forecast_created_at].
        # NB: we have to work around a Django bug where DateField and DateTimeField come out of the database as either
        # datetime.date/datetime.datetime objects (postgres) or strings (sqlite3)
        exp_row = (forecast_model, forecast_model.forecasts.count(),
                   str(time_zero.timezero_date),  # oldest_forecast_tz_date
                   str(time_zero.timezero_date),  # newest_forecast_tz_date
                   forecast.id, forecast.created_at.utctimetuple())  # newest_forecast_created_at
        act_rows = models_summary_table_rows_for_project(project)
        act_rows = [(act_rows[0][0], act_rows[0][1],
                     str(act_rows[0][2]),  # oldest_forecast_tz_date
                     str(act_rows[0][3]),  # newest_forecast_tz_date
                     act_rows[0][4],
                     act_rows[0][5].utctimetuple())]  # newest_forecast_created_at

        sql = f"""SELECT created_at FROM {Forecast._meta.db_table} WHERE id = %s;"""
        with connection.cursor() as cursor:
            cursor.execute(sql, (forecast.pk,))
            rows = cursor.fetchall()

        self.assertEqual([exp_row], act_rows)

        # test a second forecast
        time_zero2 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast2 = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                            time_zero=time_zero2)
        exp_row = (forecast_model, forecast_model.forecasts.count(),
                   str(time_zero.timezero_date),  # oldest_forecast_tz_date
                   str(time_zero2.timezero_date),  # newest_forecast_tz_date
                   forecast2.id, forecast2.created_at.utctimetuple())  # newest_forecast_created_at
        act_rows = models_summary_table_rows_for_project(project)
        act_rows = [(act_rows[0][0], act_rows[0][1],
                     str(act_rows[0][2]),  # oldest_forecast_tz_date
                     str(act_rows[0][3]),  # newest_forecast_tz_date
                     act_rows[0][4],
                     act_rows[0][5].utctimetuple())]  # newest_forecast_created_at
        self.assertEqual([exp_row], act_rows)


    def test_unit_rows_for_project(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        # recall that _make_docs_project() calls cache_forecast_metadata():
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)  # 2011, 10, 2

        # case: one model with one timezero. recall rows:
        # (model, newest_forecast_tz_date, newest_forecast_id,
        #  num_present_unit_names, present_unit_names, missing_unit_names):
        exp_rows = [(forecast_model, str(time_zero.timezero_date), forecast.id, 3, '(all)', '')]
        act_rows = [(row[0], str(row[1]), row[2], row[3], row[4], row[5]) for row in unit_rows_for_project(project)]
        self.assertEqual(exp_rows, act_rows)

        # case: add a second forecast for a newer timezero
        time_zero2 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2011, 10, 3))
        forecast2 = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions-non-dup.json',
                                            time_zero=time_zero2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions-non-dup.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, is_validate_cats=False)
            cache_forecast_metadata(forecast2)  # required by _forecast_ids_to_present_unit_or_target_id_sets()

        exp_rows = [(forecast_model, str(time_zero2.timezero_date), forecast2.id, 3, '(all)', '')]
        act_rows = [(row[0], str(row[1]), row[2], row[3], row[4], row[5]) for row in unit_rows_for_project(project)]
        self.assertEqual(exp_rows, act_rows)

        # case: add a second model with only forecasts for one unit
        forecast_model2 = ForecastModel.objects.create(project=project, name=forecast_model.name + '2',
                                                       abbreviation=forecast_model.abbreviation + '2')
        time_zero3 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2011, 10, 4))
        forecast3 = Forecast.objects.create(forecast_model=forecast_model2, source='docs-predictions.json',
                                            time_zero=time_zero3, notes="a small prediction file")
        json_io_dict = {
            "meta": {},
            "predictions": [{"unit": "loc1",
                             "target": "pct next week",
                             "class": "point",
                             "prediction": {"value": 2.1}}]}
        load_predictions_from_json_io_dict(forecast3, json_io_dict, is_validate_cats=False)
        cache_forecast_metadata(forecast3)  # required by _forecast_ids_to_present_unit_or_target_id_sets()

        exp_rows = [(forecast_model, str(time_zero2.timezero_date), forecast2.id, 3,
                     '(all)', ''),
                    (forecast_model2, str(time_zero3.timezero_date), forecast3.id, 1,
                     'location1', 'location2, location3')]
        act_rows = [(row[0], str(row[1]), row[2], row[3], row[4], row[5]) for row in unit_rows_for_project(project)]
        self.assertEqual(exp_rows, act_rows)

        # case: exposes bug: syntax error when no forecasts in project:
        #   psycopg2.errors.SyntaxError: syntax error at or near ")"
        #   LINE 6:             WHERE f.id IN ()
        forecast.delete()
        forecast2.delete()
        forecast3.delete()
        # (model, newest_forecast_tz_date, newest_forecast_id, num_present_unit_names, present_unit_names,
        #  missing_unit_names):
        exp_rows = [(forecast_model, 'None', None, 0, '', '(all)'),
                    (forecast_model2, 'None', None, 0, '', '(all)')]
        act_rows = [(row[0], str(row[1]), row[2], row[3], row[4], row[5]) for row in unit_rows_for_project(project)]
        self.assertEqual(sorted(exp_rows, key=lambda _: _[0].id), sorted(act_rows, key=lambda _: _[0].id))


    def test_target_rows_for_project(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        # recall that _make_docs_project() calls cache_forecast_metadata():
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)  # 2011, 10, 2

        # case: one model with one timezero that has five groups of one target each.
        # recall: `group_targets(project.targets.all())` (only one target/group in this case):
        #   {'week ahead percentage positive tests': [(1, 'pct next week', 'continuous', 'percentage positive tests', True, 1, 'MMWR_WEEK_LAST_TIMEZERO_MONDAY')],
        #    'week ahead cases':                     [(2, 'cases next week', 'discrete', 'cases', True, 2, 'MMWR_WEEK_LAST_TIMEZERO_MONDAY')],
        #    'season severity':                      [(3, 'season severity', 'nominal', 'season severity', False, None, None)],
        #    'above baseline':                       [(4, 'above baseline', 'binary', 'above baseline', False, None, None)],
        #    'Season peak week':                     [(5, 'Season peak week', 'date', 'season peak week', False, None, None)]})
        exp_rows = [(forecast_model, str(time_zero.timezero_date), forecast.id, 'Season peak week', 1),
                    (forecast_model, str(time_zero.timezero_date), forecast.id, 'above baseline', 1),
                    (forecast_model, str(time_zero.timezero_date), forecast.id, 'season severity', 1),
                    (forecast_model, str(time_zero.timezero_date), forecast.id, 'week ahead cases', 1),
                    (forecast_model, str(time_zero.timezero_date), forecast.id,
                     'week ahead percentage positive tests', 1)]
        act_rows = [(row[0], str(row[1]), row[2], row[3], row[4]) for row in target_rows_for_project(project)]
        self.assertEqual(sorted(exp_rows, key=lambda _: _[0].id), sorted(act_rows, key=lambda _: _[0].id))

        # case: add a second forecast for a newer timezero
        time_zero2 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2011, 10, 3))
        forecast2 = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions-non-dup.json',
                                            time_zero=time_zero2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions-non-dup.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, is_validate_cats=False)
            cache_forecast_metadata(forecast2)  # required by _forecast_ids_to_present_unit_or_target_id_sets()

        exp_rows = [(forecast_model, str(time_zero2.timezero_date), forecast2.id, 'Season peak week', 1),
                    (forecast_model, str(time_zero2.timezero_date), forecast2.id, 'above baseline', 1),
                    (forecast_model, str(time_zero2.timezero_date), forecast2.id, 'season severity', 1),
                    (forecast_model, str(time_zero2.timezero_date), forecast2.id, 'week ahead cases', 1),
                    (forecast_model, str(time_zero2.timezero_date), forecast2.id,
                     'week ahead percentage positive tests', 1)]
        act_rows = [(row[0], str(row[1]), row[2], row[3], row[4]) for row in target_rows_for_project(project)]
        self.assertEqual(sorted(exp_rows, key=lambda _: _[0].id), sorted(act_rows, key=lambda _: _[0].id))

        # case: add a second model with only forecasts for one target
        forecast_model2 = ForecastModel.objects.create(project=project, name=forecast_model.name + '2',
                                                       abbreviation=forecast_model.abbreviation + '2')
        time_zero3 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2011, 10, 4))
        forecast3 = Forecast.objects.create(forecast_model=forecast_model2, source='docs-predictions.json',
                                            time_zero=time_zero3, notes="a small prediction file")
        json_io_dict = {"meta": {},
                        "predictions": [{"unit": "loc1",
                                         "target": "pct next week",
                                         "class": "point",
                                         "prediction": {"value": 2.1}}]}
        load_predictions_from_json_io_dict(forecast3, json_io_dict, is_validate_cats=False)
        cache_forecast_metadata(forecast3)  # required by _forecast_ids_to_present_unit_or_target_id_sets()

        exp_rows = exp_rows + [(forecast_model2, str(time_zero3.timezero_date), forecast3.id,
                                'week ahead percentage positive tests', 1)]
        act_rows = [(row[0], str(row[1]), row[2], row[3], row[4]) for row in target_rows_for_project(project)]
        self.assertEqual(sorted(exp_rows, key=lambda _: _[0].id), sorted(act_rows, key=lambda _: _[0].id))

        # case: no forecasts
        forecast.delete()
        forecast2.delete()
        forecast3.delete()
        exp_rows = [(forecast_model, '', '', '', 0),
                    (forecast_model2, '', '', '', 0)]
        act_rows = [(row[0], str(row[1]), row[2], row[3], row[4]) for row in target_rows_for_project(project)]
        self.assertEqual(sorted(exp_rows, key=lambda _: _[0].id), sorted(act_rows, key=lambda _: _[0].id))
