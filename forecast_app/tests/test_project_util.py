import datetime
import json
import logging
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, Target
from utils.project import create_project_from_json, config_dict_from_project, _target_dict_for_target
from utils.utilities import get_or_create_super_po_mo_users


logging.getLogger().setLevel(logging.ERROR)


class ProjectUtilTestCase(TestCase):
    """
    """


    def test_config_dict_from_project(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            input_project_dict = json.load(fp)
        project = create_project_from_json(input_project_dict, po_user)
        output_project_config = config_dict_from_project(project)
        self.assertEqual(input_project_dict, output_project_config)


    def test_create_project_from_json(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
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

        self.assertEqual(11, project.locations.count())
        self.assertEqual(7, project.targets.count())

        # spot-check a Location
        location = project.locations.filter(name='US National').first()
        self.assertIsNotNone(location)

        # spot-check a Target
        target = project.targets.filter(name='1 wk ahead').first()
        self.assertEqual(Target.CONTINUOUS_TARGET_TYPE, target.type)
        self.assertEqual('percent', target.unit)
        self.assertTrue(target.is_step_ahead)
        self.assertEqual(1, target.step_ahead_increment)

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


    def test_create_project_from_json_project_validation(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)

        # note: owner permissions tested by test_views_and_rest_api.py

        # test missing top level fields
        for field_name in ['name', 'is_public', 'description', 'home_url', 'logo_url', 'core_data',
                           'time_interval_type', 'visualization_y_label', 'locations', 'targets', 'timezeros']:
            field_value = project_dict[field_name]
            with self.assertRaises(RuntimeError) as context:
                del (project_dict[field_name])
                create_project_from_json(project_dict, po_user)
            self.assertIn("Wrong keys in project_dict", str(context.exception))
            project_dict[field_name] = field_value

        # test locations
        project_dict['locations'] = [{}]
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("one of the location_dicts had no 'name' field", str(context.exception))

        # note: targets tested in test_create_project_from_json_target_validation()

        # test timezero missing fields
        project_dict['locations'] = [{"name": "HHS Region 1"}]  # reset to valid
        timezero_config = {'timezero_date': '2017-12-01',
                           'data_version_date': None,
                           'is_season_start': False}
        project_dict['timezeros'] = [timezero_config]
        for field_name in ['timezero_date', 'data_version_date', 'is_season_start']:  # required fields
            field_value = timezero_config[field_name]
            with self.assertRaises(RuntimeError) as context:
                del (timezero_config[field_name])
                create_project_from_json(project_dict, po_user)
            self.assertIn("Wrong keys in timezero_config", str(context.exception))
            timezero_config[field_name] = field_value  # reset to valid

        # test optional 'season_name' field
        timezero_config['is_season_start'] = True
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn('season_name not found but is required when is_season_start', str(context.exception))
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
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)

        # test valid types
        minimal_target_dict = {'name': 'n', 'description': 'd', 'is_step_ahead': False}  # no 'type'
        target_type_int_to_required_keys_and_values = {
            Target.CONTINUOUS_TARGET_TYPE: [('unit', 'month')],  # 'range' optional
            Target.DISCRETE_TARGET_TYPE: [('unit', 'month')],  # 'range' optional
            Target.NOMINAL_TARGET_TYPE: [('cats', ['a', 'b'])],
            Target.BINARY_TARGET_TYPE: [],  # binary has no required keys
            Target.DATE_TARGET_TYPE: [('unit', 'month'), ('cats', ['2019-12-15', '2019-12-22'])]}
        type_int_to_name = {type_int: type_name for type_int, type_name in Target.TARGET_TYPE_CHOICES}
        for type_int, required_keys_and_values in target_type_int_to_required_keys_and_values.items():
            test_target_dict = dict(minimal_target_dict)  # copy
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
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]

        # test missing target required fields. optional keys are tested below
        for field_name in ['name', 'description', 'type', 'is_step_ahead']:  # required
            field_value = first_target_dict[field_name]
            with self.assertRaises(RuntimeError) as context:
                del (first_target_dict[field_name])
                create_project_from_json(project_dict, po_user)
            self.assertIn("Wrong required keys in target_dict", str(context.exception))
            first_target_dict[field_name] = field_value  # reset to valid


    def test_create_project_from_json_target_optional_fields(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]

        # test optional 'step_ahead_increment': required only if 'is_step_ahead'
        first_target_dict['is_step_ahead'] = True  # was False w/no 'step_ahead_increment'
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("step_ahead_increment not found but is required when is_step_ahead", str(context.exception))

        # test optional fields, based on type:
        # 1) test optional 'unit'. three cases a-c follow
        # 1a) required but not passed: ['continuous', 'discrete', 'date']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        for target_type in ['continuous', 'discrete', 'date']:
            first_target_dict['type'] = target_type
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'unit' not passed but is required for type_name={target_type}", str(context.exception))

        # 1b) optional: ok to pass or not pass: []: no need to validate

        # 1c) invalid but passed: ['nominal', 'binary']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict['unit'] = 'month'
        for target_type in ['nominal', 'binary']:
            first_target_dict['type'] = target_type
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'unit' passed but is invalid for type_name={target_type}", str(context.exception))

        # 2) test optional 'range'. three cases a-c follow
        # 2a) required but not passed: []: no need to validate
        # 2b) optional: ok to pass or not pass: ['continuous', 'discrete']: no need to validate

        # 2c) invalid but passed: ['nominal', 'binary', 'date']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict['range'] = [1, 2]
        for target_type in ['nominal', 'binary', 'date']:
            first_target_dict['type'] = target_type
            if target_type == 'date':
                first_target_dict['unit'] = 'biweek'
            else:
                first_target_dict.pop('unit', None)
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'range' passed but is invalid for type_name={target_type}", str(context.exception))

        # 3) test optional 'cats'. three cases a-c follow
        # 3a) required but not passed: ['nominal', 'date']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict.pop('cats', None)
        first_target_dict.pop('unit', None)
        for target_type in ['nominal', 'date']:
            first_target_dict['type'] = target_type
            if target_type in ['continuous', 'discrete', 'date']:
                first_target_dict['unit'] = 'biweek'
            else:
                first_target_dict.pop('unit', None)
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'cats' not passed but is required for type_name='{target_type}'", str(context.exception))

        # 3b) optional: ok to pass or not pass: ['continuous', 'discrete']: no need to validate

        # 3c) invalid but passed: ['binary']
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


    def test_create_project_from_json_target_range_format(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            pct_next_week_target_dict = [target_dict for target_dict in project_dict['targets']
                                         if target_dict['name'] == 'pct next week'][0]
            project_dict['targets'] = [pct_next_week_target_dict]

        # loaded range is valid format: test that an error is not raised
        # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
        with self.assertRaises(Exception):
            try:
                project = create_project_from_json(project_dict, po_user)
                project.delete()
            except:
                pass
            else:
                raise Exception

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
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            pct_next_week_target_dict = [target_dict for target_dict in project_dict['targets']
                                         if target_dict['name'] == 'pct next week'][0]
            project_dict['targets'] = [pct_next_week_target_dict]

        # loaded cats is valid format: test that an error is not raised
        # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
        with self.assertRaises(Exception):
            try:
                project = create_project_from_json(project_dict, po_user)
                project.delete()
            except:
                pass
            else:
                raise Exception

        # break cats by setting to invalid format: test that an error is raised
        cats = ["not float", True, {}]  # not floats
        pct_next_week_target_dict['cats'] = cats
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("could not convert cat to data_type. cat_str='not float'", str(context.exception))


    def test_create_project_from_json_target_dates_format(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
            season_peak_week_target_dict = [target_dict for target_dict in project_dict['targets']
                                            if target_dict['name'] == 'Season peak week'][0]
            project_dict['targets'] = [season_peak_week_target_dict]

        # loaded dates are in valid 'yyyy-mm-dd' format: test that an error is not raised
        # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
        with self.assertRaises(Exception):
            try:
                project = create_project_from_json(project_dict, po_user)
                project.delete()
            except Exception:
                pass
            else:
                raise Exception

        # break dates by setting to invalid 'yyyymmdd' format: test that an error is raised
        season_peak_week_target_dict['cats'] = ['2019-12-15', '20191222']
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("could not convert cat to data_type. cat_str='20191222'", str(context.exception))


    def test_create_project_from_json_cats_lws_ranges_created(self):
        # verify that 'list' TargetCat, TargetLwr, and TargetRange instances are created.
        # docs-project.json contains examples of all five target types
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
        project = create_project_from_json(project_dict, po_user)

        # test 'pct next week' target. continuous, with range and cats (w/lwrs)
        target = project.targets.filter(name='pct next week').first()
        self.assertEqual(Target.CONTINUOUS_TARGET_TYPE, target.type)
        self.assertEqual('percent', target.unit)
        self.assertTrue(target.is_step_ahead)
        self.assertEqual(1, target.step_ahead_increment)

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
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            project_dict = json.load(fp)
        input_target_dicts = project_dict['targets']
        # does validate_and_create_targets() -> model_init = {...}  # required keys:
        project = create_project_from_json(project_dict, po_user)

        # 2. Target -> target_dict
        # does target_dict() = {...}  # required keys:
        output_target_dicts = [_target_dict_for_target(target) for target in project.targets.all()]

        # 3. they should be equal
        self.assertEqual(input_target_dicts, output_target_dicts)
