import datetime
import json
import logging
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, TimeZero, Target
from forecast_app.models.forecast_model import ForecastModel
from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, get_or_create_super_po_mo_users
from utils.project import create_project_from_json, config_dict_from_project


logging.getLogger().setLevel(logging.ERROR)


class ProjectUtilTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date=datetime.date(2017, 1, 1))
        make_cdc_locations_and_targets(cls.project)

        cls.forecast_model = ForecastModel.objects.create(project=cls.project, name='fm1')
        # cls.forecast = load_cdc_csv_forecast_file(cls.forecast_model, Path(
        #     'forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), cls.time_zero)


    def test_config_dict_from_project(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            input_project_config = json.load(fp)
            timezero_config = {'timezero_date': '2017-12-01',
                               'data_version_date': None,
                               'is_season_start': True,
                               'season_name': 'tis the season'}
            input_project_config['timezeros'] = [timezero_config]
        project = create_project_from_json(input_project_config, po_user)
        output_project_config = config_dict_from_project(project)
        self.assertEqual(input_project_config, output_project_config)


    def test_create_project_from_json(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_config = json.load(fp)
            timezero_config = {'timezero_date': '2017-12-01',
                               'data_version_date': None,
                               'is_season_start': True,
                               'season_name': 'tis the season'}
            project_config['timezeros'] = [timezero_config]
        project = create_project_from_json(project_config, po_user)

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

        # test existing project
        project_name = project_dict['name']
        project_dict['name'] = self.project.name
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn('found existing project', str(context.exception))
        project_dict['name'] = project_name

        # test time_interval_type
        project_time_interval_type = project_dict['time_interval_type']
        project_dict['time_interval_type'] = "not 'week', 'biweek', or 'month'"
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("invalid 'time_interval_type'", str(context.exception))
        project_dict['time_interval_type'] = project_time_interval_type  # reset to valid


    def test_create_project_from_json_target_types(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)

        # test valid types
        minimal_target_dict = {'name': 'n', 'description': 'd', 'is_step_ahead': False}  # no 'type'
        target_type_int_to_required_keys = {
            Target.CONTINUOUS_TARGET_TYPE: {'unit'},  # 'range' optional
            Target.DISCRETE_TARGET_TYPE: {'unit'},  # 'range' optional
            Target.NOMINAL_TARGET_TYPE: {'cat'},
            # Target.BINARY_TARGET_TYPE: set(),
            Target.DATE_TARGET_TYPE: {'unit', 'date'},
            Target.COMPOSITIONAL_TARGET_TYPE: {'cat'},
        }
        type_int_to_name = {type_int: type_name for type_int, type_name in Target.TARGET_TYPE_CHOICES}
        for type_int, required_keys in target_type_int_to_required_keys.items():
            test_target_dict = dict(minimal_target_dict)  # copy
            project_dict['targets'] = [test_target_dict]
            test_target_dict['type'] = type_int_to_name[type_int]
            for required_key in required_keys:
                test_target_dict[required_key] = 'month'  # works for unit, but others too :-) None doesn't work, though
            with self.assertRaises(Exception):
                try:
                    project = create_project_from_json(project_dict, po_user)
                    project.delete()
                except:
                    pass
                else:
                    raise Exception

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
        # field   | required                           | optional                    | invalid
        # --------+------------------------------------+-----------------------------+-----------------------------------------------------------------
        # 'unit'  | ['continuous', 'discrete', 'date'] | []                          | ['nominal', 'binary', 'compositional']
        # 'range' | []                                 | ['continuous', 'discrete']  | ['nominal', 'binary', 'date', 'compositional']
        # 'cat'   | ['nominal', 'compositional']       | ['continuous']              | ['discrete', 'binary', 'date']
        # 'date'  | ['date']                           | []                          | ['continuous', 'discrete', 'nominal', 'binary', 'compositional']
        # --------+------------------------------------+-----------------------------+-----------------------------------------------------------------

        # 1) test optional 'unit'. three cases a-c follow
        # 1a) required but not passed: ['continuous', 'discrete', 'date']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        del (first_target_dict['unit'])
        for target_type in ['continuous', 'discrete', 'date']:
            first_target_dict['type'] = target_type
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'unit' not passed but is required for type_name={target_type}", str(context.exception))

        # 1b) optional: ok to pass or not pass: []: no need to validate

        # 1c) invalid but passed: ['nominal', 'binary', 'compositional']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict['unit'] = 'month'  # works for unit, but others too :-) None doesn't work, though
        for target_type in ['nominal', 'binary', 'compositional']:
            first_target_dict['type'] = target_type
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'unit' passed but is invalid for type_name={target_type}", str(context.exception))

        # 2) test optional 'range'. three cases a-c follow
        # 2a) required but not passed: []: no need to validate
        # 2b) optional: ok to pass or not pass: ['continuous', 'discrete']: no need to validate

        # 2c) invalid but passed: ['nominal', 'binary', 'date', 'compositional']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict['range'] = 'month'  # works for unit, but others too :-) None doesn't work, though
        for target_type in ['nominal', 'binary', 'date', 'compositional']:
            first_target_dict['type'] = target_type
            if target_type == 'date':
                first_target_dict['unit'] = 'u'
            else:
                first_target_dict.pop('unit', None)
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'range' passed but is invalid for type_name={target_type}", str(context.exception))

        # 3) test optional 'cat'. three cases a-c follow
        # 3a) required but not passed: ['nominal', 'compositional']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict.pop('cat', None)
        first_target_dict.pop('unit', None)
        for target_type in ['nominal', 'compositional']:
            first_target_dict['type'] = target_type
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'cat' not passed but is required for type_name={target_type}", str(context.exception))

        # 3b) optional: ok to pass or not pass: ['continuous']: no need to validate

        # 3c) invalid but passed: ['discrete', 'binary', 'date']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict['cat'] = 'month'  # works for unit, but others too :-) None doesn't work, though
        for target_type in ['discrete', 'binary', 'date']:
            first_target_dict['type'] = target_type
            if target_type in ['discrete', 'date']:
                first_target_dict['unit'] = 'u'
            else:
                first_target_dict.pop('unit', None)
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'cat' passed but is invalid for type_name={target_type}", str(context.exception))

        # 4) test optional 'date'. three cases a-c follow
        # 4a) required but not passed: ['date']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict.pop('date', None)
        for target_type in ['date']:
            first_target_dict['type'] = target_type
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'date' not passed but is required for type_name={target_type}", str(context.exception))

        # 4b) optional: ok to pass or not pass: []: no need to validate

        # 4c) invalid but passed: ['continuous', 'discrete', 'nominal', 'binary', 'compositional']
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            project_dict = json.load(fp)
            first_target_dict = project_dict['targets'][0]  # 'Season onset'
            project_dict['targets'] = [first_target_dict]
        first_target_dict['date'] = 'month'  # works for unit, but others too :-) None doesn't work, though
        for target_type in ['continuous', 'discrete', 'nominal', 'binary', 'compositional']:
            first_target_dict['type'] = target_type
            if target_type in ['continuous', 'discrete', 'date']:
                first_target_dict['unit'] = 'u'
            else:
                first_target_dict.pop('unit', None)
            if target_type in ['nominal', 'compositional']:
                first_target_dict['cat'] = 'c'
            else:
                first_target_dict.pop('cat', None)
            with self.assertRaises(RuntimeError) as context:
                create_project_from_json(project_dict, po_user)
            self.assertIn(f"'date' passed but is invalid for type_name={target_type}", str(context.exception))


    def test_create_project_from_json_lists(self):
        # verify that TargetCat, TargetLwr, TargetDate, and TargetRange instances are created.
        # project-config-example.json contains examples of all six target types
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/project-config-example.json')) as fp:
            project_config = json.load(fp)
        project = create_project_from_json(project_config, po_user)

        # test 'pct next week' target. continuous, with range and cat
        target = project.targets.filter(name='pct next week').first()
        self.assertEqual(Target.CONTINUOUS_TARGET_TYPE, target.type)
        self.assertEqual('percent', target.unit)
        self.assertTrue(target.is_step_ahead)
        self.assertEqual(1, target.step_ahead_increment)

        ranges = target.ranges.all().order_by('value_f')  # value_i, value_f
        self.assertEqual(2, len(ranges))
        self.assertIsNone(ranges[0].value_i)
        self.assertEqual(0, ranges[0].value_f)
        self.assertIsNone(ranges[1].value_i)
        self.assertEqual(100, ranges[1].value_f)

        cats = target.cats.all().order_by('value_f')  # value_i, value_f
        self.assertEqual(10, len(cats))
        self.assertEqual([], cats.values_list('value_i', flat=True))
        self.assertEqual([0, 1.0, 1.1, 2.0, 2.2, 3.0, 3.3, 5.0, 10, 50], cats.values_list('value_f', flat=True))

        # test 'cases next week' target. discrete, with range
        target = project.targets.filter(name='cases next week').first()
        self.fail()

        # test 'season severity' target. nominal, with range
        target = project.targets.filter(name='season severity').first()
        self.fail()

        # test 'above baseline' target. binary
        target = project.targets.filter(name='above baseline').first()
        self.fail()

        # test 'Season peak week' target. date, with dates
        target = project.targets.filter(name='Season peak week').first()
        self.fail()

        # test 'Next season flu strain composition' target. compositional, with cat
        target = project.targets.filter(name='Next season flu strain composition').first()
        self.fail()
