import datetime
import json
import logging
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, TimeZero, Target
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import load_cdc_csv_forecast_file
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
        cls.forecast = load_cdc_csv_forecast_file(cls.forecast_model, Path(
            'forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), cls.time_zero)


    def test_config_dict_from_project(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
            input_project_config = json.load(fp)
            timezero_config = {'timezero_date': '20171201',
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
            timezero_config = {'timezero_date': '20171201',
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
        self.assertEqual('percent', target.unit)
        self.assertTrue(target.is_step_ahead)
        self.assertEqual(Target.POINT_FLOAT, target.point_value_type)

        self.assertFalse(target.ok_bincat_distribution)
        self.assertTrue(target.ok_binlwr_distribution)  # BinLwr
        self.assertFalse(target.ok_binary_distribution)
        self.assertTrue(target.ok_named_distribution)  # Named
        self.assertTrue(target.ok_point_prediction)  # Point
        self.assertTrue(target.ok_sample_distribution)  # Sample
        self.assertFalse(target.ok_samplecat_distribution)

        # check the TimeZero
        time_zero = project.timezeros.first()
        self.assertIsNotNone(time_zero)
        self.assertEqual(datetime.date(2017, 12, 1), time_zero.timezero_date)
        self.assertIsNone(time_zero.data_version_date)
        self.assertEqual(timezero_config['is_season_start'], time_zero.is_season_start)
        self.assertEqual(timezero_config['season_name'], time_zero.season_name)

        # test "lwr" validation
        project.delete()
        with open('forecast_app/tests/projects/cdc-project.json') as fp:
            cdc_project_json = json.load(fp)
        peak_percentage_target = cdc_project_json['targets'][2]
        del (peak_percentage_target['lwr'])  # no lwr
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(cdc_project_json, po_user)
        self.assertIn("required lwr entry is missing for BinLwr prediction type", str(context.exception))

        peak_percentage_target['lwr'] = []  # empty lwr
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(cdc_project_json, po_user)
        self.assertIn("required lwr entry is missing for BinLwr prediction type", str(context.exception))

        peak_percentage_target['lwr'] = [1, 'what!? this is not a number!', 2.0]  # non-numeric lwr
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(cdc_project_json, po_user)
        self.assertIn("found a non-numeric BinLwr lwr", str(context.exception))

        # test existing project
        create_project_from_json(Path('forecast_app/tests/projects/cdc-project.json'), po_user)
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(cdc_project_json, po_user)
        self.assertIn("found existing project", str(context.exception))


    def test_create_project_from_json_validation(self):
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

        # test target missing fields
        project_dict['locations'] = [{"name": "HHS Region 1"}]  # reset to valid
        first_target_dict = project_dict['targets'][0]
        project_dict['targets'] = [first_target_dict]
        for field_name in ['name', 'description', 'unit', 'is_date', 'is_step_ahead', 'step_ahead_increment',
                           'point_value_type', 'prediction_types']:
            field_value = first_target_dict[field_name]
            with self.assertRaises(RuntimeError) as context:
                del (first_target_dict[field_name])
                create_project_from_json(project_dict, po_user)
            self.assertIn("Wrong keys in target_dict", str(context.exception))
            first_target_dict[field_name] = field_value

        # test timezero missing fields
        timezero_config = {'timezero_date': '20171201',
                           'data_version_date': None,
                           'is_season_start': True,
                           'season_name': 'tis the season'}
        project_dict['timezeros'] = [timezero_config]
        for field_name in ['timezero_date', 'data_version_date', 'is_season_start', 'season_name']:
            field_value = timezero_config[field_name]
            with self.assertRaises(RuntimeError) as context:
                del (timezero_config[field_name])
                create_project_from_json(project_dict, po_user)
            self.assertIn("Wrong keys in timezero_config", str(context.exception))
            timezero_config[field_name] = field_value

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
        project_dict['time_interval_type'] = project_time_interval_type

        # test point_value_type
        first_target_dict['point_value_type'] = "not 'INTEGER', 'FLOAT', or 'TEXT'"
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("invalid 'point_value_type'", str(context.exception))
        first_target_dict['point_value_type'] = 'INTEGER'  # reset to valid

        # test prediction_type: 'BinCat', 'BinLwr', 'Binary', 'Named', 'Point', 'Sample', or 'SampleCat'
        first_target_dict['prediction_types'] = ["not 'BinCat', 'BinLwr', etc."]
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(project_dict, po_user)
        self.assertIn("invalid 'prediction_type'", str(context.exception))
        first_target_dict['prediction_types'] = ["BinCat", "Binary", "SampleCat"]  # reset to valid
