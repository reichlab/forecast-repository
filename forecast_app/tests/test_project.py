from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel


TEST_CONFIG_DICT = {
    "target_to_week_increment": {
        "1 wk ahead": 1,
        "2 wk ahead": 2,
        "3 wk ahead": 3,
        "4 wk ahead": 4
    },
    "location_to_delphi_region": {
        "US National": "nat",
        "HHS Region 1": "hhs1",
        "HHS Region 2": "hhs2",
        "HHS Region 3": "hhs3",
        "HHS Region 4": "hhs4",
        "HHS Region 5": "hhs5",
        "HHS Region 6": "hhs6",
        "HHS Region 7": "hhs7",
        "HHS Region 8": "hhs8",
        "HHS Region 9": "hhs9",
        "HHS Region 10": "hhs10"
    }
}


class ProjectTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=TEST_CONFIG_DICT)
        cls.project.load_template(Path('2016-2017_submission_template.csv'))

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date="2017-01-01")
        cls.forecast = cls.forecast_model.load_forecast(Path('model_error/ensemble/EW1-KoTstable-2017-01-17.csv'),
                                                        cls.time_zero)


    def test_load_template(self):
        # load a template -> verify csv_filename and is_template_loaded()
        self.assertTrue(self.project.is_template_loaded())
        self.assertEqual('2016-2017_submission_template.csv', self.project.csv_filename)

        # create a project, don't load a template, verify csv_filename and is_template_loaded()
        new_project = Project.objects.create(config_dict=TEST_CONFIG_DICT)
        self.assertFalse(new_project.is_template_loaded())
        self.assertFalse(new_project.csv_filename)

        # verify load_forecast() fails
        new_forecast_model = ForecastModel.objects.create(project=new_project)
        with self.assertRaises(RuntimeError) as context:
            new_forecast_model.load_forecast(Path('EW1-KoTsarima-2017-01-17.csv'), self.time_zero)
        self.assertIn("Cannot validate forecast data", str(context.exception))


    def test_project_template_validation(self):
        # header incorrect or has no lines: already checked by load_csv_data()

        new_project = Project.objects.create(config_dict=TEST_CONFIG_DICT)

        # no locations
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('EW1-no-locations-2017-01-17.csv'))
        self.assertIn("Template has no locations", str(context.exception))

        # a target without a point value
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('EW1-target-no-point-2017-01-17.csv'))
        self.assertIn("Target has no point value", str(context.exception))

        # a target without a bin
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('EW1-target-no-bins-2017-01-17.csv'))
        self.assertIn("Target has no bins", str(context.exception))

        # a target that's not in every location
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('EW1-target-missing-from-location-2017-01-17.csv'))
        self.assertIn("Target(s) was not found in every location", str(context.exception))

        # a target bin that did not sum to 1.0
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('EW1-bin-doesnt-sum-to-one-2017-01-17.csv'))
        self.assertIn("Bin did not sum to 1.0", str(context.exception))


    def test_project_template_data_accessors(self):
        self.assertEqual(8019, len(self.project.get_data_rows()))  # individual rows via SQL
        self.assertEqual(8019, len(self.project.cdcdata_set.all()))  # individual rows as CDCData instances

        # test Project template accessors (via ModelWithCDCData) - the twin to test_forecast_data_accessors()
        exp_locations = {'HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                         'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National'}
        self.assertEqual(exp_locations, self.project.get_locations())

        exp_targets = ['1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead', 'Season onset', 'Season peak percentage',
                       'Season peak week']
        self.assertEqual(exp_targets, sorted(self.project.get_targets('US National')))

        self.assertEqual('week', self.project.get_target_unit('US National', 'Season onset'))
        self.assertEqual(51.0, self.project.get_target_point_value('US National', 'Season onset'))

        self.assertEqual('percent', self.project.get_target_unit('US National', 'Season peak percentage'))
        self.assertEqual(1.5, self.project.get_target_point_value('US National', 'Season peak percentage'))

        act_bins = self.project.get_target_bins('US National', 'Season onset')
        self.assertEqual(34, len(act_bins))

        # spot-check bin boundaries
        start_end_val_tuples = [(1.0, 2.0, 0.029411765),
                                (20.0, 21.0, 0.029411765),
                                (40.0, 41.0, 0.029411765),
                                (52.0, 53.0, 0.029411765)]
        for start_end_val_tuple in start_end_val_tuples:
            self.assertIn(start_end_val_tuple, act_bins)


    def test_project_config_dict_validation(self):
        with self.assertRaises(RuntimeError) as context:
            Project.objects.create(config_dict=None)
        self.assertIn("config_dict did not contain both required keys", str(context.exception))
