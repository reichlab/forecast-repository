from pathlib import Path

from django.core.exceptions import ValidationError
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
        cls.project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date='2017-01-01')
        cls.forecast = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), cls.time_zero)


    def test_load_template(self):
        # load a template -> verify csv_filename and is_template_loaded()
        self.assertTrue(self.project.is_template_loaded())
        self.assertEqual('2016-2017_submission_template.csv', self.project.csv_filename)

        # create a project, don't load a template, verify csv_filename and is_template_loaded()
        project2 = Project.objects.create(config_dict=TEST_CONFIG_DICT)
        time_zero2 = TimeZero.objects.create(project=project2, timezero_date='2017-01-01')
        self.assertFalse(project2.is_template_loaded())
        self.assertFalse(project2.csv_filename)
        self.assertEqual(0, project2.cdcdata_set.count())

        # verify load_forecast() fails
        new_forecast_model = ForecastModel.objects.create(project=project2)
        with self.assertRaises(RuntimeError) as context:
            new_forecast_model.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv'), time_zero2)
        self.assertIn("Cannot validate forecast data", str(context.exception))


    def test_delete_template(self):
        # create a project, don't load a template, verify csv_filename and is_template_loaded()
        project2 = Project.objects.create(config_dict=TEST_CONFIG_DICT)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        project2.delete_template()
        self.assertFalse(project2.is_template_loaded())
        self.assertFalse(project2.csv_filename)
        self.assertEqual(0, project2.cdcdata_set.count())


    def test_project_template_validation(self):
        # header incorrect or has no lines: already checked by load_csv_data()

        new_project = Project.objects.create(config_dict=TEST_CONFIG_DICT)

        # no locations
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('forecast_app/tests/EW1-no-locations-2017-01-17.csv'))
        self.assertIn("Template has no locations", str(context.exception))

        # a target without a point value
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('forecast_app/tests/EW1-target-no-point-2017-01-17.csv'))
        self.assertIn("First row was not the point row", str(context.exception))

        # expose a bug in ModelWithCDCData.insert_data() that depended on the 'type' column's case (tested for
        # 'Point') - should *not* raise "Target has no point value" b/c it *does* have a point row:
        #     TH01,1_biweek_ahead,point,cases,NA,NA,NA
        new_project.load_template(Path('forecast_app/tests/thai-template-lowercase-type.csv'))

        # a target without a bin
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('forecast_app/tests/EW1-target-no-bins-2017-01-17.csv'))
        self.assertIn("Target has no bins", str(context.exception))

        # a target that's not in every location
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('forecast_app/tests/EW1-target-missing-from-location-2017-01-17.csv'))
        self.assertIn("Target(s) was not found in every location", str(context.exception))

        # bad row type - neither 'point' nor 'bin'
        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('forecast_app/tests/thai-template-bad-row-type-point.csv'))
        self.assertIn("row_type was neither 'point' nor 'bin'", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            new_project.load_template(Path('forecast_app/tests/thai-template-bad-row-type-bin.csv'))
        self.assertIn("row_type was neither 'point' nor 'bin'", str(context.exception))


    def test_project_template_data_accessors(self):
        self.assertEqual(8019, len(self.project.get_data_rows()))  # individual rows via SQL
        self.assertEqual(8019, self.project.cdcdata_set.count())  # individual rows as CDCData instances

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
        config_dict = {
            "target_to_week_increment": {},
        }
        with self.assertRaises(ValidationError) as context:
            Project.objects.create(config_dict=config_dict)  # missing "location_to_delphi_region"
        self.assertIn("config_dict did not contain both required keys", str(context.exception))

        config_dict = {
            "location_to_delphi_region": {}
        }
        with self.assertRaises(ValidationError) as context:
            Project.objects.create(config_dict=config_dict)  # missing "target_to_week_increment"
        self.assertIn("config_dict did not contain both required keys", str(context.exception))


    def test_timezeros_unique(self):
        project = Project.objects.create()
        with self.assertRaises(ValidationError) as context:
            timezeros = [TimeZero.objects.create(project=project, timezero_date='2017-01-01'),
                         TimeZero.objects.create(project=project, timezero_date='2017-01-01')]
            project.timezeros.add(*timezeros)
            project.save()
        self.assertIn("found duplicate TimeZero.timezero_date", str(context.exception))


    def test_get_num_rows(self):
        time_zero2 = TimeZero.objects.create(project=self.project, timezero_date='2017-01-02')
        self.forecast_model.load_forecast(Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'),
                                          time_zero2)
        self.assertEqual(self.project.get_num_rows(), 8019)  # template
        self.assertEqual(self.project.get_num_forecast_rows(), 8019 * 2)
        self.assertEqual(self.project.get_num_forecast_rows_estimated(), 8019 * 2)  # exact b/c uniform forecasts


    def test_timezero_seasons(self):
        project2 = Project.objects.create(config_dict=TEST_CONFIG_DICT)
        # 2015-01-01 <no season>  time_zero1    not within
        # 2015-02-01 <no season>  time_zero2    not within
        # 2016-02-01 season1      time_zero3  start
        # 2017-01-01   ""         time_zero4    within
        # 2017-02-01 season2      time_zero5  start
        # 2018-01-01 season3      time_zero6  start
        time_zero1 = TimeZero.objects.create(project=project2, timezero_date='2015-01-01',
                                             is_season_start=False)  # no season for this TZ. explicit arg
        time_zero2 = TimeZero.objects.create(project=project2, timezero_date='2015-02-01',
                                             is_season_start=False)  # ""
        time_zero3 = TimeZero.objects.create(project=project2, timezero_date='2016-02-01',
                                             is_season_start=True, season_name='season1')  # start season1. 2 TZs
        time_zero4 = TimeZero.objects.create(project=project2, timezero_date='2017-01-01')  # in season1. default args
        time_zero5 = TimeZero.objects.create(project=project2, timezero_date='2017-02-01',
                                             is_season_start=True, season_name='season2')  # start season2. 1 TZ
        time_zero6 = TimeZero.objects.create(project=project2, timezero_date='2018-01-01',
                                             is_season_start=True, season_name='season3')  # start season3. 1 TZ

        # above create() calls test valid TimeZero season values

        # test invalid TimeZero season values
        with self.assertRaises(ValidationError) as context:
            TimeZero.objects.create(project=project2, timezero_date='2017-01-01',
                                    is_season_start=True, season_name=None)  # season start, no season name (passed)
        self.assertIn('passed is_season_start with no season_name', str(context.exception))

        with self.assertRaises(ValidationError) as context:
            TimeZero.objects.create(project=project2, timezero_date='2017-01-01',
                                    is_season_start=True)  # season start, no season name (default)
        self.assertIn('passed is_season_start with no season_name', str(context.exception))

        with self.assertRaises(ValidationError) as context:
            TimeZero.objects.create(project=project2, timezero_date='2017-01-01',
                                    is_season_start=False, season_name='season4')  # no season start, season name
        self.assertIn('passed season_name but no is_season_start', str(context.exception))

        # test seasons()
        self.assertEqual(['season1', 'season2', 'season3'], sorted(project2.seasons()))

        # test start_end_dates_for_season()
        self.assertEqual((time_zero3.timezero_date, time_zero4.timezero_date),
                         project2.start_end_dates_for_season('season1'))  # two TZs
        self.assertEqual((time_zero5.timezero_date, time_zero5.timezero_date),
                         project2.start_end_dates_for_season('season2'))  # only one TZ -> start == end
        self.assertEqual((time_zero6.timezero_date, time_zero6.timezero_date),
                         project2.start_end_dates_for_season('season3'))  # ""

        # test timezeros_in_season()
        with self.assertRaises(RuntimeError) as context:
            project2.timezeros_in_season('not a valid season')
        self.assertIn('invalid season_name', str(context.exception))

        self.assertEqual([time_zero3, time_zero4], project2.timezeros_in_season('season1'))
        self.assertEqual([time_zero5], project2.timezeros_in_season('season2'))
        self.assertEqual([time_zero6], project2.timezeros_in_season('season3'))

        # test timezeros_in_season() w/no season, but followed by some seasons
        self.assertEqual([time_zero1, time_zero2], project2.timezeros_in_season(None))

        # test timezeros_in_season() w/no season, followed by no seasons, i.e., no seasons at all in the project
        project3 = Project.objects.create(config_dict=TEST_CONFIG_DICT)
        time_zero7 = TimeZero.objects.create(project=project3, timezero_date='2015-01-01')
        self.assertEqual([time_zero7], project3.timezeros_in_season(None))

        # test start_end_dates_for_season()
        self.assertEqual((time_zero7.timezero_date, time_zero7.timezero_date),
                         project3.start_end_dates_for_season(None))
