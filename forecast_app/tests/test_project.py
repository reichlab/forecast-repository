import datetime
from pathlib import Path

from django.core.exceptions import ValidationError
from django.test import TestCase

from forecast_app.models import Project, TimeZero, Target
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import CDC_CONFIG_DICT
from utils.make_2016_2017_flu_contest_project import create_cdc_targets
from utils.make_thai_moph_project import create_thai_targets


class ProjectTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        create_cdc_targets(cls.project)
        cls.project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date='2017-01-01')
        cls.forecast = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), cls.time_zero)


    def test_load_truth_data(self):
        self.project.load_truth_data(Path('forecast_app/tests/truth_data/truths-ok.csv'))
        self.assertEqual(7, self.project.truth_data_qs().count())
        self.assertTrue(self.project.is_truth_data_loaded())
        self.assertEqual('truths-ok.csv', self.project.truth_csv_filename)

        self.project.delete_truth_data()
        self.assertFalse(self.project.is_truth_data_loaded())
        self.assertFalse(self.project.truth_csv_filename)

        # csv references non-existent TimeZero in Project: should not raise error
        self.project.load_truth_data(Path('forecast_app/tests/truth_data/truths-bad-timezero.csv'))

        # csv references non-existent location in Project: should not raise error
        self.project.load_truth_data(Path('forecast_app/tests/truth_data/truths-bad-location.csv'))

        # csv references non-existent target in Project: should not raise error
        self.project.load_truth_data(Path('forecast_app/tests/truth_data/truths-bad-target.csv'))

        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        self.assertEqual(0, project2.truth_data_qs().count())
        self.assertFalse(project2.is_truth_data_loaded())

        # no template
        with self.assertRaises(RuntimeError) as context:
            project2.load_truth_data(Path('forecast_app/tests/truth_data/truths-ok.csv'))
        self.assertIn('Template not loaded', str(context.exception))

        TimeZero.objects.create(project=project2, timezero_date='2017-01-01')
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        project2.load_truth_data(Path('forecast_app/tests/truth_data/truths-ok.csv'))
        self.assertEqual(7, project2.truth_data_qs().count())

        # test get_truth_data_preview()
        exp_truth_preview = [
            (datetime.date(2017, 1, 1), 'US National', '1 wk ahead', 0.73102),
            (datetime.date(2017, 1, 1), 'US National', '2 wk ahead', 0.688338),
            (datetime.date(2017, 1, 1), 'US National', '3 wk ahead', 0.732049),
            (datetime.date(2017, 1, 1), 'US National', '4 wk ahead', 0.911641),
            (datetime.date(2017, 1, 1), 'US National', 'Season peak percentage', None),
            (datetime.date(2017, 1, 1), 'US National', 'Season peak week', None),
            (datetime.date(2017, 1, 1), 'US National', 'Season onset', 201747.0)
        ]
        self.assertEqual(exp_truth_preview, project2.get_truth_data_preview())


    def test_load_template(self):
        # load a template -> verify csv_filename and is_template_loaded()
        self.assertTrue(self.project.is_template_loaded())
        self.assertEqual('2016-2017_submission_template.csv', self.project.csv_filename)

        # create a project, don't load a template, verify csv_filename and is_template_loaded()
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
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
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        project2.delete_template()
        self.assertFalse(project2.is_template_loaded())
        self.assertFalse(project2.csv_filename)
        self.assertEqual(0, project2.cdcdata_set.count())


    def test_validate_template(self):
        # header incorrect or has no lines: already checked by load_csv_data()

        new_project = Project.objects.create(config_dict=CDC_CONFIG_DICT)

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
        self.assertEqual(exp_targets, sorted(self.project.get_targets_for_location('US National')))

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
            "foo": "bar"
            # "visualization-y-label": "y-label!"
        }
        with self.assertRaises(ValidationError) as context:
            Project.objects.create(config_dict=config_dict)
        self.assertIn("config_dict did not contain the required keys", str(context.exception))


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
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)

        Target.objects.create(project=project2, name="1 wk ahead", description="d",
                              is_step_ahead=True, step_ahead_increment=1)
        Target.objects.create(project=project2, name="2 wk ahead", description="d",
                              is_step_ahead=True, step_ahead_increment=3)
        Target.objects.create(project=project2, name="3 wk ahead", description="d",
                              is_step_ahead=True, step_ahead_increment=3)
        Target.objects.create(project=project2, name="4 wk ahead", description="d",
                              is_step_ahead=True, step_ahead_increment=4)

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
        self.assertIn('Invalid season_name', str(context.exception))

        self.assertEqual([time_zero3, time_zero4], project2.timezeros_in_season('season1'))
        self.assertEqual([time_zero5], project2.timezeros_in_season('season2'))
        self.assertEqual([time_zero6], project2.timezeros_in_season('season3'))

        # test timezeros_in_season() w/no season, but followed by some seasons
        self.assertEqual([time_zero1, time_zero2], project2.timezeros_in_season(None))

        # test timezeros_in_season() w/no season, followed by no seasons, i.e., no seasons at all in the project
        project3 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        time_zero7 = TimeZero.objects.create(project=project3, timezero_date='2015-01-01')
        self.assertEqual([time_zero7], project3.timezeros_in_season(None))

        # test start_end_dates_for_season()
        self.assertEqual((time_zero7.timezero_date, time_zero7.timezero_date),
                         project3.start_end_dates_for_season(None))

        # test location_to_max_val()
        forecast_model = ForecastModel.objects.create(project=project2)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        forecast_model.load_forecast(Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'),
                                     time_zero3)
        exp_location_to_max_val = {'HHS Region 1': 2.06145600601835, 'HHS Region 10': 2.89940153907353,
                                   'HHS Region 2': 4.99776594895244, 'HHS Region 3': 2.99944727598047,
                                   'HHS Region 4': 2.62168214634388, 'HHS Region 5': 2.19233072084465,
                                   'HHS Region 6': 4.41926018901693, 'HHS Region 7': 2.79371802884364,
                                   'HHS Region 8': 1.69920709944699, 'HHS Region 9': 3.10232205135854,
                                   'US National': 3.00101461253164}
        act_location_to_max_val = project2.location_to_max_val('season1', project2.visualization_targets())
        self.assertEqual(exp_location_to_max_val, act_location_to_max_val)


    def test_target_step_ahead(self):
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)

        target = Target.objects.create(project=project2, name="Test target", description="d",
                                       is_step_ahead=True, step_ahead_increment=1)
        self.assertTrue(target.is_step_ahead)
        self.assertEqual(1, target.step_ahead_increment)

        # test validation. note that we cannot validate combinations of is_step_ahead step_ahead_increment b/c
        # Project.clean() is not passed the original args, and b/c is not nullable and defaults to zero. thus, here
        # we just show that all combinations are OK
        Target.objects.create(project=project2, name="Test target", description="d", is_step_ahead=True)
        Target.objects.create(project=project2, name="Test target", description="d", step_ahead_increment=1)
        Target.objects.create(project=project2, name="Test target", description="d",
                              is_step_ahead=False, step_ahead_increment=1)


    def test_visualization_targets(self):
        self.assertEqual(['1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead'],
                         sorted(self.project.visualization_targets()))


    def test_reference_target_for_actual_values(self):
        self.assertEqual(Target.objects.filter(project=self.project).filter(name='1 wk ahead').first(),
                         self.project.reference_target_for_actual_values())

        project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        create_cdc_targets(project)
        Target.objects.filter(project=project).filter(name='1 wk ahead').delete()
        self.assertEqual(Target.objects.filter(project=project).filter(name='2 wk ahead').first(),
                         project.reference_target_for_actual_values())

        project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        create_thai_targets(project)
        self.assertEqual(Target.objects.filter(project=project).filter(name='0_biweek_ahead').first(),
                         project.reference_target_for_actual_values())

        project = Project.objects.create(config_dict=CDC_CONFIG_DICT)  # no Targets
        self.assertIsNone(project.reference_target_for_actual_values())


    def test_actual_values(self):
        project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        create_cdc_targets(project)

        # create TimeZeros only for the first few in truths-2017-2018-reichlab.csv (other truth will be skipped)
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 23))
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 30))
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 8, 6))

        project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        project.load_truth_data(Path('utils/ensemble-truth-table-script/truths-2017-2018-reichlab.csv'))
        exp_loc_tz_date_to_actual_val = {
            'HHS Region 1': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.303222],
                datetime.date(2017, 8, 6): [0.286054]},
            'HHS Region 10': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.364459],
                datetime.date(2017, 8, 6): [0.240377]},
            'HHS Region 2': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [1.32634],
                datetime.date(2017, 8, 6): [1.34713]},
            'HHS Region 3': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.797999],
                datetime.date(2017, 8, 6): [0.586092]},
            'HHS Region 4': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.476357],
                datetime.date(2017, 8, 6): [0.483647]},
            'HHS Region 5': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.602327],
                datetime.date(2017, 8, 6): [0.612967]},
            'HHS Region 6': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [1.15229],
                datetime.date(2017, 8, 6): [0.96867]},
            'HHS Region 7': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.174172],
                datetime.date(2017, 8, 6): [0.115888]},
            'HHS Region 8': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.33984],
                datetime.date(2017, 8, 6): [0.359646]},
            'HHS Region 9': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.892872],
                datetime.date(2017, 8, 6): [0.912778]},
            'US National': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): [0.73102],
                datetime.date(2017, 8, 6): [0.688338]},
        }
        self.assertEqual(exp_loc_tz_date_to_actual_val, project.location_timezero_date_to_actual_val())

        # test location_to_actual_points()
        exp_location_to_actual_points = {'HHS Region 1': [None, 0.303222, 0.286054],
                                         'HHS Region 10': [None, 0.364459, 0.240377],
                                         'HHS Region 2': [None, 1.32634, 1.34713],
                                         'HHS Region 3': [None, 0.797999, 0.586092],
                                         'HHS Region 4': [None, 0.476357, 0.483647],
                                         'HHS Region 5': [None, 0.602327, 0.612967],
                                         'HHS Region 6': [None, 1.15229, 0.96867],
                                         'HHS Region 7': [None, 0.174172, 0.115888],
                                         'HHS Region 8': [None, 0.33984, 0.359646],
                                         'HHS Region 9': [None, 0.892872, 0.912778],
                                         'US National': [None, 0.73102, 0.688338]}
        self.assertEqual(exp_location_to_actual_points,
                         Project.location_to_actual_points(exp_loc_tz_date_to_actual_val))

        # test location_to_actual_max_val()
        exp_location_to_actual_max_val = {'HHS Region 1': 0.303222, 'HHS Region 10': 0.364459, 'HHS Region 2': 1.34713,
                                          'HHS Region 3': 0.797999, 'HHS Region 4': 0.483647, 'HHS Region 5': 0.612967,
                                          'HHS Region 6': 1.15229, 'HHS Region 7': 0.174172, 'HHS Region 8': 0.359646,
                                          'HHS Region 9': 0.912778, 'US National': 0.73102}
        self.assertEqual(exp_location_to_actual_max_val,
                         Project.location_to_actual_max_val(exp_loc_tz_date_to_actual_val))

        del exp_loc_tz_date_to_actual_val['HHS Region 1'][datetime.date(2017, 7, 30)]  # leave only None
        del exp_loc_tz_date_to_actual_val['HHS Region 1'][datetime.date(2017, 8, 6)]  # ""
        exp_location_to_actual_max_val = {'HHS Region 1': None, 'HHS Region 10': 0.364459, 'HHS Region 2': 1.34713,
                                          'HHS Region 3': 0.797999, 'HHS Region 4': 0.483647, 'HHS Region 5': 0.612967,
                                          'HHS Region 6': 1.15229, 'HHS Region 7': 0.174172, 'HHS Region 8': 0.359646,
                                          'HHS Region 9': 0.912778, 'US National': 0.73102}
        self.assertEqual(exp_location_to_actual_max_val,
                         Project.location_to_actual_max_val(exp_loc_tz_date_to_actual_val))

        # test 2 step ahead target first one available
        project.targets.get(name='1 wk ahead').delete()
        exp_loc_tz_date_to_actual_val = {
            'HHS Region 1': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.286054]},
            'HHS Region 10': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.240377]},
            'HHS Region 2': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [1.34713]},
            'HHS Region 3': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.586092]},
            'HHS Region 4': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.483647]},
            'HHS Region 5': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.612967]},
            'HHS Region 6': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.96867]},
            'HHS Region 7': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.115888]},
            'HHS Region 8': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.359646]},
            'HHS Region 9': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.912778]},
            'US National': {
                datetime.date(2017, 7, 23): None,
                datetime.date(2017, 7, 30): None,
                datetime.date(2017, 8, 6): [0.688338]}
        }
        self.assertEqual(exp_loc_tz_date_to_actual_val, project.location_timezero_date_to_actual_val())

        # test no step ahead targets available
        project.targets.all().delete()
        self.assertEqual({}, project.location_timezero_date_to_actual_val())


    def test_0_step_target(self):
        project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        create_cdc_targets(project)

        # create TimeZeros only for the first few in truths-2017-2018-reichlab.csv (other truth will be skipped)
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 23))
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 7, 30))
        TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 8, 6))

        project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        project.load_truth_data(Path('utils/ensemble-truth-table-script/truths-2017-2018-reichlab.csv'))

        # change '1 wk ahead' to '0 wk ahead' in Target and truth data
        target = project.targets.get(name='1 wk ahead')
        target.name = '0 wk ahead'
        target.step_ahead_increment = 0
        target.save()

        project.truth_data_qs().filter(target='1 wk ahead').update(target='0 wk ahead')

        exp_loc_tz_date_to_actual_val = {
            'HHS Region 1': {datetime.date(2017, 7, 23): [0.303222],
                             datetime.date(2017, 7, 30): [0.286054],
                             datetime.date(2017, 8, 6): [0.341359]},
            'HHS Region 10': {datetime.date(2017, 7, 23): [0.364459],
                              datetime.date(2017, 7, 30): [0.240377],
                              datetime.date(2017, 8, 6): [0.126923]},
            'HHS Region 2': {datetime.date(2017, 7, 23): [1.32634],
                             datetime.date(2017, 7, 30): [1.34713],
                             datetime.date(2017, 8, 6): [1.15738]},
            'HHS Region 3': {datetime.date(2017, 7, 23): [0.797999],
                             datetime.date(2017, 7, 30): [0.586092],
                             datetime.date(2017, 8, 6): [0.611163]},
            'HHS Region 4': {datetime.date(2017, 7, 23): [0.476357],
                             datetime.date(2017, 7, 30): [0.483647],
                             datetime.date(2017, 8, 6): [0.674289]},
            'HHS Region 5': {datetime.date(2017, 7, 23): [0.602327],
                             datetime.date(2017, 7, 30): [0.612967],
                             datetime.date(2017, 8, 6): [0.637141]},
            'HHS Region 6': {datetime.date(2017, 7, 23): [1.15229],
                             datetime.date(2017, 7, 30): [0.96867],
                             datetime.date(2017, 8, 6): [1.02289]},
            'HHS Region 7': {datetime.date(2017, 7, 23): [0.174172],
                             datetime.date(2017, 7, 30): [0.115888],
                             datetime.date(2017, 8, 6): [0.112074]},
            'HHS Region 8': {datetime.date(2017, 7, 23): [0.33984],
                             datetime.date(2017, 7, 30): [0.359646],
                             datetime.date(2017, 8, 6): [0.326402]},
            'HHS Region 9': {datetime.date(2017, 7, 23): [0.892872],
                             datetime.date(2017, 7, 30): [0.912778],
                             datetime.date(2017, 8, 6): [1.012]},
            'US National': {datetime.date(2017, 7, 23): [0.73102],
                            datetime.date(2017, 7, 30): [0.688338],
                            datetime.date(2017, 8, 6): [0.732049]}
        }
        self.assertEqual(exp_loc_tz_date_to_actual_val, project.location_timezero_date_to_actual_val())
