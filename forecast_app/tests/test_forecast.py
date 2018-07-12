import datetime
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.data import CDCData
from forecast_app.models.forecast import Forecast
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import CDC_CONFIG_DICT
from utils.make_2016_2017_flu_contest_project import create_cdc_targets
from utils.utilities import rescale


class ForecastTestCase(TestCase):
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


    def test_load_forecast(self):
        self.assertEqual(1, len(self.forecast_model.forecasts.all()))

        self.assertIsInstance(self.forecast, Forecast)
        self.assertEqual('EW1-KoTstable-2017-01-17.csv', self.forecast.csv_filename)

        forecast_cdcdata_set = self.forecast.cdcdata_set
        self.assertEqual(8019, forecast_cdcdata_set.count())  # excluding header

        # spot-check a few rows
        act_qs = forecast_cdcdata_set.filter(location='US National', row_type=CDCData.POINT_ROW_TYPE).order_by('id').values_list('value', flat=True)
        self.assertEqual([50.0012056690978, 4.96302456525203, 3.30854920241938, 3.00101461253164, 2.72809349594878, 2.5332588357381, 2.42985946508278],
                         list(act_qs))

        # test empty file
        with self.assertRaises(RuntimeError) as context:
            self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-bad_file_no_header-2017-01-17.csv'),
                                              self.time_zero)
        self.assertIn('Empty file', str(context.exception))

        # test a bad data file header
        with self.assertRaises(RuntimeError) as context:
            self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-bad_file_header-2017-01-17.csv'),
                                              self.time_zero)
        self.assertIn('Invalid header', str(context.exception))

        # test load_forecast() with timezero not in the project
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)  # no TimeZeros
        create_cdc_targets(project2)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))

        forecast_model2 = ForecastModel.objects.create(project=project2)
        with self.assertRaises(RuntimeError) as context:
            forecast_model2.load_forecast(  # TimeZero doesn't matter b/c project has none
                Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), self.time_zero)
        self.assertIn("time_zero was not in project", str(context.exception))


    def test_load_forecasts_from_dir(self):
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        create_cdc_targets(project2)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 23),  # 20161023-KoTstable-20161109.cdc.csv
                                data_version_date=None)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 30),  # 20161030-KoTstable-20161114.cdc.csv
                                data_version_date=None)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 11, 6),  # 20161106-KoTstable-20161121.cdc.csv
                                data_version_date=None)
        forecast_model2 = ForecastModel.objects.create(project=project2)

        # copy the two files from 'forecast_app/tests/load_forecasts' to a temp dir, run the loader, and then copy a
        # third file over to test that it skips already-loaded ones
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            test_file_dir = Path('forecast_app/tests/load_forecasts')
            shutil.copy(str(test_file_dir / '20161023-KoTstable-20161109.cdc.csv'), str(temp_dir))
            shutil.copy(str(test_file_dir / '20161030-KoTstable-20161114.cdc.csv'), str(temp_dir))

            forecasts = forecast_model2.load_forecasts_from_dir(temp_dir)
            self.assertEqual(2, len(forecasts))
            self.assertEqual(2, len(forecast_model2.forecasts.all()))

            # copy third file and test only new loaded
            shutil.copy(str(test_file_dir / 'third-file/20161106-KoTstable-20161121.cdc.csv'), str(temp_dir))
            forecasts = forecast_model2.load_forecasts_from_dir(temp_dir)
            self.assertEqual(1, len(forecasts))


    def test_forecast_data_validation(self):
        with self.assertRaises(RuntimeError) as context:
            self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-locations-dont-match-2017-01-17.csv'),
                                              self.time_zero)
        self.assertIn("First row was not the point row", str(context.exception))  # turns out this is the first failure

        with self.assertRaises(RuntimeError) as context:
            self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-targets-dont-match-2017-01-17.csv'),
                                              self.time_zero)
        self.assertIn("Targets did not match template", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-wrong-number-of-bins-2017-01-17.csv'),
                                              self.time_zero)
        self.assertIn("Bins did not match template", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-bin-doesnt-sum-to-one-2017-01-17.csv'),
                                              self.time_zero)
        self.assertIn("Bin did not sum to 1.0", str(context.exception))

        # target units match. also tests that all targets have a point value
        with self.assertRaises(RuntimeError) as context:
            self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-units-dont-match-2017-01-17.csv'),
                                              self.time_zero)
        self.assertIn("Target unit not found or didn't match template", str(context.exception))


    @unittest.skip
    def test_forecast_data_validation_additional(self):
        # test points lie within the range of point values in the template. see Nick's comment
        # ( https://github.com/reichlab/forecast-repository/issues/18#issuecomment-335654340 ):
        # The thought was that for each target we could look at all of the values in the point rows for that target and
        # would end up with a vector of numbers. The minimum and maximum of those numbers would define the acceptable
        # range for the point values in the files themselves. E.g., if the predictions for target K should never be
        # negative, then whoever made the template file would explicitly place a zero in at least one of the target K
        # point rows. And none of those rows would have negative values. Is this too "cute" of a way to set the max/min
        # ranges for testing? Alternatively, we could hard-code them as part of the project.
        self.fail()  # todo

        # see @josh's comment ( https://reichlab.slack.com/archives/C57HNDFN0/p1507744847000350 ):
        # how important is the left handedness of the bins? by that i mean that bin_start_incl and bin_end_notincl
        # versus bin_start_notincl and bin_end_incl
        self.fail()  # todo


    def test_forecast_data_accessors(self):  # (via ModelWithCDCData)
        # test get_data_rows()
        self.assertEqual(8019, len(self.forecast.get_data_rows()))

        # test get_data_preview()
        exp_preview = [('US National', 'Season onset', CDCData.POINT_ROW_TYPE, 'week', None, None, 50.0012056690978),
                       ('US National', 'Season onset', CDCData.BIN_ROW_TYPE, 'week', 40, 41, 1.95984004521967e-05),
                       ('US National', 'Season onset', CDCData.BIN_ROW_TYPE, 'week', 41, 42, 1.46988003391476e-05),
                       ('US National', 'Season onset', CDCData.BIN_ROW_TYPE, 'week', 42, 43, 6.98193016109509e-06),
                       ('US National', 'Season onset', CDCData.BIN_ROW_TYPE, 'week', 43, 44, 3.79719008761312e-06),
                       ('US National', 'Season onset', CDCData.BIN_ROW_TYPE, 'week', 44, 45, 4.28715009891804e-06),
                       ('US National', 'Season onset', CDCData.BIN_ROW_TYPE, 'week', 45, 46, 1.59237003674098e-05),
                       ('US National', 'Season onset', CDCData.BIN_ROW_TYPE, 'week', 46, 47, 3.0989970715036e-05),
                       ('US National', 'Season onset', CDCData.BIN_ROW_TYPE, 'week', 47, 48, 5.3895601243541e-05),
                       ('US National', 'Season onset', CDCData.BIN_ROW_TYPE, 'week', 48, 49, 7.49638817296525e-05)]
        self.assertEqual(exp_preview, self.forecast.get_data_preview())

        exp_locations = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                         'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(exp_locations, sorted(self.forecast.get_locations()))

        exp_targets = ['1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead', 'Season onset', 'Season peak percentage',
                       'Season peak week']
        self.assertEqual(exp_targets, sorted(self.forecast.get_target_names_for_location('US National')))
        self.assertEqual(exp_targets, sorted(self.forecast.get_target_names()))

        self.assertEqual('week', self.forecast.get_target_unit('US National', 'Season onset'))
        self.assertEqual(50.0012056690978, self.forecast.get_target_point_value('US National', 'Season onset'))

        self.assertEqual('percent', self.forecast.get_target_unit('US National', 'Season peak percentage'))
        self.assertEqual(3.30854920241938,
                         self.forecast.get_target_point_value('US National', 'Season peak percentage'))

        act_bins = self.forecast.get_target_bins('US National', 'Season onset')
        self.assertEqual(34, len(act_bins))

        # spot-check bin boundaries
        start_end_val_tuples = [(1, 2, 9.7624532252505e-05),
                                (20, 21, 1.22490002826229e-07),
                                (40, 41, 1.95984004521967e-05),
                                (52, 53, 0.000147110493394302)]
        for start_end_val_tuple in start_end_val_tuples:
            self.assertIn(start_end_val_tuple, act_bins)


    def test_forecast_delete(self):
        # add a second forecast, check its associated ForecastData rows were added, delete it, and test that the data was
        # deleted (via CASCADE)
        self.assertEqual(1, self.forecast_model.forecasts.count())  # from setUpTestData()
        self.assertEqual(8019, self.forecast.cdcdata_set.count())  # ""

        forecast2 = self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv'),
                                                      self.time_zero)
        self.assertEqual(2, self.forecast_model.forecasts.count())  # includes new
        self.assertEqual(8019, forecast2.cdcdata_set.count())  # new
        self.assertEqual(8019, self.forecast.cdcdata_set.count())  # didn't change

        forecast2.delete()
        self.assertEqual(0, forecast2.cdcdata_set.count())


    def test_get_location_dicts_download_format_small_forecast(self):
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        create_cdc_targets(project2)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template-small.csv'))
        time_zero = TimeZero.objects.create(project=project2,
                                            timezero_date=datetime.date.today(),
                                            data_version_date=None)
        forecast_model2 = ForecastModel.objects.create(project=project2)
        forecast2 = forecast_model2.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv'),
                                                  time_zero)

        # act_list has tuples for bins, but loaded json has lists, so we do in-place conversion of tuples to lists
        act_list = forecast2.get_location_dicts_download_format()
        for location_dict in act_list:
            for target_dict in location_dict['targets']:
                target_dict['bins'] = [list(bin_list) for bin_list in target_dict['bins']]

        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17-small-exp-download.json', 'r') as fp:
            exp_list = json.load(fp)['locations']  # ignore the file's 'metadata'
            self.assertEqual(exp_list, act_list)


    def test_get_location_dicts_internal_format_small_forecast(self):
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        create_cdc_targets(project2)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template-small.csv'))
        time_zero = TimeZero.objects.create(project=project2,
                                            timezero_date=datetime.date.today(),
                                            data_version_date=None)
        forecast_model2 = ForecastModel.objects.create(project=project2)
        forecast2 = forecast_model2.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv'),
                                                  time_zero)
        # act_location_dicts has tuples for bins, but loaded json has lists, so we do in-place conversion of tuples to lists
        act_location_dicts = forecast2.get_location_dicts_internal_format()
        for location_name, location_dict in act_location_dicts.items():
            for target_name, target_dict in location_dict.items():
                target_dict['bins'] = [list(bin_list) for bin_list in target_dict['bins']]

        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17-small-exp-location-dicts-internal.json', 'r') as fp:
            exp_location_dicts = json.load(fp)
            self.assertEqual(exp_location_dicts, act_location_dicts)


    def test_get_location_dicts_internal_format(self):
        act_location_dicts = self.forecast.get_location_dicts_internal_format()
        exp_locations = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                         'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(exp_locations, list(act_location_dicts.keys()))  # tests order

        # spot-check one location's targets
        exp_targets = ['Season onset', 'Season peak week', 'Season peak percentage',
                       '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead']  # Target creation order
        self.assertEqual(exp_targets, list(act_location_dicts['US National'].keys()))  # tests order

        # spot-check a target
        self.assertEqual(50.0012056690978, act_location_dicts['US National']['Season onset']['point'])

        exp_bins = [
            (40.0, 41.0, 1.95984004521967e-05), (41.0, 42.0, 1.46988003391476e-05), (42.0, 43.0, 6.98193016109509e-06),
            (43.0, 44.0, 3.79719008761312e-06), (44.0, 45.0, 4.28715009891804e-06), (45.0, 46.0, 1.59237003674098e-05),
            (46.0, 47.0, 3.0989970715036e-05), (47.0, 48.0, 5.3895601243541e-05), (48.0, 49.0, 7.49638817296525e-05),
            (49.0, 50.0, 0.000110241002543607), (50.0, 51.0, 0.998941808865584), (51.0, 52.0, 0.000165973953829541),
            (52.0, 53.0, 0.000147110493394302), (1.0, 2.0, 9.7624532252505e-05), (2.0, 3.0, 5.41405812491935e-05),
            (3.0, 4.0, 3.8951820898741e-05), (4.0, 5.0, 4.99759211531016e-05), (5.0, 6.0, 4.09116609439607e-05),
            (6.0, 7.0, 3.60120608309115e-05), (7.0, 8.0, 2.51104505793771e-05), (8.0, 9.0, 2.09457904832853e-05),
            (9.0, 10.0, 1.99658704606754e-05), (10.0, 11.0, 1.6536150381541e-05), (11.0, 12.0, 6.00201013848525e-06),
            (12.0, 13.0, 2.20482005087213e-06), (13.0, 14.0, 3.6747000847869e-07), (14.0, 15.0, 1.22490002826229e-07),
            (15.0, 16.0, 1.22490002826229e-07), (16.0, 17.0, 1.22490002826229e-07), (17.0, 18.0, 1.22490002826229e-07),
            (18.0, 19.0, 1.22490002826229e-07), (19.0, 20.0, 1.22490002826229e-07), (20.0, 21.0, 1.22490002826229e-07),
            (None, None, 1.22490002826229e-07)]
        act_bins = act_location_dicts['US National']['Season onset']['bins']
        self.assertEqual(34, len(act_bins))
        self.assertEqual(exp_bins, act_bins)  # tests order

        # spot-check a few units
        self.assertEqual('week', act_location_dicts['US National']['Season onset']['unit'])
        self.assertEqual('percent', act_location_dicts['HHS Region 1']['1 wk ahead']['unit'])


    def test_get_loc_dicts_int_format_for_csv_file(self):
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        create_cdc_targets(project2)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template-small.csv'))
        time_zero = TimeZero.objects.create(project=project2,
                                            timezero_date=datetime.date.today(),
                                            data_version_date=None)
        forecast_model2 = ForecastModel.objects.create(project=project2)
        template = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')
        forecast2 = forecast_model2.load_forecast(template, time_zero)
        exp_location_dicts = forecast2.get_location_dicts_internal_format()  # tested elsewhere
        act_location_dicts = forecast2.get_loc_dicts_int_format_for_csv_file(template)
        self.assertEqual(exp_location_dicts, act_location_dicts)


    def test_forecast_for_time_zero(self):
        time_zero = TimeZero.objects.create(project=self.project,
                                            timezero_date=datetime.date.today(),
                                            data_version_date=None)
        self.assertEqual(None, self.forecast_model.forecast_for_time_zero(time_zero))

        forecast2 = self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv'),
                                                      time_zero)
        self.assertEqual(forecast2, self.forecast_model.forecast_for_time_zero(time_zero))

        forecast2.delete()


    def test_sparkline_data(self):
        with self.assertRaises(ValueError) as context:
            rescale([])
        self.assertIn('invalid argument', str(context.exception))

        with self.assertRaises(ValueError) as context:
            rescale([1])
        self.assertIn('invalid argument', str(context.exception))

        with self.assertRaises(ValueError) as context:
            rescale([1, 1])
        self.assertIn('invalid argument', str(context.exception))

        self.assertEqual([0.0, 25.0, 50.0, 75.0, 100.0], rescale([1, 2, 3, 4, 5]))
        self.assertEqual([0.0, 50.0, 100.0], rescale([-1, 0, 1]))

        # values for EW1-KoTstable-2017-01-17.csv, 'US National', 'Season peak week'
        values_from_db = [0.0000388312283001796, 0.0000432690829630572, 0.0000427143511301975, 0.0000482616694587946,
                          0.0000366123009687408, 0.0000316197144730033, 0.0000249629324786868, 0.0000499258649573737,
                          0.0000848739704275352, 0.000148113399373542, 0.000217454878481005, 0.000290124748585627,
                          0.183889955515593, 0.000328955976885807, 0.0813603183066327, 0.113514362560564,
                          0.0918622520724573, 0.0636838880421198, 0.0985233865781112, 0.104099128806362,
                          0.0403122043568698, 0.0641961804893339, 0.0951013531890285, 0.025085879433318,
                          0.0182445605042546, 0.0103074209280581, 0.00205822405270794, 0.0018802440369379,
                          0.00102908781284421, 0.000848733083646314, 0.00133599391065648, 0.000699739274163662,
                          0.000581366927856728]
        rescaled_values_from_db = rescale(values_from_db)
        rescaled_vals_from_xl = [0.00754265160901892, 0.00995630012390493, 0.00965459405954418, 0.01267165470315170,
                                 0.00633582735157593, 0.00362047277232906, 0.00000000000000000, 0.01357677289623400,
                                 0.03258425495096150, 0.06697874628808760, 0.10469200433318200, 0.14421549876444100,
                                 100.00000000000000000, 0.16533492326969400, 44.23645536405590000, 61.72431088358680000,
                                 49.94821898924800000, 34.62264578770470000, 53.57105899379260000, 56.60357875185930000,
                                 21.91131702582250000, 34.90127003260140000, 51.70989263416820000, 13.63006418392060000,
                                 9.90922595748613000, 5.59239573075953000, 1.10584461547792000, 1.00904532091423000,
                                 0.54612075211197100, 0.44802990476572100, 0.71304001907005500, 0.36699555048792100,
                                 0.30261551563521500]
        rescaled_vals_from_forecast = self.forecast.rescaled_bin_for_loc_and_target('US National', 'Season peak week')

        # double check that manually-computed rescale from Excel matches Python. use assertAlmostEquals() b/c
        # self.assertEqual(rescaled_values_from_db, rescaled_vals_from_xl) fails (different precision b/w Excel and
        # Python):
        for v1, v2 in zip(rescaled_values_from_db, rescaled_vals_from_xl):
            self.assertAlmostEquals(v1, v2)

        self.assertEqual(rescaled_values_from_db, rescaled_vals_from_forecast)
