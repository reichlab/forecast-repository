import datetime
import unittest
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast import Forecast
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.tests.test_project import TEST_CONFIG_DICT


class ForecastTestCase(TestCase):
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


    def test_load_forecast(self):
        self.assertEqual(1, len(self.forecast_model.forecasts.all()))

        self.assertIsInstance(self.forecast, Forecast)
        self.assertEqual('EW1-KoTstable-2017-01-17.csv', self.forecast.csv_filename)

        cdc_data_rows = self.forecast.cdcdata_set.all()
        self.assertEqual(8019, len(cdc_data_rows))  # excluding header

        # spot-check a few rows
        self.assertEqual(['US National', 'Season onset', 'p', 'week', None, None, 50.0012056690978],
                         cdc_data_rows[0].data_row())  # note 'NA' -> None
        self.assertEqual(['US National', 'Season onset', 'b', 'week', None, None, 1.22490002826229e-07],
                         cdc_data_rows[34].data_row())  # note 'none' -> None
        self.assertEqual(['HHS Region 10', '4 wk ahead', 'b', 'percent', 13, 100, 0.00307617873070836],
                         cdc_data_rows[8018].data_row())

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
        exp_preview = [('US National', 'Season onset', 'p', 'week', None, None, 50.0012056690978),
                       ('US National', 'Season onset', 'b', 'week', 40, 41, 1.95984004521967e-05),
                       ('US National', 'Season onset', 'b', 'week', 41, 42, 1.46988003391476e-05),
                       ('US National', 'Season onset', 'b', 'week', 42, 43, 6.98193016109509e-06),
                       ('US National', 'Season onset', 'b', 'week', 43, 44, 3.79719008761312e-06),
                       ('US National', 'Season onset', 'b', 'week', 44, 45, 4.28715009891804e-06),
                       ('US National', 'Season onset', 'b', 'week', 45, 46, 1.59237003674098e-05),
                       ('US National', 'Season onset', 'b', 'week', 46, 47, 3.0989970715036e-05),
                       ('US National', 'Season onset', 'b', 'week', 47, 48, 5.3895601243541e-05),
                       ('US National', 'Season onset', 'b', 'week', 48, 49, 7.49638817296525e-05)]
        self.assertEqual(exp_preview, self.forecast.get_data_preview())

        exp_locations = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                         'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(exp_locations, sorted(self.forecast.get_locations()))

        exp_targets = ['1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead', 'Season onset', 'Season peak percentage',
                       'Season peak week']
        self.assertEqual(exp_targets, sorted(self.forecast.get_targets('US National')))

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
        self.assertEqual(1, len(self.forecast_model.forecasts.all()))  # from setUpTestData()
        self.assertEqual(8019, len(self.forecast.cdcdata_set.all()))  # ""

        forecast2 = self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv'),
                                                      self.time_zero)
        self.assertEqual(2, len(self.forecast_model.forecasts.all()))  # includes new
        self.assertEqual(8019, len(forecast2.cdcdata_set.all()))  # new
        self.assertEqual(8019, len(self.forecast.cdcdata_set.all()))  # didn't change

        forecast2.delete()
        self.assertEqual(0, len(forecast2.cdcdata_set.all()))


    def test_get_location_target_dict(self):
        act_dict = self.forecast.get_location_target_dict()

        exp_locations = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                         'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(sorted(exp_locations), sorted(act_dict.keys()))

        # spot-check one location's targets
        exp_targets = ['1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead', 'Season onset', 'Season peak percentage',
                       'Season peak week']
        self.assertEqual(sorted(exp_targets), sorted(act_dict['US National'].keys()))

        # spot-check a target
        self.assertEqual(50.0012056690978, act_dict['US National']['Season onset']['point'])

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
        act_bins = act_dict['US National']['Season onset']['bins']
        self.assertEqual(34, len(act_bins))

        # per https://stackoverflow.com/questions/18411560/python-sort-list-with-none-at-the-end
        exp_bins = sorted(exp_bins, key=lambda x: (x[0] is None or x[1] is None, x))
        act_bins = sorted(act_bins, key=lambda x: (x[0] is None or x[1] is None, x))
        self.assertEqual(exp_bins, act_bins)

        # spot-check a few units
        self.assertEqual('week', act_dict['US National']['Season onset']['unit'])
        self.assertEqual('percent', act_dict['HHS Region 1']['1 wk ahead']['unit'])


    def test_forecast_for_time_zero(self):
        time_zero = TimeZero.objects.create(project=self.project,
                                            timezero_date=datetime.date.today(),  # todo str()?
                                            data_version_date=None)
        self.assertEqual(None, self.forecast_model.forecast_for_time_zero(time_zero))

        forecast2 = self.forecast_model.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv'),
                                                      time_zero)
        self.assertEqual(forecast2, self.forecast_model.forecast_for_time_zero(time_zero))

        forecast2.delete()
