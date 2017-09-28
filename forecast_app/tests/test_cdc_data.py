import datetime
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast import Forecast
from forecast_app.models.forecast_model import ForecastModel
from utils.utilities import filename_components, mean_absolute_error


#
# ---- mock for cdc_format_utils.true_value_for_epi_week ----
#

EPI_YR_WK_TO_ACTUAL_WILI = {
    (2016, 51): 2.74084,
    (2016, 52): 3.36496,
    (2017, 1): 3.0963,
    (2017, 2): 3.08492,
    (2017, 3): 3.51496,
    (2017, 4): 3.8035,
    (2017, 5): 4.45059,
    (2017, 6): 5.07947,
}


# static mock function for delphi_wili_for_epi_week(). location_name is ignored
def mock_wili_for_epi_week_fcn(forecast_model, year, week, location_name):
    return EPI_YR_WK_TO_ACTUAL_WILI[(year, week)]


class CDCDataTestCase(TestCase):
    """
    Tests loading, accessing, and deleting data from CDCData:
    
    - ForecastModel.load_forecasts_from_model_dir(csv_file_dir) -> runs load_forecast() on all files in csv_file_dir
    - Forecast.delete() -> deletes rows from the CDCData table for the Forecast
    
    """


    @classmethod
    def setUpTestData(cls):
        config_dict = {
            'target_to_week_increment': {
                '1 wk ahead': 1,
                '2 wk ahead': 2,
                '3 wk ahead': 3,
                '4 wk ahead': 4,
            },
            'location_to_delphi_region': {
                'US National': 'nat',
                'HHS Region 1': 'hhs1',
                'HHS Region 2': 'hhs2',
                'HHS Region 3': 'hhs3',
                'HHS Region 4': 'hhs4',
                'HHS Region 5': 'hhs5',
                'HHS Region 6': 'hhs6',
                'HHS Region 7': 'hhs7',
                'HHS Region 8': 'hhs8',
                'HHS Region 9': 'hhs9',
                'HHS Region 10': 'hhs10',
            },
        }
        project = Project.objects.create(config_dict=config_dict)
        cls.forecast_model = ForecastModel.objects.create(project=project)
        cls.forecast = cls.forecast_model.load_forecast(Path('model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), None)


    def test_filename_components(self):
        filename_component_tuples = (('EW1-KoTstable-2017-01-17.csv', (1, 'KoTstable', datetime.date(2017, 1, 17))),
                                     ('-KoTstable-2017-01-17.csv', ()),
                                     ('EW1--2017-01-17.csv', ()),
                                     ('EW1-KoTstable--01-17.csv', ()),
                                     ('EW1-KoTstable--01-17.txt', ()))
        for filename, component in filename_component_tuples:
            self.assertEqual(component, filename_components(filename))


    def test_load_forecast(self):
        self.assertEqual(1, len(self.forecast_model.forecast_set.all()))

        self.assertIsInstance(self.forecast, Forecast)
        self.assertEqual('EW1-KoTstable-2017-01-17.csv', self.forecast.data_filename)

        cdc_data_rows = self.forecast.cdcdata_set.all()
        self.assertEqual(8019, len(cdc_data_rows))  # excluding header

        # spot-check a few rows
        self.assertEqual(['US National', 'Season onset', 'p', 'week', None, None, 50.0012056690978],
                         cdc_data_rows[0].data_row())  # note 'NA' -> None
        self.assertEqual(['US National', 'Season onset', 'b', 'week', None, None, 1.22490002826229e-07],
                         cdc_data_rows[34].data_row())  # note 'none' -> None
        self.assertEqual(['HHS Region 10', '4 wk ahead', 'b', 'percent', 13, 100, 0.00307617873070836],
                         cdc_data_rows[8018].data_row())

        # test a bad data file
        with self.assertRaises(RuntimeError) as context:
            self.forecast_model.load_forecast(Path('model_error_calculations.txt'), None)
        self.assertIn('Invalid header', str(context.exception))


    def test_cdc_data_accessors(self):
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

        # test locations
        exp_locations = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                         'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(exp_locations, sorted(self.forecast.get_locations()))

        # test targets
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


    def test_mean_absolute_error(self):
        # load other three forecasts from 'ensemble' model. will delete them when done so that other tests don't fail.
        # setUpTestData() has already loaded 'model_error/EW1-KoTstable-2017-01-17.csv'
        forecast2 = self.forecast_model.load_forecast(Path('model_error/ensemble/EW2-KoTstable-2017-01-23.csv'), None)
        forecast3 = self.forecast_model.load_forecast(Path('model_error/ensemble/EW51-KoTstable-2017-01-03.csv'), None)
        forecast4 = self.forecast_model.load_forecast(Path('model_error/ensemble/EW52-KoTstable-2017-01-09.csv'), None)

        # 'mini' season for testing. from:
        #   model_error_calculations.txt -> model_error_calculations.py -> model_error_calculations.xlsx:
        target_to_exp_mae = {'1 wk ahead': 0.215904853,
                             '2 wk ahead': 0.458186984,
                             '3 wk ahead': 0.950515864,
                             '4 wk ahead': 1.482010693}
        for target, exp_mae in target_to_exp_mae.items():
            act_mae = mean_absolute_error(self.forecast_model, 2016, 'US National', target,
                                          wili_for_epi_week_fcn=mock_wili_for_epi_week_fcn)
            self.assertAlmostEqual(exp_mae, act_mae)

        # clean up
        forecast2.delete()
        forecast3.delete()
        forecast4.delete()


    def test_forecast_delete(self):
        # add a second forecast, check its associated CDCData rows were added, delete it, and test that the data was
        # deleted (via CASCADE)
        self.assertEqual(1, len(self.forecast_model.forecast_set.all()))  # from setUpTestData()
        self.assertEqual(8019, len(self.forecast.cdcdata_set.all()))  # ""

        forecast2 = self.forecast_model.load_forecast(Path('EW1-KoTsarima-2017-01-17.csv'), None)
        self.assertEqual(2, len(self.forecast_model.forecast_set.all()))  # includes new
        self.assertEqual(8019, len(forecast2.cdcdata_set.all()))  # new
        self.assertEqual(8019, len(self.forecast.cdcdata_set.all()))  # didn't change

        forecast2.delete()
        self.assertEqual(0, len(forecast2.cdcdata_set.all()))


    def test_get_location_target_dict(self):
        act_dict = self.forecast.get_location_target_dict()

        exp_locations = ['HHS Region 1', 'HHS Region 10', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4',
                         'HHS Region 5', 'HHS Region 6', 'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'US National']
        self.assertEqual(exp_locations, list(act_dict.keys()))

        # spot-check one location's targets
        exp_targets = ['1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead', 'Season onset', 'Season peak percentage',
                       'Season peak week']
        self.assertEqual(exp_targets, list(act_dict['US National'].keys()))

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
        self.assertEqual(exp_bins, act_bins)


    def test_forecast_for_time_zero(self):
        time_zero = TimeZero.objects.create(project=None,
                                            timezero_date=datetime.date.today(),  # todo str()?
                                            data_version_date=None)
        self.assertEqual(None, self.forecast_model.forecast_for_time_zero(time_zero))

        forecast2 = self.forecast_model.load_forecast(Path('EW1-KoTsarima-2017-01-17.csv'), time_zero)
        self.assertEqual(forecast2, self.forecast_model.forecast_for_time_zero(time_zero))

        forecast2.delete()
