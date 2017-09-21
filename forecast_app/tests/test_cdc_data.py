import datetime
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project
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
        cls.forecast = cls.forecast_model.load_forecast(Path('EW1-KoTstable-2017-01-17.csv'), None)


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
        # setUpTestData() has already loaded 'EW1-KoTstable-2017-01-17.csv'
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


    def test_project_constraints(self):
        # - models must have same targets, etc.
        # - time_zeros, etc.
        # - all files much match across dirs - recall one had an extra file
        # - all files must have same locations and targets
        # - todo others
        self.fail()  # todo


    def test_forecast_constraints(self):
        # - bins should add to 1.0
        # - point prediction should be within max bin
        # - todo others
        self.fail()  # todo
