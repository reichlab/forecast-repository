import datetime
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project
from forecast_app.models.forecast_model import ForecastModel
from utils.utilities import filename_components, mean_absolute_error


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


class UtilitiesTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.config_dict = {
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
        cls.project = Project.objects.create(config_dict=cls.config_dict)
        cls.project.load_template(Path('2016-2017_submission_template.csv'))

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)
        cls.forecast = cls.forecast_model.load_forecast(Path('model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), None)


    def test_filename_components(self):
        filename_component_tuples = (('EW1-KoTstable-2017-01-17.csv', (1, 'KoTstable', datetime.date(2017, 1, 17))),
                                     ('-KoTstable-2017-01-17.csv', ()),
                                     ('EW1--2017-01-17.csv', ()),
                                     ('EW1-KoTstable--01-17.csv', ()),
                                     ('EW1-KoTstable--01-17.txt', ()))
        for filename, component in filename_component_tuples:
            self.assertEqual(component, filename_components(filename))


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
