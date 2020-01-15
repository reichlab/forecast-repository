import datetime
from pathlib import Path

import pymmwr
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import load_cdc_csv_forecast_file, make_cdc_locations_and_targets, season_start_year_from_ew_and_year
from utils.make_thai_moph_project import cdc_csv_filename_components


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


class UtilsTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()
        make_cdc_locations_and_targets(cls.project)

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)

        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
        season_start_year = season_start_year_from_ew_and_year(1, 2017)
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 1)))
        cls.forecast1 = load_cdc_csv_forecast_file(season_start_year, cls.forecast_model, csv_file_path, time_zero)

        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW2-KoTstable-2017-01-23.csv')  # EW02 2017
        season_start_year = season_start_year_from_ew_and_year(1, 2017)
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 2)))
        cls.forecast2 = load_cdc_csv_forecast_file(season_start_year, cls.forecast_model, csv_file_path, time_zero)

        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW51-KoTstable-2017-01-03.csv')  # EW51 2016
        season_start_year = season_start_year_from_ew_and_year(51, 2016)
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 51)))
        cls.forecast3 = load_cdc_csv_forecast_file(season_start_year, cls.forecast_model, csv_file_path, time_zero)

        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW52-KoTstable-2017-01-09.csv')  # EW52 2016
        season_start_year = season_start_year_from_ew_and_year(52, 2016)
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 52)))
        cls.forecast4 = load_cdc_csv_forecast_file(season_start_year, cls.forecast_model, csv_file_path, time_zero)


    def test_name_components_from_cdc_csv_filename(self):
        filename_to_exp_component_tuples = {
            '20170419-gam_lag1_tops3-20170516.cdc.csv':
                (datetime.date(2017, 4, 19), 'gam_lag1_tops3', datetime.date(2017, 5, 16)),
            '20161023-KoTstable-20161109.cdc.csv':
                (datetime.date(2016, 10, 23), 'KoTstable', datetime.date(2016, 11, 9)),
            '20161023-KoTstable.cdc.csv':
                (datetime.date(2016, 10, 23), 'KoTstable', None),

            '': None,
            '20170419-gam_lag1_tops3-20170516.csv': None,
            'gam_lag1_tops3-20170516.csv': None,
            '-gam_lag1_tops3-20170516.cdc.csv': None,
            '20170419--.cdc.csv': None,
            '20170419-.cdc.csv': None,
            '20170419.cdc.csv': None,
            '20170419-gam_lag1_tops3-.cdc.csv': None,
            '20170419-gam-lag1-tops3-20170516.cdc.csv': None,
            '20170419-gam/lag1*tops3-20170516.cdc.csv': None,
        }
        for cdc_csv_filename, exp_components in filename_to_exp_component_tuples.items():
            self.assertEqual(exp_components, cdc_csv_filename_components(cdc_csv_filename))
