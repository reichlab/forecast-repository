import datetime
from pathlib import Path

import pymmwr
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import epi_week_filename_components_2016_2017_flu_contest, epi_week_filename_components_ensemble, \
    load_cdc_csv_forecast_file, cdc_csv_filename_components, first_model_subdirectory
from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, season_start_year_for_date, \
    CDC_CONFIG_DICT


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


class UtilitiesTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        make_cdc_locations_and_targets(cls.project)

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)

        # EW1-KoTstable-2017-01-17.csv -> EW1 in 2017:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 1)))
        cls.forecast1 = load_cdc_csv_forecast_file(cls.forecast_model, Path(
            'forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), time_zero)

        # EW2-KoTstable-2017-01-23.csv -> EW2 in 2017:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 2)))
        cls.forecast2 = load_cdc_csv_forecast_file(cls.forecast_model, Path(
            'forecast_app/tests/model_error/ensemble/EW2-KoTstable-2017-01-23.csv'), time_zero)

        # EW51-KoTstable-2017-01-03.csv -> EW51 in 2016:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 51)))
        cls.forecast3 = load_cdc_csv_forecast_file(cls.forecast_model, Path(
            'forecast_app/tests/model_error/ensemble/EW51-KoTstable-2017-01-03.csv'), time_zero)

        # EW52-KoTstable-2017-01-09.csv -> EW52 in 2016:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 52)))
        cls.forecast4 = load_cdc_csv_forecast_file(cls.forecast_model, Path(
            'forecast_app/tests/model_error/ensemble/EW52-KoTstable-2017-01-09.csv'), time_zero)


    def test_epi_week_filename_components_2016_2017_flu_contest(self):
        filename_components_tuples = (('EW1-KoTstable-2017-01-17.csv', (1, 'KoTstable', datetime.date(2017, 1, 17))),
                                      ('-KoTstable-2017-01-17.csv', None),
                                      ('EW1--2017-01-17.csv', None),
                                      ('EW1-KoTstable-2017-01-17.txt', None))
        for filename, components in filename_components_tuples:
            self.assertEqual(components, epi_week_filename_components_2016_2017_flu_contest(filename))


    def test_epi_week_filename_components_ensemble(self):
        filename_components_tuples = (('EW01-2011-CU_EAKFC_SEIRS.csv', (1, 2011, 'CU_EAKFC_SEIRS')),
                                      ('EW01-2011-CUBMA.csv', (1, 2011, 'CUBMA')),
                                      ('-2011-CUBMA.csv', None),
                                      ('EW01--2011-CUBMA.csv', None),
                                      ('EW01-CUBMA.csv', None),
                                      ('EW01-2011.csv', None),
                                      ('EW01-2011-CUBMA.txt', None))
        for filename, components in filename_components_tuples:
            self.assertEqual(components, epi_week_filename_components_ensemble(filename))


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


    def test_first_model_subdirectory(self):
        no_cdc_files_path = Path('forecast_app/tests/first_model_subdirs_no')
        self.assertIsNone(first_model_subdirectory(no_cdc_files_path))

        yes_cdc_files_path = Path('forecast_app/tests/first_model_subdirs_yes')
        self.assertEqual(Path('forecast_app/tests/first_model_subdirs_yes/model1'),
                         first_model_subdirectory(yes_cdc_files_path))


    def test_season_start_year_for_date(self):
        date_exp_season_start_year = [
            (pymmwr.mmwr_week_to_date(2016, 29), 2015),
            (pymmwr.mmwr_week_to_date(2016, 30), 2016),
            (pymmwr.mmwr_week_to_date(2016, 52), 2016),
            (pymmwr.mmwr_week_to_date(2017, 1), 2016),
            (pymmwr.mmwr_week_to_date(2017, 29), 2016),
            (pymmwr.mmwr_week_to_date(2017, 30), 2017),
        ]
        for date, exp_season_start_year in date_exp_season_start_year:
            self.assertEqual(exp_season_start_year, season_start_year_for_date(date))
