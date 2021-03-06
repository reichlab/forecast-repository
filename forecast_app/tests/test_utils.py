import datetime
from pathlib import Path

import pymmwr
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc_io import load_cdc_csv_forecast_file, make_cdc_units_and_targets
from utils.make_thai_moph_project import cdc_csv_filename_components


class UtilsTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()
        make_cdc_units_and_targets(cls.project)

        cls.forecast_model = ForecastModel.objects.create(project=cls.project, name='name', abbreviation='abbrev')

        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 1)))
        cls.forecast1 = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, time_zero)

        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW2-KoTstable-2017-01-23.csv')  # EW02 2017
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 2)))
        cls.forecast2 = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, time_zero)

        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW51-KoTstable-2017-01-03.csv')  # EW51 2016
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 51)))
        cls.forecast3 = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, time_zero)

        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW52-KoTstable-2017-01-09.csv')  # EW52 2016
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 52)))
        cls.forecast4 = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, time_zero)


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
