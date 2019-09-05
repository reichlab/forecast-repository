import csv
import datetime
import json
from pathlib import Path

import pymmwr
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import epi_week_filename_components_2016_2017_flu_contest, epi_week_filename_components_ensemble, \
    load_cdc_csv_forecast_file, cdc_csv_filename_components, first_model_subdirectory, cdc_cvs_rows_from_json_io_dict
from utils.forecast import json_io_dict_from_forecast
from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, season_start_year_for_date


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

        # EW1-KoTstable-2017-01-17.csv -> EW1 in 2017:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 1)))
        cls.forecast1 = load_cdc_csv_forecast_file(
            cls.forecast_model, Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), time_zero)

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


    def test_cdc_csv_rows_from_json_io_dict(self):
        # no meta
        with self.assertRaises(RuntimeError) as context:
            cdc_cvs_rows_from_json_io_dict({})
        self.assertIn('no meta section found in json_io_dict', str(context.exception))

        # no meta > targets
        with self.assertRaises(RuntimeError) as context:
            cdc_cvs_rows_from_json_io_dict({'meta': {}})
        self.assertIn('no targets section found in json_io_dict meta section', str(context.exception))

        # no predictions
        with self.assertRaises(RuntimeError) as context:
            cdc_cvs_rows_from_json_io_dict({'meta': {'targets': []}})
        self.assertIn('no predictions section found in json_io_dict', str(context.exception))

        # invalid prediction class
        for invalid_prediction_class in ['Binary', 'Named', 'Sample', 'SampleCat']:  # ok: 'BinCat', 'BinLwr', 'Point'
            with self.assertRaises(RuntimeError) as context:
                json_io_dict = {'meta': {'targets': []},
                                'predictions': [{'class': invalid_prediction_class}]}
                cdc_cvs_rows_from_json_io_dict(json_io_dict)
            self.assertIn('invalid prediction_dict class', str(context.exception))

        # prediction dict target not found in meta > targets
        with open('forecast_app/tests/predictions/predictions-example.json') as fp:
            json_io_dict = json.load(fp)

            # remove invalid prediction classes
            del (json_io_dict['predictions'][6])  # 'SampleCat'
            del (json_io_dict['predictions'][5])  # 'Sample'
            del (json_io_dict['predictions'][3])  # 'Named'
            del (json_io_dict['predictions'][2])  # 'Binary

        with self.assertRaises(RuntimeError) as context:
            # remove arbitrary meta target. doesn't matter b/c all are referenced
            del (json_io_dict['meta']['targets'][0])
            cdc_cvs_rows_from_json_io_dict(json_io_dict)
        self.assertIn('prediction_dict target not found in meta targets', str(context.exception))

        # blue sky: small forecast
        project = Project.objects.create()
        make_cdc_locations_and_targets(project)
        time_zero = TimeZero.objects.create(project=project,
                                            timezero_date=datetime.date(2016, 10, 30),
                                            # 20161030-KoTstable-20161114.cdc.csv {'year': 2016, 'week': 44, 'day': 1}
                                            data_version_date=datetime.date(2016, 10, 29))
        forecast_model = ForecastModel.objects.create(project=project)
        forecast = load_cdc_csv_forecast_file(
            forecast_model, Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv'), time_zero)
        with open(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')) as csv_fp:
            csv_reader = csv.reader(csv_fp, delimiter=',')
            next(csv_reader)  # skip header
            exp_cdc_cvs_rows = list(map(_xform_cdc_csv_row, sorted(csv_reader)))

        json_io_dict = json_io_dict_from_forecast(forecast)
        act_cdc_cvs_rows = sorted(cdc_cvs_rows_from_json_io_dict(json_io_dict))
        self.assertEqual(exp_cdc_cvs_rows, act_cdc_cvs_rows)


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


# test_cdc_csv_rows_from_json_io_dict() helper that transforms expected row values to float() as needed to match actual
def _xform_cdc_csv_row(row):
    location, target, row_type, unit, bin_start_incl, bin_end_notincl, value = row
    if row_type == 'Bin' and unit == 'percent':
        try:
            bin_start_incl = float(bin_start_incl)
            bin_end_notincl = float(bin_end_notincl)
            value = float(value)
        except ValueError:
            pass

    if row_type == 'Bin' and unit == 'week':
        try:
            value = float(value)
        except ValueError:
            pass

    if row_type == 'Point' and unit == 'percent':
        try:
            value = float(value)
        except ValueError:
            pass

    return [location, target, row_type, unit, bin_start_incl, bin_end_notincl, value]
