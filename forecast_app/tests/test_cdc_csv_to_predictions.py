import datetime
import json
from pathlib import Path

from django.test import TestCase

from forecast_app.models import PointPrediction, Project, ForecastModel, TimeZero, Forecast
from utils.cdc import json_io_dict_from_cdc_csv_file, monday_date_from_ew_and_season_start_year, \
    make_cdc_locations_and_targets
from utils.forecast import load_predictions_from_json_io_dict, PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS


class CdcCsvToPredictionsTestCase(TestCase):
    """
    Tests loading and converting CDC CSV files into prediction dicts and ultimately into data rows.
    """


    @classmethod
    def setUpTestData(cls):
        cls.cdc_csv_path = Path('forecast_app/tests/cdc-csv-predictions/EW01-2011-ReichLab_kde_US_National.csv')


    def test_monday_date_from_ew_and_season_start_year(self):
        ew_week_ss_year_exp_monday_date = [(1, 2010, datetime.date(2011, 1, 3)),  # Monday of: EW01 2011
                                           (29, 2010, datetime.date(2011, 7, 18)),  # "" EW29 2011
                                           (30, 2010, datetime.date(2010, 7, 26)),  # "" EW30 2010
                                           (31, 2010, datetime.date(2010, 8, 2)),  # "" EW31 2010
                                           (52, 2010, datetime.date(2010, 12, 27)),  # "" EW52 2010
                                           (1, 2011, datetime.date(2012, 1, 2)),  # "" EW01 2012
                                           (29, 2011, datetime.date(2012, 7, 16)),  # "" EW29 2012
                                           (30, 2011, datetime.date(2011, 7, 25)),  # "" EW30 2011
                                           (31, 2011, datetime.date(2011, 8, 1)),  # "" EW31 2011
                                           (52, 2011, datetime.date(2011, 12, 26))]  # "" EW52 2011
        for ew_week, season_start_year, exp_monday_date in ew_week_ss_year_exp_monday_date:
            self.assertEqual(exp_monday_date, monday_date_from_ew_and_season_start_year(ew_week, season_start_year))


    def test_json_io_dict_from_cdc_csv_file_points(self):
        cdc_csv_path = Path('forecast_app/tests/cdc-csv-predictions/EW01-2011-ReichLab_kde_US_National_points.csv')
        exp_json_path = Path('forecast_app/tests/cdc-csv-predictions/EW01-2011-ReichLab_kde_US_National.json')
        with open(cdc_csv_path) as cdc_csv_fp, \
                open(exp_json_path) as exp_json_fp:
            exp_json_io_dict = json.load(exp_json_fp)
            exp_predictions = [prediction_dict for prediction_dict in exp_json_io_dict['predictions']
                               if prediction_dict['class'] == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction]]
            act_json_io_dict = json_io_dict_from_cdc_csv_file(2011, cdc_csv_fp)
            self.assertEqual(7, len(act_json_io_dict['predictions']))
            self.assertEqual(exp_predictions, act_json_io_dict['predictions'])


    def test_json_io_dict_from_cdc_csv_file(self):
        # from EW01-2011-ReichLab_kde_US_National.csv
        exp_json_path = Path('forecast_app/tests/cdc-csv-predictions/EW01-2011-ReichLab_kde_US_National.json')
        with open(self.cdc_csv_path) as cdc_csv_fp, \
                open(exp_json_path) as exp_json_fp:
            exp_json_io_dict = json.load(exp_json_fp)
            act_json_io_dict = json_io_dict_from_cdc_csv_file(2011, cdc_csv_fp)
            self.assertEqual(exp_json_io_dict, act_json_io_dict)

        # test a larger csv file
        with open('forecast_app/tests/cdc-csv-predictions/EW01-2011-ReichLab_kde.csv') as cdc_csv_fp:
            act_json_io_dict = json_io_dict_from_cdc_csv_file(2011, cdc_csv_fp)
            # each location/target pair has 2 prediction dicts: one point and one bin
            # there are 11 locations and 7 targets = 77 * 2 = 154 dicts total
            self.assertEqual(154, len(act_json_io_dict['predictions']))


    def test_load_predictions_from_cdc_csv_file(self):
        # sanity-check that the predictions get converted and then loaded into the database
        project = Project.objects.create()
        make_cdc_locations_and_targets(project)

        forecast_model = ForecastModel.objects.create(project=project)
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        cdc_csv_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        forecast = Forecast.objects.create(forecast_model=forecast_model, source=cdc_csv_path.name, time_zero=time_zero)

        with open(self.cdc_csv_path) as cdc_csv_fp:
            json_io_dict = json_io_dict_from_cdc_csv_file(2011, cdc_csv_fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict, False)
        self.assertEqual(729, forecast.get_num_rows())
        self.assertEqual(0, forecast.named_distribution_qs().count())
        self.assertEqual(0, forecast.sample_distribution_qs().count())
        self.assertEqual(7, forecast.point_prediction_qs().count())
        self.assertEqual(722, forecast.bin_distribution_qs().count())  # 729 - 7
