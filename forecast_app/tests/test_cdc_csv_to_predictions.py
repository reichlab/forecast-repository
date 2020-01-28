import datetime
import json
from pathlib import Path

from django.test import TestCase

from forecast_app.models import PointPrediction, Project, ForecastModel, TimeZero, Forecast
from utils.cdc import ew_and_year_from_cdc_file_name, season_start_year_from_ew_and_year, \
    json_io_dict_from_cdc_csv_file, monday_date_from_ew_and_season_start_year, make_cdc_locations_and_targets
from utils.forecast import load_predictions_from_json_io_dict, PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS


class CdcCsvToPredictionsTestCase(TestCase):
    """
    Tests loading and converting CDC CSV files into prediction dicts and ultimately into data rows.
    """


    @classmethod
    def setUpTestData(cls):
        cls.cdc_csv_path = Path('forecast_app/tests/cdc-csv-predictions/EW01-2011-ReichLab_kde_US_National.csv')


    def test_ew_and_year_from_cdc_file_name(self):
        components = ew_and_year_from_cdc_file_name(self.cdc_csv_path.name)
        self.assertEqual((1, 2011), components)


    def test_season_start_year_from_ew_and_year(self):
        # recall: SEASON_START_EW_NUMBER = 30
        self.assertEqual(2009, season_start_year_from_ew_and_year(1, 2010))  # ... 2009/2010 season
        self.assertEqual(2009, season_start_year_from_ew_and_year(2, 2010))
        self.assertEqual(2009, season_start_year_from_ew_and_year(29, 2010))  # end 2009/2010 season

        self.assertEqual(2010, season_start_year_from_ew_and_year(30, 2010))  # start 2010/2011 season
        self.assertEqual(2010, season_start_year_from_ew_and_year(31, 2010))
        self.assertEqual(2010, season_start_year_from_ew_and_year(51, 2010))
        self.assertEqual(2010, season_start_year_from_ew_and_year(52, 2010))
        self.assertEqual(2010, season_start_year_from_ew_and_year(1, 2011))
        self.assertEqual(2010, season_start_year_from_ew_and_year(2, 2011))
        self.assertEqual(2010, season_start_year_from_ew_and_year(29, 2011))  # end 2010/2011 season

        self.assertEqual(2011, season_start_year_from_ew_and_year(30, 2011))  # start 2011/2012 season
        self.assertEqual(2011, season_start_year_from_ew_and_year(31, 2011))
        self.assertEqual(2011, season_start_year_from_ew_and_year(51, 2011))
        self.assertEqual(2011, season_start_year_from_ew_and_year(52, 2011))  # 2011/2012 season...


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
        with open(cdc_csv_path) as cdc_csv_fp, \
                open('forecast_app/tests/cdc-csv-predictions/EW01-2011-ReichLab_kde_US_National.json') as exp_json_fp:
            exp_json_io_dict = json.load(exp_json_fp)
            exp_predictions = [prediction_dict for prediction_dict in exp_json_io_dict['predictions']
                               if prediction_dict['class'] == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction]]
            ew_and_year = ew_and_year_from_cdc_file_name(cdc_csv_path.name)
            season_start_year = season_start_year_from_ew_and_year(ew_and_year[0], ew_and_year[1])
            act_json_io_dict = json_io_dict_from_cdc_csv_file(season_start_year, cdc_csv_fp)
            self.assertEqual(7, len(act_json_io_dict['predictions']))
            self.assertEqual(exp_predictions, act_json_io_dict['predictions'])


    def test_json_io_dict_from_cdc_csv_file(self):
        with open(self.cdc_csv_path) as cdc_csv_fp, \
                open(self.cdc_csv_path.with_suffix('.json')) as exp_json_fp:
            exp_json_io_dict = json.load(exp_json_fp)
            ew_and_year = ew_and_year_from_cdc_file_name(self.cdc_csv_path.name)
            season_start_year = season_start_year_from_ew_and_year(ew_and_year[0], ew_and_year[1])
            act_json_io_dict = json_io_dict_from_cdc_csv_file(season_start_year, cdc_csv_fp)
            self.assertEqual(exp_json_io_dict, act_json_io_dict)

        # test a larger csv file
        with open('forecast_app/tests/cdc-csv-predictions/EW01-2011-ReichLab_kde.csv') as cdc_csv_fp:
            ew_and_year = ew_and_year_from_cdc_file_name('EW01-2011-ReichLab_kde.csv')
            season_start_year = season_start_year_from_ew_and_year(ew_and_year[0], ew_and_year[1])
            act_json_io_dict = json_io_dict_from_cdc_csv_file(season_start_year, cdc_csv_fp)
            # - 15 prediction dicts per region * 11 regions = 165 dicts total
            # - dicts for each region:
            #   2 per each 'n wk ahead' target (1 bin, 1 point) * 4 'n wk ahead' targets = 8 dicts
            #   1 "Season onset binary" dict
            #   2 "Season onset date" dicts (1 bin, 1 point)
            #   2 "Season peak percentage" dicts (1 bin, 1 point)
            #   2 "Season peak week" dicts (1 bin, 1 point)
            # = 15 dicts total
            self.assertEqual(165, len(act_json_io_dict['predictions']))

            # spot-check EW conversion to Mondays in YYYY_MM_DD_DATE_FORMAT
            self.assertEqual(-1, -2)


    def test_load_predictions_from_cdc_csv_file(self):
        # sanity-check that the predictions get converted and then loaded into the database
        project = Project.objects.create()
        make_cdc_locations_and_targets(project)

        forecast_model = ForecastModel.objects.create(project=project)
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        cdc_csv_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        forecast = Forecast.objects.create(forecast_model=forecast_model, source=cdc_csv_path.name, time_zero=time_zero)

        with open(self.cdc_csv_path) as cdc_csv_fp:
            ew_and_year = ew_and_year_from_cdc_file_name(self.cdc_csv_path.name)
            season_start_year = season_start_year_from_ew_and_year(ew_and_year[0], ew_and_year[1])
            json_io_dict = json_io_dict_from_cdc_csv_file(season_start_year, cdc_csv_fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict)
            self.assertEqual(729, forecast.get_num_rows())
            self.assertEqual(0, forecast.named_distribution_qs().count())
            self.assertEqual(0, forecast.sample_distribution_qs().count())
            self.assertEqual(7, forecast.point_prediction_qs().count())
            self.assertEqual(722, forecast.bin_distribution_qs().count())  # 729 - 7
