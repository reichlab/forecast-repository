import datetime
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from django.test import TestCase

from forecast_app.models import Project, TimeZero, Score
from forecast_app.models.forecast import Forecast
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.tests.test_scores import _make_thai_log_score_project
from utils.cdc import load_cdc_csv_forecast_file, make_cdc_locations_and_targets
from utils.forecast import json_io_dict_from_forecast, load_predictions_from_json_io_dict
from utils.make_thai_moph_project import load_cdc_csv_forecasts_from_dir
from utils.project import load_truth_data


class ForecastTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()
        make_cdc_locations_and_targets(cls.project)
        cls.forecast_model = ForecastModel.objects.create(project=cls.project)
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date=datetime.date(2017, 1, 1))
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
        cls.forecast = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, cls.time_zero)


    def test_load_forecast_created_at_field(self):
        project2 = Project.objects.create()
        make_cdc_locations_and_targets(project2)
        time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date.today())
        forecast_model2 = ForecastModel.objects.create(project=project2)
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        forecast2 = load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, time_zero2)
        self.assertIsNotNone(forecast2.created_at)


    def test_load_forecast(self):
        self.assertEqual(1, len(self.forecast_model.forecasts.all()))
        self.assertIsInstance(self.forecast, Forecast)
        self.assertEqual('EW1-KoTstable-2017-01-17.csv', self.forecast.source)
        self.assertEqual(8019, self.forecast.get_num_rows())  # excluding header

        # check 'US National' targets
        us_nat_points_qs = self.forecast.point_prediction_qs() \
            .filter(location__name='US National') \
            .order_by('location__name', 'target__name') \
            .values_list('location__name', 'target__name', 'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
        us_nat_bin_qs = self.forecast.bin_distribution_qs() \
            .filter(location__name='US National') \
            .order_by('location__name', 'target__name') \
            .values_list('location__name', 'target__name', 'prob', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')

        # spot-check a few point rows
        exp_points = [('US National', '1 wk ahead', None, 3.00101461253164, None, None, None),  # _i, _f, _t, _d, _b
                      ('US National', '2 wk ahead', None, 2.72809349594878, None, None, None),
                      ('US National', '3 wk ahead', None, 2.5332588357381, None, None, None),
                      ('US National', '4 wk ahead', None, 2.42985946508278, None, None, None),
                      ('US National', 'Season onset', None, None, '2016-12-12', None, None),
                      ('US National', 'Season peak percentage', None, 3.30854920241938, None, None, None),
                      ('US National', 'Season peak week', None, None, None, datetime.date(2017, 1, 30), None)]
        act_points_qs = self.forecast.point_prediction_qs() \
            .filter(location__name='US National') \
            .order_by('location__name', 'target__name') \
            .values_list('location__name', 'target__name', 'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
        self.assertEqual(exp_points, list(act_points_qs))

        # test empty file
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bad_file_no_header-2017-01-17.csv')  # EW01 2017?
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn('empty file', str(context.exception))

        # test a bad data file header
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bad_file_header-2017-01-17.csv')  # EW01 2017?
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn('invalid header', str(context.exception))

        # test load_forecast() with timezero not in the project
        project2 = Project.objects.create()  # no TimeZeros
        make_cdc_locations_and_targets(project2)

        forecast_model2 = ForecastModel.objects.create(project=project2)
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, self.time_zero)
        self.assertIn("time_zero was not in project", str(context.exception))


    def test_load_forecast_skips_zero_values(self):
        forecast2 = Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)
        with open('forecast_app/tests/predictions/cdc_zero_probabilities.json') as fp:
            json_io_dict = json.load(fp)
        load_predictions_from_json_io_dict(forecast2, json_io_dict)

        # test points: both should be there (points are not skipped)
        self.assertEqual(2, forecast2.point_prediction_qs().count())

        # test bins: 2 out of 6 have zero probabilities and should be skipped
        exp_bins = [('HHS Region 1', '1 wk ahead', 0.2, None, 0.1, None, None, None),  # _i, _f, _t, _d, _b
                    ('HHS Region 1', '1 wk ahead', 0.8, None, 0.2, None, None, None),
                    ('US National', 'Season onset', 0.1, None, None, 'cat2', None, None),
                    ('US National', 'Season onset', 0.9, None, None, 'cat3', None, None)]
        bin_distribution_qs = forecast2.bin_distribution_qs() \
            .order_by('location__name', 'target__name') \
            .values_list('location__name', 'target__name', 'prob', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')
        self.assertEqual(4, bin_distribution_qs.count())
        self.assertEqual(exp_bins, list(bin_distribution_qs))


    def test_load_forecast_thai_point_json_type(self):
        # exposes a bug where 0-valued bin points are loaded as null
        project2, forecast_model2, forecast2, time_zero2 = _make_thai_log_score_project()
        act_json_io_dict = json_io_dict_from_forecast(forecast2)  # recall json predictions are sorted by location, type
        exp_pred_dict = {'location': 'TH01', 'target': '1_biweek_ahead', 'class': 'point', 'prediction': {'value': 0}}
        act_pred_dict = [pred_dict for pred_dict in act_json_io_dict['predictions']
                         if (pred_dict['location'] == 'TH01') and (pred_dict['target'] == '1_biweek_ahead')][0]
        self.assertEqual(exp_pred_dict, act_pred_dict)


    def test_load_forecasts_from_dir(self):
        project2 = Project.objects.create()
        make_cdc_locations_and_targets(project2)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 23),  # 20161023-KoTstable-20161109.cdc.csv
                                data_version_date=None)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 30),  # 20161030-KoTstable-20161114.cdc.csv
                                data_version_date=None)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 11, 6),  # 20161106-KoTstable-20161121.cdc.csv
                                data_version_date=None)
        forecast_model2 = ForecastModel.objects.create(project=project2)

        # copy the two files from 'forecast_app/tests/load_forecasts' to a temp dir, run the loader, and then copy a
        # third file over to test that it skips already-loaded ones
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            test_file_dir = Path('forecast_app/tests/load_forecasts')
            shutil.copy(str(test_file_dir / '20161023-KoTstable-20161109.cdc.csv'), str(temp_dir))
            shutil.copy(str(test_file_dir / '20161030-KoTstable-20161114.cdc.csv'), str(temp_dir))

            forecasts = load_cdc_csv_forecasts_from_dir(forecast_model2, temp_dir, 2016)
            self.assertEqual(2, len(forecasts))
            self.assertEqual(2, len(forecast_model2.forecasts.all()))

            # copy third file and test only new loaded
            shutil.copy(str(test_file_dir / 'third-file/20161106-KoTstable-20161121.cdc.csv'), str(temp_dir))
            forecasts = load_cdc_csv_forecasts_from_dir(forecast_model2, temp_dir, 2016)
            self.assertEqual(1, len(forecasts))


    def test_forecast_data_validation(self):
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bad-point-na-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn("None point values are only valid for 'Season onset' targets", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bin-doesnt-sum-to-one-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn("Bin did not sum to 1.0", str(context.exception))

        # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
        with self.assertRaises(Exception):
            try:
                # date-based Point row w/NA value is OK:
                csv_file_path = Path('forecast_app/tests/EW1-ok-point-na-2017-01-17.csv')  # EW01 2017
                load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
            except:
                pass
            else:
                raise Exception

        self.fail()  # todo xx


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


    def test_forecast_data_and_accessors(self):
        # test points
        point_prediction_qs = self.forecast.point_prediction_qs() \
            .order_by('location__name', 'target__name') \
            .values_list('location__name', 'target__name', 'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
        self.assertEqual(77, point_prediction_qs.count())  # 11 locations x 7 targets x 1 point/location-target pair

        # spot-check a location
        exp_points = [('US National', '1 wk ahead', None, 3.00101461253164, None, None, None),  # _i, _f, _t, _d, _b
                      ('US National', '2 wk ahead', None, 2.72809349594878, None, None, None),
                      ('US National', '3 wk ahead', None, 2.5332588357381, None, None, None),
                      ('US National', '4 wk ahead', None, 2.42985946508278, None, None, None),
                      ('US National', 'Season onset', None, None, '2016-12-12', None, None),
                      ('US National', 'Season peak percentage', None, 3.30854920241938, None, None, None),
                      ('US National', 'Season peak week', None, None, None, datetime.date(2017, 1, 30), None)]
        # re: EW translations to absolute dates:
        # - self.forecast's season_start_year is 2016
        # - EW '50.0012056690978' within that file rounds down to EW50
        # - EW50 in season_start_year is 2016 -> EW50 2016 (50 >= SEASON_START_EW_NUMBER -> is in season_start_year)
        # - EW50 2016 is the week ending Saturday 12/17/2016. that week's Monday is 12/12/2016, formatted as
        #   '2016-12-12'. it says a string b/c the target is nominal
        # similarly:
        # - EW '4.96302456525203' within that file rounds up to EW05
        # - EW05 in season_start_year is 2016 -> EW05 2017 (5 < SEASON_START_EW_NUMBER -> is in season_start_year + 1)
        # - EW05 2017 is the week ending Saturday 2/4/2017. that week's Monday is 1/30/2017, formatted as '2017-01-30'.
        #   it is a datetime.date b/c the target is date
        self.assertEqual(exp_points, list(point_prediction_qs.filter(location__name='US National')))

        # test bins
        bin_distribution_qs = self.forecast.bin_distribution_qs() \
            .order_by('location__name', 'target__name') \
            .values_list('location__name', 'target__name', 'prob', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')
        self.assertEqual(7942, bin_distribution_qs.count())

        # spot-check a location and date-based target ('Season onset') which is actually nominal (text), but contains
        # date strings (due to 'none' values not being date objects)
        exp_bins = [
            # EW40 2016. Sat end: 10/8/2016 -> Mon: 10/3/2016:
            ('US National', 'Season onset', 1.95984004521967e-05, '2016-10-03'),
            ('US National', 'Season onset', 1.46988003391476e-05, '2016-10-10'),
            ('US National', 'Season onset', 6.98193016109509e-06, '2016-10-17'),
            ('US National', 'Season onset', 3.79719008761312e-06, '2016-10-24'),
            ('US National', 'Season onset', 4.28715009891804e-06, '2016-10-31'),
            ('US National', 'Season onset', 1.59237003674098e-05, '2016-11-07'),
            ('US National', 'Season onset', 3.0989970715036e-05, '2016-11-14'),
            ('US National', 'Season onset', 5.3895601243541e-05, '2016-11-21'),
            ('US National', 'Season onset', 7.49638817296525e-05, '2016-11-28'),
            ('US National', 'Season onset', 0.000110241002543607, '2016-12-05'),
            ('US National', 'Season onset', 0.998941808865584, '2016-12-12'),
            ('US National', 'Season onset', 0.000165973953829541, '2016-12-19'),
            # EW52 2016. Sat end: 12/31/2016 -> Mon: 12/26/2016:
            ('US National', 'Season onset', 0.000147110493394302, '2016-12-26'),
            # EW01 2017. Sat end: 1/7/2017 -> Mon: 1/2/2017:
            ('US National', 'Season onset', 9.7624532252505e-05, '2017-01-02'),
            ('US National', 'Season onset', 5.41405812491935e-05, '2017-01-09'),
            ('US National', 'Season onset', 3.8951820898741e-05, '2017-01-16'),
            ('US National', 'Season onset', 4.99759211531016e-05, '2017-01-23'),
            ('US National', 'Season onset', 4.09116609439607e-05, '2017-01-30'),
            ('US National', 'Season onset', 3.60120608309115e-05, '2017-02-06'),
            ('US National', 'Season onset', 2.51104505793771e-05, '2017-02-13'),
            ('US National', 'Season onset', 2.09457904832853e-05, '2017-02-20'),
            ('US National', 'Season onset', 1.99658704606754e-05, '2017-02-27'),
            ('US National', 'Season onset', 1.6536150381541e-05, '2017-03-06'),
            ('US National', 'Season onset', 6.00201013848525e-06, '2017-03-13'),
            ('US National', 'Season onset', 2.20482005087213e-06, '2017-03-20'),
            ('US National', 'Season onset', 3.6747000847869e-07, '2017-03-27'),
            ('US National', 'Season onset', 1.22490002826229e-07, '2017-04-03'),
            ('US National', 'Season onset', 1.22490002826229e-07, '2017-04-10'),
            ('US National', 'Season onset', 1.22490002826229e-07, '2017-04-17'),
            ('US National', 'Season onset', 1.22490002826229e-07, '2017-04-24'),
            ('US National', 'Season onset', 1.22490002826229e-07, '2017-05-01'),
            ('US National', 'Season onset', 1.22490002826229e-07, '2017-05-08'),
            # EW20 2017. Sat end: 5/20/2017 -> Mon: 5/15/2017:
            ('US National', 'Season onset', 1.22490002826229e-07, '2017-05-15'),
            ('US National', 'Season onset', 1.22490002826229e-07, 'none')]
        bin_distribution_qs = self.forecast.bin_distribution_qs() \
            .filter(location__name='US National', target__name='Season onset') \
            .order_by('location__name', 'target__name', 'cat_t') \
            .values_list('location__name', 'target__name', 'prob', 'cat_t')
        self.assertEqual(34, bin_distribution_qs.count())
        self.assertEqual(exp_bins, list(bin_distribution_qs))

        # spot-check a location an an actual date-based target ('Season peak week')
        exp_bins = [
            # EW40 2016. Sat end: 10/8/2016 -> Mon: 10/3/2016:
            ('US National', 'Season peak week', 3.88312283001796e-05, datetime.date(2016, 10, 3)),
            ('US National', 'Season peak week', 4.32690829630572e-05, datetime.date(2016, 10, 10)),
            ('US National', 'Season peak week', 4.27143511301975e-05, datetime.date(2016, 10, 17)),
            ('US National', 'Season peak week', 4.82616694587946e-05, datetime.date(2016, 10, 24)),
            ('US National', 'Season peak week', 3.66123009687408e-05, datetime.date(2016, 10, 31)),
            ('US National', 'Season peak week', 3.16197144730033e-05, datetime.date(2016, 11, 7)),
            ('US National', 'Season peak week', 2.49629324786868e-05, datetime.date(2016, 11, 14)),
            ('US National', 'Season peak week', 4.99258649573737e-05, datetime.date(2016, 11, 21)),
            ('US National', 'Season peak week', 8.48739704275352e-05, datetime.date(2016, 11, 28)),
            ('US National', 'Season peak week', 0.000148113399373542, datetime.date(2016, 12, 5)),
            ('US National', 'Season peak week', 0.000217454878481005, datetime.date(2016, 12, 12)),
            ('US National', 'Season peak week', 0.000290124748585627, datetime.date(2016, 12, 19)),
            # EW52 2016. Sat end: 12/31/2016 -> Mon: 12/26/2016:
            ('US National', 'Season peak week', 0.183889955515593, datetime.date(2016, 12, 26)),
            # EW01 2017. Sat end: 1/7/2017 -> Mon: 1/2/2071:
            ('US National', 'Season peak week', 0.000328955976885807, datetime.date(2017, 1, 2)),
            ('US National', 'Season peak week', 0.0813603183066327, datetime.date(2017, 1, 9)),
            ('US National', 'Season peak week', 0.113514362560564, datetime.date(2017, 1, 16)),
            ('US National', 'Season peak week', 0.0918622520724573, datetime.date(2017, 1, 23)),
            ('US National', 'Season peak week', 0.0636838880421198, datetime.date(2017, 1, 30)),
            ('US National', 'Season peak week', 0.0985233865781112, datetime.date(2017, 2, 6)),
            ('US National', 'Season peak week', 0.104099128806362, datetime.date(2017, 2, 13)),
            ('US National', 'Season peak week', 0.0403122043568698, datetime.date(2017, 2, 20)),
            ('US National', 'Season peak week', 0.0641961804893339, datetime.date(2017, 2, 27)),
            ('US National', 'Season peak week', 0.0951013531890285, datetime.date(2017, 3, 6)),
            ('US National', 'Season peak week', 0.025085879433318, datetime.date(2017, 3, 13)),
            ('US National', 'Season peak week', 0.0182445605042546, datetime.date(2017, 3, 20)),
            ('US National', 'Season peak week', 0.0103074209280581, datetime.date(2017, 3, 27)),
            ('US National', 'Season peak week', 0.00205822405270794, datetime.date(2017, 4, 3)),
            ('US National', 'Season peak week', 0.0018802440369379, datetime.date(2017, 4, 10)),
            ('US National', 'Season peak week', 0.00102908781284421, datetime.date(2017, 4, 17)),
            ('US National', 'Season peak week', 0.000848733083646314, datetime.date(2017, 4, 24)),
            ('US National', 'Season peak week', 0.00133599391065648, datetime.date(2017, 5, 1)),
            ('US National', 'Season peak week', 0.000699739274163662, datetime.date(2017, 5, 8)),
            # EW20 2017. Sat end: 5/20/2017 -> Mon: 5/15/2017:
            ('US National', 'Season peak week', 0.000581366927856728, datetime.date(2017, 5, 15))]
        bin_distribution_qs = self.forecast.bin_distribution_qs() \
            .filter(location__name='US National', target__name='Season peak week') \
            .order_by('location__name', 'target__name', 'cat_d') \
            .values_list('location__name', 'target__name', 'prob', 'cat_d')
        self.assertEqual(33, bin_distribution_qs.count())
        self.assertEqual(exp_bins, list(bin_distribution_qs))


    def test_forecast_delete(self):
        # add a second forecast, check its rows were added, delete it, and test that the data was deleted (via CASCADE)
        self.assertEqual(1, self.forecast_model.forecasts.count())  # from setUpTestData()
        self.assertEqual(8019, self.forecast.get_num_rows())  # ""

        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')  # EW01 2017
        forecast2 = load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertEqual(2, self.forecast_model.forecasts.count())  # includes new
        self.assertEqual(5237, forecast2.get_num_rows())  # 8019 total rows - 2782 zero-valued bin rows = 5237 non-zero
        self.assertEqual(8019, self.forecast.get_num_rows())  # didn't change

        forecast2.delete()
        self.assertEqual(1, self.forecast_model.forecasts.count())  # back to one
        self.assertEqual(0, forecast2.get_num_rows())  # cascaded DELETE


    def test_forecast_for_time_zero(self):
        time_zero = TimeZero.objects.create(project=self.project,
                                            timezero_date=datetime.date.today(),
                                            data_version_date=None)
        self.assertEqual(None, self.forecast_model.forecast_for_time_zero(time_zero))

        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17.csv')  # EW01 2017
        forecast2 = load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, time_zero)
        self.assertEqual(forecast2, self.forecast_model.forecast_for_time_zero(time_zero))

        forecast2.delete()


    def test_model_score_change_forecasts(self):
        # creating a new model should set its score_change.changed_at
        project2 = Project.objects.create()
        make_cdc_locations_and_targets(project2)
        time_zero = TimeZero.objects.create(project=project2, timezero_date=datetime.date.today())
        forecast_model2 = ForecastModel.objects.create(project=project2)
        self.assertIsInstance(forecast_model2.score_change.changed_at, datetime.datetime)

        # adding a forecast should update its model's score_change.changed_at
        before_changed_at = forecast_model2.score_change.changed_at
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        forecast2 = load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, time_zero)
        self.assertNotEqual(before_changed_at, forecast_model2.score_change.changed_at)
        self.assertLess(before_changed_at, forecast_model2.score_change.changed_at)  # was updated later

        # deleting a forecast should update its model's score_change.changed_at
        before_changed_at = forecast_model2.score_change.changed_at
        forecast2.delete()
        self.assertNotEqual(before_changed_at, forecast_model2.score_change.changed_at)
        self.assertLess(before_changed_at, forecast_model2.score_change.changed_at)  # was updated later

        # bulk-deleting a model's forecasts will update its score_change.changed_at. (this basically tests that a signal
        # is used instead of a customized delete() - see set_model_changed_at() comment
        for _ in range(2):
            csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, time_zero)
        before_changed_at = forecast_model2.score_change.changed_at
        forecast_model2.forecasts.all().delete()
        self.assertNotEqual(before_changed_at, forecast_model2.score_change.changed_at)
        self.assertLess(before_changed_at, forecast_model2.score_change.changed_at)  # was updated later


    def test_model_score_change_truths(self):
        project2 = Project.objects.create()
        make_cdc_locations_and_targets(project2)
        # adding project truth should update all of its models' score_change.changed_at. test with no models -> ensure
        # Project._update_model_score_changes() is called
        with patch('forecast_app.models.Project._update_model_score_changes') as update_mock:
            load_truth_data(project2, Path('forecast_app/tests/truth_data/truths-ok.csv'))
            self.assertEqual(2, update_mock.call_count)  # called once each: delete_truth_data(), load_truth_data()

        # adding project truth should update all of its models' score_change.changed_at. test with one model
        forecast_model2 = ForecastModel.objects.create(project=project2)
        before_changed_at = forecast_model2.score_change.changed_at
        load_truth_data(project2, Path('forecast_app/tests/truth_data/truths-ok.csv'))
        # refresh_from_db() per https://stackoverflow.com/questions/35330693/django-testcase-not-saving-my-models :
        forecast_model2.score_change.refresh_from_db()
        self.assertNotEqual(before_changed_at, forecast_model2.score_change.changed_at)

        # deleting project truth should update all of its models' score_change.changed_at
        before_changed_at = forecast_model2.score_change.changed_at
        project2.delete_truth_data()
        forecast_model2.score_change.refresh_from_db()
        self.assertNotEqual(before_changed_at, forecast_model2.score_change.changed_at)


    def test_enqueue_update_scores_for_all_models(self):
        # tests that Score.enqueue_update_scores_for_all_models() should only enqueue scores for changed models

        # test that with ModelScoreChanges but no ScoreLastUpdate, all Score/ForecastModel pairs are updated
        with patch('rq.queue.Queue.enqueue') as enqueue_mock:
            Score.enqueue_update_scores_for_all_models(is_only_changed=True)
            self.assertEqual(5, enqueue_mock.call_count)  # 5 scores * 1 model

        # make all ScoreLastUpdates be after self.forecast_model's update, which means none should update
        Score.ensure_all_scores_exist()
        for score in Score.objects.all():
            score.set_last_update_for_forecast_model(self.forecast_model)
        with patch('rq.queue.Queue.enqueue') as enqueue_mock:
            Score.enqueue_update_scores_for_all_models(is_only_changed=True)
            enqueue_mock.assert_not_called()

        # same, but pass is_only_changed=False -> all Score/ForecastModel pairs should update
        with patch('rq.queue.Queue.enqueue') as enqueue_mock:
            Score.enqueue_update_scores_for_all_models(is_only_changed=False)
            self.assertEqual(5, enqueue_mock.call_count)

        # loading truth should result in all Score/ForecastModel pairs being updated
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/truths-ok.csv'))
        with patch('rq.queue.Queue.enqueue') as enqueue_mock:
            Score.enqueue_update_scores_for_all_models(is_only_changed=True)
            self.assertEqual(5, enqueue_mock.call_count)
