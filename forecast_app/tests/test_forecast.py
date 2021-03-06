import datetime
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from django.test import TestCase
from rest_framework.test import APIRequestFactory
from rq.timeouts import JobTimeoutException

from forecast_app.models import Project, TimeZero, Job
from forecast_app.models.forecast import Forecast
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.views import _upload_forecast_worker
from utils.cdc_io import load_cdc_csv_forecast_file, make_cdc_units_and_targets
from utils.forecast import json_io_dict_from_forecast, load_predictions_from_json_io_dict
from utils.make_minimal_projects import _make_docs_project
from utils.make_thai_moph_project import load_cdc_csv_forecasts_from_dir
from utils.project import create_project_from_json
from utils.utilities import get_or_create_super_po_mo_users


class ForecastTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()
        make_cdc_units_and_targets(cls.project)
        cls.forecast_model = ForecastModel.objects.create(project=cls.project, name='name', abbreviation='abbrev')
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date=datetime.date(2017, 1, 1))
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
        cls.forecast = load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, cls.time_zero)
        cls.forecast.issue_date -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        cls.forecast.save()


    def test_load_forecast_created_at_field(self):
        project2 = Project.objects.create()
        make_cdc_units_and_targets(project2)
        time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date.today())
        forecast_model2 = ForecastModel.objects.create(project=project2, name='name', abbreviation='abbrev')
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        forecast2 = load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, time_zero2)
        self.assertIsNotNone(forecast2.created_at)


    def test_load_forecast(self):
        self.assertEqual(1, len(self.forecast_model.forecasts.all()))
        self.assertIsInstance(self.forecast, Forecast)
        self.assertEqual('EW1-KoTstable-2017-01-17.csv', self.forecast.source)
        self.assertEqual(8019, self.forecast.get_num_rows())  # excluding header

        # check 'US National' targets: spot-check a few point rows
        exp_points = [('US National', '1 wk ahead', None, 3.00101461253164, None, None, None),  # _i, _f, _t, _d, _b
                      ('US National', '2 wk ahead', None, 2.72809349594878, None, None, None),
                      ('US National', '3 wk ahead', None, 2.5332588357381, None, None, None),
                      ('US National', '4 wk ahead', None, 2.42985946508278, None, None, None),
                      ('US National', 'Season onset', None, None, '2016-12-12', None, None),
                      ('US National', 'Season peak percentage', None, 3.30854920241938, None, None, None),
                      ('US National', 'Season peak week', None, None, None, datetime.date(2017, 1, 30), None)]
        act_points_qs = self.forecast.point_prediction_qs() \
            .filter(unit__name='US National') \
            .order_by('unit__name', 'target__name') \
            .values_list('unit__name', 'target__name', 'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
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
        make_cdc_units_and_targets(project2)

        forecast_model2 = ForecastModel.objects.create(project=project2, name='name', abbreviation='abbrev')
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, self.time_zero)
        self.assertIn("time_zero was not in project", str(context.exception))


    def test_load_forecast_skips_zero_values(self):
        forecast2 = Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)
        with open('forecast_app/tests/predictions/cdc_zero_probabilities.json') as fp:
            json_io_dict = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict, False)

        # test points: both should be there (points are not skipped)
        self.assertEqual(2, forecast2.point_prediction_qs().count())

        # test bins: 2 out of 6 have zero probabilities and should be skipped
        exp_bins = [('HHS Region 1', '1 wk ahead', 0.2, None, 0.1, None, None, None),  # _i, _f, _t, _d, _b
                    ('HHS Region 1', '1 wk ahead', 0.8, None, 0.2, None, None, None),
                    ('US National', 'Season onset', 0.1, None, None, 'cat2', None, None),
                    ('US National', 'Season onset', 0.9, None, None, 'cat3', None, None)]
        bin_distribution_qs = forecast2.bin_distribution_qs() \
            .order_by('unit__name', 'target__name', 'prob') \
            .values_list('unit__name', 'target__name', 'prob', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')
        self.assertEqual(4, bin_distribution_qs.count())
        self.assertEqual(exp_bins, list(bin_distribution_qs))


    def test_load_forecasts_from_dir(self):
        project2 = Project.objects.create()
        make_cdc_units_and_targets(project2)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 23),  # 20161023-KoTstable-20161109.cdc.csv
                                data_version_date=None)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 30),  # 20161030-KoTstable-20161114.cdc.csv
                                data_version_date=None)
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 11, 6),  # 20161106-KoTstable-20161121.cdc.csv
                                data_version_date=None)
        forecast_model2 = ForecastModel.objects.create(project=project2, name='name', abbreviation='abbrev')

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


    def test_cdc_forecast_data_validation(self):
        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bad-point-na-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn("None point values are only valid for 'Season onset' targets", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            csv_file_path = Path('forecast_app/tests/EW1-bin-doesnt-sum-to-one-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        self.assertIn("Entries in the database rows in the `prob` column must be numbers in [0, 1]",
                      str(context.exception))

        try:
            # date-based Point row w/NA value is OK:
            csv_file_path = Path('forecast_app/tests/EW1-ok-point-na-2017-01-17.csv')  # EW01 2017
            load_cdc_csv_forecast_file(2016, self.forecast_model, csv_file_path, self.time_zero)
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")


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
            .order_by('unit__name', 'target__name') \
            .values_list('unit__name', 'target__name', 'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
        self.assertEqual(77, point_prediction_qs.count())  # 11 units x 7 targets x 1 point/unit-target pair

        # spot-check a unit
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
        self.assertEqual(exp_points, list(point_prediction_qs.filter(unit__name='US National')))

        # test bins
        bin_distribution_qs = self.forecast.bin_distribution_qs() \
            .order_by('unit__name', 'target__name') \
            .values_list('unit__name', 'target__name', 'prob', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')
        self.assertEqual(7942, bin_distribution_qs.count())

        # spot-check a unit and date-based target ('Season onset') which is actually nominal (text), but contains
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
            .filter(unit__name='US National', target__name='Season onset') \
            .order_by('unit__name', 'target__name', 'cat_t') \
            .values_list('unit__name', 'target__name', 'prob', 'cat_t')
        self.assertEqual(34, bin_distribution_qs.count())
        self.assertEqual(exp_bins, list(bin_distribution_qs))

        # spot-check a unit an an actual date-based target ('Season peak week')
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
            .filter(unit__name='US National', target__name='Season peak week') \
            .order_by('unit__name', 'target__name', 'cat_d') \
            .values_list('unit__name', 'target__name', 'prob', 'cat_d')
        self.assertEqual(33, bin_distribution_qs.count())
        self.assertEqual(exp_bins, list(bin_distribution_qs))


    def test_delete_forecast(self):
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


    def test_json_io_dict_from_forecast(self):
        # tests that the json_io_dict_from_forecast()'s output order for SampleDistributions is preserved
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='name', abbreviation='abbrev')
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict_in, False)
            # note: using APIRequestFactory was the only way I could find to pass a request object. o/w you get:
            #   AssertionError: `HyperlinkedIdentityField` requires the request in the serializer context.
            json_io_dict_out = json_io_dict_from_forecast(forecast, APIRequestFactory().request())

        # test round trip. ignore meta, but spot-check it first
        out_meta = json_io_dict_out['meta']
        self.assertEqual({'targets', 'forecast', 'units'}, set(out_meta.keys()))
        self.assertEqual({'cats', 'unit', 'name', 'is_step_ahead', 'type', 'description', 'id', 'url'},
                         set(out_meta['targets'][0].keys()))
        self.assertEqual({'time_zero', 'forecast_model', 'created_at', 'issue_date', 'notes', 'forecast_data', 'source',
                          'id', 'url'},
                         set(out_meta['forecast'].keys()))
        self.assertEqual({'id', 'name', 'url'}, set(out_meta['units'][0].keys()))
        self.assertIsInstance(out_meta['forecast']['time_zero'], dict)  # test that time_zero is expanded, not URL

        del (json_io_dict_in['meta'])
        del (json_io_dict_out['meta'])

        # delete the two zero probability bins in the input (they are discarded when loading predictions)
        # - [11] "unit": "location3", "target": "cases next week", "class": "bin"
        # - [14] "unit": "location1", "target": "season severity", "class": "bin"
        del (json_io_dict_in['predictions'][11]['prediction']['cat'][0])  # 0
        del (json_io_dict_in['predictions'][11]['prediction']['prob'][0])  # 0.0
        del (json_io_dict_in['predictions'][14]['prediction']['cat'][0])  # 'mild'
        del (json_io_dict_in['predictions'][14]['prediction']['prob'][0])  # 0.0

        json_io_dict_in['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
        json_io_dict_out['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))

        self.assertEqual(json_io_dict_out, json_io_dict_in)

        # spot-check some sample predictions
        sample_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                            if (pred_dict['unit'] == 'location3')
                            and (pred_dict['target'] == 'pct next week')
                            and (pred_dict['class'] == 'sample')][0]
        self.assertEqual([2.3, 6.5, 0.0, 10.0234, 0.0001], sample_pred_dict['prediction']['sample'])

        sample_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                            if (pred_dict['unit'] == 'location2')
                            and (pred_dict['target'] == 'season severity')
                            and (pred_dict['class'] == 'sample')][0]
        self.assertEqual(['moderate', 'severe', 'high', 'moderate', 'mild'], sample_pred_dict['prediction']['sample'])

        sample_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                            if (pred_dict['unit'] == 'location1')
                            and (pred_dict['target'] == 'Season peak week')
                            and (pred_dict['class'] == 'sample')][0]
        self.assertEqual(['2020-01-05', '2019-12-15'], sample_pred_dict['prediction']['sample'])

        # spot-check some quantile predictions
        quantile_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                              if (pred_dict['unit'] == 'location2')
                              and (pred_dict['target'] == 'pct next week')
                              and (pred_dict['class'] == 'quantile')][0]
        self.assertEqual([0.025, 0.25, 0.5, 0.75, 0.975], quantile_pred_dict['prediction']['quantile'])
        self.assertEqual([1.0, 2.2, 2.2, 5.0, 50.0], quantile_pred_dict['prediction']['value'])

        quantile_pred_dict = [pred_dict for pred_dict in json_io_dict_out['predictions']
                              if (pred_dict['unit'] == 'location2')
                              and (pred_dict['target'] == 'Season peak week')
                              and (pred_dict['class'] == 'quantile')][0]
        self.assertEqual([0.5, 0.75, 0.975], quantile_pred_dict['prediction']['quantile'])
        self.assertEqual(["2019-12-22", "2019-12-29", "2020-01-05"], quantile_pred_dict['prediction']['value'])


    def test__upload_forecast_worker_bad_inputs(self):
        # test `_upload_forecast_worker()` error conditions. this test is complicated by that function's use of
        # the `job_cloud_file` context manager. solution is per https://stackoverflow.com/questions/60198229/python-patch-context-manager-to-return-object
        with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                patch('utils.forecast.load_predictions_from_json_io_dict') as load_preds_mock, \
                patch('utils.forecast.cache_forecast_metadata') as cache_metatdata_mock:
            job = Job.objects.create()
            job.input_json = {}  # no 'forecast_pk'
            job.save()
            job_cloud_file_mock.return_value.__enter__.return_value = (job, None)  # 2-tuple: (job, cloud_file_fp)
            _upload_forecast_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            load_preds_mock.assert_not_called()

            # test no 'filename'
            job.input_json = {'forecast_pk': None}  # no 'filename'
            job.save()
            _upload_forecast_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            job.refresh_from_db()
            load_preds_mock.assert_not_called()
            self.assertEqual(Job.FAILED, job.status)

            # test bad 'forecast_pk'
            job.input_json = {'forecast_pk': -1, 'filename': None}
            job.save()
            _upload_forecast_worker(job.pk)  # should fail and not call load_predictions_from_json_io_dict()
            job.refresh_from_db()
            load_preds_mock.assert_not_called()
            self.assertEqual(Job.FAILED, job.status)


    def test__upload_forecast_worker_deletes_forecast(self):
        # verifies that _upload_forecast_worker() deletes the (presumably empty) Forecast that's passed to it by
        # upload functions if the file is invalid. here we mock load_predictions_from_json_io_dict() to throw the two
        # exceptions that cause deletes: JobTimeoutException and Exception
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        forecast.issue_date -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        forecast.save()

        for exception, exp_job_status in [(Exception('load_preds_mock Exception'), Job.FAILED),
                                          (JobTimeoutException('load_preds_mock JobTimeoutException'), Job.TIMEOUT)]:
            with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                    patch('utils.forecast.load_predictions_from_json_io_dict') as load_preds_mock, \
                    patch('utils.forecast.cache_forecast_metadata') as cache_metatdata_mock, \
                    open('forecast_app/tests/predictions/docs-predictions.json') as cloud_file_fp:
                load_preds_mock.side_effect = exception
                forecast2 = Forecast.objects.create(forecast_model=forecast_model, time_zero=time_zero)
                job = Job.objects.create()
                job.input_json = {'forecast_pk': forecast2.pk, 'filename': 'a name!'}
                job.save()

                job_cloud_file_mock.return_value.__enter__.return_value = (job, cloud_file_fp)
                try:
                    _upload_forecast_worker(job.pk)
                except JobTimeoutException as jte:
                    pass  # expected re-raise of this exception
                job.refresh_from_db()
                self.assertEqual(exp_job_status, job.status)
                self.assertIsNone(Forecast.objects.filter(id=forecast2.id).first())  # deleted


    def test__upload_forecast_worker_atomic(self):
        # test `_upload_forecast_worker()` does not create a Forecast if subsequent calls to
        # `load_predictions_from_json_io_dict()` or `cache_forecast_metadata()` fail. this test is complicated by that
        # function's use of the `job_cloud_file` context manager. solution is per https://stackoverflow.com/questions/60198229/python-patch-context-manager-to-return-object
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        forecast.issue_date -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        forecast.save()

        with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                patch('utils.forecast.load_predictions_from_json_io_dict') as load_preds_mock, \
                patch('utils.forecast.cache_forecast_metadata') as cache_metatdata_mock:
            forecast2 = Forecast.objects.create(forecast_model=forecast_model, time_zero=time_zero)
            job = Job.objects.create()
            job.input_json = {'forecast_pk': forecast2.pk, 'filename': 'a name!'}
            job.save()

            job_cloud_file_mock.return_value.__enter__.return_value = (job, None)  # 2-tuple: (job, cloud_file_fp)

            # test that no Forecast is created when load_predictions_from_json_io_dict() fails
            load_preds_mock.side_effect = Exception('load_preds_mock Exception')
            num_forecasts_before = forecast_model.forecasts.count()
            _upload_forecast_worker(job.pk)
            job.refresh_from_db()
            self.assertEqual(num_forecasts_before - 1, forecast_model.forecasts.count())  # -1 b/c forecast2 deleted
            self.assertEqual(Job.FAILED, job.status)

            # test when cache_forecast_metadata() fails
            load_preds_mock.reset_mock(side_effect=True)
            cache_metatdata_mock.side_effect = Exception('cache_metatdata_mock Exception')
            num_forecasts_before = forecast_model.forecasts.count()
            _upload_forecast_worker(job.pk)
            job.refresh_from_db()
            self.assertEqual(num_forecasts_before, forecast_model.forecasts.count())
            self.assertEqual(Job.FAILED, job.status)


    def test__upload_forecast_worker_blue_sky(self):
        # blue sky to verify load_predictions_from_json_io_dict() and cache_forecast_metadata() are called. also tests
        # that _upload_forecast_worker() correctly sets job.output_json. this test is complicated by that function's use
        # of the `job_cloud_file` context manager. solution is per https://stackoverflow.com/questions/60198229/python-patch-context-manager-to-return-object
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        forecast.issue_date -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        forecast.save()

        with patch('forecast_app.models.job.job_cloud_file') as job_cloud_file_mock, \
                patch('utils.forecast.load_predictions_from_json_io_dict') as load_preds_mock, \
                patch('utils.forecast.cache_forecast_metadata') as cache_metatdata_mock, \
                open('forecast_app/tests/predictions/docs-predictions.json') as cloud_file_fp:
            job = Job.objects.create()
            job.input_json = {'forecast_pk': forecast.pk, 'filename': 'a name!'}
            job.save()
            job_cloud_file_mock.return_value.__enter__.return_value = (job, cloud_file_fp)
            _upload_forecast_worker(job.pk)
            job.refresh_from_db()
            load_preds_mock.assert_called_once()
            cache_metatdata_mock.assert_called_once()
            self.assertEqual(Job.SUCCESS, job.status)
            self.assertEqual(job.input_json['forecast_pk'], job.output_json['forecast_pk'])
