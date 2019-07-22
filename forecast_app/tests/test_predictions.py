import tempfile
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, ForecastModel, TimeZero, PointPrediction, NamedDistribution, \
    BinLwrDistribution, SampleDistribution, BinCatDistribution, SampleCatDistribution, BinaryDistribution
from forecast_app.models.forecast import Forecast
from forecast_app.models.prediction import calc_named_distribution
from utils.cdc import convert_cdc_csv_to_predictions_files
from utils.forecast import _prediction_class_for_csv_header, load_predictions
from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, CDC_CONFIG_DICT


#
# initial single file for driving zoltar2 development. will be split into separate ones as the code develops
#

class PredictionsTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        make_cdc_locations_and_targets(cls.project)

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date='2017-01-01')
        cls.cdc_csv_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')
        cls.forecast = Forecast.objects.create(forecast_model=cls.forecast_model, csv_filename=cls.cdc_csv_path.name,
                                               time_zero=cls.time_zero)


    def test_concrete_subclasses(self):
        """
        this test makes sure the current set of concrete Prediction subclasses hasn't changed since the last time this
        test was updated. it's here as a kind of sanity check to catch the case where the Prediction class hierarchy has
        changed, but not code that depends on the seven (as of this writing) specific subclasses
        """
        from forecast_app.models import Prediction


        concrete_subclasses = Prediction.concrete_subclasses()
        exp_subclasses = {'NamedDistribution', 'SampleDistribution', 'SampleCatDistribution', 'BinCatDistribution',
                          'BinLwrDistribution', 'PointPrediction', 'BinaryDistribution'}
        self.assertEqual(exp_subclasses, {concrete_subclass.__name__ for concrete_subclass in concrete_subclasses})


    def test_convert_cdc_csv_to_predictions_files(self):
        with tempfile.NamedTemporaryFile(mode='r+') as points_fp, \
                tempfile.NamedTemporaryFile(mode='r+') as binlwr_fp, \
                tempfile.NamedTemporaryFile(mode='r+') as bincat_fp:
            convert_cdc_csv_to_predictions_files('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv',
                                                 points_fp, binlwr_fp, bincat_fp)

            self._assert_csv_files_equal(Path('forecast_app/tests/predictions/exp-points.csv'), points_fp.name)
            self._assert_csv_files_equal(Path('forecast_app/tests/predictions/exp-binlwr.csv'), binlwr_fp.name)
            self._assert_csv_files_equal(Path('forecast_app/tests/predictions/exp-bincat.csv'), bincat_fp.name)


    def test_csv_header_to_prediction_class(self):
        exp_header_to_class = {
            ('location', 'target', 'cat', 'prob'): BinCatDistribution,
            ('location', 'target', 'lwr', 'prob'): BinLwrDistribution,
            ('location', 'target', 'prob'): BinaryDistribution,
            ('location', 'target', 'family', 'param1', 'param2', 'param3'): NamedDistribution,
            ('location', 'target', 'value'): PointPrediction,
            ('location', 'target', 'sample'): SampleDistribution,
            ('location', 'target', 'cat', 'sample'): SampleCatDistribution,
        }
        for csv_header, exp_prediction_class in exp_header_to_class.items():
            self.assertEqual(exp_prediction_class, _prediction_class_for_csv_header(csv_header))


    def test_load_bincat_csv_file(self):
        with tempfile.NamedTemporaryFile(mode='r+') as points_fp, \
                tempfile.NamedTemporaryFile(mode='r+') as binlwr_fp, \
                tempfile.NamedTemporaryFile(mode='r+') as bincat_fp:
            convert_cdc_csv_to_predictions_files(self.cdc_csv_path, points_fp, binlwr_fp, bincat_fp)
            load_predictions(self.forecast, bincat_fp)

            bincat_qs = BinCatDistribution.objects.filter()
            self.assertEqual(22, bincat_qs.count())

            # sanity-check one location
            act_usnat_bincats = list(bincat_qs \
                                     .filter(location__name='US National') \
                                     .order_by('target__pk') \
                                     .values_list('target__name', 'cat', 'prob'))
            exp_usnat_bincats = [('Season onset', '40', 1.0),
                                 ('Season peak week', '40', 1.0)]
            self.assertEqual(exp_usnat_bincats, act_usnat_bincats)


    def test_load_binlwr_csv_file(self):
        with tempfile.NamedTemporaryFile(mode='r+') as points_fp, \
                tempfile.NamedTemporaryFile(mode='r+') as binlwr_fp, \
                tempfile.NamedTemporaryFile(mode='r+') as bincat_fp:
            convert_cdc_csv_to_predictions_files(self.cdc_csv_path, points_fp, binlwr_fp, bincat_fp)
            load_predictions(self.forecast, binlwr_fp)

            binlwr_qs = BinLwrDistribution.objects.filter()
            self.assertEqual(55, binlwr_qs.count())

            # sanity-check one location
            act_usnat_binlwrs = list(binlwr_qs \
                                     .filter(location__name='US National') \
                                     .order_by('target__pk') \
                                     .values_list('target__name', 'lwr', 'prob'))
            exp_usnat_binlwrs = [('Season peak percentage', 0.0, 1.0),
                                 ('1 wk ahead', 0.0, 1.0),
                                 ('2 wk ahead', 0.0, 1.0),
                                 ('3 wk ahead', 0.0, 1.0),
                                 ('4 wk ahead', 0.0, 1.0)]
            self.assertEqual(exp_usnat_binlwrs, act_usnat_binlwrs)


    def test_load_binary_csv_file(self):
        with open('forecast_app/tests/predictions/binary-predictions.csv') as binary_fp:
            load_predictions(self.forecast, binary_fp)

            binary_qs = BinaryDistribution.objects.filter()
            self.assertEqual(7, binary_qs.count())

            act_binary = list(binary_qs \
                              .order_by('location__pk') \
                              .values_list('location__name', 'target__name', 'prob'))
            exp_binary = [('HHS Region 1', 'Season peak week', 30.0),
                          ('HHS Region 2', 'Season peak percentage', 0.1),
                          ('HHS Region 3', '1 wk ahead', 0.2),
                          ('HHS Region 4', '2 wk ahead', 0.3),
                          ('HHS Region 5', '3 wk ahead', 0.4),
                          ('HHS Region 6', '4 wk ahead', 0.5),
                          ('US National', 'Season onset', 40.0)]
            self.assertEqual(exp_binary, act_binary)


    def test_load_named_distribution_csv_file(self):
        # NB: named_distributions.csv has the non-CDC target 'not-a-cdc-target', which does *not* cause an error when
        # loading b/c all dispatched-to _load_*() methods call _create_missing_locations_and_targets_rows()
        with open('forecast_app/tests/predictions/named_distributions.csv') as named_dists_fp:
            load_predictions(self.forecast, named_dists_fp)

            named_dists_qs = NamedDistribution.objects.filter()
            self.assertEqual(9, named_dists_qs.count())

            act_named_dists = list(named_dists_qs \
                                   .order_by('location__pk') \
                                   .values_list('location__name', 'target__name', 'family', 'param1', 'param2',
                                                'param3'))
            exp_named_dists = [
                ('HHS Region 1', 'Season peak week', 1, 3.3, 4.4, 0.0),
                ('HHS Region 2', 'Season peak percentage', 2, 5.5, 6.6, 0.0),
                ('HHS Region 3', '1 wk ahead', 3, 7.7, 8.8, 0.0),
                ('HHS Region 4', '2 wk ahead', 4, 9.9, 0.0, 0.0),
                ('HHS Region 5', '3 wk ahead', 5, 10.0, 11.11, 0.0),
                ('HHS Region 6', '4 wk ahead', 6, 12.12, 13.13, 0.0),
                ('HHS Region 7', 'not-a-cdc-target', 7, 14.14, 15.15, 0.0),
                ('HHS Region 8', 'Season peak week', 8, 16.16, 17.17, 0.0),
                ('US National', 'Season onset', 0, 1.1, 2.2, 0.0)
            ]
            self.assertEqual(exp_named_dists, act_named_dists)


    def test_load_points_csv_file(self):
        with tempfile.NamedTemporaryFile(mode='r+') as points_fp, \
                tempfile.NamedTemporaryFile(mode='r+') as binlwr_fp, \
                tempfile.NamedTemporaryFile(mode='r+') as bincat_fp:
            convert_cdc_csv_to_predictions_files(self.cdc_csv_path, points_fp, binlwr_fp, bincat_fp)
            load_predictions(self.forecast, points_fp)

            points_qs = PointPrediction.objects.filter()
            self.assertEqual(77, points_qs.count())

            # sanity-check one location
            act_usnat_points = list(points_qs \
                                    .filter(location__name='US National') \
                                    .order_by('target__pk') \
                                    .values_list('target__name', 'value_i', 'value_f', 'value_t'))
            exp_usnat_points = [('Season onset', None, None, '50'),
                                ('Season peak week', None, None, '5'),
                                ('Season peak percentage', None, 3.9, None),
                                ('1 wk ahead', None, 3.2, None),
                                ('2 wk ahead', None, 3.3, None),
                                ('3 wk ahead', None, 3.3, None),
                                ('4 wk ahead', None, 3.2, None)]
            self.assertEqual(exp_usnat_points, act_usnat_points)


    def test_load_sample_csv_file(self):
        with open('forecast_app/tests/predictions/sample-predictions.csv') as samples_fp:
            load_predictions(self.forecast, samples_fp)

            samples_qs = SampleDistribution.objects.filter()
            self.assertEqual(7, samples_qs.count())

            act_samples = list(samples_qs \
                               .order_by('location__pk') \
                               .values_list('location__name', 'target__name', 'sample'))
            exp_samples = [('HHS Region 1', 'Season peak week', 30.0),
                           ('HHS Region 2', 'Season peak percentage', 0.1),
                           ('HHS Region 3', '1 wk ahead', 0.2),
                           ('HHS Region 4', '2 wk ahead', 0.3),
                           ('HHS Region 5', '3 wk ahead', 0.4),
                           ('HHS Region 6', '4 wk ahead', 0.5),
                           ('US National', 'Season onset', 40.0)]
            self.assertEqual(exp_samples, act_samples)


    def test_load_sample_cat_csv_file(self):
        with open('forecast_app/tests/predictions/samplecat-predictions.csv') as samplecats_fp:
            load_predictions(self.forecast, samplecats_fp)

            samplecats_qs = SampleCatDistribution.objects.filter()
            self.assertEqual(7, samplecats_qs.count())

            act_samplecats = list(samplecats_qs \
                                  .order_by('location__pk') \
                                  .values_list('location__name', 'target__name', 'cat', 'sample'))
            exp_samplecats = [('HHS Region 1', 'Season peak week', 'b', 'b1'),
                              ('HHS Region 2', 'Season peak percentage', 'c', 'c1'),
                              ('HHS Region 3', '1 wk ahead', 'd', 'd1'),
                              ('HHS Region 4', '2 wk ahead', 'e', 'e1'),
                              ('HHS Region 5', '3 wk ahead', 'f', 'f1'),
                              ('HHS Region 6', '4 wk ahead', 'g', 'g1'),
                              ('US National', 'Season onset', 'a', 'a1')]
            self.assertEqual(exp_samplecats, act_samplecats)


    def test_forecast_prediction_accessors(self):
        # load all 7 types of Predictions, call Forecast.*_qs() functions
        prediction_file_names = [  # in 'forecast_app/tests/predictions'
            'exp-points.csv',
            'named_distributions.csv',
            'exp-binlwr.csv',
            'sample-predictions.csv',
            'exp-bincat.csv',
            'samplecat-predictions.csv',
            'binary-predictions.csv',
        ]
        for prediction_file_name in prediction_file_names:
            with open('forecast_app/tests/predictions/' + prediction_file_name) as prediction_fp:
                load_predictions(self.forecast, prediction_fp)
        self.assertEqual(22, self.forecast.bincat_distribution_qs().count())
        self.assertEqual(55, self.forecast.binlwr_distribution_qs().count())
        self.assertEqual(7, self.forecast.binary_distribution_qs().count())
        self.assertEqual(9, self.forecast.named_distribution_qs().count())
        self.assertEqual(77, self.forecast.point_prediction_qs().count())
        self.assertEqual(7, self.forecast.sample_distribution_qs().count())
        self.assertEqual(7, self.forecast.samplecat_distribution_qs().count())


    def test_bad_csv_headers(self):
        # sanity-check the above test_csv_header_to_prediction_class() to ensure it's being called in at least one case
        # of a bad header
        with open(self.cdc_csv_path) as bad_header_fp:
            with self.assertRaises(RuntimeError) as context:
                load_predictions(self.forecast, bad_header_fp)
            self.assertIn('csv_header did not match expected types', str(context.exception))


    def test_all_types_of_validations(self):
        # - sample-predictions.csv: 'sample' must coerce to float()
        # - maybe predx-based validations (probs sum to 1, etc.) - see 7/2 p2
        # - ...
        self.fail()  # todo xx


    def test_target_acceptable_foreast_data_formats(self):
        # BooleanFields for each of the seven possibilities:
        #   - ok_point_prediction
        #   - ok_named_distribution
        #   - ok_binlwr_distribution
        #   - ok_sample_distribution
        #   - ok_bincat_distribution
        #   - ok_samplecat_distribution
        #   - ok_binary_distribution
        self.fail()  # todo xx


    def test_calc_named_distribution(self):
        abbrev_parms_exp_value = [
            ('norm', None, None, None, None),  # todo xx
            ('lnorm', None, None, None, None),
            ('gamma', None, None, None, None),
            ('beta', None, None, None, None),
            ('bern', None, None, None, None),
            ('binom', None, None, None, None),
            ('pois', None, None, None, None),
            ('nbinom', None, None, None, None),
            ('nbinom2', None, None, None, None),
        ]
        for named_dist_abbrev, param1, param2, param3, exp_val in abbrev_parms_exp_value:
            self.assertEqual(exp_val, calc_named_distribution(named_dist_abbrev, param1, param2, param3))

        with self.assertRaises(RuntimeError) as context:
            calc_named_distribution(None, None, None, None)
        self.assertIn("invalid abbreviation", str(context.exception))


    def _assert_csv_files_equal(self, exp_file, act_file):
        # helper that replaces filecmp.cmp(), which does not account for the difference b/w DOS-style and unix-style CRs
        with open(exp_file) as exp_file_fp, open(act_file) as act_file_fp:
            exp_file_fp.seek(0)
            act_file_fp.seek(0)
            exp_file_lines = exp_file_fp.readlines()
            act_file_lines = act_file_fp.readlines()
            self.assertEqual(exp_file_lines, act_file_lines)
