import json
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, ForecastModel, TimeZero
from forecast_app.models.forecast import Forecast
from forecast_app.models.prediction import calc_named_distribution
from utils.cdc import convert_cdc_csv_file_to_dict
from utils.forecast import load_predictions, _prediction_dicts_to_db_rows
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
        with open(self.cdc_csv_path) as cdc_csv_fp, \
                open('forecast_app/tests/predictions/exp-predictions.json') as exp_json_fp:
            exp_dict = json.load(exp_json_fp)
            act_dict = convert_cdc_csv_file_to_dict(self.forecast, cdc_csv_fp)
            # test the three top-level components separately for TDD
            self.assertEqual(list(exp_dict.keys()), list(act_dict.keys()))
            self.assertEqual(set(exp_dict['locations']), set(act_dict['locations']))
            self.assertEqual(exp_dict['predictions'], act_dict['predictions'])
            self.assertEqual(sorted(exp_dict['targets'], key=lambda _: _['name']),
                             sorted(act_dict['targets'], key=lambda _: _['name']))

            # test forecast separately to account for runtime differences (forecast.id, created_at, etc.) Do so by
            # 'patching' the runtime-specific differences. note: we could have used Django templates as in
            # test_flusight.py, but this was simpler
            exp_dict['forecast']['id'] = self.forecast.id
            exp_dict['forecast']['forecast_model_id'] = self.forecast.forecast_model.id
            exp_dict['forecast']['created_at'] = self.forecast.created_at
            exp_dict['forecast']['time_zero']['timezero_date'] = self.time_zero.timezero_date
            exp_dict['forecast']['time_zero']['data_version_date'] = self.time_zero.data_version_date
            self.assertEqual(exp_dict['forecast'], act_dict['forecast'])


    def test_forecast_prediction_accessors_json(self):
        # load all seven types of Predictions, call Forecast.*_qs() functions
        with open('forecast_app/tests/predictions/predictions-example.json') as fp:
            top_level_dict = json.load(fp)
            load_predictions(self.forecast, top_level_dict)
            self.assertEqual(2, self.forecast.bincat_distribution_qs().count())
            self.assertEqual(2, self.forecast.binlwr_distribution_qs().count())
            self.assertEqual(1, self.forecast.binary_distribution_qs().count())
            self.assertEqual(1, self.forecast.named_distribution_qs().count())
            self.assertEqual(1, self.forecast.point_prediction_qs().count())
            self.assertEqual(2, self.forecast.sample_distribution_qs().count())
            self.assertEqual(2, self.forecast.samplecat_distribution_qs().count())


    def test_forecast_prediction_accessors_cdc(self):
        # load the three types of predictions that come from cdc.csv files, call Forecast.*_qs() functions
        with open(self.cdc_csv_path) as cdc_csv_fp:
            top_level_dict = convert_cdc_csv_file_to_dict(self.forecast, cdc_csv_fp)
            load_predictions(self.forecast, top_level_dict)
            self.assertEqual(22, self.forecast.bincat_distribution_qs().count())
            self.assertEqual(55, self.forecast.binlwr_distribution_qs().count())
            self.assertEqual(0, self.forecast.binary_distribution_qs().count())
            self.assertEqual(0, self.forecast.named_distribution_qs().count())
            self.assertEqual(77, self.forecast.point_prediction_qs().count())
            self.assertEqual(0, self.forecast.sample_distribution_qs().count())
            self.assertEqual(0, self.forecast.samplecat_distribution_qs().count())


    def test_prediction_dicts_to_db_rows(self):
        with open('forecast_app/tests/predictions/predictions-example.json') as fp:
            prediction_dicts = json.load(fp)['predictions']  # ignore 'forecast', 'locations', and 'targets'
            bincat_rows, binlwr_rows, binary_rows, named_rows, point_rows, sample_rows, samplecat_rows = \
                _prediction_dicts_to_db_rows(prediction_dicts)
            self.assertEqual([['US National', '1 wk ahead', 'cat1', 0.1],
                              ['US National', '1 wk ahead', 'cat2', 0.9]],
                             bincat_rows)
            self.assertEqual([['HHS Region 1', '2 wk ahead', 0.0, 0.1],
                              ['HHS Region 1', '2 wk ahead', 0.1, 0.9]],
                             binlwr_rows)
            self.assertEqual([['HHS Region 2', '3 wk ahead', 0.5]],
                             binary_rows)
            self.assertEqual([['HHS Region 3', '4 wk ahead', 'gamma', 1.1, 2.2, 3.3]],
                             named_rows)
            self.assertEqual([['HHS Region 4', 'Season onset', 1]],
                             point_rows)
            self.assertEqual([['HHS Region 5', 'Season peak percentage', 1.1],
                              ['HHS Region 5', 'Season peak percentage', 2.2]],
                             sample_rows)
            self.assertEqual([['HHS Region 6', 'Season peak week', 'cat1', 'cat1 sample'],
                              ['HHS Region 6', 'Season peak week', 'cat2', 'cat2 sample']],
                             samplecat_rows)


    def test_bad_csv_file(self):
        # can't coerce, etc.
        self.fail()  # todo xx


    def test_unrecognized_prediction_class(self):
        self.fail()  # todo xx


    def test_all_types_of_validations(self):
        # see [Zoltar2 Prediction Validation](https://docs.google.com/document/d/1WtYdjhVSKkdlU6mHe_qYBdyIUnPSNBa0QCg1WgnN2qQ/edit)
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
