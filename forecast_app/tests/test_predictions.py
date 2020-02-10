import datetime
import json
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Forecast
from forecast_app.models import ForecastModel, TimeZero
from forecast_app.models.prediction import calc_named_distribution
from utils.cdc import make_cdc_locations_and_targets
from utils.forecast import load_predictions_from_json_io_dict, _prediction_dicts_to_validated_db_rows
from utils.project import create_project_from_json
from utils.utilities import get_or_create_super_po_mo_users


#
# initial single file for driving zoltar2 development. todo xx will be split into separate ones as the code develops
#

class PredictionsTestCase(TestCase):
    """
    """

    def test_concrete_subclasses(self):
        """
        this test makes sure the current set of concrete Prediction subclasses hasn't changed since the last time this
        test was updated. it's here as a kind of sanity check to catch the case where the Prediction class hierarchy has
        changed, but not code that depends on the seven (as of this writing) specific subclasses
        """
        from forecast_app.models import Prediction


        concrete_subclasses = Prediction.concrete_subclasses()
        exp_subclasses = {'BinDistribution', 'NamedDistribution', 'PointPrediction', 'SampleDistribution'}
        self.assertEqual(exp_subclasses, {concrete_subclass.__name__ for concrete_subclass in concrete_subclasses})


    def test_load_predictions_from_json_io_dict(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project)
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)

        # test json with no 'predictions'
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(forecast, {})
        self.assertIn("json_io_dict had no 'predictions' key", str(context.exception))

        # load all four types of Predictions, call Forecast.*_qs() functions. see docs-predictionsexp-rows.xlsx.

        # counts from docs-predictionsexp-rows.xlsx: point: 11, named: 3, bin: 30 (3 zero prob), sample: 23
        # = total rows: 67
        #
        # counts based on .json file:
        # - 'pct next week':    point: 3, named: 1 , bin: 3, sample: 5 = 12
        # - 'cases next week':  point: 2, named: 1 , bin: 3, sample: 3 = 10
        # - 'season severity':  point: 2, named: 0 , bin: 3, sample: 5 = 10
        # - 'above baseline':   point: 1, named: 0 , bin: 1, sample: 6 =  8
        # - 'Season peak week': point: 3, named: 0 , bin: 7, sample: 4 = 13
        # = total rows: 53 - 2 zero prob = 51

        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict)
        self.assertEqual(51, forecast.get_num_rows())
        self.assertEqual(15, forecast.bin_distribution_qs().count())  # 17 - 2 zero prob
        self.assertEqual(2, forecast.named_distribution_qs().count())
        self.assertEqual(11, forecast.point_prediction_qs().count())
        self.assertEqual(23, forecast.sample_distribution_qs().count())


    def test_prediction_dicts_to_db_rows(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project)
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)
        make_cdc_locations_and_targets(project)

        # see above: counts from docs-predictionsexp-rows.xlsx
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            prediction_dicts = json.load(fp)['predictions']  # ignore 'forecast', 'locations', and 'targets'
            bin_rows, named_rows, point_rows, sample_rows = \
                _prediction_dicts_to_validated_db_rows(forecast, prediction_dicts)
        self.assertEqual(15, len(bin_rows))  # 17 - 2 zero prob
        self.assertEqual(2, len(named_rows))
        self.assertEqual(11, len(point_rows))
        self.assertEqual(23, len(sample_rows))
        self.assertEqual([['location2', 'pct next week', 1.1, 0.3],
                          ['location2', 'pct next week', 2.2, 0.2],
                          ['location2', 'pct next week', 3.3, 0.5],
                          ['location3', 'cases next week', 1, 0.1],
                          ['location3', 'cases next week', 2, 0.9],
                          ['location1', 'season severity', 'moderate', 0.1],
                          ['location1', 'season severity', 'severe', 0.9],
                          ['location2', 'above baseline', True, 0.9],
                          ['location1', 'Season peak week', '2019-12-15', 0.01],
                          ['location1', 'Season peak week', '2019-12-22', 0.1],
                          ['location1', 'Season peak week', '2019-12-29', 0.89],
                          ['location2', 'Season peak week', '2019-12-15', 0.01],
                          ['location2', 'Season peak week', '2019-12-22', 0.05],
                          ['location2', 'Season peak week', '2019-12-29', 0.05],
                          ['location2', 'Season peak week', '2020-01-05', 0.89]],
                         bin_rows)
        self.assertEqual([['location1', 'pct next week', 'norm', 1.1, 2.2, None],
                          ['location1', 'cases next week', 'pois', 1.1, None, None]],
                         named_rows)
        self.assertEqual([['location1', 'pct next week', 2.1],
                          ['location2', 'pct next week', 2.0],
                          ['location3', 'pct next week', 3.567],
                          ['location2', 'cases next week', 5],
                          ['location3', 'cases next week', 10],
                          ['location1', 'season severity', 'mild'],
                          ['location2', 'season severity', 'moderate'],
                          ['location1', 'above baseline', True],
                          ['location1', 'Season peak week', '2019-12-22'],
                          ['location2', 'Season peak week', '2020-01-05'],
                          ['location3', 'Season peak week', '2019-12-29']],
                         point_rows)
        self.assertEqual([['location3', 'pct next week', 2.3],
                          ['location3', 'pct next week', 6.5],
                          ['location3', 'pct next week', 0.0],
                          ['location3', 'pct next week', 10.0234],
                          ['location3', 'pct next week', 0.0001],
                          ['location2', 'cases next week', 0],
                          ['location2', 'cases next week', 2],
                          ['location2', 'cases next week', 5],
                          ['location2', 'season severity', 'moderate'],
                          ['location2', 'season severity', 'severe'],
                          ['location2', 'season severity', 'high'],
                          ['location2', 'season severity', 'moderate'],
                          ['location2', 'season severity', 'mild'],
                          ['location2', 'above baseline', True],
                          ['location2', 'above baseline', False],
                          ['location2', 'above baseline', True],
                          ['location3', 'above baseline', False],
                          ['location3', 'above baseline', True],
                          ['location3', 'above baseline', True],
                          ['location1', 'Season peak week', '2020-01-05'],
                          ['location1', 'Season peak week', '2019-12-15'],
                          ['location3', 'Season peak week', '2020-01-06'],
                          ['location3', 'Season peak week', '2019-12-16']],
                         sample_rows)


    def test_prediction_dicts_to_db_rows_invalid(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project)
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero)
        make_cdc_locations_and_targets(project)

        # test for invalid location
        with self.assertRaises(RuntimeError) as context:
            bad_prediction_dicts = [
                {"location": "bad location", "target": "1 wk ahead", "class": "BinCat", "prediction": {}}
            ]
            _prediction_dicts_to_validated_db_rows(forecast, bad_prediction_dicts)
        self.assertIn('prediction_dict referred to an undefined Location', str(context.exception))

        # test for invalid target
        with self.assertRaises(RuntimeError) as context:
            bad_prediction_dicts = [
                {"location": "HHS Region 1", "target": "bad target", "class": "bad class", "prediction": {}}
            ]
            _prediction_dicts_to_validated_db_rows(forecast, bad_prediction_dicts)
        self.assertIn('prediction_dict referred to an undefined Target', str(context.exception))

        # test for invalid prediction_class
        with self.assertRaises(RuntimeError) as context:
            bad_prediction_dicts = [
                {"location": "HHS Region 1", "target": "1 wk ahead", "class": "bad class", "prediction": {}}
            ]
            _prediction_dicts_to_validated_db_rows(forecast, bad_prediction_dicts)
        self.assertIn('invalid prediction_class', str(context.exception))


    def test_calc_named_distribution(self):
        abbrev_parms_exp_value = [
            ('norm', None, None, None, None),  # todo xx
            ('lnorm', None, None, None, None),
            ('gamma', None, None, None, None),
            ('beta', None, None, None, None),
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
