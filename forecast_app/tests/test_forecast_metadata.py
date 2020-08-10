import json

from django.test import TestCase

from forecast_app.models import ForecastMetaPrediction, ForecastMetaUnit, ForecastMetaTarget, Forecast
from utils.forecast import cache_forecast_metadata, clear_forecast_metadata, load_predictions_from_json_io_dict
from utils.make_minimal_projects import _make_docs_project
from utils.utilities import get_or_create_super_po_mo_users


class ForecastTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        cls.project, cls.time_zero, cls.forecast_model, cls.forecast = _make_docs_project(po_user)


    def test_cache_forecast_metadata_predictions(self):
        self.assertEqual(0, ForecastMetaPrediction.objects.filter(forecast=self.forecast).count())

        cache_forecast_metadata(self.forecast)
        meta_cache_prediction_qs = ForecastMetaPrediction.objects.filter(forecast=self.forecast)
        self.assertEqual(1, meta_cache_prediction_qs.count())

        meta_cache_prediction = meta_cache_prediction_qs.first()  # only one row
        self.assertEqual(11, meta_cache_prediction.point_count)
        self.assertEqual(2, meta_cache_prediction.named_count)
        self.assertEqual(16, meta_cache_prediction.bin_count)
        self.assertEqual(23, meta_cache_prediction.sample_count)
        self.assertEqual(10, meta_cache_prediction.quantile_count)

        # second run first deletes existing rows, resulting in the same number as before
        cache_forecast_metadata(self.forecast)
        self.assertEqual(1, ForecastMetaPrediction.objects.filter(forecast=self.forecast).count())


    def test_cache_forecast_metadata_units(self):
        self.assertEqual(0, ForecastMetaUnit.objects.filter(forecast=self.forecast).count())

        cache_forecast_metadata(self.forecast)
        meta_cache_unit_qs = ForecastMetaUnit.objects.filter(forecast=self.forecast)
        self.assertEqual(3, meta_cache_unit_qs.count())
        self.assertEqual(set(self.project.units.all()), set([fmu.unit for fmu in meta_cache_unit_qs]))

        # second run first deletes existing rows, resulting in the same number as before
        cache_forecast_metadata(self.forecast)
        self.assertEqual(3, meta_cache_unit_qs.count())


    def test_cache_forecast_metadata_targets(self):
        self.assertEqual(0, ForecastMetaTarget.objects.filter(forecast=self.forecast).count())

        cache_forecast_metadata(self.forecast)
        meta_cache_target_qs = ForecastMetaTarget.objects.filter(forecast=self.forecast)
        self.assertEqual(5, meta_cache_target_qs.count())
        self.assertEqual(set(self.project.targets.all()), set([fmt.target for fmt in meta_cache_target_qs]))

        # second run first deletes existing rows, resulting in the same number as before
        cache_forecast_metadata(self.forecast)
        self.assertEqual(5, meta_cache_target_qs.count())


    def test_cache_forecast_metadata_clears_first(self):
        self.assertEqual(0, ForecastMetaPrediction.objects.filter(forecast=self.forecast).count())
        self.assertEqual(0, ForecastMetaUnit.objects.filter(forecast=self.forecast).count())
        self.assertEqual(0, ForecastMetaTarget.objects.filter(forecast=self.forecast).count())

        # first run creates rows, second run first deletes existing rows, resulting in the same number as before
        for _ in range(2):
            cache_forecast_metadata(self.forecast)
            self.assertEqual(1, ForecastMetaPrediction.objects.filter(forecast=self.forecast).count())
            self.assertEqual(3, ForecastMetaUnit.objects.filter(forecast=self.forecast).count())
            self.assertEqual(5, ForecastMetaTarget.objects.filter(forecast=self.forecast).count())

        clear_forecast_metadata(self.forecast)
        self.assertEqual(0, ForecastMetaPrediction.objects.filter(forecast=self.forecast).count())
        self.assertEqual(0, ForecastMetaUnit.objects.filter(forecast=self.forecast).count())
        self.assertEqual(0, ForecastMetaTarget.objects.filter(forecast=self.forecast).count())


    def test_cache_forecast_metadata_second_forecast(self):
        # make sure only the passed forecast is cached
        forecast2 = Forecast.objects.create(forecast_model=self.forecast_model, source='docs-predictions.json',
                                            time_zero=self.time_zero, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, False)

        self.assertEqual(0, ForecastMetaPrediction.objects.filter(forecast=self.forecast).count())
        self.assertEqual(0, ForecastMetaUnit.objects.filter(forecast=self.forecast).count())
        self.assertEqual(0, ForecastMetaTarget.objects.filter(forecast=self.forecast).count())

        self.assertEqual(0, ForecastMetaPrediction.objects.filter(forecast=forecast2).count())
        self.assertEqual(0, ForecastMetaUnit.objects.filter(forecast=forecast2).count())
        self.assertEqual(0, ForecastMetaTarget.objects.filter(forecast=forecast2).count())

        cache_forecast_metadata(self.forecast)
        self.assertEqual(1, ForecastMetaPrediction.objects.filter(forecast=self.forecast).count())
        self.assertEqual(3, ForecastMetaUnit.objects.filter(forecast=self.forecast).count())
        self.assertEqual(5, ForecastMetaTarget.objects.filter(forecast=self.forecast).count())

        self.assertEqual(0, ForecastMetaPrediction.objects.filter(forecast=forecast2).count())
        self.assertEqual(0, ForecastMetaUnit.objects.filter(forecast=forecast2).count())
        self.assertEqual(0, ForecastMetaTarget.objects.filter(forecast=forecast2).count())
