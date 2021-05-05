import datetime
import json

from django.db.models import QuerySet
from django.test import TestCase

from forecast_app.models import ForecastMetaPrediction, ForecastMetaUnit, ForecastMetaTarget, Forecast
from utils.forecast import cache_forecast_metadata, clear_forecast_metadata, load_predictions_from_json_io_dict, \
    forecast_metadata, is_forecast_metadata_available, forecast_metadata_counts_for_project
from utils.make_minimal_projects import _make_docs_project
from utils.utilities import get_or_create_super_po_mo_users


class ForecastMetadataTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        # recall that _make_docs_project() calls cache_forecast_metadata(), but the below tests assume it doesn't, so
        # we clear here
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        cls.project, cls.time_zero, cls.forecast_model, cls.forecast = _make_docs_project(po_user)
        clear_forecast_metadata(cls.forecast)
        cls.forecast.issued_at -= datetime.timedelta(days=1)  # older version avoids unique constraint errors
        cls.forecast.save()


    def test_cache_forecast_metadata_predictions(self):
        self.assertEqual(0, ForecastMetaPrediction.objects.filter(forecast=self.forecast).count())

        cache_forecast_metadata(self.forecast)
        forecast_meta_prediction_qs = ForecastMetaPrediction.objects.filter(forecast=self.forecast)
        self.assertEqual(1, forecast_meta_prediction_qs.count())

        meta_cache_prediction = forecast_meta_prediction_qs.first()  # only one row
        self.assertIsNotNone(meta_cache_prediction)
        self.assertEqual(11, meta_cache_prediction.point_count)
        self.assertEqual(2, meta_cache_prediction.named_count)
        self.assertEqual(6, meta_cache_prediction.bin_count)
        self.assertEqual(7, meta_cache_prediction.sample_count)
        self.assertEqual(3, meta_cache_prediction.quantile_count)

        # second run first deletes existing rows, resulting in the same number as before
        cache_forecast_metadata(self.forecast)
        self.assertEqual(1, ForecastMetaPrediction.objects.filter(forecast=self.forecast).count())


    def test_cache_forecast_metadata_units(self):
        self.assertEqual(0, ForecastMetaUnit.objects.filter(forecast=self.forecast).count())

        cache_forecast_metadata(self.forecast)
        forecast_meta_unit_qs = ForecastMetaUnit.objects.filter(forecast=self.forecast)
        self.assertEqual(3, forecast_meta_unit_qs.count())
        self.assertEqual(set(self.project.units.all()), set([fmu.unit for fmu in forecast_meta_unit_qs]))

        # second run first deletes existing rows, resulting in the same number as before
        cache_forecast_metadata(self.forecast)
        self.assertEqual(3, forecast_meta_unit_qs.count())


    def test_cache_forecast_metadata_targets(self):
        self.assertEqual(0, ForecastMetaTarget.objects.filter(forecast=self.forecast).count())

        cache_forecast_metadata(self.forecast)
        forecast_meta_target_qs = ForecastMetaTarget.objects.filter(forecast=self.forecast)
        self.assertEqual(5, forecast_meta_target_qs.count())
        self.assertEqual(set(self.project.targets.all()), set([fmt.target for fmt in forecast_meta_target_qs]))

        # second run first deletes existing rows, resulting in the same number as before
        cache_forecast_metadata(self.forecast)
        self.assertEqual(5, forecast_meta_target_qs.count())


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
        forecast2 = Forecast.objects.create(forecast_model=self.forecast_model, source='docs-predictions-non-dup.json',
                                            time_zero=self.time_zero, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions-non-dup.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, is_validate_cats=False)

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


    def test_metadata_for_forecast(self):
        cache_forecast_metadata(self.forecast)
        forecast_meta_prediction, forecast_meta_unit_qs, forecast_meta_target_qs = forecast_metadata(self.forecast)

        self.assertIsInstance(forecast_meta_prediction, ForecastMetaPrediction)
        self.assertEqual(11, forecast_meta_prediction.point_count)
        self.assertEqual(2, forecast_meta_prediction.named_count)
        self.assertEqual(6, forecast_meta_prediction.bin_count)
        self.assertEqual(7, forecast_meta_prediction.sample_count)
        self.assertEqual(3, forecast_meta_prediction.quantile_count)

        self.assertIsInstance(forecast_meta_unit_qs, QuerySet)
        self.assertEqual(3, len(forecast_meta_unit_qs))
        self.assertEqual({ForecastMetaUnit}, set(map(type, forecast_meta_unit_qs)))

        self.assertIsInstance(forecast_meta_target_qs, QuerySet)
        self.assertEqual(5, len(forecast_meta_target_qs))
        self.assertEqual({ForecastMetaTarget}, set(map(type, forecast_meta_target_qs)))


    def test_is_forecast_metadata_available(self):
        self.assertFalse(is_forecast_metadata_available(self.forecast))

        cache_forecast_metadata(self.forecast)
        self.assertTrue(is_forecast_metadata_available(self.forecast))


    def tests_forecast_metadata_counts_for_project(self):
        forecast2 = Forecast.objects.create(forecast_model=self.forecast_model, source='docs-predictions-non-dup.json',
                                            time_zero=self.time_zero, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions-non-dup.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, is_validate_cats=False)
        cache_forecast_metadata(self.forecast)
        cache_forecast_metadata(forecast2)
        forecast_id_to_counts = forecast_metadata_counts_for_project(self.project)
        #  f_id:  [(point_count, named_count, bin_count, sample_count, quantile_count), num_names, num_targets]
        # {   1:  [(11,          2,           6,        7,           3),             3,         5          ],
        #     2:  [(11,          2,           6,        7,           3),             3,         5          ]}
        self.assertEqual(sorted([self.forecast.id, forecast2.id]), sorted(forecast_id_to_counts.keys()))
        self.assertEqual([(11, 2, 6, 7, 3), 3, 5], forecast_id_to_counts[self.forecast.id])
        self.assertEqual([(11, 2, 6, 7, 3), 3, 5], forecast_id_to_counts[forecast2.id])
