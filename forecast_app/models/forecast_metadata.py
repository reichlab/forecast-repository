from django.db import models
from django.db.models import IntegerField

from forecast_app.models import Forecast
from utils.utilities import basic_str


#
# This file defines three models that, together, implement the caching of Forecast meatadata.
#

class ForecastMetadataCache(models.Model):
    """
    Abstract base class representing a type of Forecast model cache.
    """


    class Meta:
        abstract = True


    forecast = models.ForeignKey(Forecast, on_delete=models.CASCADE)


class ForecastMetaPrediction(ForecastMetadataCache):
    """
    Caches this metadata for Forecasts: prediction type counts.
    """

    point_count = IntegerField(default=None, null=True)  # number of PointPredictions in this forecast
    named_count = IntegerField(default=None, null=True)  # "" NamedDistribution ""
    bin_count = IntegerField(default=None, null=True)  # "" BinDistribution ""
    sample_count = IntegerField(default=None, null=True)  # "" SampleDistribution ""
    quantile_count = IntegerField(default=None, null=True)  # "" QuantileDistribution ""


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.point_count, self.named_count, self.bin_count, self.sample_count,
                    self.quantile_count))


    def __str__(self):  # todo
        return basic_str(self)


class ForecastMetaUnit(ForecastMetadataCache):
    """
    Caches this metadata for Forecasts: units that are present.
    """

    unit = models.ForeignKey('Unit', related_name='unit_cache', on_delete=models.CASCADE)


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.unit.pk))


    def __str__(self):  # todo
        return basic_str(self)


class ForecastMetaTarget(ForecastMetadataCache):
    """
    Caches this metadata for Forecasts: targets that are present.
    """

    target = models.ForeignKey('Target', related_name='target_cache', on_delete=models.CASCADE)


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.target.pk))


    def __str__(self):  # todo
        return basic_str(self)
