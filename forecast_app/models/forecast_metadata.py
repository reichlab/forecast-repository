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
    Caches this metadata for Forecasts: prediction type counts from my forecast's PredictionElements.
    """

    point_count = IntegerField()  # number of point predictions in this forecast
    named_count = IntegerField()  # "" named ""
    bin_count = IntegerField()  # "" bin ""
    sample_count = IntegerField()  # "" sample ""
    quantile_count = IntegerField()  # "" quantile ""
    mean_count = IntegerField()  # "" mean ""
    median_count = IntegerField()  # "" median ""
    mode_count = IntegerField()  # "" mode ""


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.point_count, self.named_count, self.bin_count, self.sample_count,
                    self.quantile_count, self.mean_count, self.median_count, self.mode_count))


    def __str__(self):  # todo
        return basic_str(self)


class ForecastMetaUnit(ForecastMetadataCache):
    """
    Caches this metadata for Forecasts: units that are present.
    """

    unit = models.ForeignKey('Unit', on_delete=models.CASCADE)


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.unit.pk))


    def __str__(self):  # todo
        return basic_str(self)


class ForecastMetaTarget(ForecastMetadataCache):
    """
    Caches this metadata for Forecasts: targets that are present.
    """

    target = models.ForeignKey('Target', on_delete=models.CASCADE)


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.target.pk))


    def __str__(self):  # todo
        return basic_str(self)
