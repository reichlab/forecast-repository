from django.db import models
from django.urls import reverse

from forecast_app.models.data import ForecastData, ModelWithCDCData
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.project import TimeZero
from utils.utilities import basic_str


class Forecast(ModelWithCDCData):
    """
    Represents a model's forecasted data. There is one Forecast for each of my ForecastModel's Project's TimeZeros.
    """

    cdc_data_class = ForecastData  # the CDCData class I'm paired with. used by ModelWithCDCData

    forecast_model = models.ForeignKey(ForecastModel, on_delete=models.CASCADE, null=True)

    # NB: these TimeZeros must be the exact objects as the ones in my ForecastModel's Project, b/c there is no __eq__()
    time_zero = models.ForeignKey(TimeZero, on_delete=models.CASCADE, null=True,
                                  help_text="TimeZero that this forecast is in relation to")

    data_filename = models.CharField(max_length=200,
                                     help_text="Original CSV file name of this forecast's data source")


    def __repr__(self):
        return str((self.pk, self.time_zero, self.data_filename))


    def __str__(self):  # todo
        return basic_str(self)


    def get_absolute_url(self):
        return reverse('forecast-detail', args=[str(self.id)])
