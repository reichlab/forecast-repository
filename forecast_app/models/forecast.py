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

    forecast_model = models.ForeignKey(ForecastModel, on_delete=models.CASCADE)

    # NB: these TimeZeros must be the exact objects as the ones in my ForecastModel's Project, b/c there is no __eq__()
    time_zero = models.ForeignKey(TimeZero, on_delete=models.CASCADE,
                                  help_text="TimeZero that this forecast is in relation to")


    def __repr__(self):
        return str((self.pk, self.time_zero, self.csv_filename))


    def __str__(self):  # todo
        return basic_str(self)


    def get_absolute_url(self):
        return reverse('forecast-detail', args=[str(self.id)])


# NB: only works for abstract superclasses. per https://stackoverflow.com/questions/927729/how-to-override-the-verbose-name-of-a-superclass-model-field-in-django
Forecast._meta.get_field('csv_filename').help_text = "CSV file name of this forecast's data source"
