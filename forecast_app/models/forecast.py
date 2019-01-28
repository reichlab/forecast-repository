from django.db import models
from django.urls import reverse

from forecast_app.models.data import ForecastData, ModelWithCDCData
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.project import TimeZero
from utils.utilities import basic_str, rescale


class Forecast(ModelWithCDCData):
    """
    Represents a model's forecasted data. There is one Forecast for each of my ForecastModel's Project's TimeZeros.
    """

    cdc_data_class = ForecastData  # the CDCData class I'm paired with. used by ModelWithCDCData

    forecast_model = models.ForeignKey(ForecastModel, related_name='forecasts', on_delete=models.CASCADE)

    # NB: these TimeZeros must be the exact objects as the ones in my ForecastModel's Project, b/c there is no __eq__()
    time_zero = models.ForeignKey(TimeZero, on_delete=models.CASCADE,
                                  help_text="TimeZero that this forecast is in relation to.")

    # when this instance was created. basically the post-validation save date:
    created_at = models.DateTimeField(auto_now_add=True)


    def __repr__(self):
        return str((self.pk, self.time_zero, self.csv_filename))


    def __str__(self):  # todo
        return basic_str(self)


    def get_absolute_url(self):
        return reverse('forecast-detail', args=[str(self.pk)])


    def get_class(self):
        """
        :return: view utility that simply returns a my class as a string. used by delete_modal_snippet.html
        """
        return self.__class__.__name__


    def html_id(self):
        """
        :return: view utility that returns a unique HTML id for this object. used by delete_modal_snippet.html
        """
        return self.__class__.__name__ + '_' + str(self.pk)


    @property
    def name(self):
        """
        We define the name property so that delete_modal_snippet.html can show something identifiable when asking to
        confirm deleting a Forecast. All other deletable models have 'name' fields (Project and ForecastModel).
        """
        return self.csv_filename


    def is_user_ok_to_delete(self, user):
        return user.is_superuser or (user == self.forecast_model.project.owner) or (user == self.forecast_model.owner)


    def rescaled_bin_for_loc_and_target(self, location_name, target_name):
        """
        Used for sparkline calculations.

        :return: list of scaled (0-100) values for the passed location and target
        """
        # bin_start_incl, bin_end_notincl, value:
        values = [_[2] for _ in self.get_target_bins(location_name, target_name)]
        return rescale(values)


    def locations_qs(self):  # concrete method
        return self.forecast_model.project.locations_qs()


    def targets_qs(self):  # concrete method
        return self.forecast_model.project.targets_qs()


# NB: only works for abstract superclasses. via https://stackoverflow.com/questions/927729/how-to-override-the-verbose-name-of-a-superclass-model-field-in-django
Forecast._meta.get_field('csv_filename').help_text = "CSV file name of this forecast's data source."
