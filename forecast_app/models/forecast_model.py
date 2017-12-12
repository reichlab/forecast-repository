from django.contrib.auth.models import User
from django.db import models, transaction
from django.urls import reverse

import forecast_app.models.forecast  # we want Forecast, but import only the module to avoid circular imports
from forecast_app.models.project import Project
from utils.utilities import basic_str, filename_components


class ForecastModel(models.Model):
    """
    Represents a project's model entry by a competing team, including metadata, model-specific auxiliary data beyond
    core data, and a list of the actual forecasts.
    """
    owner = models.ForeignKey(User, blank=True, null=True, help_text="The model's owner.")

    project = models.ForeignKey(Project, on_delete=models.CASCADE)

    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000, help_text="A few paragraphs describing the model. Should include "
                                                              "information on reproducing the model’s results.")

    home_url = models.URLField(help_text="The model's home site.")

    aux_data_url = models.URLField(
        null=True, blank=True,
        help_text="Optional model-specific auxiliary data directory or Zip file containing data files (e.g., "
                  "CSV files) beyond Project.core_data that were used by this model.")


    def __repr__(self):
        return str((self.pk, self.name))


    def __str__(self):  # todo
        return basic_str(self)


    def get_absolute_url(self):
        return reverse('model-detail', args=[str(self.id)])


    def get_class(self):
        """
        :return: view utility that simply returns a my class as a string. used by delete_modal_snippet.html
        """
        return self.__class__.__name__


    def html_id(self):
        """
        :return: view utility that returns a unique HTML id for this object. used by delete_modal_snippet.html
        """
        return self.__class__.__name__ + '_' + str(self.id)


    @transaction.atomic
    def load_forecast(self, csv_file_path, time_zero, file_name=None):
        """
        Loads the data from the passed Path into my corresponding ForecastData. First validates the data against my
        Project's template.

        :param csv_file_path: Path to a CDC CSV forecast file
        :param time_zero: the TimeZero this forecast applies to
        :param file_name: optional name to use for the file. if None (default), uses csv_file_path. helpful b/c uploaded
            files have random csv_file_path file names, so original ones must be extracted and passed separately
        :return: returns a new Forecast for it.
            raises a RuntimeError if the data could not be loaded
        """
        # NB: does not check if a Forecast already exists for time_zero and file_name
        file_name = file_name if file_name else csv_file_path.name

        if not filename_components(file_name):
            raise RuntimeError("Bad file name (not CDC format): {}".format(file_name))

        new_forecast = forecast_app.models.forecast.Forecast.objects.create(forecast_model=self, time_zero=time_zero,
                                                                            csv_filename=file_name)
        new_forecast.load_csv_data(csv_file_path)
        self.project.validate_forecast_data(new_forecast)
        return new_forecast


    def time_zero_for_timezero_date_str(self, timezero_date_str):
        """
        :return: the first TimeZero in forecast_model's Project that has a timezero_date matching timezero_date
        """
        for time_zero in self.project.timezero_set.all():
            if time_zero.timezero_date == timezero_date_str:
                return time_zero

        return None


    def forecast_for_time_zero(self, time_zero):
        """
        :return: the first Forecast in me corresponding to time_zero. returns None o/w. NB: tests for object equality
        """
        for forecast in self.forecast_set.all():
            if forecast.time_zero == time_zero:
                return forecast

        return None
