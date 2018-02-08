import click
from django.contrib.auth.models import User
from django.db import models, transaction
from django.urls import reverse

import forecast_app.models.forecast  # we want Forecast, but import only the module to avoid circular imports
from forecast_app.models.project import Project
from utils.utilities import basic_str, cdc_csv_components_from_data_dir


class ForecastModel(models.Model):
    """
    Represents a project's model entry by a competing team, including metadata, model-specific auxiliary data beyond
    core data, and a list of the actual forecasts.
    """
    owner = models.ForeignKey(User, blank=True, null=True, help_text="The model's owner.")

    project = models.ForeignKey(Project, related_name='models', on_delete=models.CASCADE)

    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000, help_text="A few paragraphs describing the model. Please see "
                                                              "documentation forwhat should be included here - "
                                                              "information on reproducing the model’s results, etc.")

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
        return reverse('model-detail', args=[str(self.pk)])


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


    @transaction.atomic
    def load_forecast(self, csv_file_path, time_zero, file_name=None):
        """
        Loads the data from the passed Path into my corresponding ForecastData. First validates the data against my
        Project's template. NB: does not check if a Forecast already exists for time_zero and file_name.

        :param csv_file_path: Path to a CDC CSV forecast file
        :param time_zero: the TimeZero this forecast applies to
        :param file_name: optional name to use for the file. if None (default), uses csv_file_path. helpful b/c uploaded
            files have random csv_file_path file names, so original ones must be extracted and passed separately
        :return: returns a new Forecast for it.
            raises a RuntimeError if the data could not be loaded
        """
        # validate time_zero
        if time_zero not in self.project.timezeros.all():
            raise RuntimeError("time_zero was not in project. time_zero={}, project.timezeros={}"
                               .format(time_zero, self.project.timezeros.all()))

        file_name = file_name if file_name else csv_file_path.name
        new_forecast = forecast_app.models.forecast.Forecast.objects.create(forecast_model=self, time_zero=time_zero,
                                                                            csv_filename=file_name)
        new_forecast.load_csv_data(csv_file_path)
        self.project.validate_forecast_data(new_forecast)
        return new_forecast


    def load_forecasts_from_dir(self, data_dir, success_callback=None, fail_callback=None):
        """
        Adds Forecast objects to me using the cdc csv files under data_dir. Assumes TimeZeros match those in my Project.
        Returns a list of them. Skips files that cause load_forecast() to raise a RuntimeError.

        :param data_dir: Path of the directory that contains cdc csv files
        :param success_callback: a function of one arg (cdc_csv_file) that's called after a Forecast has loaded
        :param fail_callback: a function of two args (cdc_csv_file, exception) that's called after a Forecast has
            failed to load
        :return list of loaded Forecasts
        """
        forecasts = []
        for cdc_csv_file, time_zero, _, _ in cdc_csv_components_from_data_dir(data_dir):
            time_zero = self.project.time_zero_for_timezero_date(time_zero)
            if not time_zero:
                raise RuntimeError("no time_zero found. cdc_csv_file={}, time_zero={}\nProject time_zeros={}"
                                   .format(cdc_csv_file, time_zero, self.project.timezeros.all()))

            try:
                forecast = self.load_forecast(cdc_csv_file, time_zero)
                forecasts.append(forecast)
                if success_callback:
                    success_callback(cdc_csv_file)
            except RuntimeError as rte:
                if fail_callback:
                    fail_callback(cdc_csv_file, rte)
        if not forecasts:
            click.echo("Warning: no valid forecast files found in directory: {}".format(data_dir))
        return forecasts


    def forecast_for_time_zero(self, time_zero):
        """
        :return: the first Forecast in me corresponding to time_zero. returns None o/w. NB: tests for object equality
        """
        for forecast in self.forecasts.all():
            if forecast.time_zero == time_zero:
                return forecast

        return None
