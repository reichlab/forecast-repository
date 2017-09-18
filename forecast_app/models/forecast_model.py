import csv

from django.db import connection
from django.db import models, transaction
from django.urls import reverse

import forecast_app.models.forecast  # we want Forecast, but import only the module to avoid circular imports
from forecast_app.models.project import Project
from utils.utilities import basic_str, parse_value, filename_components, increment_week, delphi_wili_for_epi_week, \
    week_increment_for_target_name


class ForecastModel(models.Model):
    """
    Represents a project's model entry by a competing team, including metadata, model-specific auxiliary data beyond
    core data, and a list of the actual forecasts.
    """
    project = models.ForeignKey(Project, on_delete=models.CASCADE, null=True)

    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000, help_text="A few paragraphs describing the model. should include "
                                                              "information on reproducing the model’s results")

    url = models.URLField(help_text="The model's development URL")

    auxiliary_data = models.URLField(null=True,
                                     help_text="optional model-specific Zip file containing data files (e.g., CSV "
                                               "files) beyond Project.core_data that were used by the this model")

    def __repr__(self):
        return str((self.pk, self.name))

    def __str__(self):  # todo
        return basic_str(self)

    def get_absolute_url(self):
        return reverse('forecastmodel-detail', args=[str(self.id)])

    @transaction.atomic
    # def load_forecast(self, csv_file_path, time_zero):  # alternative implementation using direct SQL INSERTs
    def load_forecast_via_sql(self, csv_file_path, time_zero):  # alternative implementation using direct SQL INSERTs
        """
        :param csv_file_path: Path to a CDC CSV forecast file
        :param time_zero: the TimeZero this forecast applies to
        :return: loads the data from the passed Path into my corresponding CDCData, and returns a new Forecast for it
        """
        forecast = forecast_app.models.forecast.Forecast.objects.create(
            forecast_model=self, time_zero=time_zero, data_filename=csv_file_path.name)

        # insert the data using direct SQL. for now simply use separate INSERTs per row
        with open(str(csv_file_path)) as csv_path_fp, \
                connection.cursor() as cursor:
            csv_reader = csv.reader(csv_path_fp, delimiter=',')
            next(csv_reader)  # skip header
            for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
                location, target, row_type, unit, bin_start_incl, bin_end_notincl, value = row[:7]
                forecast.insert_data(cursor, location, target, row_type, unit,
                                     bin_start_incl, bin_end_notincl, value)
        # done
        return forecast

    @transaction.atomic
    # def load_forecast_using_managed(self, csv_file_path, time_zero):
    def load_forecast(self, csv_file_path, time_zero):  # alternative implementation using ORM
        """
        :param csv_file_path: Path to a CDC CSV forecast file
        :param time_zero: the TimeZero this forecast applies to
        :return: loads the data from the passed Path into my corresponding CDCData, and returns a new Forecast for it
        """
        forecast = forecast_app.models.forecast.Forecast.objects.create(
            forecast_model=self, time_zero=time_zero, data_filename=csv_file_path.name)

        # NB: bulk insert the data using the ORM might be slow! if so, use load_forecast_via_sql()
        cdc_data_objs = []
        with open(str(csv_file_path)) as csv_path_fp:
            csv_reader = csv.reader(csv_path_fp, delimiter=',')
            next(csv_reader)  # skip header
            for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
                location, target, row_type, unit, bin_start_incl, bin_end_notincl, value = row[:7]
                cdc_data_objs.append(forecast_app.models.forecast.CDCData(
                    forecast=forecast, location=location, target=target,
                    row_type=forecast_app.models.forecast.CDCData.POINT_ROW_TYPE if row_type == 'Point' else forecast_app.models.forecast.CDCData.BIN_ROW_TYPE,
                    unit=unit,
                    bin_start_incl=parse_value(bin_start_incl),
                    bin_end_notincl=parse_value(bin_end_notincl),
                    value=value))
        forecast_app.models.forecast.CDCData.objects.bulk_create(cdc_data_objs)  # ignore returned list of new objects

        # done
        return forecast

    def time_zero_for_timezero_date_str(self, timezero_date_str):
        """
        :return: the first TimeZero in forecast_model's Project that has a timezero_date matching timezero_date
        """
        for time_zero in self.project.timezero_set.all():
            if time_zero.timezero_date == timezero_date_str:
                return time_zero

        return None

    def mean_absolute_error(self, season_start_year, location, target,
                            wili_for_epi_week_fcn=delphi_wili_for_epi_week):
        """
        :param:season_start_year: year of the season, e.g., 2016 for the season 2016-2017
        :param:true_value_for_epi_week_fcn: a function of three args (year, week, location_name) that returns the
            true/actual wili value for an epi week
        :return: mean absolute error (scalar) for my predictions for a location and target
        """
        cdc_file_name_to_abs_error = {}
        for forecast in self.forecast_set.all():
            # set timezero week and year, inferring the latter based on @Evan's @Nick's reply:
            # > We used week 30.  I don't think this is a standardized concept outside of our lab though."
            # > We use separate concepts for a "season" and a "year". So, e.g. the "2016/2017 season" starts with
            # > EW30-2016 and ends with EW29-2017.
            # todo abstract this to elsewhere
            timezero_week = filename_components(forecast.data_filename)[0]
            timezero_year = season_start_year if timezero_week >= 30 else season_start_year + 1
            future_year, future_week = increment_week(timezero_year, timezero_week,
                                                      week_increment_for_target_name(target))
            true_value = wili_for_epi_week_fcn(future_year, future_week, location)
            predicted_value = forecast.get_target_point_value(location, target)
            abs_error = abs(predicted_value - true_value)
            # print('xx', forecast, ':', timezero_week, timezero_year, '.', future_week, future_year, '.', true_value, predicted_value, abs_error)
            cdc_file_name_to_abs_error[forecast.data_filename] = abs_error

        return sum(cdc_file_name_to_abs_error.values()) / len(cdc_file_name_to_abs_error)
