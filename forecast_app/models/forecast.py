from collections import OrderedDict

from django.db import models, connection
from django.urls import reverse

from forecast_app.models.data import ForecastData, CDCData, ModelWithCDCData
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


    def get_num_rows(self):
        return len(self.get_data_rows())  # todo query only for count(*)


    def get_data_preview(self):
        """
        :return: a preview of my data in the form of a table that's represented as a nested list of rows
        """
        return self.get_data_rows()[:10]  # todo query LIMIT 10


    def get_locations(self):
        """
        :return: a set of Location names corresponding to my ForecastData
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT location
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s
            GROUP BY location;
        """.format(cdcdata_table_name=ForecastData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk])
            rows = cursor.fetchall()
            return {row[0] for row in rows}


    def get_targets(self, location):
        """
        :return: a set of target names for a location
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT target
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s AND location = %s
            GROUP BY target;
        """.format(cdcdata_table_name=ForecastData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk, location])
            rows = cursor.fetchall()
            return {row[0] for row in rows}


    def get_target_unit(self, location, target):
        """
        :return: name of the unit column. arbitrarily uses the point row's unit
        """
        cdc_data_results = self.forecastdata_set.filter(location=location, target=target,
                                                        row_type=CDCData.POINT_ROW_TYPE)
        return cdc_data_results[0].unit


    def get_target_point_value(self, location, target):
        """
        :return: point value for a location and target 
        """
        cdc_data_results = self.forecastdata_set.filter(location=location, target=target,
                                                        row_type=CDCData.POINT_ROW_TYPE)
        return cdc_data_results[0].value


    def get_target_bins(self, location, target):
        """
        :return: the CDCData.BIN_ROW_TYPE rows of mine for a location and target
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT bin_start_incl, bin_end_notincl, value
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s AND row_type = %s AND location = %s and target = %s;
        """.format(cdcdata_table_name=ForecastData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk, CDCData.BIN_ROW_TYPE, location, target])
            rows = cursor.fetchall()
            return [(bin_start_incl, bin_end_notincl, value) for bin_start_incl, bin_end_notincl, value in rows]


    def get_location_target_dict(self):
        """
        :return: all my data in hierarchical format as a dict of the form:

            return val: {location1: target_dict_1, ...}
                target_dict: {target1: {'point': point_val, 'bins': bin_list}}
                    bin_list: [[bin_start_incl1, bin_end_notincl1, value1], ...]
        """
        location_target_dict = OrderedDict()
        for location in sorted(self.get_locations()):
            target_dict = OrderedDict()
            for target in sorted(self.get_targets(location)):
                point_value = self.get_target_point_value(location, target)
                bins = self.get_target_bins(location, target)
                target_dict[target] = {'point': point_value, 'bins': bins}
            location_target_dict[location] = target_dict
        return location_target_dict
