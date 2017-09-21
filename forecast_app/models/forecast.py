from django.db import models, connection
from django.urls import reverse

from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.project import TimeZero
from utils.utilities import basic_str, parse_value


class Forecast(models.Model):
    """
    Represents a model's forecasted data. There is one Forecast for each of my ForecastModel's Project's TimeZeros.
    """
    forecast_model = models.ForeignKey(ForecastModel, on_delete=models.CASCADE, null=True)

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


    def get_data_rows(self):
        """
        Main accessor of my data. Abstracts where data is located.

        :return: a list of my rows, excluding CDCData PK and Forecast FK
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT id, location, target, row_type, unit, bin_start_incl, bin_end_notincl, value,
             {forecast_model_name}_id
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s;
        """.format(cdcdata_table_name=CDCData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk])
            rows = cursor.fetchall()
            return [row[1:-1] for row in rows]


    def get_num_rows(self):
        return len(self.get_data_rows())  # todo query only for count(*)


    def get_data_preview(self):
        """
        :return: a preview of my data in the form of a table that's represented as a nested list of rows
        """
        return self.get_data_rows()[:10]  # todo query LIMIT 10


    def get_locations(self):
        """
        :return: a set of Location names corresponding to my CDCData
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT location
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s
            GROUP BY location;
        """.format(cdcdata_table_name=CDCData._meta.db_table,
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
        """.format(cdcdata_table_name=CDCData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk, location])
            rows = cursor.fetchall()
            return {row[0] for row in rows}


    def get_target_unit(self, location, target):
        """
        :return: name of the unit column. arbitrarily uses the point row's unit
        """
        cdc_data_results = self.cdcdata_set.filter(location=location, target=target, row_type=CDCData.POINT_ROW_TYPE)
        return cdc_data_results[0].unit



    def get_target_point_value(self, location, target):
        """
        :return: point value for a location and target 
        """
        cdc_data_results = self.cdcdata_set.filter(location=location, target=target, row_type=CDCData.POINT_ROW_TYPE)
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
        """.format(cdcdata_table_name=CDCData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk, CDCData.BIN_ROW_TYPE, location, target])
            rows = cursor.fetchall()
            return [(bin_start_incl, bin_end_notincl, value) for bin_start_incl, bin_end_notincl, value in rows]


    def insert_data(self, cursor, location, target, row_type, unit, bin_start_incl, bin_end_notincl, value):
        """
        Called by ForecastModel.load_forecast_via_sql(), inserts the passed data into my CDCData table.
        """
        # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use CDCData._meta.fields ?
        column_names = ', '.join(['location', 'target', 'row_type', 'unit', 'bin_start_incl', 'bin_end_notincl',
                                  'value', Forecast._meta.model_name + '_id'])
        row_type = CDCData.POINT_ROW_TYPE if row_type == 'Point' else CDCData.BIN_ROW_TYPE
        sql = """
                    INSERT INTO {cdcdata_table_name} ({column_names})
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """.format(cdcdata_table_name=CDCData._meta.db_table,
                           column_names=column_names)
        # we use parse_value() to handle non-numeric cases like 'NA' and 'none'
        cursor.execute(sql, [location, target, row_type, unit,
                             parse_value(bin_start_incl), parse_value(bin_end_notincl), parse_value(value), self.pk])


class CDCData(models.Model):
    """
    Contains the content of a CDC format CSV file as documented in about.html . Content is manually managed by
    ForecastModel.load_forecast. Django manages migration (CREATE TABLE) and cascading deletion.
    """
    forecast = models.ForeignKey(Forecast, on_delete=models.CASCADE, null=True)

    # the standard CDC format columns from the source forecast.data_filename:
    location = models.CharField(max_length=200, db_index=True)
    target = models.CharField(max_length=200, db_index=True)

    POINT_ROW_TYPE = 'p'
    BIN_ROW_TYPE = 'b'
    ROW_TYPE_CHOICES = ((POINT_ROW_TYPE, 'Point'),
                        (BIN_ROW_TYPE, 'Bin'))
    row_type = models.CharField(max_length=1, choices=ROW_TYPE_CHOICES, db_index=True)

    unit = models.CharField(max_length=200, db_index=True)

    bin_start_incl = models.FloatField(null=True)
    bin_end_notincl = models.FloatField(null=True)
    value = models.FloatField()


    def __repr__(self):
        return str((self.pk, self.forecast.pk, *self.data_row()))


    def __str__(self):  # todo
        return basic_str(self)


    def data_row(self):
        return [self.location, self.target, self.row_type, self.unit,
                self.bin_start_incl, self.bin_end_notincl, self.value]
