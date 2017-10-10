import csv

from django.db import models, connection

from utils.utilities import basic_str, parse_value


#
# ---- abstract class representing models with data ----
#

class ModelWithCDCData(models.Model):
    """
    Abstract class representing a Model with associated CDCData. todo should use proper Python abstract class feature.
    """

    # the CDCData subclass that is paired with this subclass. must be specified by my subclasses
    # todo validate: a subclass of CDCData
    cdc_data_class = None


    def load_csv_data(self, csv_template_file_path):
        """
        Loads the CDC data in csv_template_file_path into my CDCData table.

        :param csv_template_file_path:
        :return: None
        """
        # insert the data using direct SQL. for now simply use separate INSERTs per row
        with open(str(csv_template_file_path)) as csv_path_fp, \
                connection.cursor() as cursor:
            csv_reader = csv.reader(csv_path_fp, delimiter=',')

            # validate header. must be 7 columns (or 8 with the last one being '') matching
            try:
                orig_header = next(csv_reader)
            except StopIteration:
                raise RuntimeError("Empty file")

            header = orig_header
            if (len(header) == 8) and (header[7] == ''):
                header = header[:7]
            header = [i.replace('"', '') for i in header]
            if header != ['Location', 'Target', 'Type', 'Unit', 'Bin_start_incl', 'Bin_end_notincl', 'Value']:
                raise RuntimeError("Invalid header: {}".format(', '.join(orig_header)))

            for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
                if (len(row) == 8) and (row[7] == ''):
                    row = row[:7]
                if len(row) != 7:
                    raise RuntimeError("Invalid row (wasn't 7 columns): {!r}".format(row))

                location, target, row_type, unit, bin_start_incl, bin_end_notincl, value = row
                self.insert_data(cursor, location, target, row_type, unit,
                                 bin_start_incl, bin_end_notincl, value)


    def insert_data(self, cursor, location, target, row_type, unit, bin_start_incl, bin_end_notincl, value):
        """
        Inserts the passed data into a row in my associated CDCData table.
        """
        # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
        column_names = ', '.join(['location', 'target', 'row_type', 'unit', 'bin_start_incl', 'bin_end_notincl',
                                  'value', self.__class__._meta.model_name + '_id'])
        row_type = CDCData.POINT_ROW_TYPE if row_type == 'Point' else CDCData.BIN_ROW_TYPE
        sql = """
                    INSERT INTO {cdcdata_table_name} ({column_names})
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """.format(cdcdata_table_name=self.cdc_data_class._meta.db_table, column_names=column_names)
        # we use parse_value() to handle non-numeric cases like 'NA' and 'none'

        if not self.pk:
            raise Exception("Instance is not saved the the database, so can't insert data: {!r}".format(self))

        cursor.execute(sql, [location, target, row_type, unit,
                             parse_value(bin_start_incl), parse_value(bin_end_notincl), parse_value(value), self.pk])


    def get_data_rows(self):
        """
        Returns all of my data as a a list of rows, excluding any PKs and FKs columns.
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
                SELECT location, target, row_type, unit, bin_start_incl, bin_end_notincl, value
                FROM {cdcdata_table_name}
                WHERE {forecast_model_name}_id = %s;
            """.format(cdcdata_table_name=self.cdc_data_class._meta.db_table,
                       forecast_model_name=self.__class__._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk])
            return cursor.fetchall()  # rows


#
# ---- classes representing data. each of these is ~implictly paired with a  xx ----
#

class CDCData(models.Model):
    """
    Contains the content of a CDC format CSV file's row as documented in about.html . Content is manually managed by
    code, such as by ForecastModel.load_forecast(). Django manages migration (CREATE TABLE) and cascading deletion.
    """

    # the standard CDC format columns from the source forecast.data_filename:
    location = models.CharField(max_length=200)
    target = models.CharField(max_length=200)

    POINT_ROW_TYPE = 'p'
    BIN_ROW_TYPE = 'b'
    ROW_TYPE_CHOICES = ((POINT_ROW_TYPE, 'Point'),
                        (BIN_ROW_TYPE, 'Bin'))
    row_type = models.CharField(max_length=1, choices=ROW_TYPE_CHOICES)

    unit = models.CharField(max_length=200)

    bin_start_incl = models.FloatField(null=True)
    bin_end_notincl = models.FloatField(null=True)
    value = models.FloatField()


    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=['location', 'target', 'row_type', 'unit']),
        ]


    def __repr__(self):
        return str((self.pk, *self.data_row()))


    def __str__(self):  # todo
        return basic_str(self)


    def data_row(self):
        return [self.location, self.target, self.row_type, self.unit,
                self.bin_start_incl, self.bin_end_notincl, self.value]


class ProjectTemplateData(CDCData):
    """
    Represents data corresponding to a Project.
    """

    project = models.ForeignKey('Project', on_delete=models.CASCADE, null=True)


    def __repr__(self):
        return str((self.pk, self.project.pk, *self.data_row()))


class ForecastData(CDCData):
    """
    Represents data corresponding to a Forecast.
    """

    forecast = models.ForeignKey('Forecast', on_delete=models.CASCADE, null=True)


    def __repr__(self):
        return str((self.pk, self.forecast.pk, *self.data_row()))
