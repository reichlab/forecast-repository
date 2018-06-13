import csv
import io
from itertools import groupby

from django.db import models, connection
from django.db.models import Count

from utils.utilities import basic_str, parse_value, CDC_CSV_HEADER


#
# ---- abstract class representing models with data ----
#

POSTGRES_NULL_VALUE = 'NULL'  # used for Postgres-specific loading of rows from csv data files


class ModelWithCDCData(models.Model):
    """
    Abstract class representing a Model with associated CDCData. todo should use proper Python abstract class feature.
    """

    # the CDCData subclass that is paired with this subclass. must be specified by my subclasses
    # todo validate: a subclass of CDCData
    cdc_data_class = None

    csv_filename = models.CharField(max_length=200, help_text="<overridden by subclasses>")


    class Meta:
        abstract = True


    def load_csv_data(self, cdc_csv_file):
        """
        Loads the CDC data in cdc_csv_file (a Path) into my CDCData table.

        :param cdc_csv_file:
        :return: None
        """
        if not self.pk:
            raise RuntimeError("Instance is not saved the the database, so can't insert data: {!r}".format(self))

        # insert the data using direct SQL. we use psycopg2 extensions to the DB API if we're connected to a Postgres
        # server. otherwise we use execute_many() as a fallback. the reason we don't simply use the latter for Postgres
        # is because its implementation is slow ( http://initd.org/psycopg/docs/extras.html#fast-execution-helpers ).
        with open(str(cdc_csv_file)) as cdc_csv_file_fp, \
                connection.cursor() as cursor:
            rows = ModelWithCDCData.read_cdc_csv_file_rows(cdc_csv_file_fp, self.pk)  # add self.pk to end of each row
            if not rows:
                return

            # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
            table_name = self.cdc_data_class._meta.db_table
            model_name = self.__class__._meta.model_name
            columns = ['location', 'target', 'row_type', 'unit', 'bin_start_incl', 'bin_end_notincl', 'value',
                       model_name + '_id']
            if connection.vendor == 'postgresql':
                string_io = io.StringIO()
                csv_writer = csv.writer(string_io, delimiter=',')
                for location, target, row_type, unit, bin_start_incl, bin_end_notincl, value, self_pk in rows:
                    # note that we translate None -> POSTGRES_NULL_VALUE for the three nullable columns
                    csv_writer.writerow([location, target, row_type, unit,
                                         bin_start_incl if bin_start_incl is not None else POSTGRES_NULL_VALUE,
                                         bin_end_notincl if bin_end_notincl is not None else POSTGRES_NULL_VALUE,
                                         value if value is not None else POSTGRES_NULL_VALUE,
                                         self_pk])
                string_io.seek(0)
                cursor.copy_from(string_io, table_name, columns=columns, sep=',', null=POSTGRES_NULL_VALUE)
            else:  # 'sqlite', etc.
                sql = """
                    INSERT INTO {cdcdata_table_name} ({column_names})
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """.format(cdcdata_table_name=table_name, column_names=(', '.join(columns)))
                cursor.executemany(sql, rows)


    @staticmethod
    def read_cdc_csv_file_rows(cdc_csv_file_fp, model_with_cdcdata_pk):
        """
        Loads the rows from cdc_csv_file_fp, cleans them, and then returns them as a list.

        :param cdc_csv_file_fp: the *.cdc.csv data file to load - either a data file or a template one
        :param model_with_cdcdata_pk: an int PK to be included at the end of every row (will result in eight rows), or
            None (7 rows)
        :return: list of rows
        """
        csv_reader = csv.reader(cdc_csv_file_fp, delimiter=',')

        # validate header. must be 7 columns (or 8 with the last one being '') matching
        try:
            orig_header = next(csv_reader)
        except StopIteration:
            raise RuntimeError("Empty file")

        header = orig_header
        if (len(header) == 8) and (header[7] == ''):
            header = header[:7]
        header = [h.lower() for h in [i.replace('"', '') for i in header]]
        if header != CDC_CSV_HEADER:
            raise RuntimeError("Invalid header: {}".format(', '.join(orig_header)))

        # collect the rows. first we load them all into memory (processing and validating them as we go)
        rows = []
        for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
            if (len(row) == 8) and (row[7] == ''):
                row = row[:7]

            if len(row) != 7:
                raise RuntimeError("Invalid row (wasn't 7 columns): {!r}".format(row))

            location, target, row_type, unit, bin_start_incl, bin_end_notincl, value = row

            # validate row_type
            row_type = row_type.lower()
            if (row_type != CDCData.POINT_ROW_TYPE) and (row_type != CDCData.BIN_ROW_TYPE):
                raise RuntimeError("row_type was neither '{}' nor '{}': "
                                   .format(CDCData.POINT_ROW_TYPE, CDCData.BIN_ROW_TYPE))

            # use parse_value() to handle non-numeric cases like 'NA' and 'none'
            bin_start_incl = parse_value(bin_start_incl)
            bin_end_notincl = parse_value(bin_end_notincl)
            value = parse_value(value)

            # todo it's likely more efficient to instead put self.pk into the query itself, but not sure how to use '%s' with executemany outside of VALUES. could do it with a separate UPDATE query, I suppose. both queries would need to be in one transaction
            if model_with_cdcdata_pk:
                rows.append(
                    (location, target, row_type, unit, bin_start_incl, bin_end_notincl, value, model_with_cdcdata_pk))
            else:
                rows.append((location, target, row_type, unit, bin_start_incl, bin_end_notincl, value))

        return rows


    def get_data_rows(self, is_order_by_pk=None):
        """
        Returns all of my data as a a list of rows, excluding any PKs and FKs columns.

        :param is_order_by_pk: flag that controls whether the result is ordered by pk or not. default is no ordering
            (which is faster than ordering)
        """
        query_set = self.cdcdata_set.order_by('id') if is_order_by_pk else self.cdcdata_set.all()
        return [(cdc_data.location, cdc_data.target, cdc_data.row_type, cdc_data.unit,
                 cdc_data.bin_start_incl, cdc_data.bin_end_notincl, cdc_data.value)
                for cdc_data in query_set]


    def get_num_rows(self):
        """
        :return: the total of number of data rows in me, i.e., the number of Project template rows, or the number of
            Forecast rows
        """
        return self.cdcdata_set.count()


    def get_data_preview(self):
        """
        :return: view helper function that returns a preview of my data in the form of a table that's represented as a
            nested list of rows
        """
        return [(cdc_data.location, cdc_data.target, cdc_data.row_type, cdc_data.unit,
                 cdc_data.bin_start_incl, cdc_data.bin_end_notincl, cdc_data.value)
                for cdc_data in (self.cdcdata_set.all()[:10])]


    def get_locations(self):
        """
        :return: a set of all Location names in my data
        """
        return set(self.cdcdata_set.values_list('location', flat=True).distinct())


    def get_targets(self):
        """
        :return: a set of all target names in my data
        """
        return set(self.cdcdata_set.values_list('target', flat=True).distinct())


    def get_targets_for_location(self, location):
        """
        :return: a set of target names for a location in my data
        """
        return set(self.cdcdata_set.filter(location=location).values_list('target', flat=True).distinct())


    def get_target_unit(self, location, target):
        """
        :return: name of the unit column. arbitrarily uses the point row's unit. return None if not found
        """
        cdc_data_results = self.cdcdata_set.filter(location=location, target=target, row_type=CDCData.POINT_ROW_TYPE)
        return cdc_data_results[0].unit if len(cdc_data_results) != 0 else None


    def get_target_point_value(self, location, target):
        """
        NB: called repeatedly, this method is pretty slow. Probably better for callers to get point values for *all*
        locations and targets (maybe).

        :return: point value for a location and target
        """
        cdc_data_results = self.cdcdata_set.filter(location=location, target=target, row_type=CDCData.POINT_ROW_TYPE)
        return cdc_data_results[0].value if len(cdc_data_results) != 0 else None


    def get_target_bins(self, location, target):
        """
        :return: the CDCData.BIN_ROW_TYPE rows of mine for a location and target. returns a 3-tuple:
            (bin_start_incl, bin_end_notincl, value)
        """
        cdc_data_results = self.cdcdata_set.filter(location=location, target=target, row_type=CDCData.BIN_ROW_TYPE)
        return [(cdc_data.bin_start_incl, cdc_data.bin_end_notincl, cdc_data.value) for cdc_data in cdc_data_results]


    #
    # ---- data download-related functions ----
    #

    def get_data_rows_for_location_dicts(self):
        """
        :return: all my rows for use by get_location_dicts_*(). differs from get_data_rows(), which 1) uses the ORM
            instead of SQL, and 2) does not ORDER BY.
        """
        # query notes:
        # - ORDER BY: location and target make output alphabetical. it also ensures groupby() will work
        # - row_type DESC ensures CDCData.POINT_ROW_TYPE comes before CDCData.BIN_ROW_TYPE - a requirement of below
        # - id ASC ensures bins are ordered same as original csv file
        rows = self.cdcdata_set \
            .order_by('location', 'target', '-row_type', 'id') \
            .values_list('location', 'target', 'row_type', 'unit', 'bin_start_incl', 'bin_end_notincl', 'value')
        return rows


    def get_location_dicts_download_format(self):
        """
        :return: a list of dicts containing my data, suitable for JSON export of my data. each dict in the list is of
            the form:

        location_dict = {'name': location_name, 'targets': [target_dict1, target_dict2, ...]}

        where:

        target_dict1 = {'name': target_name, 'unit': target_unit, 'point': target_point, 'bins': target_bin_list}
        target_bin_list = [[bin_start_incl1, bin_end_notincl1, value1],
                           [bin_start_incl2, bin_end_notincl2, value2],
                           ...
                           ]

        This method differs from get_location_dicts_internal_format*() methods in that this one is user-facing for downloads,
        whereas the latter is used as a compact internal representation that saves having to query the database many
        times.
        """
        rows = self.get_data_rows_for_location_dicts()
        locations = []
        for location, location_grouper in groupby(rows, key=lambda _: _[0]):
            targets = []
            for target, target_grouper in groupby(location_grouper, key=lambda _: _[1]):
                # NB: this assumes that the first row is always the CDCData.POINT_ROW_TYPE point value, thanks to
                # ORDER BY, which is not true when, for example, a target has no point row. and since this method is
                # called by Project.validate_template_data(), we need to check it here
                point_row = next(target_grouper)
                if point_row[2] != CDCData.POINT_ROW_TYPE:
                    raise RuntimeError("First row was not the point row: {}".format(point_row))

                targets.append({'name': target,
                                'unit': point_row[3],
                                'point': point_row[-1],
                                'bins': [bin_list[-3:] for bin_list in target_grouper]})
            locations.append({'name': location, 'targets': targets})
        return locations


    @staticmethod
    def get_location_dicts_internal_format_for_cdc_csv_file(cdc_csv_file):
        """
        Returns same as get_location_dicts_internal_format(), but is passed a template file (Path) to load from instead of using
        my table's data.
        """
        with open(str(cdc_csv_file)) as cdc_csv_file_fp:
            rows = ModelWithCDCData.read_cdc_csv_file_rows(cdc_csv_file_fp, None)  # no self.pk at end of each row


            # sort so groupby() will work
            def key(row):
                location, target, row_type, unit, bin_start_incl, bin_end_notincl, value = row
                # row_type: we want this order: CDCData.POINT_ROW_TYPE before CDCData.BIN_ROW_TYPE - this is for
                # _get_location_dicts_internal_format_for_rows()
                return location, target, 0 if row_type == CDCData.POINT_ROW_TYPE else 1


            rows.sort(key=key)

            return ModelWithCDCData._get_location_dicts_internal_format_for_rows(rows)


    def get_location_dicts_internal_format(self):
        """
        Returns all of my data as a dict. used as a compact internal representation (a temporary in-memory cache) that
        saves having to query the database many times. This is an alternative to more granular SQL queries - see
        get_locations() and get_target_*() methods above, which end up having a lot of overhead when processing bins.

        :return: my data in hierarchical format as a dict of the form:

            {location1: target_dict_1, location2: target_dict_2, ...}

            where each target_dict is of the form:

            {target1: {'unit': unit1, 'point': point_val1, 'bins': bin_list1},
             target2: {'unit': unit2, 'point': point_val2, 'bins': bin_list2},
             ...
            }

            where each bin_list is like:

            [[bin_start_incl1, bin_end_notincl1, value1],
             [bin_start_incl2, bin_end_notincl2, value2],
             ...
            ]

        NB: For performance, instead of using data accessors like self.get_locations() and self.get_target_bins(), we
        load all rows into memory and then iterate over them there.
        """
        rows = self.get_data_rows_for_location_dicts()
        return ModelWithCDCData._get_location_dicts_internal_format_for_rows(rows)


    @staticmethod
    def _get_location_dicts_internal_format_for_rows(rows):
        location_target_dict = {}
        for location, location_grouper in groupby(rows, key=lambda _: _[0]):
            target_dict = {}
            for target, target_grouper in groupby(location_grouper, key=lambda _: _[1]):
                # NB: this assumes that the first row is always the CDCData.POINT_ROW_TYPE point value, thanks to
                # ORDER BY, which is not true when, for example, a target has no point row. and since this method is
                # called by Project.validate_template_data(), we need to check it here
                point_row = next(target_grouper)
                if point_row[2] != CDCData.POINT_ROW_TYPE:
                    raise RuntimeError("First row was not the point row: {}".format(point_row))

                target_dict[target] = {'unit': point_row[3],
                                       'point': point_row[-1],
                                       'bins': [bin_list[-3:] for bin_list in target_grouper]}
            location_target_dict[location] = target_dict
        return location_target_dict


#
# ---- classes representing data. each of these is ~implictly paired with a  xx ----
#

class CDCData(models.Model):
    """
    Contains the content of a CDC format CSV file's row as documented in about.html . Content is manually managed by
    code, such as by ForecastModel.load_forecast(). Django manages migration (CREATE TABLE) and cascading deletion.
    """

    # the standard CDC format columns from the source forecast.csv_filename:
    location = models.CharField(max_length=200)
    target = models.CharField(max_length=200)

    POINT_ROW_TYPE = 'point'
    BIN_ROW_TYPE = 'bin'
    ROW_TYPE_CHOICES = ((POINT_ROW_TYPE, 'Point'),
                        (BIN_ROW_TYPE, 'Bin'))
    row_type = models.CharField(max_length=5, choices=ROW_TYPE_CHOICES)

    unit = models.CharField(max_length=200)

    bin_start_incl = models.FloatField(null=True)  # nullable b/c some bins have non-numeric values, e.g., 'NA'
    bin_end_notincl = models.FloatField(null=True)  # ""
    value = models.FloatField(null=True)


    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=['location']),
            models.Index(fields=['target']),
            models.Index(fields=['row_type']),
            models.Index(fields=['unit']),
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

    project = models.ForeignKey('Project', on_delete=models.CASCADE, null=True,
                                related_name='cdcdata_set')  # NB: related_name in all CDCData sublclasses must be the same


    def __repr__(self):
        return str((self.pk, self.project.pk, *self.data_row()))


class ForecastData(CDCData):
    """
    Represents data corresponding to a Forecast.
    """

    forecast = models.ForeignKey('Forecast', on_delete=models.CASCADE, null=True,
                                 related_name='cdcdata_set')  # NB: related_name in all CDCData sublclasses must be the same


    def __repr__(self):
        return str((self.pk, self.forecast.pk, *self.data_row()))
