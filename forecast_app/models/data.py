import csv
import io
from itertools import groupby

from django.db import models, connection

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


    def locations_qs(self):  # abstract method
        """
        :return: a QuerySet of all my Locations, queried from the database
        """
        raise NotImplementedError()


    def targets_qs(self):  # abstract method
        """
        :return: a QuerySet of all my Targets, queried from the database
        """
        raise NotImplementedError()


    def load_csv_data(self, csv_file_path_or_fp, skip_zero_bins):
        """
        Loads the CDC data in csv_file_path_or_fp (a Path) into my CDCData table.

        :param csv_file_path_or_fp: Path to a CDC CSV forecast file, OR an already-open file-like object
        :param skip_zero_bins: passed to read_cdc_csv_file_rows()
        """
        if not self.pk:
            raise RuntimeError("Instance is not saved the the database, so can't insert data: {!r}".format(self))

        # https://stackoverflow.com/questions/1661262/check-if-object-is-file-like-in-python
        if isinstance(csv_file_path_or_fp, io.IOBase):
            self._load_csv_data(csv_file_path_or_fp, skip_zero_bins)
        else:
            with open(str(csv_file_path_or_fp)) as cdc_csv_file_fp:
                self._load_csv_data(cdc_csv_file_fp, skip_zero_bins)


    def _load_csv_data(self, cdc_csv_file_fp, skip_zero_bins):
        """
        Inserts the data using direct SQL. Dynamically creates Locations and Targets if they're not found.

        We use psycopg2 extensions to the DB API if we're connected to a Postgres server. otherwise we use
        execute_many() as a fallback. the reason we don't simply use the latter for Postgres is because its
        implementation is slow ( http://initd.org/psycopg/docs/extras.html#fast-execution-helpers ).
        """
        with connection.cursor() as cursor:
            # add self.pk to end of each row. is_append_model_with_cdcdata_pk, skip_zero_bins:
            location_names, target_names, rows = self.read_cdc_csv_file_rows(cdc_csv_file_fp, True, skip_zero_bins)
            if not rows:
                return

            # create missing Locations and Targets, then patch rows to replace names with PKs. note that unlike
            # Locations, which only have a name, Targets have additional fields that need filling out by users. but here
            # all we can set are names
            from forecast_app.models import Project, Location, Target  # avoid circular imports


            project = self if isinstance(self, Project) else self.forecast_model.project

            location_name_to_pk = {location.name: location.id for location in self.locations_qs().all()}
            for location_name in location_names:
                if location_name not in location_name_to_pk:
                    location_name_to_pk[location_name] = Location.objects.create(project=project, name=location_name).pk

            target_name_to_pk = {target.name: target.id for target in self.targets_qs().all()}
            for target_name in target_names:
                if target_name not in target_name_to_pk:
                    target_name_to_pk[target_name] = Target.objects.create(project=project, name=target_name).pk

            for row in rows:  # location_name, target_name, row_type, bin_start_incl, bin_end_notincl, value, self_pk
                row[0] = location_name_to_pk[row[0]]
                row[1] = target_name_to_pk[row[1]]

            # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
            table_name = self.cdc_data_class._meta.db_table
            model_name = self.__class__._meta.model_name
            columns = [self.cdc_data_class._meta.get_field('location').column,
                       self.cdc_data_class._meta.get_field('target').column,
                       self.cdc_data_class._meta.get_field('is_point_row').column,
                       self.cdc_data_class._meta.get_field('bin_start_incl').column,
                       self.cdc_data_class._meta.get_field('bin_end_notincl').column,
                       self.cdc_data_class._meta.get_field('value').column,
                       model_name + '_id']
            if connection.vendor == 'postgresql':
                string_io = io.StringIO()
                csv_writer = csv.writer(string_io, delimiter=',')
                for location_id, target_id, row_type, bin_start_incl, bin_end_notincl, value, self_pk in rows:
                    # note that we translate None -> POSTGRES_NULL_VALUE for the three nullable columns
                    csv_writer.writerow([location_id, target_id, row_type,
                                         bin_start_incl if bin_start_incl is not None else POSTGRES_NULL_VALUE,
                                         bin_end_notincl if bin_end_notincl is not None else POSTGRES_NULL_VALUE,
                                         value if value is not None else POSTGRES_NULL_VALUE,
                                         self_pk])
                string_io.seek(0)
                cursor.copy_from(string_io, table_name, columns=columns, sep=',', null=POSTGRES_NULL_VALUE)
            else:  # 'sqlite', etc.
                sql = """
                    INSERT INTO {cdcdata_table_name} ({column_names})
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """.format(cdcdata_table_name=table_name, column_names=(', '.join(columns)))
                cursor.executemany(sql, rows)


    def read_cdc_csv_file_rows(self, cdc_csv_file_fp, is_append_model_with_cdcdata_pk, skip_zero_bins):
        """
        Loads the rows from cdc_csv_file_fp, cleans them, and then returns them as a list. Does some basic validation,
        but does not check locations and targets against the template. This is b/c Locations and Targets might not yet
        exist (if they're dynamically created by this method's callers).

        :param cdc_csv_file_fp: the *.cdc.csv data file to load - either a data file or a template one
        :param is_append_model_with_cdcdata_pk: true if my PK should be included at the end of every row (will result
            in eight rows), or None (7 rows)
        :param skip_zero_bins: True if bin rows with a value of zero should be skipped
        :return: a 3-tuple: (location_names, target_names, rows) where the first two are sets and the last is a list of
            rows: location_name, target_name, row_type, bin_start_incl, bin_end_notincl, value,
            [, model_with_cdcdata_pk]  <- only if model_with_cdcdata_pk. NB: does not include unit
        """
        csv_reader = csv.reader(cdc_csv_file_fp, delimiter=',')

        # validate header. must be 7 columns (or 8 with the last one being '') matching
        try:
            orig_header = next(csv_reader)
        except StopIteration:  # a kind of Exception, so much come first
            raise RuntimeError("Empty file.")
        except Exception as exc:
            raise RuntimeError("Error reading from cdc_csv_file_fp={}. exc={}".format(cdc_csv_file_fp, exc))

        header = orig_header
        if (len(header) == 8) and (header[7] == ''):
            header = header[:7]
        header = [h.lower() for h in [i.replace('"', '') for i in header]]
        if header != CDC_CSV_HEADER:
            raise RuntimeError("Invalid header: {}".format(', '.join(orig_header)))

        # collect the rows. first we load them all into memory (processing and validating them as we go)
        locations = set()
        targets = set()
        rows = []
        for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
            if (len(row) == 8) and (row[7] == ''):
                row = row[:7]

            if len(row) != 7:
                raise RuntimeError("Invalid row (wasn't 7 columns): {!r}".format(row))

            location_name, target_name, row_type, unit, bin_start_incl, bin_end_notincl, value = row  # unit ignored

            # validate row_type
            row_type = row_type.lower()
            if (row_type != CDCData.POINT_ROW_TYPE) and (row_type != CDCData.BIN_ROW_TYPE):
                raise RuntimeError("row_type was neither '{}' nor '{}': "
                                   .format(CDCData.POINT_ROW_TYPE, CDCData.BIN_ROW_TYPE))
            is_point_row = (row_type == CDCData.POINT_ROW_TYPE)

            locations.add(location_name)
            targets.add(target_name)

            # use parse_value() to handle non-numeric cases like 'NA' and 'none'
            bin_start_incl = parse_value(bin_start_incl)
            bin_end_notincl = parse_value(bin_end_notincl)
            value = parse_value(value)

            # skip bin rows with a value of zero - a storage (and thus performance) optimization that does not affect
            # score calculation, etc. see [Consider not storing bin rows with zero values #84](https://github.com/reichlab/forecast-repository/issues/84)
            # Note however from that issue:
            #   Point 3 means Zoltar's export features (CSV and JSON formats) will not include those skipped rows. Thus,
            #   the exported CSV files will not be identical to the imported ones. This represents the first change in
            #   Zoltar in which data is lost.
            if skip_zero_bins and (row_type == CDCData.BIN_ROW_TYPE) and (value == 0):
                continue

            # todo it's likely more efficient to instead put self.pk into the query itself, but not sure how to use '%s' with executemany outside of VALUES. could do it with a separate UPDATE query, I suppose. both queries would need to be in one transaction
            if is_append_model_with_cdcdata_pk:
                rows.append([location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value, self.id])
            else:
                rows.append([location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value])

        return locations, targets, rows


    @classmethod
    def transform_row_to_output_format(cls, row):
        """
        :param: row: location, target, is_point_row, bin_start_incl, bin_end_notincl, value
        :return: row with is_point_row replaced by either CDCData.POINT_ROW_TYPE or CDCData.BIN_ROW_TYPE
        """
        location, target, is_point_row, unit, bin_start_incl, bin_end_notincl, value = row
        row_type = CDCData.POINT_ROW_TYPE if is_point_row else CDCData.BIN_ROW_TYPE
        return [location, target, row_type, unit, bin_start_incl, bin_end_notincl, value]


    def get_data_rows(self, is_order_by_pk=False):
        """
        Returns all of my data as a a list of rows, excluding any PKs and FKs columns. Target names are returned.

        :param is_order_by_pk: flag that controls whether the result is ordered by pk or not. default is no ordering,
            which is faster than ordering but nondeterministic
        """
        query_set = self.cdcdata_set.order_by('id') if is_order_by_pk else self.cdcdata_set.all()
        return [ModelWithCDCData.transform_row_to_output_format(row)
                for row in query_set.values_list('location__name', 'target__name', 'is_point_row', 'target__unit',
                                                 'bin_start_incl', 'bin_end_notincl', 'value')]


    def get_num_rows(self):
        """
        :return: the total of number of data rows in me, i.e., the number of Project template rows, or the number of
            Forecast rows
        """
        return self.cdcdata_set.count()


    def get_data_preview(self):
        """
        :return: view helper function that returns a preview of my data in the form of a table that's represented as a
            nested list of rows. Target names are returned.
        """
        return [ModelWithCDCData.transform_row_to_output_format(row)
                for row in self.cdcdata_set.values_list('location__name', 'target__name', 'is_point_row',
                                                        'target__unit',
                                                        'bin_start_incl', 'bin_end_notincl', 'value')[:10]]


    def get_location_names(self):
        """
        :return: queries all my data and returns a set of all Location names
        """
        return set(self.cdcdata_set.values_list('location__name', flat=True).distinct())


    def get_target_names(self):
        """
        :return: queries all my data and returns a set of all target names in my data
        """
        return set(self.cdcdata_set.values_list('target__name', flat=True).distinct())


    def get_target_names_for_location(self, location_name):
        """
        :return: a set of target names for a location in my data
        """
        return set(self.cdcdata_set.filter(location__name=location_name)
                   .values_list('target__name', flat=True)
                   .distinct())


    def get_target_point_value(self, location_name, target_name):
        """
        NB: called repeatedly, this method is pretty slow. Probably better for callers to get point values for *all*
        locations and targets (maybe).

        :return: point value for a location_name and target_name
        """
        cdc_data_results = self.cdcdata_set.filter(location__name=location_name, target__name=target_name,
                                                   is_point_row=True)
        return cdc_data_results[0].value if len(cdc_data_results) != 0 else None


    def get_target_bins(self, location_name, target_name):
        """
        :return: the CDCData.BIN_ROW_TYPE rows of mine for a location and target. returns a 3-tuple:
            (bin_start_incl, bin_end_notincl, value)
        """
        cdc_data_results = self.cdcdata_set.filter(location__name=location_name, target__name=target_name,
                                                   is_point_row=False)  # bin
        return [(cdc_data.bin_start_incl, cdc_data.bin_end_notincl, cdc_data.value) for cdc_data in cdc_data_results]


    #
    # ---- data download-related functions ----
    #

    def _get_data_row_qs_for_location_dicts(self):
        """
        :return: all my rows for use by get_location_dicts_*(). differs from get_data_rows(), which 1) uses the ORM
            instead of SQL, and 2) does not ORDER BY. Target ids are returned.
        """
        # query notes: ORDER BY:
        # - location__id and target__id make output deterministic. it also ensures groupby() will work
        # - is_point_row DESC ensures CDCData.POINT_ROW_TYPE comes before CDCData.BIN_ROW_TYPE - a requirement of below
        # - id ASC ensures bins are ordered same as original csv file
        rows = self.cdcdata_set \
            .order_by('location__id', 'target__id', '-is_point_row', 'id') \
            .values_list('location__id', 'target__id', 'is_point_row', 'target__unit',
                         'bin_start_incl', 'bin_end_notincl', 'value')
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
        row_qs = self._get_data_row_qs_for_location_dicts()
        location_pks_to_names = {location.id: location.name for location in self.locations_qs().all()}
        target_pks_to_names = {target.id: target.name for target in self.targets_qs().all()}
        locations = []
        for location_id, location_grouper in groupby(row_qs, key=lambda _: _[0]):
            targets = []
            for target_id, target_grouper in groupby(location_grouper, key=lambda _: _[1]):
                # NB: this assumes that the first row is always the CDCData.POINT_ROW_TYPE point value, thanks to
                # ORDER BY, which is not true when, for example, a target has no point row. and since this method is
                # called by Project.validate_template_data(), we need to check it here

                # location__id, target__id, is_point_row, target__unit, bin_start_incl, bin_end_notincl, value:
                point_row = next(target_grouper)
                if not point_row[2]:  # is_point_row
                    raise RuntimeError("First row was not the point row: {}".format(point_row))

                targets.append({'name': target_pks_to_names[target_id],
                                'unit': point_row[3],
                                'point': point_row[-1],
                                'bins': [bin_list[-3:] for bin_list in target_grouper]})
            locations.append({'name': location_pks_to_names[location_id], 'targets': targets})
        return locations


    def get_loc_dicts_int_format_for_csv_file(self, cdc_csv_file):
        """
        :return same as get_location_dicts_internal_format(), but is passed a template file (Path) to load from instead
            of using my table's data.
        """
        with open(str(cdc_csv_file)) as cdc_csv_file_fp:
            # no self.pk at end of each row. is_append_model_with_cdcdata_pk, skip_zero_bins:
            location_names, target_names, rows = self.read_cdc_csv_file_rows(cdc_csv_file_fp, False, False)


            # sort so groupby() will work
            def key(row):
                location, target, is_point_row, bin_start_incl, bin_end_notincl, value = row
                # row_type: we want this order: CDCData.POINT_ROW_TYPE before CDCData.BIN_ROW_TYPE - this is for
                # _get_location_dicts_internal_format_for_rows()
                return location, target, not is_point_row


            rows.sort(key=key)

            # NB: we are passing location and target strings, not PKs:
            return self._get_location_dicts_internal_format_for_rows(rows)


    def get_location_dicts_internal_format(self):
        """
        Returns all of my data as a dict. used as a compact internal representation (a temporary in-memory cache) that
        saves having to query the database many times. This is an alternative to more granular SQL queries - see
        get_location_*() and get_target_*() methods above, which end up having a lot of overhead when processing bins.

        :return: my data in hierarchical format as a dict of the form:

            {location1_name: target_dict_1, location2: target_dict_2, ...}

            where each target_dict is of the form:

            {target1_name: {'point': point_val1, 'bins': bin_list1},
             target2_name: {'point': point_val2, 'bins': bin_list2},
             ...
            }

            where each bin_list is like:

            [[bin_start_incl1, bin_end_notincl1, value1],
             [bin_start_incl2, bin_end_notincl2, value2],
             ...
            ]

        NB: For performance, instead of using data accessors like self.get_location_names() and self.get_target_bins(), we
        load all rows into memory and then iterate over them there.
        """
        # NB: we are passing location and target PKs, not strings:
        return self._get_location_dicts_internal_format_for_rows(self._get_data_row_qs_for_location_dicts())


    def _get_location_dicts_internal_format_for_rows(self, rows):
        # NB: rows can be one of two formats: a QuerySet (containing tuples) or a list of lists. depending on that, the
        # first two items (location and target) are either PKs (for QuerySets) or strings (for lists)
        location_pk_to_name = {location.id: location.name for location in self.locations_qs().all()}
        target_pk_to_name = {target.id: target.name for target in self.targets_qs().all()}
        location_target_dict = {}
        for location_id_or_string, location_grouper in groupby(rows, key=lambda _: _[0]):
            location_name = location_pk_to_name[location_id_or_string] if isinstance(location_id_or_string, int) \
                else location_id_or_string
            target_dict = {}
            for target_id_or_string, target_grouper in groupby(location_grouper, key=lambda _: _[1]):
                # NB: this assumes that the first row is always the CDCData.POINT_ROW_TYPE point value, thanks to
                # ORDER BY, which is not true when, for example, a target has no point row. and since this method is
                # called by Project.validate_template_data(), we need to check it here

                # location__id (or name), target__id (or name), is_point_row, target__unit, bin_start_incl, bin_end_notincl, value:
                point_row = next(target_grouper)
                if not point_row[2]:  # is_point_row
                    raise RuntimeError("First row was not the point row: {}".format(point_row))

                target_name = target_pk_to_name[target_id_or_string] if isinstance(target_id_or_string, int) \
                    else target_id_or_string
                bins = [tuple(bin_list[-3:]) for bin_list in target_grouper]
                target_dict[target_name] = {'point': point_row[-1], 'bins': bins}
            location_target_dict[location_name] = target_dict
        return location_target_dict


#
# ---- classes representing data. each of these is ~implictly paired with a  xx ----
#

class CDCData(models.Model):
    """
    Contains the content of a CDC format CSV file's row as documented in about.html . Content is manually managed by
    code, such as by ForecastModel.load_forecast(). Django manages migration (CREATE TABLE) and cascading deletion.
    """
    POINT_ROW_TYPE = 'point'
    BIN_ROW_TYPE = 'bin'

    # the standard CDC format columns
    location = models.ForeignKey('Location', blank=True, null=True, on_delete=models.SET_NULL)
    target = models.ForeignKey('Target', blank=True, null=True, on_delete=models.SET_NULL)
    is_point_row = models.BooleanField()  # True if this is a POINT row. Is a BIN row o/w
    bin_start_incl = models.FloatField(null=True)  # nullable b/c some bins have non-numeric values, e.g., 'NA'
    bin_end_notincl = models.FloatField(null=True)  # ""
    value = models.FloatField(null=True)


    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=['location']),
            models.Index(fields=['target']),
            models.Index(fields=['is_point_row']),
        ]


    def __repr__(self):
        return str((self.pk, self.location, self.target.name, self.is_point_row,
                    self.bin_start_incl, self.bin_end_notincl, self.value))


    def __str__(self):  # todo
        return basic_str(self)


    def _data_row(self):  # returns Target name
        return [self.location, self.target.name, self.is_point_row,
                self.bin_start_incl, self.bin_end_notincl, self.value]


class ProjectTemplateData(CDCData):
    """
    Represents data corresponding to a Project.
    """

    project = models.ForeignKey('Project', on_delete=models.CASCADE, null=True,
                                related_name='cdcdata_set')  # NB: related_name in all CDCData sublclasses must be the same


    def __repr__(self):
        return str((self.pk, self.project.pk, self.location.pk, self.target.pk, self.is_point_row,
                    self.bin_start_incl, self.bin_end_notincl, self.value))


class ForecastData(CDCData):
    """
    Represents data corresponding to a Forecast.
    """

    forecast = models.ForeignKey('Forecast', on_delete=models.CASCADE, null=True,
                                 related_name='cdcdata_set')  # NB: related_name in all CDCData sublclasses must be the same


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.location.pk, self.target.pk, self.is_point_row,
                    self.bin_start_incl, self.bin_end_notincl, self.value))
