import csv
import io

from django.db import models, connection
from django.urls import reverse

from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.project import TimeZero, POSTGRES_NULL_VALUE
from utils.utilities import basic_str, parse_value


class Forecast(models.Model):
    """
    Represents a model's forecasted data. There is one Forecast for each of my ForecastModel's Project's TimeZeros.
    """

    # csv file loading class variables
    POINT_PREDICTION_HEADER = 'location', 'target', 'value'
    NAMED_DISTRIBUTION_HEADER = 'location', 'target', 'family', 'param1', 'param2', 'param3'
    BINLWR_DISTRIBUTION_HEADER = 'location', 'target', 'lwr', 'prob'
    SAMPLE_DISTRIBUTION_HEADER = 'location', 'target', 'sample'
    BINCAT_DISTRIBUTION_HEADER = 'location', 'target', 'cat', 'prob'
    SAMPLECAT_DISTRIBUTION_HEADER = 'location', 'target', 'cat', 'sample'
    BINARY_DISTRIBUTION_HEADER = 'location', 'target', 'prob'

    forecast_model = models.ForeignKey(ForecastModel, related_name='forecasts', on_delete=models.CASCADE)

    csv_filename = models.TextField(help_text="file name of the source of this forecast's prediction data")

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


    #
    # prediction-specific accessors
    #

    def get_num_rows(self):
        """
        :return: the total of number of data rows in me, for all types of Predictions
        """
        from forecast_app.models import Prediction  # avoid circular imports


        return sum(concrete_prediction_class.objects.filter(forecast=self).count()
                   for concrete_prediction_class in Prediction.concrete_subclasses())


    def bincat_distribution_qs(self):
        from forecast_app.models import BinCatDistribution


        return self._predictions_qs(BinCatDistribution)


    def binlwr_distribution_qs(self):
        from forecast_app.models import BinLwrDistribution


        return self._predictions_qs(BinLwrDistribution)


    def binary_distribution_qs(self):
        from forecast_app.models import BinaryDistribution


        return self._predictions_qs(BinaryDistribution)


    def named_distribution_qs(self):
        from forecast_app.models import NamedDistribution


        return self._predictions_qs(NamedDistribution)


    def point_prediction_qs(self):
        from forecast_app.models import PointPrediction


        return self._predictions_qs(PointPrediction)


    def sample_distribution_qs(self):
        from forecast_app.models import SampleDistribution


        return self._predictions_qs(SampleDistribution)


    def samplecat_distribution_qs(self):
        from forecast_app.models import SampleCatDistribution


        return self._predictions_qs(SampleCatDistribution)


    def _predictions_qs(self, prediction_subclass):
        # *_prediction_qs() helper that returns a QuerySet for all of my Predictions of type prediction_subclass
        return prediction_subclass.objects.filter(forecast=self)


    #
    # prediction-loading functions
    #

    @classmethod
    def prediction_class_for_csv_header(cls, csv_header):
        """
        :param csv_header: a sequence of strings representing a csv file's headers
        :return: a Prediction subclass to use for loading that kind of file
        """
        # avoid circular imports
        from forecast_app.models import BinCatDistribution, BinLwrDistribution, BinaryDistribution, NamedDistribution, \
            PointPrediction, SampleDistribution, SampleCatDistribution


        header_to_class = {
            cls.BINCAT_DISTRIBUTION_HEADER: BinCatDistribution,
            cls.BINLWR_DISTRIBUTION_HEADER: BinLwrDistribution,
            cls.BINARY_DISTRIBUTION_HEADER: BinaryDistribution,
            cls.NAMED_DISTRIBUTION_HEADER: NamedDistribution,
            cls.POINT_PREDICTION_HEADER: PointPrediction,
            cls.SAMPLE_DISTRIBUTION_HEADER: SampleDistribution,
            cls.SAMPLECAT_DISTRIBUTION_HEADER: SampleCatDistribution,
        }
        if csv_header in header_to_class:
            return header_to_class[csv_header]
        else:
            all_headers = [cls.POINT_PREDICTION_HEADER, cls.NAMED_DISTRIBUTION_HEADER, cls.BINLWR_DISTRIBUTION_HEADER,
                           cls.SAMPLE_DISTRIBUTION_HEADER, cls.BINCAT_DISTRIBUTION_HEADER,
                           cls.SAMPLECAT_DISTRIBUTION_HEADER, cls.BINARY_DISTRIBUTION_HEADER]
            raise RuntimeError(f"csv_header did not match expected types. csv_header={csv_header!r}, "
                               f"valid headers: {all_headers}")


    def load_predictions(self, predictions_file):
        """
        Loads the prediction data in predictions_file. The type of predictions loaded are based on the file's headers.
        """
        # avoid circular imports
        from forecast_app.models import PointPrediction, NamedDistribution, BinLwrDistribution, SampleDistribution, \
            BinCatDistribution, SampleCatDistribution, BinaryDistribution


        csv_reader = csv.reader(predictions_file, delimiter=',')
        try:
            csv_header = next(csv_reader)
        except StopIteration:  # a kind of Exception, so much come first
            raise RuntimeError("empty file.")
        except Exception as exc:
            raise RuntimeError(f"error reading from predictions_file={predictions_file}. exc={exc}")

        prediction_class = self.prediction_class_for_csv_header(tuple(csv_header))  # raises
        prediction_class_to_load_fcn = {
            BinCatDistribution: self._load_bincat_predictions,
            BinLwrDistribution: self._load_binlwr_predictions,
            BinaryDistribution: self._load_binary_predictions,
            NamedDistribution: self._load_named_distribution_predictions,
            PointPrediction: self._load_point_predictions,
            SampleDistribution: self._load_sample_predictions,
            SampleCatDistribution: self._load_samplecat_predictions,
        }
        if prediction_class in prediction_class_to_load_fcn:
            prediction_class_to_load_fcn[prediction_class](csv_reader)
        else:
            raise NotImplementedError(f"no {prediction_class.__name__} loading yet")


    def _load_bincat_predictions(self, csv_reader):
        """
        Loads the rows in csv_reader as BinCatDistributions. See BINCAT_DISTRIBUTION_HEADER.
        """
        from forecast_app.models import BinCatDistribution  # avoid circular imports


        # after this, rows will be: [location, target, cat, prob]:
        location_names, target_names, rows = self._read_csv_file_rows(csv_reader,
                                                                      len(Forecast.BINCAT_DISTRIBUTION_HEADER))
        if not rows:
            return

        # after this, rows will be: [location, target, cat, prob]:
        self._create_missing_locations_and_targets_rows(location_names, target_names, rows)

        # after this, rows will be: [location_id, target_id, value_i, value_f, value_t, self_pk]:
        self._add_self_pk_rows(rows)

        # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
        prediction_class = BinCatDistribution
        columns_names = [prediction_class._meta.get_field('location').column,
                         prediction_class._meta.get_field('target').column,
                         prediction_class._meta.get_field('cat').column,
                         prediction_class._meta.get_field('prob').column,
                         Forecast._meta.model_name + '_id']
        self._insert_rows(prediction_class, columns_names, rows)


    def _load_binlwr_predictions(self, csv_reader):
        """
        Loads the rows in csv_reader as BinLwrDistributions. See BINLWR_DISTRIBUTION_HEADER.
        """
        from forecast_app.models import BinLwrDistribution  # avoid circular imports


        # after this, rows will be: [location, target, lwr, prob]:
        location_names, target_names, rows = self._read_csv_file_rows(csv_reader,
                                                                      len(Forecast.BINLWR_DISTRIBUTION_HEADER))
        if not rows:
            return

        # after this, rows will be: [location_id, target_id, lwr, prob]:
        self._create_missing_locations_and_targets_rows(location_names, target_names, rows)

        # after this, rows will be: [location_id, target_id, lwr, prob, self_pk]:
        self._add_self_pk_rows(rows)

        # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
        prediction_class = BinLwrDistribution
        columns_names = [prediction_class._meta.get_field('location').column,
                         prediction_class._meta.get_field('target').column,
                         prediction_class._meta.get_field('lwr').column,
                         prediction_class._meta.get_field('prob').column,
                         Forecast._meta.model_name + '_id']
        self._insert_rows(prediction_class, columns_names, rows)


    def _load_binary_predictions(self, csv_reader):
        """
        Loads the rows in csv_reader as BinaryDistributions. See BINARY_DISTRIBUTION_HEADER.
        """
        from forecast_app.models import BinaryDistribution  # avoid circular imports


        # after this, rows will be: [location, target, prob]:
        location_names, target_names, rows = self._read_csv_file_rows(csv_reader,
                                                                      len(Forecast.BINARY_DISTRIBUTION_HEADER))
        if not rows:
            return

        # after this, rows will be: [location, target, prob]:
        self._create_missing_locations_and_targets_rows(location_names, target_names, rows)

        # after this, rows will be: [location_id, target_id, value_i, value_f, value_t, self_pk]:
        self._add_self_pk_rows(rows)

        # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
        prediction_class = BinaryDistribution
        columns_names = [prediction_class._meta.get_field('location').column,
                         prediction_class._meta.get_field('target').column,
                         prediction_class._meta.get_field('prob').column,
                         Forecast._meta.model_name + '_id']
        self._insert_rows(prediction_class, columns_names, rows)


    def _load_named_distribution_predictions(self, csv_reader):
        """
        Loads the rows in csv_reader as NamedDistribution concrete subclasses. See NAMED_DISTRIBUTION_HEADER.
        Recall that each subclass has different IVs, so we use a hard-coded mapping to decide the subclass based on the
        `family` column.
        """
        from forecast_app.models import NamedDistribution  # avoid circular imports


        # after this, rows will be: [location, target, family, param1, param2, param3]:
        location_names, target_names, rows = self._read_csv_file_rows(csv_reader,
                                                                      len(Forecast.NAMED_DISTRIBUTION_HEADER))
        if not rows:
            return

        # after this, rows will be: [location_id, target_id, family, param1, param2, param3]:
        self._create_missing_locations_and_targets_rows(location_names, target_names, rows)

        # after this, rows will be: [location_id, target_id, family_id, param1, param2, param3]:
        self._replace_family_abbrev_with_id_rows(rows)

        # after this, rows will be: [location_id, target_id, family_id, param1_or_0, param2_or_0, param3_or_0]:
        self._replace_null_params_with_zeros_rows(rows)

        # after this, rows will be: [location_id, target_id, family_id, param1, param2, param3, self_pk]:
        self._add_self_pk_rows(rows)

        # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
        prediction_class = NamedDistribution
        columns_names = [prediction_class._meta.get_field('location').column,
                         prediction_class._meta.get_field('target').column,
                         prediction_class._meta.get_field('family').column,
                         prediction_class._meta.get_field('param1').column,
                         prediction_class._meta.get_field('param2').column,
                         prediction_class._meta.get_field('param3').column,
                         Forecast._meta.model_name + '_id']
        self._insert_rows(prediction_class, columns_names, rows)


    def _load_point_predictions(self, csv_reader):
        """
        Loads the rows in csv_reader as PointPredictions. See POINT_PREDICTION_HEADER.
        """
        from forecast_app.models import PointPrediction  # avoid circular imports


        # after this, rows will be: [location, target, value]:
        location_names, target_names, rows = self._read_csv_file_rows(csv_reader, len(Forecast.POINT_PREDICTION_HEADER))
        if not rows:
            return

        # after this, rows will be: [location_id, target_id, value]:
        self._create_missing_locations_and_targets_rows(location_names, target_names, rows)

        # after this, rows will be: [location_id, target_id, value_i, value_f, value_t]:
        self._replace_value_with_three_types_rows(rows)

        # after this, rows will be: [location_id, target_id, value_i, value_f, value_t, self_pk]:
        self._add_self_pk_rows(rows)

        # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
        prediction_class = PointPrediction
        columns_names = [prediction_class._meta.get_field('location').column,
                         prediction_class._meta.get_field('target').column,
                         prediction_class._meta.get_field('value_i').column,
                         prediction_class._meta.get_field('value_f').column,
                         prediction_class._meta.get_field('value_t').column,
                         Forecast._meta.model_name + '_id']
        self._insert_rows(prediction_class, columns_names, rows)


    def _load_sample_predictions(self, csv_reader):
        """
        Loads the rows in csv_reader as SampleDistribution. See SAMPLE_DISTRIBUTION_HEADER.
        """
        from forecast_app.models import SampleDistribution  # avoid circular imports


        # after this, rows will be: [location, target, sample]:
        location_names, target_names, rows = self._read_csv_file_rows(csv_reader,
                                                                      len(Forecast.SAMPLE_DISTRIBUTION_HEADER))
        if not rows:
            return

        # after this, rows will be: [location_id, target_id, sample]:
        self._create_missing_locations_and_targets_rows(location_names, target_names, rows)

        # after this, rows will be: [location_id, target_id, sample, self_pk]:
        self._add_self_pk_rows(rows)

        # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
        prediction_class = SampleDistribution
        columns_names = [prediction_class._meta.get_field('location').column,
                         prediction_class._meta.get_field('target').column,
                         prediction_class._meta.get_field('sample').column,
                         Forecast._meta.model_name + '_id']
        self._insert_rows(prediction_class, columns_names, rows)


    def _load_samplecat_predictions(self, csv_reader):
        """
        Loads the rows in csv_reader as SampleCatDistributions. See SAMPLECAT_DISTRIBUTION_HEADER.
        """
        from forecast_app.models import SampleCatDistribution  # avoid circular imports


        # after this, rows will be: [location, target, cat, sample]:
        location_names, target_names, rows = self._read_csv_file_rows(csv_reader,
                                                                      len(Forecast.SAMPLECAT_DISTRIBUTION_HEADER))
        if not rows:
            return

        # after this, rows will be: [location, target, cat, sample]:
        self._create_missing_locations_and_targets_rows(location_names, target_names, rows)

        # after this, rows will be: [location_id, target_id, value_i, value_f, value_t, self_pk]:
        self._add_self_pk_rows(rows)

        # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
        prediction_class = SampleCatDistribution
        columns_names = [prediction_class._meta.get_field('location').column,
                         prediction_class._meta.get_field('target').column,
                         prediction_class._meta.get_field('cat').column,
                         prediction_class._meta.get_field('sample').column,
                         Forecast._meta.model_name + '_id']
        self._insert_rows(prediction_class, columns_names, rows)


    def _read_csv_file_rows(self, csv_reader, exp_num_rows):
        """
        Loads the rows from cdc_csv_file_fp, cleans them, and then returns them as a list. Validates exp_num_rows, but
        does not check locations and targets. This is b/c Locations and Targets might not yet exist (if they're
        dynamically created by this method's callers).

        :return: a 3-tuple: (location_names, target_names, rows) where the first two are sets and the last is a list of
            rows: location_name, target_name, parsed_value]
        """
        locations = set()
        targets = set()
        rows = []
        for row in csv_reader:
            if len(row) != exp_num_rows:
                raise RuntimeError(f"Invalid row (wasn't {exp_num_rows} columns): {row!r}")

            location_name, target_name = row[0], row[1]
            locations.add(location_name)
            targets.add(target_name)
            rows.append(row)
        return locations, targets, rows


    def _create_missing_locations_and_targets_rows(self, location_names, target_names, rows):
        """
        Creates missing Locations and Targets in my Project, then does an in-place rows replacement of target and
        location names with PKs. note that unlike Locations, which only have a name, Targets have additional fields
        that need filling out by users. But here all we can set are names.
        """
        from forecast_app.models import Location, Target  # avoid circular imports


        project = self.forecast_model.project
        location_name_to_pk = {location.name: location.id for location in project.locations.all()}
        for location_name in location_names:
            if location_name not in location_name_to_pk:
                location_name_to_pk[location_name] = Location.objects.create(project=project, name=location_name).pk

        target_name_to_pk = {target.name: target.id for target in project.targets.all()}
        for target_name in target_names:
            if target_name not in target_name_to_pk:
                target_name_to_pk[target_name] = Target.objects.create(project=project, name=target_name,
                                                                       point_value_type=Target.POINT_FLOAT).pk  # todo point_value_type?

        for row in rows:  # location_name, target_name, value, self_pk
            row[0] = location_name_to_pk[row[0]]
            row[1] = target_name_to_pk[row[1]]


    def _replace_value_with_three_types_rows(self, rows):
        """
        Does an in-place rows replacement of values with the three type-specific values - value_i, value_f, and value_t.
        Recall that exactly one will be non-NULL (i.e., not None).
        """

        from forecast_app.models import Target  # avoid circular imports


        target_pk_to_point_value_type = {target.pk: target.point_value_type for target in
                                         self.forecast_model.project.targets.all()}
        for row in rows:
            target_pk = row[1]
            value = row[2]
            value_i = parse_value(value) if target_pk_to_point_value_type[target_pk] == Target.POINT_INTEGER else None
            value_f = parse_value(value) if target_pk_to_point_value_type[target_pk] == Target.POINT_FLOAT else None
            value_t = value if target_pk_to_point_value_type[target_pk] == Target.POINT_TEXT else None
            row[2:] = [value_i, value_f, value_t]


    def _replace_family_abbrev_with_id_rows(self, rows):
        """
        Does an in-place rows replacement of family abbreviations with ids in NamedDistribution.FAMILY_CHOICES (ints).
        """
        from forecast_app.models.prediction import NamedDistribution  # avoid circular imports


        for row in rows:
            family = row[2]
            if family in NamedDistribution.FAMILY_ABBREVIATION_TO_FAMILY_ID:
                row[2] = NamedDistribution.FAMILY_ABBREVIATION_TO_FAMILY_ID[family]
            else:
                raise RuntimeError(f"invalid family. family='{family}', "
                                   f"families={NamedDistribution.FAMILY_ABBREVIATION_TO_FAMILY_ID.keys()}")


    def _replace_null_params_with_zeros_rows(self, rows):
        """
        Does an in-place rows replacement of empty params with zeros."
        """
        for row in rows:
            row[3] = row[3] or 0  # param1
            row[4] = row[4] or 0  # param2
            row[5] = row[5] or 0  # param3


    def _add_self_pk_rows(self, rows):
        """
        Does an in-place rows addition of my pk to the end.
        """
        for row in rows:
            row.append(self.pk)


    @staticmethod
    def _insert_rows(prediction_class, columns_names, rows):
        """
        Does the actual INSERT of rows into the database table corresponding to prediction_class. For speed, we directly
        insert via SQL rather than the ORM. We use psycopg2 extensions to the DB API if we're connected to a Postgres
        server. Otherwise we use execute_many() as a fallback. The reason we don't simply use the latter for Postgres
        is because its implementation is slow ( http://initd.org/psycopg/docs/extras.html#fast-execution-helpers ).
        """
        table_name = prediction_class._meta.db_table
        with connection.cursor() as cursor:
            if connection.vendor == 'postgresql':
                string_io = io.StringIO()
                csv_writer = csv.writer(string_io, delimiter=',')
                for row in rows:
                    location_id, target_id = row[0], row[1]
                    prediction_items = row[2:-1]
                    self_pk = row[-1]

                    for idx in range(len(prediction_items)):
                        # value_i if value_i is not None else POSTGRES_NULL_VALUE
                        prediction_item = prediction_items[idx]
                        prediction_items[idx] = prediction_item if prediction_item is not None else POSTGRES_NULL_VALUE

                    csv_writer.writerow([location_id, target_id] + prediction_items + [self_pk])
                string_io.seek(0)
                cursor.copy_from(string_io, table_name, columns=columns_names, sep=',', null=POSTGRES_NULL_VALUE)
            else:  # 'sqlite', etc.
                column_names = (', '.join(columns_names))
                values_percent_s = ', '.join(['%s'] * len(columns_names))
                sql = f"""
                    INSERT INTO {table_name} ({column_names})
                    VALUES ({values_percent_s});
                    """
                cursor.executemany(sql, rows)
