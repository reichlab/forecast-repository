import csv
import io

from django.db import connection, transaction

from forecast_app.models import BinCatDistribution, BinLwrDistribution, BinaryDistribution, NamedDistribution, \
    PointPrediction, SampleDistribution, SampleCatDistribution, Forecast, Location, Target
from forecast_app.models.project import POSTGRES_NULL_VALUE


#
# load_predictions()
#

@transaction.atomic
def load_predictions(forecast, top_level_dict):
    """
    Loads the prediction data into forecast from top_level_dict as returned by convert_cdc_csv_file_to_dict(). See
    predictions-example.json for an example. Once loaded then validates the forecast data.
    """
    # forecast = top_level_dict['forecast']
    location_names = [location_dict['name'] for location_dict in top_level_dict['locations']]
    target_names = [target_dict['name'] for target_dict in top_level_dict['targets']]
    predictions = top_level_dict['predictions']
    bincat_rows, binlwr_rows, binary_rows, named_rows, point_rows, sample_rows, samplecat_rows = \
        _prediction_dicts_to_db_rows(predictions)
    _load_bincat_rows(forecast, location_names, target_names, bincat_rows)
    _load_binlwr_rows(forecast, location_names, target_names, binlwr_rows)
    _load_binary_rows(forecast, location_names, target_names, binary_rows)
    _load_named_rows(forecast, location_names, target_names, named_rows)
    _load_point_rows(forecast, location_names, target_names, point_rows)
    _load_sample_rows(forecast, location_names, target_names, sample_rows)
    _load_samplecat_rows(forecast, location_names, target_names, samplecat_rows)


def _prediction_dicts_to_db_rows(prediction_dicts):
    """
    Returns a 7-tuple of rows suitable for bulk-loading into a database:

        bincat_rows, binlwr_rows, binary_rows, named_rows, point_rows, sample_rows, samplecat_rows

    Each row is Prediction class-specific.

    :param prediction_dicts: the 'predictions' portion of as returned by convert_cdc_csv_file_to_dict()
    """
    bincat_rows = []  # return value. filled next
    binlwr_rows = []
    binary_rows = []
    named_rows = []
    point_rows = []
    sample_rows = []
    samplecat_rows = []
    for prediction_dict in prediction_dicts:
        location_name = prediction_dict['location']
        target_name = prediction_dict['target']
        prediction_class = prediction_dict['class']
        prediction_data = prediction_dict['prediction']
        if prediction_class == 'BinCat':
            for cat, prob in zip(prediction_data['cat'], prediction_data['prob']):
                bincat_rows.append([location_name, target_name, cat, prob])
        elif prediction_class == 'BinLwr':
            for lwr, prob in zip(prediction_data['lwr'], prediction_data['prob']):
                binlwr_rows.append([location_name, target_name, lwr, prob])
        elif prediction_class == 'Binary':
            binary_rows.append([location_name, target_name, prediction_data['prob']])
        elif prediction_class == 'Named':
            named_rows.append([location_name, target_name, prediction_data['family'],
                               prediction_data['param1'], prediction_data['param2'], prediction_data['param3']])
        elif prediction_class == 'Point':
            point_rows.append([location_name, target_name, prediction_data['value']])
        elif prediction_class == 'Sample':
            for sample in prediction_data['sample']:
                sample_rows.append([location_name, target_name, sample])
        elif prediction_class == 'SampleCat':
            for cat, sample in zip(prediction_data['cat'], prediction_data['sample']):
                samplecat_rows.append([location_name, target_name, cat, sample])
    return bincat_rows, binlwr_rows, binary_rows, named_rows, point_rows, sample_rows, samplecat_rows


def _load_bincat_rows(forecast, location_names, target_names, rows):
    """
    Loads the rows in prediction_data_dict as BinCatDistributions.
    """
    # incoming rows: [location_name, target_name, cat, prob]

    # after this, rows will be: [location_id, target_id, cat, prob]:
    _create_missing_locations_and_targets_rows(forecast, location_names, target_names, rows)

    # after this, rows will be: [location_id, target_id, cat, prob, self_pk]:
    _add_forecast_pk_rows(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = BinCatDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('cat').column,
                     prediction_class._meta.get_field('prob').column,
                     Forecast._meta.model_name + '_id']
    _insert_rows(prediction_class, columns_names, rows)


def _load_binlwr_rows(forecast, location_names, target_names, rows):
    """
    Loads the rows in csv_reader as BinLwrDistributions.
    """
    # incoming rows: [location_name, target_name, lwr, prob]

    # after this, rows will be: [location_id, target_id, lwr, prob]:
    _create_missing_locations_and_targets_rows(forecast, location_names, target_names, rows)

    # after this, rows will be: [location_id, target_id, lwr, prob, self_pk]:
    _add_forecast_pk_rows(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = BinLwrDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('lwr').column,
                     prediction_class._meta.get_field('prob').column,
                     Forecast._meta.model_name + '_id']
    _insert_rows(prediction_class, columns_names, rows)


def _load_binary_rows(forecast, location_names, target_names, rows):
    """
    Loads the rows in csv_reader as BinaryDistributions.
    """
    # incoming rows: [location_name, target_name, prob]

    # after this, rows will be: [location_id, target_id, prob]:
    _create_missing_locations_and_targets_rows(forecast, location_names, target_names, rows)

    # after this, rows will be: [location_id, target_id, prob, self_pk]:
    _add_forecast_pk_rows(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = BinaryDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('prob').column,
                     Forecast._meta.model_name + '_id']
    _insert_rows(prediction_class, columns_names, rows)


def _load_named_rows(forecast, location_names, target_names, rows):
    """
    Loads the rows in csv_reader as NamedDistribution concrete subclasses. Recall that each subclass has different IVs,
    so we use a hard-coded mapping to decide the subclass based on the `family` column.
    """
    # incoming rows: [location_name, target_name, family, param1, param2, param3]

    # after this, rows will be: [location_id, target_id, family, param1, param2, param3]:
    _create_missing_locations_and_targets_rows(forecast, location_names, target_names, rows)

    # after this, rows will be: [location_id, target_id, family_id, param1, param2, param3]:
    _replace_family_abbrev_with_id_rows(forecast, rows)

    # after this, rows will be: [location_id, target_id, family_id, param1_or_0, param2_or_0, param3_or_0]:
    _replace_null_params_with_zeros_rows(forecast, rows)

    # after this, rows will be: [location_id, target_id, family_id, param1, param2, param3, self_pk]:
    _add_forecast_pk_rows(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = NamedDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('family').column,
                     prediction_class._meta.get_field('param1').column,
                     prediction_class._meta.get_field('param2').column,
                     prediction_class._meta.get_field('param3').column,
                     Forecast._meta.model_name + '_id']
    _insert_rows(prediction_class, columns_names, rows)


def _load_point_rows(forecast, location_names, target_names, rows):
    """
    Loads the rows in csv_reader as PointPredictions.
    """
    # incoming rows: [location_name, target_name, value]

    # after this, rows will be: [location_id, target_id, value]:
    _create_missing_locations_and_targets_rows(forecast, location_names, target_names, rows)

    # after this, rows will be: [location_id, target_id, value_i, value_f, value_t]:
    _replace_value_with_three_types_rows(forecast, rows)

    # after this, rows will be: [location_id, target_id, value_i, value_f, value_t, self_pk]:
    _add_forecast_pk_rows(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = PointPrediction
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('value_i').column,
                     prediction_class._meta.get_field('value_f').column,
                     prediction_class._meta.get_field('value_t').column,
                     Forecast._meta.model_name + '_id']
    _insert_rows(prediction_class, columns_names, rows)


def _load_sample_rows(forecast, location_names, target_names, rows):
    """
    Loads the rows in csv_reader as SampleDistribution. See SAMPLE_DISTRIBUTION_HEADER.
    """
    # incoming rows: [location_name, target_name, sample]

    # after this, rows will be: [location_id, target_id, sample]:
    _create_missing_locations_and_targets_rows(forecast, location_names, target_names, rows)

    # after this, rows will be: [location_id, target_id, sample, self_pk]:
    _add_forecast_pk_rows(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = SampleDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('sample').column,
                     Forecast._meta.model_name + '_id']
    _insert_rows(prediction_class, columns_names, rows)


def _load_samplecat_rows(forecast, location_names, target_names, rows):
    """
    Loads the rows in csv_reader as SampleCatDistributions.
    """
    # incoming rows: [location_name, target_name, cat, sample]

    # after this, rows will be: [location_id, target_id, cat, sample]:
    _create_missing_locations_and_targets_rows(forecast, location_names, target_names, rows)

    # after this, rows will be: [location_id, target_id, value_i, value_f, value_t, self_pk]:
    _add_forecast_pk_rows(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = SampleCatDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('cat').column,
                     prediction_class._meta.get_field('sample').column,
                     Forecast._meta.model_name + '_id']
    _insert_rows(prediction_class, columns_names, rows)


def _create_missing_locations_and_targets_rows(forecast, location_names, target_names, rows):
    """
    Creates missing Locations and Targets in my Project, then does an in-place rows replacement of target and
    location names with PKs. note that unlike Locations, which only have a name, Targets have additional fields
    that need filling out by users. But here all we can set are names.
    """
    project = forecast.forecast_model.project
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


def _replace_value_with_three_types_rows(forecast, rows):
    """
    Does an in-place rows replacement of values with the three type-specific values - value_i, value_f, and value_t.
    Recall that exactly one will be non-NULL (i.e., not None).
    """
    target_pk_to_point_value_type = {target.pk: target.point_value_type for target in
                                     forecast.forecast_model.project.targets.all()}
    for row in rows:
        target_pk = row[1]
        value = row[2]
        value_i = value if target_pk_to_point_value_type[target_pk] == Target.POINT_INTEGER else None
        value_f = value if target_pk_to_point_value_type[target_pk] == Target.POINT_FLOAT else None
        value_t = value if target_pk_to_point_value_type[target_pk] == Target.POINT_TEXT else None
        row[2:] = [value_i, value_f, value_t]


def _replace_family_abbrev_with_id_rows(forecast, rows):
    """
    Does an in-place rows replacement of family abbreviations with ids in NamedDistribution.FAMILY_CHOICES (ints).
    """
    for row in rows:
        family = row[2]
        if family in NamedDistribution.FAMILY_ABBREVIATION_TO_FAMILY_ID:
            row[2] = NamedDistribution.FAMILY_ABBREVIATION_TO_FAMILY_ID[family]
        else:
            raise RuntimeError(f"invalid family. family='{family}', "
                               f"families={NamedDistribution.FAMILY_ABBREVIATION_TO_FAMILY_ID.keys()}")


def _replace_null_params_with_zeros_rows(forecast, rows):
    """
    Does an in-place rows replacement of empty params with zeros."
    """
    for row in rows:
        row[3] = row[3] or 0  # param1
        row[4] = row[4] or 0  # param2
        row[5] = row[5] or 0  # param3


def _add_forecast_pk_rows(forecast, rows):
    """
    Does an in-place rows addition of my pk to the end.
    """
    for row in rows:
        row.append(forecast.pk)


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
