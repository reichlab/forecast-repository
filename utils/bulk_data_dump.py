import json
import logging
import shutil
import tempfile
from pathlib import Path

import click
import django
from django.db import connection
from django.shortcuts import get_object_or_404
from django.utils.text import slugify


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.project_queries import validate_forecasts_query
from forecast_app.models import Forecast, PredictionElement, Project, Unit, Target, TimeZero, ForecastModel, \
    PredictionData


logger = logging.getLogger(__name__)

# 10E+06
MAX_NUM_PRED_ELES = 2_000_000  # used by create_and_fill_temp_tables() as a limit


@click.command()
@click.argument('project_name', type=click.STRING, required=True)
@click.argument('query_file', type=click.File())
@click.argument('output_dir', type=click.Path(file_okay=False, exists=True))
def bulk_data_dump_app(project_name, query_file, output_dir):
    """
    An app that dumps the data in `project_name` to `output_dir` as a zip of eight CSV files, first filtering using
    `query_file`.

    :param project_name: the name of an existing Project to dump data from
    :param query_file: a JSON file that specifies the initial filtering to do when dumping the database. see
        `bulk_data_dump()`'s `query` arg documentation
    :param output_dir: where to save the zip file that contains the dumped CSV files
    """
    # validate project
    project = get_object_or_404(Project, name=project_name)

    # validate query
    try:
        query = json.load(query_file)
    except json.decoder.JSONDecodeError as jde:
        raise RuntimeError(f"invalid query_file: was not valid JSON. query_file={query_file}, error={jde!r}")

    # do the dump, zip the results, and copy the zip to `output_dir`
    with tempfile.TemporaryDirectory() as temp_csv_dir, \
            tempfile.TemporaryDirectory() as temp_zip_dir:
        # dump the CSV files to temp_csv_dir
        logger.info(f"bulk_data_dump_app(): dumping the data. project={project}, query={query}, "
                    f"temp_csv_dir={temp_csv_dir}")
        bulk_data_dump(project, query, temp_csv_dir)

        # zip temp_csv_dir into temp_zip_dir
        zip_file = Path(temp_zip_dir) / filename_for_args(project, query)
        logger.info(f"bulk_data_dump_app(): zipping the files. temp_csv_dir={temp_csv_dir}, zip_file={zip_file}")
        zip_file = shutil.make_archive(base_name=zip_file, format='zip', root_dir=temp_csv_dir)

        # copy the zip file
        logger.info(f"bulk_data_dump_app(): copying the zip file. zip_file={zip_file}, output_dir={output_dir}")
        shutil.copy(zip_file, output_dir)  # src, dst

        # done
        logger.info(f"bulk_data_dump_app(): done")


def bulk_data_dump(project, query, output_dir):
    """
    Dumps data from `project` into eight CSV files in `output_dir`. `query` specifies an initial filtering of the data
    to help manage the number of rows dumped.

    :param project: the Project to dump data from
    :param query: a dict that filters the dump. it is the same as the `query` arg to `query_forecasts_for_project()`
        except that 'types' and 'as_of' are disallowed. i.e., what's allowed is: 'models', 'units', 'targets', and
        'timezeros'.
    :param output_dir: where to save the CSV files to
    :return: None
    """
    error_messages, (model_ids, unit_ids, target_ids, timezero_ids, type_ints, as_of) = \
        validate_forecasts_query(project, query)
    if error_messages:
        raise RuntimeError(f"invalid query. query={query}, errors={error_messages}")
    elif type_ints or as_of:
        raise RuntimeError(f"invalid query: `type_ints` or `as_of` was passed but is disallowed. query={query}, "
                           f"type_ints={type_ints}, as_of={as_of}")

    # create, fill, and export the temp tables. default empty IDs to all objects - this simplifies the SQL (we always
    # specify WHERE `= ANY(...)`), but this might be less efficient than simply omitting WHEREs if no IDs
    logger.info(f"bulk_data_dump(): project={project}, query={query}, output_dir={output_dir}")
    model_ids = model_ids if model_ids else list(project.models.all().values_list('id', flat=True))
    unit_ids = unit_ids if unit_ids else list(project.units.all().values_list('id', flat=True))
    target_ids = target_ids if target_ids else list(project.targets.all().values_list('id', flat=True))
    timezero_ids = timezero_ids if timezero_ids else list(project.timezeros.all().values_list('id', flat=True))

    # this maps model class -> 2-tuple: (temp_table_name, column_names):
    class_to_temp_table_cols = {clazz: (f'{clazz._meta.db_table}_temp', model_field_names(clazz)) for clazz in
                                [Project, ForecastModel, Unit, Target, TimeZero, Forecast, PredictionElement,
                                 PredictionData]}
    drop_temp_tables(class_to_temp_table_cols)  # DROP all in case prev run failed before final DROPs

    logger.info(f"bulk_data_dump(): creating and filling temp tables")
    create_and_fill_temp_tables(class_to_temp_table_cols, project, model_ids, unit_ids, target_ids, timezero_ids)

    logger.info(f"bulk_data_dump(): exporting. output_dir={output_dir}")
    export_temp_tables(class_to_temp_table_cols, output_dir)

    # done
    drop_temp_tables(class_to_temp_table_cols)
    logger.info(f"bulk_data_dump(): done")


def filename_for_args(project, query):
    project_name_slug = slugify(project.name)
    ymd_hms = django.utils.timezone.now().strftime('%Y%m%d_%H%M%S')
    query_hash = PredictionElement.hash_for_prediction_data_dict(query)
    return f"{project_name_slug}_{ymd_hms}_{query_hash}"


def model_field_names(clazz):
    # this is a hard-coded version that is suitable for copying to external library
    return {
        Project: ['id', 'core_data', 'description', 'home_url', 'is_public', 'logo_url', 'name', 'time_interval_type',
                  'visualization_y_label'],
        Unit: ['id', 'project_id', 'name'],
        Target: ['id', 'project_id', 'description', 'is_step_ahead', 'name', 'step_ahead_increment', 'type', 'unit'],
        TimeZero: ['id', 'project_id', 'data_version_date', 'is_season_start', 'season_name', 'timezero_date'],
        ForecastModel: ['id', 'project_id', 'abbreviation', 'aux_data_url', 'citation', 'contributors', 'description',
                        'home_url', 'is_oracle', 'license', 'methods', 'name', 'notes', 'team_name'],
        Forecast: ['id', 'forecast_model_id', 'time_zero_id', 'created_at', 'issued_at', 'notes', 'source'],
        PredictionElement: ['id', 'forecast_id', 'target_id', 'unit_id', 'data_hash', 'is_retract', 'pred_class'],
        PredictionData: ['pred_ele_id', 'data'],
    }[clazz]


def drop_temp_tables(class_to_temp_table_cols):  # maps model class -> 2-tuple: (temp_table_name, column_names)
    with connection.cursor() as cursor:
        for temp_table_name in [_[0] for _ in class_to_temp_table_cols.values()]:
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name};")


def create_and_fill_temp_tables(class_to_temp_table_cols, project, model_ids, unit_ids, target_ids, timezero_ids):
    """
    Our logic for including rows from each table:
    - Project:        id = project.id
    - ForecastModel:  id IN model_ids
    - Unit:           id IN unit_ids
    - Target:         id IN target_ids
    - TimeZero:       id IN timezero_ids
    - PredictionElement:
        = unit.id                        IN unit_ids
        = target.id                      IN target_ids
        = forecast_id.forecast_model_id  IN model_ids
        = forecast_id.time_zero_id       IN timezero_ids
    - Forecast:
        = id IN (SELECT DISTINCT forecast_id FROM pred_ele_temp)
    - Project: PredictionData:
        = id IN (SELECT id FROM pred_ele_temp)

    Notes:
    - we convert these boolean fields to int so b/c postgres exports booleans as 't'/'f'. converting to 0/1 matches
      sqlite:
        = ForecastModel.is_oracle
        = PredictionElement.is_retract
        = Project.is_public
        = Target.is_step_ahead
        = TimeZero.is_season_start

    - todo this program only works with postgres, and fails with sqlite3 - see "WHERE IN" vs. "= ANY()"
    """

    # create Project, Unit, Target, TimeZero, and ForecastModel temp tables (similar queries). we `SELECT *` from the
    # temp tables, but not all columns are ultimately exported
    logger.info(f"create_and_fill_temp_tables(): Project, Unit, Target, TimeZero, ForecastModel")

    # class_to_temp_table_cols: maps model class -> 2-tuple: (temp_table_name, column_names):
    for clazz, ids in ((Project, [project.id]),
                       (ForecastModel, model_ids),
                       (Unit, unit_ids),
                       (Target, target_ids),
                       (TimeZero, timezero_ids)):
        temp_table_name = class_to_temp_table_cols[clazz][0]
        sql = f"""
            CREATE TEMP TABLE {temp_table_name} AS
            SELECT *
            FROM {clazz._meta.db_table}
            WHERE id = ANY(%s);
        """  # ANY per https://www.psycopg.org/docs/usage.html#lists-adaptation
        with connection.cursor() as cursor:
            cursor.execute(sql, (ids,))

    # create PredictionElement temp table
    logger.info(f"create_and_fill_temp_tables(): PredictionElement")
    pred_ele_temp_table_name = class_to_temp_table_cols[PredictionElement][0]
    sql = f"""
        CREATE TEMP TABLE {pred_ele_temp_table_name} AS
        SELECT pred_ele.*
        FROM {PredictionElement._meta.db_table} AS pred_ele
            JOIN {Forecast._meta.db_table} AS f
                ON pred_ele.forecast_id = f.id
        WHERE pred_ele.unit_id = ANY(%s)
          AND pred_ele.target_id = ANY(%s)
          AND f.forecast_model_id = ANY(%s)
          AND f.time_zero_id = ANY(%s);
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (unit_ids, target_ids, model_ids, timezero_ids))

        logger.info(f"create_and_fill_temp_tables(): counting rows in {pred_ele_temp_table_name}")
        num_rows = count_rows(cursor, pred_ele_temp_table_name)
        logger.info(f"create_and_fill_temp_tables(): # rows in {pred_ele_temp_table_name}: num_rows={num_rows}, "
                    f"MAX_NUM_PRED_ELES={MAX_NUM_PRED_ELES}")
        if num_rows > MAX_NUM_PRED_ELES:
            raise RuntimeError(f"num_rows > MAX_NUM_PRED_ELES. num_rows={num_rows},"
                               f" MAX_NUM_PRED_ELES={MAX_NUM_PRED_ELES}")

    # create Forecast temp table
    logger.info(f"create_and_fill_temp_tables(): Forecast")
    forecast_temp_table_name = class_to_temp_table_cols[Forecast][0]
    sql = f"""
        CREATE TEMP TABLE {forecast_temp_table_name} AS
        SELECT *
        FROM {Forecast._meta.db_table}
        WHERE id IN (SELECT DISTINCT forecast_id FROM {pred_ele_temp_table_name});
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (model_ids, timezero_ids,))

    # create PredictionData temp table
    logger.info(f"create_and_fill_temp_tables(): PredictionData")
    pred_data_temp_table_name = class_to_temp_table_cols[PredictionData][0]
    sql = f"""
        CREATE TEMP TABLE {pred_data_temp_table_name} AS
        SELECT *
        FROM {PredictionData._meta.db_table}
        WHERE pred_ele_id IN (SELECT id FROM {pred_ele_temp_table_name});
    """
    with connection.cursor() as cursor:
        # in case prev run failed before final DROP:
        cursor.execute(sql)

    # cast boolean fields to smallint. NB: cannot do directly ("cannot cast type boolean to smallint"), i.e., fails:
    #   ALTER TABLE {temp_table_name} ALTER COLUMN {column_name} TYPE smallint USING is_oracle::smallint;
    # this works, though - per https://www.postgresql.org/message-id/4CD3448D.60208@ultimeth.com :
    #   ALTER TABLE ALTER col_name TYPE SMALLINT USING CASE WHEN col_name THEN 1 ELSE 0 END;
    logger.info(f"create_and_fill_temp_tables(): converting boolean fields")
    for clazz, column_name in ((ForecastModel, 'is_oracle'), (PredictionElement, 'is_retract'), (Project, 'is_public'),
                               (Target, 'is_step_ahead'), (TimeZero, 'is_season_start')):
        temp_table_name = class_to_temp_table_cols[clazz][0]
        sql = f"""
            ALTER TABLE {temp_table_name} ALTER COLUMN {column_name} TYPE smallint
              USING CASE WHEN {column_name} THEN 1 ELSE 0 END;
        """
        with connection.cursor() as cursor:
            cursor.execute(sql)

    # done
    logger.info(f"create_and_fill_temp_tables(): done")


def count_rows(cursor, pred_ele_temp_table_name):
    cursor.execute(f"SELECT COUNT(*) FROM {pred_ele_temp_table_name}")
    return cursor.fetchone()[0]


def export_temp_tables(class_to_temp_table_cols, temp_csv_dir):
    with connection.cursor() as cursor:
        for clazz, (temp_table_name, column_names) in class_to_temp_table_cols.items():
            temp_file_path = Path(temp_csv_dir) / f'{clazz._meta.model_name}.csv'
            with open(temp_file_path, 'w') as temp_file_fp:
                # cursor.copy_from(string_io, temp_table_name, columns=columns_names, sep=',', null=POSTGRES_NULL_VALUE)
                # cursor.copy_to(temp_file_fp, temp_table_name)  # file, table, sep='\t', null='\\N', columns=None
                # todo xx use this so json doesn't cause problems: QUOTE e'\x01' DELIMITER e'\x02'
                sql = f"""
                    COPY {temp_table_name}({', '.join(column_names)}) TO STDIN WITH CSV;
                """
                cursor.copy_expert(sql, temp_file_fp)


if __name__ == '__main__':
    bulk_data_dump_app()
