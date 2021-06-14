import csv
import io
import json

import dateutil
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError, ClientError, ConnectionClosedError
from django.db import connection, transaction
from rest_framework.generics import get_object_or_404
from rq.timeouts import JobTimeoutException

from forecast_app.models import Job, Project, Forecast, ForecastModel, PredictionElement, PredictionData
from forecast_app.models.prediction_element import PRED_CLASS_NAME_TO_INT
from forecast_repo.settings.base import MAX_NUM_QUERY_ROWS
from utils.project import logger
from utils.project_truth import TRUTH_CSV_HEADER, oracle_model_for_project
from utils.utilities import YYYY_MM_DD_DATE_FORMAT, batched_rows


#
# query_forecasts_for_project()
#

FORECAST_CSV_HEADER = ['model', 'timezero', 'season', 'unit', 'target', 'class', 'value', 'cat', 'prob', 'sample',
                       'quantile', 'family', 'param1', 'param2', 'param3']


def query_forecasts_for_project(project, query, max_num_rows=MAX_NUM_QUERY_ROWS):
    """
    Top-level function for querying forecasts within project. Runs in the calling thread and therefore blocks.

    Returns a list of rows in a Zoltar-specific CSV row format. The columns are defined in FORECAST_CSV_HEADER. Note
    that the csv is 'sparse': not every row uses all columns, and unused ones are empty (''). However, the first four
    columns are always non-empty, i.e., every prediction has them.

    The 'class' of each row is named to be the same as Zoltar's utils.forecast.PRED_CLASS_INT_TO_NAME
    variable. Column ordering is FORECAST_CSV_HEADER.

    `query` is documented at https://docs.zoltardata.com/, but briefly, it is a dict of up to six keys, five of which
    are lists of strings. all are optional:

    - 'models': Pass zero or more model abbreviations in the models field.
    - 'units': Pass zero or more unit names in the units field.
    - 'targets': Pass zero or more target names in the targets field.
    - 'timezeros': Pass zero or more timezero dates in YYYY_MM_DD_DATE_FORMAT format in the timezeros field.
    - 'types': Pass a list of string types in the types field. Choices are PRED_CLASS_INT_TO_NAME.values().

    The sixth key allows searching based on `Forecast.issued_at`:
    - 'as_of': Passing a datetime string in the optional as_of field causes the query to return only those forecast
        versions whose issued_at is <= the as_of datetime (AKA timestamp).

    Note that _strings_ are passed to refer to object *contents*, not database IDs, which means validation will fail if
    the referred-to objects are not found. NB: If multiple objects are found with the same name then the program will
    arbitrarily choose one.

    :param project: a Project
    :param query: a dict specifying the query parameters as described above. NB: assumes it has passed validation via
        `validate_forecasts_query()`
    :param max_num_rows: the number of rows at which this function raises a RuntimeError
    :return: a list of CSV rows including the header
    """
    logger.debug(f"query_forecasts_for_project(): 1/3 validating query. query={query}, project={project}")

    # validate query
    error_messages, (model_ids, unit_ids, target_ids, timezero_ids, type_ints, as_of) = \
        validate_forecasts_query(project, query)
    if error_messages:
        raise RuntimeError(f"invalid query. query={query}, errors={error_messages}")

    forecast_model_id_to_obj = {forecast_model.pk: forecast_model for forecast_model in project.models.all()}
    timezero_id_to_obj = {timezero.pk: timezero for timezero in project.timezeros.all()}
    unit_id_to_obj = {unit.pk: unit for unit in project.units.all()}
    target_id_to_obj = {target.pk: target for target in project.targets.all()}
    timezero_to_season_name = project.timezero_to_season_name()

    yield FORECAST_CSV_HEADER

    # get the SQL then execute and iterate over resulting data
    sql = _query_forecasts_sql_for_pred_class(type_ints, model_ids, unit_ids, target_ids, timezero_ids, as_of, True)
    logger.debug(f"query_forecasts_for_project(): 2/3 executing sql. type_ints, model_ids, unit_ids, target_ids, "
                 f"timezero_ids, as_of= {type_ints}, {model_ids}, {unit_ids}, {target_ids}, {timezero_ids}, "
                 f"{as_of}")
    num_rows = 0
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        for fm_id, tz_id, pred_class, unit_id, target_id, is_retract, pred_data in batched_rows(cursor):
            # we do not have to check is_retract b/c we pass `is_include_retract=False`, which skips retractions
            num_rows += 1
            if num_rows > max_num_rows:
                raise RuntimeError(f"number of rows exceeded maximum. num_rows={num_rows}, "
                                   f"max_num_rows={max_num_rows}")

            # counterintuitively must use json.loads per https://code.djangoproject.com/ticket/31991
            pred_data = json.loads(pred_data)
            model_str, timezero_str, season, class_str = _model_tz_season_class_strs(
                forecast_model_id_to_obj[fm_id], timezero_id_to_obj[tz_id], timezero_to_season_name, pred_class)
            value, cat, prob, sample, quantile, family, param1, param2, param3 = '', '', '', '', '', '', '', '', ''
            if pred_class == PredictionElement.BIN_CLASS:
                for cat, prob in zip(pred_data['cat'], pred_data['prob']):
                    yield [model_str, timezero_str, season, unit_id_to_obj[unit_id].name,
                           target_id_to_obj[target_id].name, class_str,
                           value, cat, prob, sample, quantile, family, param1, param2, param3]
            elif pred_class == PredictionElement.NAMED_CLASS:
                family = pred_data['family']
                param1 = pred_data.get('param1', '')
                param2 = pred_data.get('param2', '')
                param3 = pred_data.get('param3', '')
                yield [model_str, timezero_str, season, unit_id_to_obj[unit_id].name,
                       target_id_to_obj[target_id].name, class_str,
                       value, cat, prob, sample, quantile, family, param1, param2, param3]
            elif pred_class == PredictionElement.POINT_CLASS:
                value = pred_data['value']
                yield [model_str, timezero_str, season, unit_id_to_obj[unit_id].name,
                       target_id_to_obj[target_id].name, class_str,
                       value, cat, prob, sample, quantile, family, param1, param2, param3]
            elif pred_class == PredictionElement.QUANTILE_CLASS:
                for quantile, value in zip(pred_data['quantile'], pred_data['value']):
                    yield [model_str, timezero_str, season, unit_id_to_obj[unit_id].name,
                           target_id_to_obj[target_id].name, class_str,
                           value, cat, prob, sample, quantile, family, param1, param2, param3]
            elif pred_class == PredictionElement.SAMPLE_CLASS:
                for sample in pred_data['sample']:
                    yield [model_str, timezero_str, season, unit_id_to_obj[unit_id].name,
                           target_id_to_obj[target_id].name, class_str,
                           value, cat, prob, sample, quantile, family, param1, param2, param3]

    # done
    logger.debug(f"query_forecasts_for_project(): 3/3 done. num_rows={num_rows}, query={query}, project={project}")


def _query_forecasts_sql_for_pred_class(pred_classes, model_ids, unit_ids, target_ids, timezero_ids, as_of,
                                        is_exclude_oracle, is_include_retract=False):
    """
    A `query_forecasts_for_project()` helper that returns an SQL query string based on my args that, when executed,
    returns a list of 7-tuples: (forecast_model_id, timezero_id, pred_class, unit_id, target_id, is_retract, pred_data),
    where:
    - pred_class: PRED_CLASS_CHOICES int
    - data: the stored json

    :param pred_classes: list of PredictionElement.PRED_CLASS_CHOICES to include or [] (includes all)
    :param model_ids: list of ForecastsModel IDs to include or None (includes all)
    :param unit_ids: "" Unit ""
    :param target_ids: "" Target ""
    :param timezero_ids: "" TimeZero ""
    :param as_of: optional as_of timezone-aware datetime object, or None if not passed in query
    :param is_exclude_oracle: True if oracle forecasts should be excluded from results
    :param is_include_retract: as passed to query_forecasts_for_project()
    :return SQL to execute. returns columns as described above
    """
    # about the query: the ranked_rows CTE groups prediction elements and then ranks then in issued_at order, which
    # implements our masking (newer issued_ats mask older ones) and merging (discarded duplicates are merged back in
    # via previous versions) search semantics. it is crucial that the CTE /not/ include is_retract b/c that's how
    # retractions are implemented: they are ranked higher than the prediction elements they mask if they're newer.
    # retracted ones are optionally removed in the outer query. the outer query's LEFT JOIN is to cover retractions,
    # which do not have prediction data.
    and_oracle = f"AND NOT fm.is_oracle" if is_exclude_oracle else ""
    and_model_ids = f"AND fm.id IN ({', '.join(map(str, model_ids))})" if model_ids else ""
    and_pred_classes = f"AND pred_ele.pred_class IN ({', '.join(map(str, pred_classes))})" if pred_classes else ""
    and_unit_ids = f"AND pred_ele.unit_id IN ({', '.join(map(str, unit_ids))})" if unit_ids else ""
    and_target_ids = f"AND pred_ele.target_id IN ({', '.join(map(str, target_ids))})" if target_ids else ""
    and_timezero_ids = f"AND f.time_zero_id IN ({', '.join(map(str, timezero_ids))})" if timezero_ids else ""

    # NB: `as_of.isoformat()` (e.g., '2021-05-05T16:11:47.302099+00:00') works with postgres but not sqlite. however,
    # the default str ('2021-05-05 16:11:47.302099+00:00') works with both:
    and_issued_at = f"AND f.issued_at <= '{as_of}'" if as_of else ""

    and_is_retract = "" if is_include_retract else "AND NOT is_retract"
    sql = f"""
        WITH ranked_rows AS (
            SELECT f.forecast_model_id             AS fm_id,
                   f.time_zero_id                  AS tz_id,
                   pred_ele.id                     AS pred_ele_id,
                   pred_ele.pred_class             AS pred_class,
                   pred_ele.unit_id                AS unit_id,
                   pred_ele.target_id              AS target_id,
                   pred_ele.is_retract             AS is_retract,
                   RANK() OVER (
                       PARTITION BY fm.id, f.time_zero_id, pred_ele.unit_id, pred_ele.target_id, pred_ele.pred_class
                       ORDER BY f.issued_at DESC) AS rownum
            FROM {PredictionElement._meta.db_table} AS pred_ele
                             JOIN {Forecast._meta.db_table} AS f
            ON pred_ele.forecast_id = f.id
                JOIN {ForecastModel._meta.db_table} AS fm on f.forecast_model_id = fm.id
            WHERE fm.project_id = %s
                {and_oracle} {and_model_ids} {and_pred_classes} {and_unit_ids} {and_target_ids} {and_timezero_ids} {and_issued_at}
        )
        SELECT ranked_rows.fm_id       AS fm_id,
               ranked_rows.tz_id       AS tz_id,
               ranked_rows.pred_class  AS pred_class,
               ranked_rows.unit_id     AS unit_id,
               ranked_rows.target_id   AS target_id,
               ranked_rows.is_retract  AS is_retract,
               pred_data.data          AS pred_data
        FROM ranked_rows
                 LEFT JOIN {PredictionData._meta.db_table} AS pred_data
        ON ranked_rows.pred_ele_id = pred_data.pred_ele_id
        WHERE ranked_rows.rownum = 1 {and_is_retract};
    """
    return sql


def _model_tz_season_class_strs(forecast_model, time_zero, timezero_to_season_name, class_int):
    from utils.forecast import PRED_CLASS_INT_TO_NAME  # avoid circular imports


    model_str = forecast_model.abbreviation if forecast_model.abbreviation else forecast_model.name
    timezero_str = time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
    season = timezero_to_season_name[time_zero]
    class_str = PRED_CLASS_INT_TO_NAME[class_int]
    return model_str, timezero_str, season, class_str


def validate_forecasts_query(project, query):
    """
    Validates `query` according to the parameters documented at https://docs.zoltardata.com/ .

    :param project: as passed from `query_forecasts_for_project()`
    :param query: ""
    :return: a 2-tuple: (error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types, as_of)) . notice the
        second element is itself a 6-tuple of validated object IDs. there are two cases, which determine the return
        values: 1) valid query: error_messages is [], and ID lists are valid integers. as_of is either None (if not
        passed) or a timezone-aware datetime object. 2) invalid query: error_messages is a list of strings, and the ID
        lists are all []. Note that types is converted to ints via PRED_CLASS_NAME_TO_INT.
    """
    from utils.forecast import PRED_CLASS_INT_TO_NAME  # avoid circular imports


    # return value. filled next
    error_messages, model_ids, unit_ids, target_ids, timezero_ids, types, as_of = [], [], [], [], [], [], None

    # validate query type
    if not isinstance(query, dict):
        error_messages.append(f"query was not a dict: {query}, query type={type(query)}")
        return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types)]

    # validate keys
    actual_keys = set(query.keys())
    expected_keys = {'models', 'units', 'targets', 'timezeros', 'types', 'as_of'}
    if not (actual_keys <= expected_keys):
        error_messages.append(f"one or more query keys were invalid. query={query}, actual_keys={actual_keys}, "
                              f"expected_keys={expected_keys}")
        # return even though we could technically continue
        return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types, as_of)]

    # validate as_of if passed. must be parsable as a timezone-aware datetime
    error_message, as_of = _validate_as_of(query)
    if error_message:
        error_messages.append(error_message)
        return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types, as_of)]

    # validate object IDs that strings refer to
    error_messages, (model_ids, unit_ids, target_ids, timezero_ids) = _validate_query_ids(project, query)

    # validate Prediction types
    if 'types' in query:
        types = query['types']
        valid_prediction_types = set(PRED_CLASS_INT_TO_NAME.values())
        if not (set(types) <= valid_prediction_types):
            error_messages.append(f"one or more types were invalid prediction types. types={set(types)}, "
                                  f"valid_prediction_types={valid_prediction_types}, query={query}")
            return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types, as_of)]

        types = [PRED_CLASS_NAME_TO_INT[class_name] for class_name in types]

    # done (may or may not be valid)
    return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types, as_of)]


def _validate_as_of(query):
    """
    :param query: as passed to `validate_forecasts_query()` or `validate_truth_query()`
    :return: a 2-tuple: (error_message, as_of) where error_message is either None (if valid) or a string. as_of is
        either None (if invalid) or a datetime
    """
    as_of_str = query.get('as_of', None)
    if as_of_str is None:
        return None, None

    if type(as_of_str) != str:
        return f"'as_of' was not a string: '{type(as_of_str)}'", None

    # parse as_of using dateutil's flexible parser and then check for timezone info. error if none found
    try:
        as_of = dateutil.parser.parse(as_of_str)
        if as_of.tzinfo is None:
            return f"'as_of' did not contain timezone info: {as_of_str!r}. parsed as: '{as_of}'", None

        return None, as_of
    except dateutil.parser._parser.ParserError as pe:
        return f"'as_of' was not a recognizable datetime format: {as_of_str!r}: {pe}", None


#
# _validate_query_ids()
#

def _validate_query_ids(project, query):
    """
    A validate_forecasts_query() helper that validates the four of the five query keys that are strings referring to
    server object IDs.

    :return: a 2-tuple: (error_messages, (model_ids, unit_ids, target_ids, timezero_ids))
    """
    # return value. filled next
    error_messages, model_ids, unit_ids, target_ids, timezero_ids = [], [], [], [], []

    # validate keys are correct type (lists), and validate object strings (must have corresponding IDs)
    if 'models' in query:
        model_abbrevs = query['models']
        if not isinstance(model_abbrevs, list):
            error_messages.append(f"'models' was not a list. models={model_abbrevs}, query={query}")
            return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids)]

        # look up ForecastModel IDs corresponding to abbreviations. recall that abbreviations are enforced to be unique
        # within a project
        model_abbrev_to_id = {model.abbreviation: model.id for model in project.models.all()}
        for model_abbrev in model_abbrevs:
            if model_abbrev not in model_abbrev_to_id:
                error_messages.append(f"model with abbreviation not found. abbreviation={model_abbrev}, "
                                      f"valid abbreviations={list(model_abbrev_to_id.keys())}, query={query}")
            else:
                model_ids.append(model_abbrev_to_id[model_abbrev])

    if 'units' in query:
        unit_names = query['units']
        if not isinstance(unit_names, list):
            error_messages.append(f"'units' was not a list. units={unit_names}, query={query}")
            return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids)]

        # look up Unit IDs corresponding to names. note that unit names are NOT currently enforced to be unique.
        # HOWEVER we do not check for multiple ones here b/c we anticipate enforcement will be added soon. thus we pick
        # an arbitrary one if there are duplicates
        unit_name_to_id = {unit.name: unit.id for unit in project.units.all()}
        for unit_name in unit_names:
            if unit_name not in unit_name_to_id:
                error_messages.append(f"unit with name not found. name={unit_name}, "
                                      f"valid names={list(unit_name_to_id.keys())}, query={query}")
            else:
                unit_ids.append(unit_name_to_id[unit_name])

    if 'timezeros' in query:
        timezero_dates = query['timezeros']
        if not isinstance(timezero_dates, list):
            error_messages.append(f"'timezeros' was not a list. timezeros={timezero_dates}, query={query}")
            return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids)]

        # look up TimeZero IDs corresponding to timezero_dates. recall that timezero_dates are enforced to be unique
        # within a project
        timezero_date_to_id = {timezero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT): timezero.id
                               for timezero in project.timezeros.all()}
        for timezero_date in timezero_dates:
            if timezero_date not in timezero_date_to_id:
                error_messages.append(f"timezero with date not found. timezero_date={timezero_date}, "
                                      f"valid dates={list(timezero_date_to_id.keys())}, query={query}")
            else:
                timezero_ids.append(timezero_date_to_id[timezero_date])

    if 'targets' in query:
        target_names = query['targets']
        if not isinstance(target_names, list):
            error_messages.append(f"'targets' was not a list. targets={target_names}, query={query}")
            return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids)]

        # look up Target IDs corresponding to names. like Units, Target names are NOT currently enforced to be unique,
        # and are handled as above with Units
        target_name_to_id = {target.name: target.id for target in project.targets.all()}
        for target_name in target_names:
            if target_name not in target_name_to_id:
                error_messages.append(f"target with name not found. name={target_name}, "
                                      f"valid names={list(target_name_to_id.keys())}, query={query}")
            else:
                target_ids.append(target_name_to_id[target_name])

    # done
    return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids)]


#
# _forecasts_query_worker()
#

# value used by _forecasts_query_worker() to set the postgres `statement_timeout` client connection parameter:
# https://www.postgresql.org/docs/9.6/runtime-config-client.html

QUERY_FORECAST_STATEMENT_TIMEOUT = 60


class IterCounter(object):
    """
    Generator (iterator, actually) wrapper that tracks the number of `yield` calls that have been made.
    per https://stackoverflow.com/questions/6309277/how-to-count-the-items-in-a-generator-consumed-by-other-code
    """


    def __init__(self, it):
        self._iter = it
        self.count = 0


    def _counterWrapper(self, it):
        for i in it:
            yield i
            self.count += 1


    def __iter__(self):
        return self._counterWrapper(self._iter)


def _forecasts_query_worker(job_pk):
    """
    enqueue() helper function

    assumes these input_json fields are present and valid:
    - 'project_pk'
    - 'query' (assume has passed `validate_forecasts_query()`)
    """
    _query_worker(job_pk, query_forecasts_for_project)


def _query_worker(job_pk, query_project_fcn):
    # imported here so that tests can patch via mock:
    from utils.cloud_file import upload_file


    # run the query
    job = get_object_or_404(Job, pk=job_pk)
    project = get_object_or_404(Project, pk=job.input_json['project_pk'])
    query = job.input_json['query']
    try:
        logger.debug(f"_query_worker(): 1/4 querying rows. query={query}. job={job}")
        # use a transaction to set the scope of the postgres `statement_timeout` parameter. statement_timeout raises
        # this error: django.db.utils.OperationalError ('canceling statement due to statement timeout'). Similarly,
        # idle_in_transaction_session_timeout raises django.db.utils.InternalError . todo does not consistently work!
        if connection.vendor == 'postgresql':
            with transaction.atomic(), connection.cursor() as cursor:
                cursor.execute(f"SET LOCAL statement_timeout = '{QUERY_FORECAST_STATEMENT_TIMEOUT}s';")
                cursor.execute(
                    f"SET LOCAL idle_in_transaction_session_timeout = '{QUERY_FORECAST_STATEMENT_TIMEOUT}s';")
                rows = query_project_fcn(project, query)
        else:
            rows = query_project_fcn(project, query)
    except JobTimeoutException as jte:
        job.status = Job.TIMEOUT
        job.save()
        logger.error(f"_query_worker(): error: {jte!r}. job={job}")
        return
    except Exception as ex:
        job.status = Job.FAILED
        job.failure_message = f"_query_worker(): error: {ex!r}"
        job.save()
        logger.error(job.failure_message + f". job={job}")
        return

    # upload the file to cloud storage
    try:
        # we need a BytesIO for upload_file() (o/w it errors: "Unicode-objects must be encoded before hashing"), but
        # writerows() needs a StringIO (o/w "a bytes-like object is required, not 'str'" error), so we use
        # TextIOWrapper. BUT: https://docs.python.org/3.6/library/io.html#io.TextIOWrapper :
        #     Text I/O over a binary storage (such as a file) is significantly slower than binary I/O over the same
        #     storage, because it requires conversions between unicode and binary data using a character codec. This can
        #     become noticeable handling huge amounts of text data like large log files.

        # note: using a context is required o/w is closed and becomes unusable:
        # per https://stackoverflow.com/questions/59079354/how-to-write-utf-8-csv-into-bytesio-in-python3 :
        with io.BytesIO() as bytes_io:
            logger.debug(f"_query_worker(): 2/4 writing rows. job={job}")
            text_io_wrapper = io.TextIOWrapper(bytes_io, 'utf-8', newline='')
            rows = IterCounter(rows)
            csv.writer(text_io_wrapper).writerows(rows)
            text_io_wrapper.flush()
            bytes_io.seek(0)

            logger.debug(f"_query_worker(): 3/4 uploading file. job={job}")
            upload_file(job, bytes_io)  # might raise S3 exception
            job.output_json = {'num_rows': rows.count}
            job.status = Job.SUCCESS
            job.save()
            logger.debug(f"_query_worker(): 4/4 done. job={job}")
    except (BotoCoreError, Boto3Error, ClientError, ConnectionClosedError) as aws_exc:
        job.status = Job.FAILED
        job.failure_message = f"_query_worker(): error: {aws_exc!r}"
        job.save()
        logger.error(job.failure_message + f". job={job}")
    except Exception as ex:
        job.status = Job.FAILED
        job.failure_message = f"_query_worker(): error: {ex!r}"
        logger.error(job.failure_message + f". job={job}")
        job.save()


#
# query_truth_for_project()
#

def query_truth_for_project(project, query, max_num_rows=MAX_NUM_QUERY_ROWS):
    """
    Top-level function for querying truth within project. Runs in the calling thread and therefore blocks.
    Returns a list of rows in a Zoltar-specific CSV row format. The columns are defined in TRUTH_CSV_HEADER, as detailed
    at https://docs.zoltardata.com/fileformats/#truth-data-format-csv .

    `query` is documented at https://docs.zoltardata.com/, but briefly, it is a dict of up to four keys, three of which
    are lists of strings:

    - 'units': "" Unit.name strings
    - 'targets': "" Target.name strings
    - 'timezeros': "" TimeZero.timezero_date strings in YYYY_MM_DD_DATE_FORMAT

    The fourth key allows searching based on `Forecast.issued_at`:
    - 'as_of': Passing a datetime string in the optional as_of field causes the query to return only those forecast
        versions whose issued_at is <= the as_of datetime (AKA timestamp).

    Note that _strings_ are passed to refer to object *contents*, not database IDs, which means validation will fail if
    the referred-to objects are not found. NB: If multiple objects are found with the same name then the program will
    arbitrarily choose one.

    NB: The returned response will contain only those rows that actually loaded from the original CSV file passed
    to Project.load_truth_data(), which will contain fewer rows if some were invalid. For that reason we change the
    filename to hopefully hint at what's going on.

    :param project: a Project
    :param query: a dict specifying the query parameters as described above. NB: assumes it has passed validation via
        `validate_truth_query()`
    :param max_num_rows: the number of rows at which this function raises a RuntimeError
    :return: a list of CSV rows including the header
    """
    # validate query
    logger.debug(f"query_truth_for_project(): 1/3 validating query. query={query}, project={project}")
    error_messages, (unit_ids, target_ids, timezero_ids, as_of) = validate_truth_query(project, query)
    if error_messages:
        raise RuntimeError(f"invalid query. query={query}, errors={error_messages}")

    timezero_id_to_obj = {timezero.pk: timezero for timezero in project.timezeros.all()}
    unit_id_to_obj = {unit.pk: unit for unit in project.units.all()}
    target_id_to_obj = {target.pk: target for target in project.targets.all()}

    yield TRUTH_CSV_HEADER

    oracle_model = oracle_model_for_project(project)
    if not oracle_model:
        return

    # get the SQL then execute and iterate over resulting data
    model_ids = [oracle_model.pk]
    sql = _query_forecasts_sql_for_pred_class(None, model_ids, unit_ids, target_ids, timezero_ids, as_of, False)
    logger.debug(f"query_truth_for_project(): 2/3 executing sql. model_ids, unit_ids, target_ids, timezero_ids, "
                 f"as_of= {model_ids}, {unit_ids}, {target_ids}, {timezero_ids}, {as_of}")
    num_rows = 0
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        for fm_id, tz_id, pred_class, unit_id, target_id, is_retract, pred_data in batched_rows(cursor):
            # we do not have to check is_retract b/c we pass `is_include_retract=False`, which skips retractions
            num_rows += 1
            if num_rows > max_num_rows:
                raise RuntimeError(f"number of rows exceeded maximum. num_rows={num_rows}, "
                                   f"max_num_rows={max_num_rows}")

            # counterintuitively must use json.loads per https://code.djangoproject.com/ticket/31991
            pred_data = json.loads(pred_data)
            tz_date = timezero_id_to_obj[tz_id].timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
            yield [tz_date, unit_id_to_obj[unit_id].name, target_id_to_obj[target_id].name, pred_data['value']]

    # done
    logger.debug(f"query_truth_for_project(): 3/3 done. num_rows={num_rows}, query={query}, project={project}")


def validate_truth_query(project, query):
    """
    Validates `query` according to the parameters documented at https://docs.zoltardata.com/ . Nearly identical to
    validate_forecasts_query() except only validates "units", "targets", "timezeros", and "as_of".

    :param project: as passed from `query_forecasts_for_project()`
    :param query: ""
    :return: a 2-tuple: (error_messages, (unit_ids, target_ids, timezero_ids, as_of))
    """
    # return value. filled next
    error_messages, unit_ids, target_ids, timezero_ids, as_of = [], [], [], [], None

    # validate query type
    if not isinstance(query, dict):
        error_messages.append(f"query was not a dict: {query}, query type={type(query)}")
        return [error_messages, (unit_ids, target_ids, timezero_ids, as_of)]

    # validate keys
    actual_keys = set(query.keys())
    expected_keys = {'units', 'targets', 'timezeros', 'as_of'}
    if not (actual_keys <= expected_keys):
        error_messages.append(f"one or more query keys were invalid. query={query}, actual_keys={actual_keys}, "
                              f"expected_keys={expected_keys}")
        # return even though we could technically continue
        return [error_messages, (unit_ids, target_ids, timezero_ids, as_of)]

    # validate as_of if passed. must be parsable as a timezone-aware datetime
    error_message, as_of = _validate_as_of(query)
    if error_message:
        error_messages.append(error_message)
        return [error_messages, (unit_ids, target_ids, timezero_ids, as_of)]

    # validate object IDs that strings refer to
    error_messages, (model_ids, unit_ids, target_ids, timezero_ids) = _validate_query_ids(project, query)

    # done (may or may not be valid)
    return [error_messages, (unit_ids, target_ids, timezero_ids, as_of)]


def _truth_query_worker(job_pk):
    """
    enqueue() helper function

    assumes these input_json fields are present and valid:
    - 'project_pk'
    - 'query' (assume has passed `validate_truth_query()`)
    """
    _query_worker(job_pk, query_truth_for_project)
