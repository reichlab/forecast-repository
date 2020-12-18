import csv
import datetime
import io
import string
from collections import defaultdict
from itertools import groupby

from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError, ClientError, ConnectionClosedError
from django.db import connection, transaction
from rest_framework.generics import get_object_or_404
from rq.timeouts import JobTimeoutException

from forecast_app.models import BinDistribution, NamedDistribution, PointPrediction, SampleDistribution, \
    QuantileDistribution, Job, Project
from forecast_repo.settings.base import MAX_NUM_QUERY_ROWS
from utils.forecast import coalesce_values
from utils.project import logger, latest_forecast_ids_for_project
from utils.project_truth import TRUTH_CSV_HEADER, truth_data_qs
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


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

    The 'class' of each row is named to be the same as Zoltar's utils.forecast.PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS
    variable. Column ordering is FORECAST_CSV_HEADER.

    `query` is documented at https://docs.zoltardata.com/, but briefly, it is a dict of up to six keys, five of which
    are lists of strings:

    - 'models': optional list of ForecastModel.abbreviation strings
    - 'units': "" Unit.name strings
    - 'targets': "" Target.name strings
    - 'timezeros': "" TimeZero.timezero_date strings in YYYY_MM_DD_DATE_FORMAT
    - 'types': optional list of type strings as defined in PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS.values()

    The sixth key allows searching based on `Forecast.issue_date`:
    - 'as_of': optional inclusive issue_date in YYYY_MM_DD_DATE_FORMAT to limit the search to. the default behavior if
               not passed is to use the newest forecast for each TimeZero.

    Note that _strings_ are passed to refer to object *contents*, not database IDs, which means validation will fail if
    the referred-to objects are not found. NB: If multiple objects are found with the same name then the program will
    arbitrarily choose one.

    :param project: a Project
    :param query: a dict specifying the query parameters as described above. NB: assumes it has passed validation via
        `validate_forecasts_query()`
    :param max_num_rows: the number of rows at which this function raises a RuntimeError
    :return: a list of CSV rows including the header
    """
    from utils.forecast import PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS  # avoid circular imports


    # validate query
    logger.debug(f"query_forecasts_for_project(): 1/4 validating query. query={query}, project={project}")
    error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types) = validate_forecasts_query(project, query)

    # get which types to include
    is_include_bin = (not types) or (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution] in types)
    is_include_named = (not types) or (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[NamedDistribution] in types)
    is_include_point = (not types) or (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction] in types)
    is_include_sample = (not types) or (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleDistribution] in types)
    is_include_quantile = (not types) or (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[QuantileDistribution] in types)

    # get Forecasts to be included, applying query's constraints
    forecast_ids = latest_forecast_ids_for_project(project, True, model_ids=model_ids, timezero_ids=timezero_ids,
                                                   as_of=query.get('as_of', None))

    # create queries for each prediction type, but don't execute them yet. first check # rows and limit if necessary.
    # note that not all will be executed, depending on the 'types' key

    # todo no unit_ids or target_ids -> do not pass '__in'
    if not unit_ids:
        unit_ids = project.units.all().values_list('id', flat=True)  # "" Units ""
    if not target_ids:
        target_ids = project.targets.all().values_list('id', flat=True)  # "" Targets ""

    bin_qs = BinDistribution.objects.filter(forecast__id__in=list(forecast_ids),
                                            unit__id__in=list(unit_ids),
                                            target__id__in=list(target_ids)) \
        .values_list('forecast__forecast_model__id', 'forecast__time_zero__id', 'unit__name', 'target__name',
                     'prob', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')
    named_qs = NamedDistribution.objects.filter(forecast__id__in=list(forecast_ids),
                                                unit__id__in=list(unit_ids),
                                                target__id__in=list(target_ids)) \
        .values_list('forecast__forecast_model__id', 'forecast__time_zero__id', 'unit__name', 'target__name',
                     'family', 'param1', 'param2', 'param3')
    point_qs = PointPrediction.objects.filter(forecast__id__in=list(forecast_ids),
                                              unit__id__in=list(unit_ids),
                                              target__id__in=list(target_ids)) \
        .values_list('forecast__forecast_model__id', 'forecast__time_zero__id', 'unit__name', 'target__name',
                     'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
    sample_qs = SampleDistribution.objects.filter(forecast__id__in=list(forecast_ids),
                                                  unit__id__in=list(unit_ids),
                                                  target__id__in=list(target_ids)) \
        .values_list('forecast__forecast_model__id', 'forecast__time_zero__id', 'unit__name', 'target__name',
                     'sample_i', 'sample_f', 'sample_t', 'sample_d', 'sample_b')
    quantile_qs = QuantileDistribution.objects.filter(forecast__id__in=list(forecast_ids),
                                                      unit__id__in=list(unit_ids),
                                                      target__id__in=list(target_ids)) \
        .values_list('forecast__forecast_model__id', 'forecast__time_zero__id', 'unit__name', 'target__name',
                     'quantile', 'value_i', 'value_f', 'value_d')

    # count number of rows to query, and error if too many
    logger.debug(f"query_forecasts_for_project(): 2/4 getting counts. query={query}, project={project}")
    is_include_query_set_pred_types = [(is_include_bin, bin_qs, 'bin'),
                                       (is_include_named, named_qs, 'named'),
                                       (is_include_point, point_qs, 'point'),
                                       (is_include_sample, sample_qs, 'sample'),
                                       (is_include_quantile, quantile_qs, 'quantile')]

    pred_type_counts = []  # filled next. NB: we do not use a list comprehension b/c we want logging for each pred_type
    for idx, (is_include, query_set, pred_type) in enumerate(is_include_query_set_pred_types):
        if is_include:
            logger.debug(f"query_forecasts_for_project(): 2{string.ascii_letters[idx]}/4 getting counts: {pred_type!r}")
            pred_type_counts.append((pred_type, query_set.count()))

    num_rows = sum([_[1] for _ in pred_type_counts])
    logger.debug(f"query_forecasts_for_project(): 3/4 preparing to query. pred_type_counts={pred_type_counts}. total "
                 f"num_rows={num_rows}. query={query}, project={project}")
    if num_rows > max_num_rows:
        raise RuntimeError(f"number of rows exceeded maximum. num_rows={num_rows}, max_num_rows={max_num_rows}")

    # output rows for each Prediction subclass
    yield FORECAST_CSV_HEADER

    forecast_model_id_to_obj = {forecast_model.pk: forecast_model for forecast_model in project.models.all()}
    timezero_id_to_obj = {timezero.pk: timezero for timezero in project.timezeros.all()}
    timezero_to_season_name = project.timezero_to_season_name()

    # add BinDistributions
    if is_include_bin:
        logger.debug(f"query_forecasts_for_project(): 3a/4 getting BinDistributions")
        # class-specific columns all default to empty:
        value, cat, prob, sample, quantile, family, param1, param2, param3 = '', '', '', '', '', '', '', '', ''
        for forecast_model_id, timezero_id, unit_name, target_name, prob, cat_i, cat_f, cat_t, cat_d, cat_b in bin_qs:
            model_str, timezero_str, season, class_str = _model_tz_season_class_strs(
                forecast_model_id_to_obj[forecast_model_id], timezero_id_to_obj[timezero_id], timezero_to_season_name,
                BinDistribution)
            cat = PointPrediction.first_non_none_value(cat_i, cat_f, cat_t, cat_d, cat_b)
            cat = cat.strftime(YYYY_MM_DD_DATE_FORMAT) if isinstance(cat, datetime.date) else cat
            yield [model_str, timezero_str, season, unit_name, target_name, class_str,
                   value, cat, prob, sample, quantile, family, param1, param2, param3]

    # add NamedDistributions
    if is_include_named:
        logger.debug(f"query_forecasts_for_project(): 3b/4 getting NamedDistributions")
        # class-specific columns all default to empty:
        value, cat, prob, sample, quantile, family, param1, param2, param3 = '', '', '', '', '', '', '', '', ''
        for forecast_model_id, timezero_id, unit_name, target_name, family, param1, param2, param3 in named_qs:
            model_str, timezero_str, season, class_str = _model_tz_season_class_strs(
                forecast_model_id_to_obj[forecast_model_id], timezero_id_to_obj[timezero_id], timezero_to_season_name,
                NamedDistribution)
            family = NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION[family]
            yield [model_str, timezero_str, season, unit_name, target_name, class_str,
                   value, cat, prob, sample, quantile, family, param1, param2, param3]

    # add PointPredictions
    if is_include_point:
        logger.debug(f"query_forecasts_for_project(): 3c/4 getting PointPredictions")
        # class-specific columns all default to empty:
        value, cat, prob, sample, quantile, family, param1, param2, param3 = '', '', '', '', '', '', '', '', ''
        for forecast_model_id, timezero_id, unit_name, target_name, value_i, value_f, value_t, value_d, value_b \
                in point_qs:
            model_str, timezero_str, season, class_str = _model_tz_season_class_strs(
                forecast_model_id_to_obj[forecast_model_id], timezero_id_to_obj[timezero_id], timezero_to_season_name,
                PointPrediction)
            value = PointPrediction.first_non_none_value(value_i, value_f, value_t, value_d, value_b)
            value = value.strftime(YYYY_MM_DD_DATE_FORMAT) if isinstance(value, datetime.date) else value
            yield [model_str, timezero_str, season, unit_name, target_name, class_str,
                   value, cat, prob, sample, quantile, family, param1, param2, param3]

    # add SampleDistribution
    if is_include_sample:
        logger.debug(f"query_forecasts_for_project(): 3d/4 getting SampleDistributions")
        # class-specific columns all default to empty:
        value, cat, prob, sample, quantile, family, param1, param2, param3 = '', '', '', '', '', '', '', '', ''
        for forecast_model_id, timezero_id, unit_name, target_name, \
            sample_i, sample_f, sample_t, sample_d, sample_b in sample_qs:
            model_str, timezero_str, season, class_str = _model_tz_season_class_strs(
                forecast_model_id_to_obj[forecast_model_id], timezero_id_to_obj[timezero_id], timezero_to_season_name,
                SampleDistribution)
            sample = PointPrediction.first_non_none_value(sample_i, sample_f, sample_t, sample_d, sample_b)
            sample = sample.strftime(YYYY_MM_DD_DATE_FORMAT) if isinstance(sample, datetime.date) else sample
            yield [model_str, timezero_str, season, unit_name, target_name, class_str,
                   value, cat, prob, sample, quantile, family, param1, param2, param3]

    # add QuantileDistribution
    if is_include_quantile:
        logger.debug(f"query_forecasts_for_project(): 3e/4 getting QuantileDistributions")
        # class-specific columns all default to empty:
        value, cat, prob, sample, quantile, family, param1, param2, param3 = '', '', '', '', '', '', '', '', ''
        for forecast_model_id, timezero_id, unit_name, target_name, quantile, value_i, value_f, value_d in quantile_qs:
            model_str, timezero_str, season, class_str = _model_tz_season_class_strs(
                forecast_model_id_to_obj[forecast_model_id], timezero_id_to_obj[timezero_id], timezero_to_season_name,
                QuantileDistribution)
            value = PointPrediction.first_non_none_value(value_i, value_f, None, value_d, None)
            value = value.strftime(YYYY_MM_DD_DATE_FORMAT) if isinstance(value, datetime.date) else value
            yield [model_str, timezero_str, season, unit_name, target_name, class_str,
                   value, cat, prob, sample, quantile, family, param1, param2, param3]

    # NB: we do not sort b/c it's expensive
    logger.debug(f"query_forecasts_for_project(): 4/4 done. num_rows={num_rows}, query={query}, project={project}")


def _model_tz_season_class_strs(forecast_model, time_zero, timezero_to_season_name, prediction_class):
    from utils.forecast import PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS  # avoid circular imports


    model_str = forecast_model.abbreviation if forecast_model.abbreviation else forecast_model.name
    timezero_str = time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
    season = timezero_to_season_name[time_zero]
    class_str = PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[prediction_class]
    return model_str, timezero_str, season, class_str


def validate_forecasts_query(project, query):
    """
    Validates `query` according to the parameters documented at https://docs.zoltardata.com/ .

    :param project: as passed from `query_forecasts_for_project()`
    :param query: ""
    :return: a 2-tuple: (error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types)) . notice the second
        element is itself a 5-tuple of validated object IDs. there are two cases, which determine the return values:
        1) valid query: error_messages is [], and ID lists are valid integers. 2) invalid query: error_messages is a
        list of strings, and the ID lists are all [].
    """
    from utils.forecast import PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS  # avoid circular imports


    # return value. filled next
    error_messages, model_ids, unit_ids, target_ids, timezero_ids, types = [], [], [], [], [], []

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
        return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types)]

    # validate as_of
    as_of = query.get('as_of', None)
    if as_of is not None:
        if type(as_of) != str:
            error_messages.append(f"'as_of' was not a string: '{type(as_of)}'")
            return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types)]

        try:
            datetime.datetime.strptime(as_of, YYYY_MM_DD_DATE_FORMAT).date()
        except ValueError:
            error_messages.append(f"'as_of' was not in YYYY-MM-DD format: {type(as_of)}")
            return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types)]

    # validate object IDs that strings refer to
    error_messages, (model_ids, unit_ids, target_ids, timezero_ids) = _validate_query_ids(project, query)

    # validate Prediction types
    if 'types' in query:
        types = query['types']
        valid_prediction_types = set(PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS.values())
        if not (set(types) <= valid_prediction_types):
            error_messages.append(f"one or more types were invalid prediction types. types={set(types)}, "
                                  f"valid_prediction_types={valid_prediction_types}, query={query}")
            return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types)]

    # done (may or may not be valid)
    return [error_messages, (model_ids, unit_ids, target_ids, timezero_ids, types)]


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

    `query` is documented at https://docs.zoltardata.com/, but briefly, it is a dict of up to three keys, all of which
    are lists of strings:

    - 'units': "" Unit.name strings
    - 'targets': "" Target.name strings
    - 'timezeros': "" TimeZero.timezero_date strings in YYYY_MM_DD_DATE_FORMAT

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
    logger.debug(f"query_truth_for_project(): 1/xx validating query. query={query}, project={project}")
    error_messages, (unit_ids, target_ids, timezero_ids) = validate_truth_query(project, query)

    # get the rows, building up a QuerySet in steps
    the_truth_data_qs = truth_data_qs(project)
    if unit_ids:
        the_truth_data_qs = the_truth_data_qs.filter(unit__id__in=unit_ids)
    if target_ids:
        the_truth_data_qs = the_truth_data_qs.filter(target__id__in=target_ids)
    if timezero_ids:
        the_truth_data_qs = the_truth_data_qs.filter(forecast__time_zero__id__in=timezero_ids)

    # get and check the number of rows
    num_rows = the_truth_data_qs.count()
    if num_rows > max_num_rows:
        raise RuntimeError(f"number of rows exceeded maximum. num_rows={num_rows}, max_num_rows={max_num_rows}")

    # done
    the_truth_data_qs = the_truth_data_qs.order_by('id') \
        .values_list('forecast__time_zero__timezero_date', 'unit__name', 'target__name',
                     'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
    return [TRUTH_CSV_HEADER] + [[timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT), unit_name, target_name,
                                  coalesce_values(value_i, value_f, value_t, value_d, value_b)]
                                 for timezero_date, unit_name, target_name, value_i, value_f, value_t, value_d, value_b
                                 in the_truth_data_qs]


def validate_truth_query(project, query):
    """
    Validates `query` according to the parameters documented at https://docs.zoltardata.com/ . Nearly identical to
    validate_forecasts_query() except only validates "units", "targets", and "timezeros".

    :param project: as passed from `query_forecasts_for_project()`
    :param query: ""
    :return: a 2-tuple: (error_messages, (unit_ids, target_ids, timezero_ids))
    """
    # return value. filled next
    error_messages, unit_ids, target_ids, timezero_ids = [], [], [], []

    # validate query type
    if not isinstance(query, dict):
        error_messages.append(f"query was not a dict: {query}, query type={type(query)}")
        return [error_messages, (unit_ids, target_ids, timezero_ids)]

    # validate keys
    actual_keys = set(query.keys())
    expected_keys = {'units', 'targets', 'timezeros'}
    if not (actual_keys <= expected_keys):
        error_messages.append(f"one or more query keys were invalid. query={query}, actual_keys={actual_keys}, "
                              f"expected_keys={expected_keys}")
        # return even though we could technically continue
        return [error_messages, (unit_ids, target_ids, timezero_ids)]

    # validate object IDs that strings refer to
    error_messages, (model_ids, unit_ids, target_ids, timezero_ids) = _validate_query_ids(project, query)

    # done (may or may not be valid)
    return [error_messages, (unit_ids, target_ids, timezero_ids)]


def _truth_query_worker(job_pk):
    """
    enqueue() helper function

    assumes these input_json fields are present and valid:
    - 'project_pk'
    - 'query' (assume has passed `validate_truth_query()`)
    """
    _query_worker(job_pk, query_truth_for_project)
