import csv
import datetime
import io
import logging
from collections import defaultdict

from django.db import transaction

from forecast_app.models import PredictionElement
from forecast_app.models.prediction_element import PRED_CLASS_INT_TO_NAME
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


logger = logging.getLogger(__name__)


#
# oracle model functions
#
# these functions help manage the single (for now) oracle model in a project. there is either zero or one
# of them. this business rule is managed via load_truth_data()
#

def oracle_model_for_project(project):
    """
    :param project: a Project
    :return: the single oracle ForecastModel in project, or None if none exists yet
    :raises RuntimeError: >1 oracle models found
    """
    oracle_models = project.models.filter(is_oracle=True)
    if len(oracle_models) > 1:
        raise RuntimeError(f"more than one oracle model found. oracle_models={oracle_models}")

    return oracle_models.first()


def create_oracle_model_for_project(project):
    """
    Creates and returns new oracle ForecastModel for project. The oracle's owner is the project's owner.

    :param project: a Project
    :return: the new ForecastModel
    :raises RuntimeError: if one already exists
    """
    from forecast_app.models import ForecastModel  # avoid circular imports


    oracle_model = oracle_model_for_project(project)
    if oracle_model:
        raise RuntimeError(f"existing oracle model found: {oracle_model}")

    oracle_model = ForecastModel.objects.create(
        owner=project.owner,
        project=project,
        name='Project Oracle',
        abbreviation='oracle',
        description='Oracle model',
        is_oracle=True,
        # team_name=None,
        # home_url=None,
        # aux_data_url=None,
    )
    return oracle_model


#
# truth data access functions
#

def truth_data_qs(project):
    """
    :return: A QuerySet of project's truth data - PredictionElement instances.
    """
    from forecast_app.models import PredictionElement  # avoid circular imports


    oracle_model = oracle_model_for_project(project)
    return PredictionElement.objects.none() if not oracle_model else \
        PredictionElement.objects.filter(forecast__forecast_model=oracle_model)


def is_truth_data_loaded(project):
    """
    :return: True if `project` has truth data loaded via load_truth_data(). Actually, returns the count, which acts as a
        boolean.
    """
    return truth_data_qs(project).exists()


def first_truth_data_forecast(project):
    """
    :param project: a Project
    :return: the first Forecast in project's oracle ForecastModel, or None if no truth is loaded
    """
    oracle_model = oracle_model_for_project(project)
    return None if not oracle_model else oracle_model.forecasts.first()


def get_truth_data_preview(project):
    """
    :return: view helper function that returns a preview of my truth data in the form of a table that's
        represented as a nested list of rows. each row: [timezero_date, unit_name, target_name, truth_value]
    """
    from forecast_app.models import PredictionData  # avoid circular imports


    oracle_model = oracle_model_for_project(project)
    if not oracle_model:
        return PredictionData.objects.none()

    # note: https://code.djangoproject.com/ticket/32483 sqlite3 json query bug -> we manually access field instead of
    # using 'data__value'
    pred_data_qs = PredictionData.objects \
        .filter(pred_ele__forecast__forecast_model=oracle_model) \
        .values_list('pred_ele__forecast__time_zero__timezero_date', 'pred_ele__unit__name', 'pred_ele__target__name',
                     'data')[:10]
    return [(tz_date, unit__name, target__name, data['value'])
            for tz_date, unit__name, target__name, data in pred_data_qs]


#
# load_truth_data()
#

POSTGRES_NULL_VALUE = 'NULL'  # used for Postgres-specific loading of rows from csv data files

TRUTH_CSV_HEADER = ['timezero', 'unit', 'target', 'value']


@transaction.atomic
def load_truth_data(project, truth_file_path_or_fp, file_name=None, is_convert_na_none=False):
    """
    Loads the data in truth_file_path (see below for file format docs), implementing our truth-as-forecasts approach
    where each group of values in the file with the same timezeros are loaded as PointData within a new Forecast
    with that TimeZero.

    Like load_csv_data(), uses direct SQL for performance, using a fast Postgres-specific routine if connected to it.
    Notes:

    - One csv file/project, which includes timezeros across all seasons.
    - Columns: timezero, unit, target, value . NB: There is no season information (see below). timezeros are
      formatted in YYYY_MM_DD_DATE_FORMAT.
    - A header must be included.
    - Missing timezeros: If the program generating the csv file does not have information for a particular project
      timezero, then it should not generate a value for it. (An alternative is to require the program to generate
      placeholder values for missing dates.)
    - Non-numeric values: Some targets will have no value, such as season onset when a baseline is not met. In those
      cases, the value should be “NA”, per
      https://predict.cdc.gov/api/v1/attachments/flusight/flu_challenge_2016-17_update.docx.
    - For date-based onset or peak targets, values must be dates in the same format as timezeros, rather than
        project-specific time intervals such as an epidemic week.
    - Validation:
        - Every timezero in the csv file must have a matching one in the project. Note that the inverse is not
          necessarily true, such as in the case above of missing timezeros.
        - Similarly, every unit and target in the csv file must a matching one in the Project.

    :param project: the Project to load truth into
    :param truth_file_path_or_fp: Path to csv file with the truth data, one line per timezero|unit|target
        combination, OR an already-open file-like object
    :param file_name: name to use for the file
    :param is_convert_na_none: as passed to Target.is_value_compatible_with_target_type()
    """
    logger.debug(f"load_truth_data(): entered. truth_file_path_or_fp={truth_file_path_or_fp}, "
                 f"file_name={file_name}")
    if not project.pk:
        raise RuntimeError("instance is not saved the the database, so can't insert data: {!r}".format(project))

    # create the (single) oracle model if necessary
    oracle_model = oracle_model_for_project(project) or create_oracle_model_for_project(project)

    # create and load oracle Forecasts for each group of rows related to the same TimeZero
    logger.debug(f"load_truth_data(): calling _load_truth_data()")
    # https://stackoverflow.com/questions/1661262/check-if-object-is-file-like-in-python
    if isinstance(truth_file_path_or_fp, io.IOBase):
        num_rows = _load_truth_data(project, oracle_model, truth_file_path_or_fp, file_name, is_convert_na_none)
    else:
        with open(str(truth_file_path_or_fp)) as truth_file_fp:
            num_rows = _load_truth_data(project, oracle_model, truth_file_fp, file_name, is_convert_na_none)

    # done
    logger.debug(f"load_truth_data(): saving. num_rows: {num_rows}")
    logger.debug(f"load_truth_data(): done")


@transaction.atomic
def _load_truth_data(project, oracle_model, truth_file_fp, file_name, is_convert_na_none):
    from forecast_app.models import Forecast  # avoid circular imports
    from utils.forecast import load_predictions_from_json_io_dict  # ""


    # load, validate, and replace with objects and parsed values.
    # rows: (timezero, unit, target, parsed_value) (first three are objects)
    logger.debug(f"_load_truth_data(): entered. calling _read_truth_data_rows()")
    rows = _read_truth_data_rows(project, truth_file_fp, is_convert_na_none)
    if not rows:
        return 0

    # group rows by timezero and then create and load oracle Forecasts for each group, passing them as
    # json_io_dicts. we leverage _load_truth_data_rows_for_forecast() by creating a json_io_dict for the truth data
    # where each truth row becomes its own 'point' prediction element. NB: these forecasts are identified as coming from
    # the same truth file via all forecasts having the same source and issued_at
    timezero_groups = defaultdict(list)
    for timezero, unit, target, parsed_value in rows:
        timezero_groups[timezero].append([unit, target, parsed_value])

    source = file_name if file_name else ''
    forecasts = []  # ones created
    logger.debug(f"_load_truth_data(): creating and loading {len(timezero_groups)} forecasts. source={source!r}")
    point_class = PRED_CLASS_INT_TO_NAME[PredictionElement.POINT_CLASS]
    for timezero, timezero_rows in timezero_groups.items():
        forecast = Forecast.objects.create(forecast_model=oracle_model, source=source, time_zero=timezero,
                                           notes=f"oracle forecast")
        prediction_dicts = [{'unit': unit.name, 'target': target.name,
                             'class': point_class,
                             'prediction': {
                                 'value': parsed_value.strftime(YYYY_MM_DD_DATE_FORMAT)
                                 if isinstance(parsed_value, datetime.date) else parsed_value}}
                            for unit, target, parsed_value in timezero_rows]
        load_predictions_from_json_io_dict(forecast, {'meta': {}, 'predictions': prediction_dicts},
                                           is_skip_validation=True)
        forecasts.append(forecast)

    # set all issued_ats to be the same - this avoids an edge case where midnight is spanned and some are a day later.
    # arbitrarily use the first forecast's issued_at
    if forecasts:
        issued_at = forecasts[0].issued_at
        logger.debug(f"_load_truth_data(): setting issued_ats to {issued_at}, # forecasts={len(forecasts)}")
        for forecast in forecasts:
            forecast.issued_at = issued_at
            forecast.save()

    logger.debug(f"_load_truth_data(): done")
    return len(rows)


def _read_truth_data_rows(project, csv_file_fp, is_convert_na_none):
    """
    Similar to _cleaned_rows_from_cdc_csv_file(), loads, validates, and cleans the rows in csv_file_fp.

    :return: a list of 8-tuples: (timezero, unit, target, parsed_value) (first three are objects)
    """
    from forecast_app.models import Target  # avoid circular imports


    csv_reader = csv.reader(csv_file_fp, delimiter=',')

    # validate header
    try:
        orig_header = next(csv_reader)
    except StopIteration:
        raise RuntimeError("empty file")

    header = orig_header
    header = [h.lower() for h in [i.replace('"', '') for i in header]]
    if header != TRUTH_CSV_HEADER:
        raise RuntimeError(f"invalid header. orig_header={orig_header!r}, expected header={TRUTH_CSV_HEADER !r}")

    # collect the rows. first we load them all into memory (processing and validating them as we go)
    rows = []  # return value. filled next

    timezero_to_missing_count = defaultdict(int)  # to minimize warnings
    unit_to_missing_count = defaultdict(int)
    target_to_missing_count = defaultdict(int)

    unit_name_to_obj = {unit.name: unit for unit in project.units.all()}
    target_name_to_obj = {target.name: target for target in project.targets.all()}
    timezero_date_to_obj = {}  # caches Project.time_zero_for_timezero_date()
    target_to_cats_values = {}  # caches Target.cats_values()
    range_to_range_tuple = {}  # caches Target.range_tuple()
    for row in csv_reader:
        if len(row) != 4:
            raise RuntimeError("Invalid row (wasn't 4 columns): {!r}".format(row))

        timezero_date, unit_name, target_name, value = row

        # validate and cache timezero_date
        if timezero_date in timezero_date_to_obj:
            time_zero = timezero_date_to_obj[timezero_date]
        else:
            time_zero = project.time_zero_for_timezero_date(datetime.datetime.strptime(
                timezero_date, YYYY_MM_DD_DATE_FORMAT))  # might be None
            timezero_date_to_obj[timezero_date] = time_zero

        if not time_zero:
            timezero_to_missing_count[timezero_date] += 1
            continue

        # validate unit and target
        if unit_name not in unit_name_to_obj:
            unit_to_missing_count[unit_name] += 1
            continue

        if target_name not in target_name_to_obj:
            target_to_missing_count[target_name] += 1
            continue

        # validate `value`. note that at this point value is a str, so we ask
        # Target.is_value_compatible_with_target_type needs to try converting to the correct data type
        target = target_name_to_obj[target_name]
        data_types = target.data_types()  # python types. recall the first is the preferred one
        is_compatible, parsed_value = Target.is_value_compatible_with_target_type(target.type, value, is_coerce=True,
                                                                                  is_convert_na_none=is_convert_na_none)
        if not is_compatible:
            raise RuntimeError(f"value was not compatible with target data type. value={value!r}, "
                               f"data_types={data_types}")

        # validate: For `discrete` and `continuous` targets (if `range` is specified):
        # - The entry in the `value` column for a specific `target`-`unit`-`timezero` combination must be contained
        #   within the `range` of valid values for the target. If `cats` is specified but `range` is not, then there is
        #   an implicit range for the ground truth value, and that is between min(`cats`) and \infty.
        # recall: "The range is assumed to be inclusive on the lower bound and open on the upper bound, # e.g. [a, b)."
        if target in target_to_cats_values:
            cats_values = target_to_cats_values[target]
        else:
            cats_values = target.cats_values()  # datetime.date instances for date targets
            target_to_cats_values[target] = cats_values

        if target in range_to_range_tuple:
            range_tuple = range_to_range_tuple[target]
        else:
            range_tuple = target.range_tuple() or (min(cats_values), float('inf')) if cats_values else None
            range_to_range_tuple[target] = range_tuple

        if (target.type in [Target.DISCRETE_TARGET_TYPE, Target.CONTINUOUS_TARGET_TYPE]) and range_tuple \
                and (parsed_value is not None) and not (range_tuple[0] <= parsed_value < range_tuple[1]):
            raise RuntimeError(f"The entry in the `value` column for a specific `target`-`unit`-`timezero` "
                               f"combination must be contained within the range of valid values for the target. "
                               f"value={parsed_value!r}, range_tuple={range_tuple}")

        # validate: For `nominal` and `date` target_types:
        #  - The entry in the `cat` column for a specific `target`-`unit`-`timezero` combination must be contained
        #    within the set of valid values for the target, as defined by the project config file.
        cats_values = set(cats_values)  # datetime.date instances for date targets
        if (target.type in [Target.NOMINAL_TARGET_TYPE, Target.DATE_TARGET_TYPE]) and cats_values \
                and (parsed_value not in cats_values):
            raise RuntimeError(f"The entry in the `cat` column for a specific `target`-`unit`-`timezero` "
                               f"combination must be contained within the set of valid values for the target. "
                               f"parsed_value={parsed_value}, cats_values={cats_values}")

        # valid

        # value_i = parsed_value if data_types[0] == Target.INTEGER_DATA_TYPE else None
        # value_f = parsed_value if data_types[0] == Target.FLOAT_DATA_TYPE else None
        # value_t = parsed_value if data_types[0] == Target.TEXT_DATA_TYPE else None
        # value_d = parsed_value if data_types[0] == Target.DATE_DATA_TYPE else None
        # value_b = parsed_value if data_types[0] == Target.BOOLEAN_DATA_TYPE else None

        rows.append((time_zero, unit_name_to_obj[unit_name], target, parsed_value))

    # report warnings
    for time_zero, count in timezero_to_missing_count.items():
        logger.warning("_read_truth_data_rows(): timezero not found in project: {}: {} row(s)"
                       .format(time_zero, count))
    for unit_name, count in unit_to_missing_count.items():
        logger.warning("_read_truth_data_rows(): Unit not found in project: {!r}: {} row(s)"
                       .format(unit_name, count))
    for target_name, count in target_to_missing_count.items():
        logger.warning("_read_truth_data_rows(): Target not found in project: {!r}: {} row(s)"
                       .format(target_name, count))

    # done
    return rows
