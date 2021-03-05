import csv
import datetime
import io
import json
import logging
import math
from collections import defaultdict

from django.db import connection, transaction
from django.db.models import Count
from django.shortcuts import get_object_or_404

from forecast_app.models import Forecast, Target, ForecastMetaPrediction, ForecastMetaUnit, ForecastMetaTarget, \
    ForecastModel, PredictionElement, PredictionData
from forecast_app.models.prediction_element import PRED_CLASS_NAME_TO_INT, PRED_CLASS_INT_TO_NAME
from utils.project import _target_dict_for_target
from utils.project_truth import POSTGRES_NULL_VALUE
from utils.utilities import YYYY_MM_DD_DATE_FORMAT, batched_rows


logger = logging.getLogger(__name__)


#
# json_io_dict_from_forecast
#

def json_io_dict_from_forecast(forecast, request):
    """
    Returns a "JSON IO dict" for exporting json a forecast from the database in the format that
    oad_predictions_from_json_io_dict() accepts. Does include the 'meta' section in the returned dict if `request` is
    not None.

    :param forecast: a Forecast whose predictions are to be outputted
    :param request: used for TargetSerializer's 'id' field. pass None to skip creating the 'meta' section
    :return a "JSON IO dict" (aka 'json_io_dict' by callers) that contains forecast's predictions. sorted by unit
        and target for visibility. see docs for details
    """
    from forecast_app.serializers import UnitSerializer, ForecastSerializer  # avoid circular imports


    # set prediction_dicts
    pred_data_qs = PredictionData.objects \
        .filter(pred_ele__forecast=forecast) \
        .values_list('pred_ele__pred_class', 'pred_ele__unit__name', 'pred_ele__target__name', 'data')
    prediction_dicts = [{'unit': unit_name,
                         'target': target_name,
                         'class': PRED_CLASS_INT_TO_NAME[pred_class],
                         'prediction': pred_data} for pred_class, unit_name, target_name, pred_data in pred_data_qs]

    # set meta
    meta = {}
    if request:
        unit_serializer_multi = UnitSerializer(forecast.forecast_model.project.units, many=True,
                                               context={'request': request})
        forecast_serializer = ForecastSerializer(forecast, context={'request': request})
        meta['forecast'] = forecast_serializer.data
        meta['units'] = sorted([dict(_) for _ in unit_serializer_multi.data],  # replace OrderedDicts
                               key=lambda _: (_['name']))
        meta['targets'] = sorted(
            [_target_dict_for_target(target, request) for target in forecast.forecast_model.project.targets.all()],
            key=lambda _: (_['name']))

    # done
    return {'meta': meta, 'predictions': sorted(prediction_dicts, key=lambda _: (_['unit'], _['target']))}


#
# load_predictions_from_json_io_dict()
#

BIN_SUM_REL_TOL = 0.001  # hard-coded magic number for prediction probability sums


@transaction.atomic
def load_predictions_from_json_io_dict(forecast, json_io_dict, is_skip_validation=False, is_validate_cats=True):
    """
    Top-level function that loads the prediction data into forecast from json_io_dict. Validates the forecast data. Note
    that we ignore the 'meta' portion of json_io_dict. Errors if any referenced Units and Targets do not exist in
    forecast's Project. Requires that `forecast` is empty of data.

    :param forecast: a Forecast to load json_io_dict's predictions into
    :param json_io_dict: a "JSON IO dict" to load from. see docs for details
    :param is_skip_validation: bypasses all validation of `json_io_dict`, including `is_validate_cats`. used for truth
        loading
    :param is_validate_cats: True if bin cat values should be validated against their Target.cats. used for testing
    """
    if forecast.pred_eles.count() != 0:
        raise RuntimeError(f"forecast already has data: {forecast}")
    elif not isinstance(json_io_dict, dict):
        raise RuntimeError(f"json_io_dict was not a dict: {json_io_dict!r}, type={type(json_io_dict)}")
    elif 'predictions' not in json_io_dict:
        raise RuntimeError(f"json_io_dict had no 'predictions' key: {json_io_dict}")

    # validate this forecast version rule: "An uploaded forecast version's issue_date cannot be prior to any non-empty
    # versions." NB: additional version validation is done by _insert_pred_ele_rows() b/c it creates a temp table of the
    # incoming forecast's prediction elements

    # get max issue_date of the existing non-empty forecasts:
    newest_non_empty_version = _newest_non_empty_version(forecast.forecast_model, forecast.time_zero)
    if newest_non_empty_version and (forecast.issue_date < newest_non_empty_version.issue_date):
        raise RuntimeError(f"invalid forecast: found an earlier non-empty version. forecast={forecast}, "
                           f"earlier_version={newest_non_empty_version}")

    # we have two types of tables to insert into (PredictionElement and PredictionData subclasses), which requires
    # loading via two passes:
    # 1) iterate over incoming prediction dicts, validating them and generating rows to insert into the
    #    PredictionElement table
    # 2) load those just-inserted rows from the database so we can get their PRIMARY KEY (autoincrement) ids, and then
    #    re-iterate over prediction dict data (cached in memory) to generate rows to insert into class-specific
    #    PredictionData subclass tables
    # pass 1/2
    data_hash_to_pred_data, pred_ele_rows = \
        _validated_pred_ele_rows_for_pred_dicts(forecast, json_io_dict['predictions'], is_skip_validation,
                                                is_validate_cats)
    del json_io_dict  # hopefully frees up memory
    _insert_pred_ele_rows(forecast, pred_ele_rows)  # tests version rule then inserts

    # pass 2/2. target_id is needed to look up target datatype to decide which sparse column to use for each singleton
    # value
    pred_data_rows = []  # appended-to next
    pred_ele_qs = PredictionElement.objects \
        .filter(forecast=forecast, is_retract=False) \
        .values_list('id', 'data_hash')
    for pred_ele_id, data_hash in pred_ele_qs.iterator():
        prediction_data = data_hash_to_pred_data[data_hash]
        pred_data_rows.append((pred_ele_id, prediction_data))
    if pred_data_rows:
        _insert_pred_data_rows(pred_data_rows)  # pred_ele_id, prediction_data


def _validated_pred_ele_rows_for_pred_dicts(forecast, prediction_dicts, is_skip_validation, is_validate_cats):
    """
    Validates prediction_dicts and returns a list of rows suitable for bulk-loading into the PredictionElement table.

    :param forecast: a Forecast that's used to validate against
    :param prediction_dicts: the 'predictions' portion of a "JSON IO dict" as returned by
        json_io_dict_from_cdc_csv_file()
    :param is_skip_validation: same as load_predictions_from_json_io_dict()
    :param is_validate_cats: ""
    :return: a 2-tuple: (data_hash_to_pred_data, pred_ele_rows):
        data_hash_to_pred_data: a dict that maps data_hash -> prediction_data. does not include if is_retract (None)
        pred_ele_rows: a list of 6-tuples: (forecast_id, pred_class_int, unit_id, target_id, is_retract, data_hash)
    """
    unit_name_to_obj = {unit.name: unit for unit in forecast.forecast_model.project.units.all()}
    target_name_to_obj = {target.name: target for target in forecast.forecast_model.project.targets.all()}

    # this variable helps to do "prediction"-level validations at the end of this function. it maps 2-tuples to a list
    # of prediction classes (strs):
    loc_targ_to_pred_classes = defaultdict(list)  # (unit_name, target_name) -> [prediction_class1, ...]

    data_hash_to_pred_data = {}  # return value. filled next
    pred_ele_rows = []  # ""
    for prediction_dict in prediction_dicts:
        unit_name = prediction_dict['unit']
        target_name = prediction_dict['target']
        pred_class = prediction_dict['class']
        prediction_data = prediction_dict['prediction']  # None if a "retracted" prediction -> insert a single NULL row
        is_retract = prediction_data is None
        loc_targ_to_pred_classes[(unit_name, target_name)].append(pred_class)
        if not is_skip_validation:
            # validate prediction class, and unit and target names (applies to all prediction classes)
            if unit_name not in unit_name_to_obj:
                raise RuntimeError(f"prediction_dict referred to an undefined Unit. unit_name={unit_name!r}. "
                                   f"existing_unit_names={unit_name_to_obj.keys()}")
            elif target_name not in target_name_to_obj:
                raise RuntimeError(f"prediction_dict referred to an undefined Target. target_name={target_name!r}. "
                                   f"existing_target_names={target_name_to_obj.keys()}")

            if pred_class not in PRED_CLASS_NAME_TO_INT:
                raise RuntimeError(f"invalid pred_class: {pred_class!r}. must be one of: "
                                   f"{list(PRED_CLASS_INT_TO_NAME.values())}. "
                                   f"prediction_dict={prediction_dict}")

            # do class-specific validation
            target = target_name_to_obj[target_name]
            if (pred_class == PRED_CLASS_INT_TO_NAME[PredictionElement.BIN_CLASS]) \
                    and not is_retract:
                _validate_bin_prediction_dict(is_validate_cats, prediction_dict, target)  # raises o/w
            elif (pred_class == PRED_CLASS_INT_TO_NAME[PredictionElement.NAMED_CLASS]) \
                    and not is_retract:
                family_abbrev = prediction_data['family']
                _validate_named_prediction_dict(family_abbrev, prediction_dict, target)  # raises o/w
            elif (pred_class == PRED_CLASS_INT_TO_NAME[PredictionElement.POINT_CLASS]) \
                    and not is_retract:
                _validate_point_prediction_dict(prediction_dict, target, prediction_data['value'])  # raises o/w
            elif (pred_class == PRED_CLASS_INT_TO_NAME[PredictionElement.SAMPLE_CLASS]) \
                    and not is_retract:
                _validate_sample_prediction_dict(prediction_dict, target)  # raises o/w
            elif not is_retract:  # pred_class == PRED_CLASS_INT_TO_NAME[PredictionElement.QUANTILE_CLASS]:
                _validate_quantile_prediction_dict(prediction_dict, target)  # raises o/w

        # valid, so update data_hash_to_pred_data and append the row
        data_hash = PredictionElement.hash_for_prediction_data_dict(prediction_data)
        if not is_retract:
            data_hash_to_pred_data[data_hash] = prediction_data
        pred_ele_rows.append((forecast.pk, PRED_CLASS_NAME_TO_INT[pred_class],
                              unit_name_to_obj[unit_name].pk, target_name_to_obj[target_name].pk,
                              is_retract, data_hash))

    # finally, do "prediction"-level validation. recall that "prediction" is defined as "a group of a prediction
    # elements(s) specific to a unit and target"
    if not is_skip_validation:
        # validate: "Within a Prediction, there cannot be more than 1 Prediction Element of the same type".
        duplicate_unit_target_tuples = [(unit, target, pred_classes) for (unit, target), pred_classes
                                        in loc_targ_to_pred_classes.items()
                                        if len(pred_classes) != len(set(pred_classes))]
        if duplicate_unit_target_tuples:
            raise RuntimeError(
                f"Within a Prediction, there cannot be more than 1 Prediction Element of the same class. "
                f"Found these duplicate unit/target tuples: {duplicate_unit_target_tuples}")

        # validate: (for both continuous and discrete target types): Within one prediction, there can be at most one of
        # the following prediction elements, but not both: {`Named`, `Bin`}.
        named_bin_conflict_tuples = [(unit, target, pred_classes) for (unit, target), pred_classes
                                     in loc_targ_to_pred_classes.items()
                                     if (PRED_CLASS_INT_TO_NAME[
                                             PredictionElement.BIN_CLASS] in pred_classes)
                                     and (PRED_CLASS_INT_TO_NAME[
                                              PredictionElement.NAMED_CLASS] in pred_classes)]
        if named_bin_conflict_tuples:
            raise RuntimeError(f"Within one prediction, there can be at most one of the following prediction elements, "
                               f"but not both: `Named`, `Bin`. Found these conflicting unit/target tuples: "
                               f"{named_bin_conflict_tuples}")

    # done!
    return data_hash_to_pred_data, pred_ele_rows


def _insert_pred_ele_rows(forecast, pred_ele_rows):
    """
    Validates forecast against prev_version and next_version, then loads pred_ele_rows into the PredictionElement table.
    Skips duplicate prediction elements in `forecast`'s model. See note in _insert_pred_data_rows() re: postgres vs.
    sqlite.

    :param forecast: the new, empty Forecast being inserted into
    :param pred_ele_rows: as returned by _validated_pred_ele_rows_for_pred_dicts():
        list of 6-tuples: (forecast_id, pred_class_int, unit_id, target_id, is_retract, data_hash)
    :raises RuntimeError: if forecast version is invalid
    """
    # in order to skip inserting duplicate rows, we insert in these steps:
    # 1) create a temp table
    # 2) insert `pred_ele_rows` into the temp table (some might be duplicates)
    # 3) validate forecast against prev_version and next_version
    # 4) delete duplicates from the temp table
    # 5) insert the temp table into PredictionElement
    # 6) drop the temp table
    temp_table_name = 'pred_ele_temp'
    pred_ele_table_name = PredictionElement._meta.db_table

    # step 1/6: create temp table
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name};")

    sql = f"""
        CREATE TEMP TABLE {temp_table_name} AS
        SELECT pred_ele.forecast_id,
               pred_ele.pred_class,
               pred_ele.unit_id,
               pred_ele.target_id,
               pred_ele.is_retract,
               pred_ele.data_hash
        FROM {pred_ele_table_name} AS pred_ele
        LIMIT 0;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)

    # step 2/6: insert rows into temp table
    columns_names = [PredictionElement._meta.get_field('forecast').column,
                     PredictionElement._meta.get_field('pred_class').column,
                     PredictionElement._meta.get_field('unit').column,
                     PredictionElement._meta.get_field('target').column,
                     PredictionElement._meta.get_field('is_retract').column,
                     PredictionElement._meta.get_field('data_hash').column]
    with connection.cursor() as cursor:
        if connection.vendor == 'postgresql':
            string_io = io.StringIO()
            csv_writer = csv.writer(string_io, delimiter=',')
            for row in pred_ele_rows:
                csv_writer.writerow(row)
            string_io.seek(0)
            cursor.copy_from(string_io, temp_table_name, columns=columns_names, sep=',', null=POSTGRES_NULL_VALUE)
        else:  # 'sqlite', etc.
            column_names = (', '.join(columns_names))
            values_percent_s = ', '.join(['%s'] * len(columns_names))
            sql = f"""
                    INSERT INTO {temp_table_name} ({column_names})
                    VALUES ({values_percent_s});
                    """
            cursor.executemany(sql, pred_ele_rows)

    # step 3/6: validate forecast against prev_version and next_version
    _validate_forecast_version(forecast, temp_table_name)  # raises RuntimeError if invalid

    # step 4/6: delete duplicates from temp table
    sql = f"""
        DELETE
        FROM {temp_table_name}
        WHERE EXISTS(SELECT *
                     FROM {pred_ele_table_name} AS pred_ele
                              JOIN {Forecast._meta.db_table} AS f ON pred_ele.forecast_id = f.id
                     WHERE f.forecast_model_id = %s
                       AND f.time_zero_id = %s
                       AND {temp_table_name}.pred_class = pred_ele.pred_class
                       AND {temp_table_name}.unit_id = pred_ele.unit_id
                       AND {temp_table_name}.target_id = pred_ele.target_id
                       AND {temp_table_name}.is_retract = pred_ele.is_retract
                       AND {temp_table_name}.data_hash = pred_ele.data_hash);
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (forecast.forecast_model.pk, forecast.time_zero.pk))

    # step 5/6: insert temp table into PredictionElement
    sql = f"""
        INSERT INTO {pred_ele_table_name} AS pred_ele (forecast_id, pred_class, unit_id, target_id,
                                                       is_retract, data_hash)
        SELECT %s, pred_class, unit_id, target_id, is_retract, data_hash
        FROM {temp_table_name};
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (forecast.pk,))

    # step 6/6: drop temp table
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name};")


def _validate_forecast_version(forecast, temp_table_name):
    """
    Validates forecast against any existing versions of it. Currently the only rule is:

        "An uploaded forecast version cannot imply any retracted prediction elements in existing versions."

    :param forecast: new forecast to validate
    :param temp_table_name: contains `forecast`'s candidate prediction elements
    :raises RuntimeError: if forecast version is invalid
    """
    prev_version, next_version = _prev_next_forecast_versions(forecast)  # either or both can be None
    if prev_version and _is_pred_eles_subset(temp_table_name, prev_version, True):
        raise RuntimeError(f"invalid forecast. forecast is a subset of previous version. forecast={forecast}, "
                           f"prev_version={prev_version}")
    elif next_version and _is_pred_eles_subset(temp_table_name, next_version, False):
        raise RuntimeError(f"invalid forecast. next version is a subset of forecast. forecast={forecast}, "
                           f"next_version={next_version}")


def _prev_next_forecast_versions(forecast):
    """
    :param forecast: a Forecast
    :return: a 2-tuple: (previous_version, next_version) where:
    - previous_version: the forecast version immediately earlier than `forecast` based on issue_date, or None if
        `forecast` has the oldest issue_date
    - next_version: similar, except next newest forecast

    Recall a forecast version is defined by the 3-tuple: (forecast_model, time_zero, issue_date). Forecast's
    'unique_version' constraint validates uniqueness at the database level before this function can ever be called.
    """
    # NB: this does two queries. alternatively we could get all versions into memory and then figure out prev and next
    versions_qs = Forecast.objects.filter(forecast_model=forecast.forecast_model, time_zero=forecast.time_zero)
    prev_version = versions_qs.filter(issue_date__lt=forecast.issue_date).order_by('-issue_date').first()
    next_version = versions_qs.filter(issue_date__gt=forecast.issue_date).order_by('issue_date').first()
    return prev_version, next_version


def _newest_non_empty_version(forecast_model, time_zero):
    """
    :param forecast_model: a ForecastModel
    :param time_zero: a TimeZero
    :return: the newest non-empty Forecast for the version indicated by (forecast_model, time_zero), based on
        issue_date, or None if there were no non-empty versions
    """
    sql = f"""
        WITH ranked_issue_dates AS (
            SELECT f.id AS f_id, f.issue_date AS issue_date, RANK() OVER (ORDER BY f.issue_date DESC) AS rank
            FROM forecast_app_forecast AS f
            WHERE f.forecast_model_id = %s
              AND f.time_zero_id = %s
              AND EXISTS(SELECT * FROM forecast_app_predictionelement AS pe WHERE f.id = pe.forecast_id))
        SELECT cte.f_id
        FROM ranked_issue_dates AS cte
        WHERE cte.rank = 1;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (forecast_model.pk, time_zero.pk,))
        f_id_max_issue_date = cursor.fetchone()
        if f_id_max_issue_date is None:
            return None

        f_id_max_issue_date = f_id_max_issue_date[0]
        return Forecast.objects.get(pk=f_id_max_issue_date) if f_id_max_issue_date is not None else None


def _is_pred_eles_subset(temp_table_name, prev_or_next_version, is_previous):
    """
    :param temp_table_name: contains a new forecast's candidate prediction elements
    :param prev_or_next_version: a Forecast to compare to temp_table_name
    :param is_previous: True if prev_or_next_version is prev_version, and False if it is next_version. controls set
        comparison order
    :return: True if temp_table_name's PredictionElements are a subset of those of prev_or_next_version, i.e., if there
        are implicit retractions
    """


    def is_empty_table(table_name_or_forecast):
        # use EXISTS instead of COUNT for performance
        if isinstance(table_name_or_forecast, Forecast):
            sql = f"SELECT EXISTS (SELECT * FROM {PredictionElement._meta.db_table} WHERE forecast_id = %s);"
            args = (table_name_or_forecast.pk,)
        else:
            sql = f"SELECT EXISTS (SELECT * FROM {table_name_or_forecast});"
            args = ()
        with connection.cursor() as cursor:
            cursor.execute(sql, args)
            is_any_rows = cursor.fetchone()[0]
            return not is_any_rows


    # first we test the special case where a table is empty. otherwise, the below queries would always return True,
    # which is meaningless when there's no rows. this case only occurs in two situations: 1) unit tests where we want to
    # set up by creating all Forecasts before loading data into them (specifically `test_implicit_retractions()`) and
    # 2) when migrating old data to new, where the Forecasts already exist and we want to preserve them. adding this
    # special case seemed a better trade-off than requiring users to never have newer Forecasts. this does not come up
    # in normal operation b/c a Forecast is created only for an upload
    if (is_previous and is_empty_table(temp_table_name)) or \
            ((not is_previous) and is_empty_table(prev_or_next_version)):
        return False  # valid (not a subset)

    # set CTE
    if is_previous:  # error if any in prev_or_next_version that are not in temp_table_name
        with_sql = f"""
            WITH except_rows AS (
                SELECT unit_id, target_id, pred_class
                FROM {PredictionElement._meta.db_table}
                WHERE forecast_id = %s
                    EXCEPT
                SELECT unit_id, target_id, pred_class
                FROM {temp_table_name}
            )
        """
    else:  # error if any in temp_table_name that are not in prev_or_next_version
        with_sql = f"""
            WITH except_rows AS (
                SELECT unit_id, target_id, pred_class
                FROM {temp_table_name}
                    EXCEPT
                SELECT unit_id, target_id, pred_class
                FROM {PredictionElement._meta.db_table}
                WHERE forecast_id = %s
            )
        """
    sql = f"""
        {with_sql}
        SELECT EXISTS (SELECT * FROM except_rows);
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (prev_or_next_version.pk,))
        is_subset = cursor.fetchone()[0]
        return is_subset


def _validate_bin_prediction_dict(is_validate_cats, prediction_dict, target):
    prediction_data = prediction_dict['prediction']

    # validate: "The number of elements in the `cat` and `prob` vectors should be identical"
    if len(prediction_data['cat']) != len(prediction_data['prob']):
        raise RuntimeError(f"The number of elements in the 'cat' and 'prob' vectors should be identical. "
                           f"|cat|={len(prediction_data['cat'])}, |prob|={len(prediction_data['prob'])}, "
                           f"prediction_dict={prediction_dict}")

    # validate: "Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `NULL` (case does
    # not matter)"
    cat_lower = [cat.lower() if isinstance(cat, str) else cat for cat in prediction_data['cat']]
    if ('' in cat_lower) or ('na' in cat_lower) or (None in cat_lower):
        raise RuntimeError(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or "
                           f"`NULL`. cat={prediction_data['cat']}, prediction_dict={prediction_dict}")

    # validate: "The data format of `cat` should correspond or be translatable to the `type` as in the target
    # definition"
    is_all_compatible = all([Target.is_value_compatible_with_target_type(target.type, cat)[0]  # is_compatible
                             for cat in prediction_data['cat']])
    if not is_all_compatible:
        raise RuntimeError(f"The data format of `cat` should correspond or be translatable to the `type` as "
                           f"in the target definition, but one of the cat values was not. "
                           f"cat_values={prediction_data['cat']}, prediction_dict={prediction_dict}")

    # validate: "Entries in `cat` must be a subset of `Target.cats` from the target definition".
    # note: for date targets we format as strings for the comparison (incoming are strings)
    cats_values = set(target.cats_values())  # datetime.date instances for date targets
    pred_data_cat_parsed = [datetime.datetime.strptime(cat, YYYY_MM_DD_DATE_FORMAT).date()
                            for cat in prediction_data['cat']] \
        if target.type == Target.DATE_TARGET_TYPE else prediction_data['cat']  # valid - see is_all_compatible above
    if is_validate_cats and not (set(pred_data_cat_parsed) <= cats_values):
        raise RuntimeError(f"Entries in `cat` must be a subset of `Target.cats` from the target definition. "
                           f"cat={prediction_data['cat']}, cats_values={cats_values}, "
                           f"prediction_dict={prediction_dict}")

    # validate: "Entries in the database rows in the `prob` column must be numbers in [0, 1]"
    prob_types_set = set(map(type, prediction_data['prob']))
    if not (prob_types_set <= {int, float}):
        raise RuntimeError(f"wrong data type in `prob` column, which should only contain "
                           f"ints or floats. prob column={prediction_data['prob']}, prob_types_set={prob_types_set}, "
                           f"prediction_dict={prediction_dict}")
    elif (min(prediction_data['prob']) < 0.0) or (max(prediction_data['prob']) > 1.0):
        raise RuntimeError(f"Entries in the database rows in the `prob` column must be numbers in [0, 1]. "
                           f"prob column={prediction_data['prob']}, prediction_dict={prediction_dict}")

    # validate: "For one prediction element, the values within prob must sum to 1.0 (values within +/- 0.001 of
    # 1 are acceptable)"
    prob_sum = sum(prediction_data['prob'])
    if not math.isclose(1.0, prob_sum, rel_tol=BIN_SUM_REL_TOL):
        raise RuntimeError(f"For one prediction element, the values within prob must sum to 1.0. "
                           f"prob_sum={prob_sum}, delta={abs(1 - prob_sum)}, rel_tol={BIN_SUM_REL_TOL}, "
                           f"prediction_dict={prediction_dict}")

    # validate: "for `Bin` Prediction Elements, there must be exactly two `cat` values labeled `true` and `false`. These
    # are the two `cats` that are implied (but not allowed to be specified) by binary target types."
    if (target.type == Target.BINARY_TARGET_TYPE) and (len(prediction_data['cat']) != 2):
        raise RuntimeError(f"for `Bin` Prediction Elements, there must be exactly two `cat` values labeled `true` and "
                           f"`false`. prediction_data['cat']={prediction_data['cat']}, "
                           f"prediction_dict={prediction_dict}")


def _validate_named_prediction_dict(family_abbrev, prediction_dict, target):
    prediction_data = prediction_dict['prediction']

    # validate: "`family`: must be one of the abbreviations shown in the table below"
    family_abbrevs = NamedData.FAMILY_CHOICES
    if family_abbrev not in family_abbrevs:
        raise RuntimeError(f"family must be one of the abbreviations shown in the table below. "
                           f"family_abbrev={family_abbrev!r}, family_abbrevs={family_abbrevs}, "
                           f"prediction_dict={prediction_dict}")

    # validate: "The Prediction's class must be valid for its target's type". note that only named and quantile
    # predictions are constrained; all other target_type/prediction_class combinations are valid
    if not Target.is_valid_named_family_for_target_type(family_abbrev, target.type):
        raise RuntimeError(f"family {family_abbrev!r} is not valid for {target.type_as_str()!r} "
                           f"target types. prediction_dict={prediction_dict}")

    # validate: "The number of param columns with non-NULL entries count must match family definition"
    num_params = 0
    if 'param1' in prediction_data:
        num_params += 1
    if 'param2' in prediction_data:
        num_params += 1
    if 'param3' in prediction_data:
        num_params += 1
    if num_params != NamedData.PARAM_TO_EXP_COUNT[family_abbrev]:
        raise RuntimeError(f"The number of param columns with non-NULL entries count must match family "
                           f"definition. family_abbrev={family_abbrev!r}, num_params={num_params}, "
                           f"expected count={NamedData.PARAM_TO_EXP_COUNT[family_abbrev]}, "
                           f"prediction_dict={prediction_dict}")
    # validate: Parameters for each distribution must be within valid ranges, which, if constraints exist, are
    # specified in the table below
    ge_0, gt_0, bw_0_1 = '>=0', '>0', '0<=&>=0'
    family_abbrev_to_param1_2_constraint_type = {
        NamedData.NORM_DIST: (None, ge_0),  # | mean | sd>=0 | - |
        NamedData.LNORM_DIST: (None, ge_0),  # | mean | sd>=0 | - |
        NamedData.GAMMA_DIST: (gt_0, gt_0),  # | shape>0 |rate>0 | - |
        NamedData.BETA_DIST: (gt_0, gt_0),  # | a>0 | b>0 | - |
        NamedData.POIS_DIST: (gt_0, None),  # | rate>0 |  - | - |
        NamedData.NBINOM_DIST: (gt_0, bw_0_1),  # | r>0 | 0<=p<=1 | - |
        NamedData.NBINOM2_DIST: (gt_0, gt_0)  # | mean>0 | disp>0 | - |
    }
    p1_constr, p2_constr = family_abbrev_to_param1_2_constraint_type[family_abbrev]
    if ((p1_constr == gt_0) and not (prediction_data['param1'] > 0)) or \
            ((p2_constr == ge_0) and not (prediction_data['param2'] >= 0)) or \
            ((p2_constr == gt_0) and not (prediction_data['param2'] > 0)) or \
            ((p2_constr == bw_0_1) and not (0 <= prediction_data['param2'] <= 1)):
        raise RuntimeError(f"Parameters for each distribution must be within valid ranges: "
                           f"prediction_dict={prediction_dict}")


def _validate_point_prediction_dict(prediction_dict, target, value):
    prediction_data = prediction_dict['prediction']

    # validate: "Entries in the database rows in the `value` column cannot be `“”`, `“NA”` or `NULL` (case does
    # not matter)"
    value_lower = value.lower() if isinstance(value, str) else value
    if (value_lower == '') or (value_lower == 'na') or (value_lower is None):
        raise RuntimeError(f"Entries in the database rows in the `value` column cannot be `“”`, `“NA”` or "
                           f"`NULL`. cat={prediction_data['value']}, prediction_dict={prediction_dict}")

    # validate: "The data format of `value` should correspond or be translatable to the `type` as in the target
    # definition". note: for date targets we format as strings for the comparison (incoming are strings)
    if not Target.is_value_compatible_with_target_type(target.type, value)[0]:  # is_compatible
        raise RuntimeError(f"The data format of `value` should correspond or be translatable to the `type` as "
                           f"in the target definition. value={value!r}, prediction_dict={prediction_dict}")

    # validate: "if `range` is specified, any values in `Point` or `Sample` Prediction Elements should be contained
    # within `range`". recall: "The range is assumed to be inclusive on the lower bound and open on the upper bound,
    # e.g. [a, b)."
    range_tuple = target.range_tuple()
    if range_tuple and not (range_tuple[0] <= value < range_tuple[1]):
        raise RuntimeError(f"if `range` is specified, any values in `Point` Prediction Elements should be contained "
                           f"within `range`. value={value!r}, range_tuple={range_tuple}, "
                           f"prediction_dict={prediction_dict}")


def _validate_sample_prediction_dict(prediction_dict, target):
    prediction_data = prediction_dict['prediction']

    # validate: "Entries in the database rows in the `sample` column cannot be `“”`, `“NA”` or `NULL` (case does
    # not matter)"
    sample_lower = [sample.lower() if isinstance(sample, str) else sample
                    for sample in prediction_data['sample']]
    if ('' in sample_lower) or ('na' in sample_lower) or (None in sample_lower):
        raise RuntimeError(f"Entries in the database rows in the `sample` column cannot be `“”`, `“NA”` or "
                           f"`NULL`. cat={prediction_data['sample']}, prediction_dict={prediction_dict}")

    # validate: "The data format of `sample` should correspond or be translatable to the `type` as in the
    # target definition"
    is_all_compatible = all([Target.is_value_compatible_with_target_type(target.type, sample)[0]  # is_compatible
                             for sample in prediction_data['sample']])
    if not is_all_compatible:
        raise RuntimeError(f"The data format of `sample` should correspond or be translatable to the `type` as "
                           f"in the target definition, but one of the sample values was not. "
                           f"sample_values={prediction_data['sample']}, prediction_dict={prediction_dict}")

    # validate: "if `range` is specified, any values in `Point` or `Sample` Prediction Elements should be contained
    # within `range`". recall: "The range is assumed to be inclusive on the lower bound and open on the upper bound,
    # e.g. [a, b)."
    range_tuple = target.range_tuple()
    if range_tuple:
        is_all_in_range = all([range_tuple[0] <= sample < range_tuple[1] for sample in prediction_data['sample']])
        if not is_all_in_range:
            raise RuntimeError(f"if `range` is specified, any values in `Sample` Prediction Elements should be "
                               f"contained within `range`. range_tuple={range_tuple}, "
                               f"sample={prediction_data['sample']}, prediction_dict={prediction_dict}")


def _le_with_tolerance(a, b):  # a <= b ?
    # `_validate_quantile_prediction_dict()` helper
    if type(a) in {int, float}:
        return True if math.isclose(a, b, rel_tol=1e-05) else a <= b  # default: rel_tol=1e-09
    else:  # date
        return a <= b


def _validate_quantile_prediction_dict(prediction_dict, target):
    prediction_data = prediction_dict['prediction']

    # validate: "The Prediction's class must be valid for its target's type". note that only named and quantile
    # predictions are constrained; all other target_type/prediction_class combinations are valid
    if (target.type == Target.NOMINAL_TARGET_TYPE) or (target.type == Target.BINARY_TARGET_TYPE):
        raise RuntimeError(f"quantile data is not valid for target type={target.type}. "
                           f"prediction_dict={prediction_dict}")

    # validate: "The number of elements in the `quantile` and `value` vectors should be identical."
    pred_data_quantiles = prediction_data['quantile']
    pred_data_values = prediction_data['value']
    if len(pred_data_quantiles) != len(pred_data_values):
        raise RuntimeError(f"The number of elements in the `quantile` and `value` vectors should be identical. "
                           f"|quantile|={len(pred_data_quantiles)}, |value|={len(pred_data_values)}, "
                           f"prediction_dict={prediction_dict}")

    # validate: "Entries in the database rows in the `quantile` column must be numbers in [0, 1].
    quantile_types_set = set(map(type, pred_data_quantiles))
    if not (quantile_types_set <= {int, float}):
        raise RuntimeError(f"wrong data type in `quantile` column, which should only contain ints or floats. "
                           f"quantile column={pred_data_quantiles}, quantile_types_set={quantile_types_set}, "
                           f"prediction_dict={prediction_dict}")
    elif (min(pred_data_quantiles) < 0.0) or (max(pred_data_quantiles) > 1.0):
        raise RuntimeError(f"Entries in the database rows in the `quantile` column must be numbers in [0, 1]. "
                           f"quantile column={pred_data_quantiles}, prediction_dict={prediction_dict}")

    # validate: `quantile`s must be unique."
    if len(set(pred_data_quantiles)) != len(pred_data_quantiles):
        raise RuntimeError(f"`quantile`s must be unique. quantile column={pred_data_quantiles}, "
                           f"prediction_dict={prediction_dict}")

    # validate: "The data format of `value` should correspond or be translatable to the `type` as in the target
    # definition."
    is_all_compatible = all([Target.is_value_compatible_with_target_type(target.type, value)[0]  # is_compatible
                             for value in pred_data_values])
    if not is_all_compatible:
        raise RuntimeError(f"The data format of `value` should correspond or be translatable to the `type` as "
                           f"in the target definition, but one of the value values was not. "
                           f"values={pred_data_values}, prediction_dict={prediction_dict}")

    # validate: "Entries in `value` must be non-decreasing as quantiles increase." (i.e., are monotonic).
    # note: for date targets we format as strings for the comparison (incoming are strings).
    # note: we do not assume quantiles are sorted, so we first sort before checking for non-decreasing
    pred_data_values = [datetime.datetime.strptime(value, YYYY_MM_DD_DATE_FORMAT).date()
                        for value in pred_data_values] \
        if target.type == Target.DATE_TARGET_TYPE else pred_data_values  # valid - see is_all_compatible above

    # per https://stackoverflow.com/questions/7558908/unpacking-a-list-tuple-of-pairs-into-two-lists-tuples
    pred_data_quantiles, pred_data_values = zip(*sorted(zip(pred_data_quantiles, pred_data_values), key=lambda _: _[0]))

    is_le_values = [_le_with_tolerance(a, b) for a, b in zip(pred_data_values, pred_data_values[1:])]
    if not all(is_le_values):
        raise RuntimeError(f"Entries in `value` must be non-decreasing as quantiles increase. "
                           f"value column={pred_data_values}, is_le_values={is_le_values}, "
                           f"prediction_dict={prediction_dict}")

    # validate: "Entries in `value` must obey existing ranges for targets." recall: "The range is assumed to be
    # inclusive on the lower bound and open on the upper bound, # e.g. [a, b)."
    range_tuple = target.range_tuple()
    if range_tuple:
        is_all_in_range = all([range_tuple[0] <= value < range_tuple[1] for value in pred_data_values])
        if not is_all_in_range:
            raise RuntimeError(f"Entries in `value` must obey existing ranges for targets. range_tuple={range_tuple}, "
                               f"pred_data_values={pred_data_values}, prediction_dict={prediction_dict}")


def _insert_pred_data_rows(rows):
    """
    Does the actual INSERT of rows into the database table corresponding to pred_data_class. For speed, we directly
    insert via SQL rather than the ORM. We use psycopg2 extensions to the DB API if we're connected to a Postgres
    server. Otherwise we use execute_many() as a fallback. The reason we don't simply use the latter for Postgres
    is because its implementation is slow ( http://initd.org/psycopg/docs/extras.html#fast-execution-helpers ).

    :param rows: list of 2-tuples: (pred_ele_id, prediction_data), where pred_ele_id is a PredictionElement.pk, and
        prediction_data is the "raw" prediction_data dict, i.e., the prediction_element dict's "prediction" dict.
    """
    # serialize to json. NB: assumes no CR or LFs in dicts!
    rows = [(idx, json.dumps(pred_data)) for idx, pred_data in rows]
    table_name = PredictionData._meta.db_table
    columns_names = PredictionData._meta.get_field('pred_ele').column, PredictionData._meta.get_field('data').column
    with connection.cursor() as cursor:
        if connection.vendor == 'postgresql':
            # bulk insert via COPY FROM. to avoid possible problems with CSV quoting and delimiters, we follow this
            # advice: http://adpgtech.blogspot.com/2014/09/importing-json-data.html :
            #   There is a small set of single-byte characters that happen to be illegal in JSON: e'\x01' and e'\x02'
            string_io = io.StringIO()
            csv_writer = csv.writer(string_io, quotechar=chr(1), delimiter=chr(2))
            csv_writer.writerows(rows)
            string_io.seek(0)
            sql = f"""
                COPY {table_name}({', '.join(columns_names)}) FROM STDIN WITH CSV QUOTE e'\x01' DELIMITER e'\x02';
            """
            cursor.copy_expert(sql, string_io)
        else:  # 'sqlite', etc.
            column_names = (', '.join(columns_names))
            values_percent_s = ', '.join(['%s'] * len(columns_names))
            sql = f"""
                    INSERT INTO {table_name} ({column_names})
                    VALUES ({values_percent_s});
                    """
            cursor.executemany(sql, rows)


#
# data_rows_from_forecast()
#

def data_rows_from_forecast(forecast, unit, target):
    """
    Returns rows for each concrete prediction type that are suitable for tabular display.

    :param forecast: a Forecast to constrain to
    :param unit: a Unit ""
    :param target: a Target ""
    :return: 5-tuple: (data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample) where
        data_rows_bin:      unit_name, target_name,  cat, prob
        data_rows_named:    unit_name, target_name,  family, param1, param2, param3
        data_rows_point:    unit_name, target_name,  value
        data_rows_quantile: unit_name, target_name,  quantile, value
        data_rows_sample:   unit_name, target_name,  sample
    """
    data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample = \
        [], [], [], [], []  # return value. filled next
    pred_data_qs = PredictionData.objects.filter(pred_ele__forecast=forecast, pred_ele__is_retract=False,
                                                 pred_ele__unit=unit, pred_ele__target=target) \
        .values_list('pred_ele__pred_class', 'data')
    for pred_class, pred_data in pred_data_qs:
        if pred_class == PredictionElement.BIN_CLASS:
            for cat, prob in zip(pred_data['cat'], pred_data['prob']):
                data_rows_bin.append((unit.name, target.name, cat, prob))
        elif pred_class == PredictionElement.NAMED_CLASS:
            data_rows_named.append((unit.name, target.name, pred_data['family'],
                                    pred_data.get('param1'), pred_data.get('param2'), pred_data.get('param3')))
        elif pred_class == PredictionElement.POINT_CLASS:
            data_rows_point.append((unit.name, target.name, pred_data['value']))
        elif pred_class == PredictionElement.QUANTILE_CLASS:
            for quantile, value in zip(pred_data['quantile'], pred_data['value']):
                data_rows_quantile.append((unit.name, target.name, quantile, value))
        elif pred_class == PredictionElement.SAMPLE_CLASS:
            for sample in pred_data['sample']:
                data_rows_sample.append((unit.name, target.name, sample))

    # done
    return data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample


#
# cache_forecast_metadata()
#

@transaction.atomic
def cache_forecast_metadata(forecast):
    """
    Top-level function that caches metadata information for forecast. Clears existing first.

    :param forecast: a Forecast whose metata is to be cached
    """
    _cache_forecast_metadata_predictions(forecast)
    _cache_forecast_metadata_units(forecast)
    _cache_forecast_metadata_targets(forecast)


def _cache_forecast_metadata_predictions(forecast):
    clear_forecast_metadata(forecast)

    # cache one ForecastMetaPrediction row for forecast. annotate() is a GROUP BY -> QuerySet of dicts like:
    # {'pred_class': 1, 'total': 2}
    rows = PredictionElement.objects.filter(forecast=forecast).values('pred_class').annotate(total=Count('id'))
    pred_class_to_counts = defaultdict(int)
    for pred_class_total_dict in rows:
        pred_class_to_counts[pred_class_total_dict['pred_class']] = pred_class_total_dict['total']
    ForecastMetaPrediction.objects.create(forecast=forecast,
                                          bin_count=pred_class_to_counts[PredictionElement.BIN_CLASS],
                                          named_count=pred_class_to_counts[PredictionElement.NAMED_CLASS],
                                          point_count=pred_class_to_counts[PredictionElement.POINT_CLASS],
                                          sample_count=pred_class_to_counts[PredictionElement.SAMPLE_CLASS],
                                          quantile_count=pred_class_to_counts[PredictionElement.QUANTILE_CLASS])


def _cache_forecast_metadata_units(forecast):
    # cache ForecastMetaUnit rows for forecast
    unit_id_to_obj = {unit.id: unit for unit in forecast.forecast_model.project.units.all()}
    pred_class_units_qs = PredictionElement.objects \
        .filter(forecast=forecast) \
        .values_list('unit', flat=True) \
        .distinct()
    found_units = [unit_id_to_obj[unit_id] for unit_id in pred_class_units_qs]
    for unit in found_units:
        ForecastMetaUnit.objects.create(forecast=forecast, unit=unit)


def _cache_forecast_metadata_targets(forecast):
    # cache ForecastMetaTarget rows for forecast
    target_id_to_object = {target.id: target for target in forecast.forecast_model.project.targets.all()}
    pred_class_targets_qs = PredictionElement.objects \
        .filter(forecast=forecast) \
        .values_list('target', flat=True) \
        .distinct()
    found_targets = [target_id_to_object[target_id] for target_id in pred_class_targets_qs]
    for target in found_targets:
        ForecastMetaTarget.objects.create(forecast=forecast, target=target)


def clear_forecast_metadata(forecast):
    """
    Top-level function that clears all metadata information for forecast.

    :param forecast: a Forecast whose metadata is to be cached
    """
    ForecastMetaPrediction.objects.filter(forecast=forecast).delete()
    ForecastMetaUnit.objects.filter(forecast=forecast).delete()
    ForecastMetaTarget.objects.filter(forecast=forecast).delete()


def _cache_forecast_metadata_worker(forecast_pk):
    """
    enqueue() helper function
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    try:
        logger.debug(f"_cache_forecast_metadata_worker(): 1/2 starting: forecast_pk={forecast_pk}")
        cache_forecast_metadata(forecast)
        logger.debug(f"_cache_forecast_metadata_worker(): 2/2 done: forecast_pk={forecast_pk}")
    except Exception as ex:
        logger.error(f"_cache_forecast_metadata_worker(): error: {ex!r}. forecast={forecast}")


#
# forecast_metadata()
#

def forecast_metadata(forecast):
    """
    Returns all metadata associated with Forecast.

    :param forecast: a Forecast
    :return: a 3-tuple: (forecast_meta_prediction, forecast_meta_unit_qs, forecast_meta_target_qs) where the latter two
        are QuerySets. The first is None if there is no cached data. The second two are empty QuerySets if no cached
        data.
    """
    forecast_meta_prediction = ForecastMetaPrediction.objects.filter(forecast=forecast).first()
    forecast_meta_unit_qs = ForecastMetaUnit.objects.filter(forecast=forecast)
    forecast_meta_target_qs = ForecastMetaTarget.objects.filter(forecast=forecast)
    return forecast_meta_prediction, forecast_meta_unit_qs, forecast_meta_target_qs


def is_forecast_metadata_available(forecast):
    """
    :param forecast: a Forecast
    :return: True if `forecast` has a ForecastMetaPrediction, and False o/w. we only check it instead of all three
        (ForecastMetaPrediction, ForecastMetaUnit, and ForecastMetaTarget) for efficiency
    """
    return ForecastMetaPrediction.objects.filter(forecast=forecast).exists()


def forecast_metadata_counts_for_project(project):
    """
    :param project: a Project
    :return: dict with metadata count information for all forecasts in `project`. the dict maps:
        forecast_id -> the 3-tuple (prediction_counts, unit_count, target_count) where:
        - prediction_counts: (point_count, named_count, bin_count, sample_count, quantile_count) - a 5-tuple
        - unit_count:        num_units
        - target_count:      num_targets
    """
    forecast_id_to_counts = defaultdict(lambda: [None, None, None])  # return value. filled next

    # query 1/2: get ForecastMetaPrediction counts
    sql = f"""
        SELECT fmp.forecast_id AS forecast_id, fmp.point_count, fmp.named_count, fmp.bin_count, fmp.sample_count, fmp.quantile_count
        FROM {ForecastMetaPrediction._meta.db_table} AS fmp
                 JOIN {Forecast._meta.db_table} AS f ON fmp.forecast_id = f.id
                 JOIN {ForecastModel._meta.db_table} AS fm ON f.forecast_model_id = fm.id
        WHERE fm.project_id = %s;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        for forecast_id, point_count, named_count, bin_count, sample_count, quantile_count in batched_rows(cursor):
            forecast_id_to_counts[forecast_id][0] = (point_count, named_count, bin_count, sample_count, quantile_count)

    # query 2/2: get ForecastMetaUnit and ForecastMetaTarget counts
    sql = f"""
        SELECT fmt.forecast_id AS forecast_id, count(*) AS num_targets, 1 AS is_target_count
        FROM {ForecastMetaTarget._meta.db_table} AS fmt
                 JOIN {Forecast._meta.db_table} AS f ON fmt.forecast_id = f.id
                 JOIN {ForecastModel._meta.db_table} AS fm ON f.forecast_model_id = fm.id
        WHERE fm.project_id = %s
        GROUP BY fmt.forecast_id, fm.project_id
        
        UNION
        
        SELECT fmu.forecast_id AS forecast_id, count(*) AS num_units, 0 AS is_target_count
        FROM {ForecastMetaUnit._meta.db_table} AS fmu
                 JOIN {Forecast._meta.db_table} AS f ON fmu.forecast_id = f.id
                 JOIN {ForecastModel._meta.db_table} AS fm ON f.forecast_model_id = fm.id
        WHERE fm.project_id = %s
        GROUP BY fmu.forecast_id, fm.project_id;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk, project.pk))
        for forecast_id, count, is_target_count in batched_rows(cursor):
            forecast_id_to_counts[forecast_id][2 if is_target_count else 1] = count

    # done
    return forecast_id_to_counts


#
# ---- NamedData ----
#

class NamedData:
    """
    Helper class that stores named data-related constants representing named distributions like normal, log normal,
    gamma, etc. These are essentially named functions (the function's `family`) with up to general-purpose three
    parameter fields - `param1`, `param2`, etc. Each parameter's semantics and calculation are defined by the family.
    """

    # family abbreviations. long name is shown but unused in code
    NORM_DIST = 'norm'  # Normal
    LNORM_DIST = 'lnorm'  # Log Normal
    GAMMA_DIST = 'gamma'  # Gamma
    BETA_DIST = 'beta'  # Beta
    POIS_DIST = 'pois'  # Poisson
    NBINOM_DIST = 'nbinom'  # Negative Binomial
    NBINOM2_DIST = 'nbinom2'  # Negative Binomial 2

    # a list of all of them for validation
    FAMILY_CHOICES = (NORM_DIST, LNORM_DIST, GAMMA_DIST, BETA_DIST, POIS_DIST, NBINOM_DIST, NBINOM2_DIST)

    # implements this table: https://docs.zoltardata.com/validation/#named-prediction-elements and helps validate:
    # "The number of param columns with non-NULL entries count must match family definition"
    PARAM_TO_EXP_COUNT = {NORM_DIST: 2, LNORM_DIST: 2, GAMMA_DIST: 2, BETA_DIST: 2, POIS_DIST: 1, NBINOM_DIST: 2,
                          NBINOM2_DIST: 2}
