import csv
import datetime
import io
import math
from collections import defaultdict
from itertools import groupby

from django.db import connection, transaction

from forecast_app.models import NamedDistribution, PointPrediction, Forecast, Target, BinDistribution, \
    SampleDistribution
from forecast_app.models.project import POSTGRES_NULL_VALUE
from utils.project import _target_dict_for_target
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS = {
    BinDistribution: 'bin',
    NamedDistribution: 'named',
    PointPrediction: 'point',
    SampleDistribution: 'sample',
}


#
# json_io_dict_from_forecast
#

def json_io_dict_from_forecast(forecast):
    """
    The database equivalent of json_io_dict_from_cdc_csv_file(), returns a "JSON IO dict" for exporting json (for
    example). See EW01-2011-ReichLab_kde_US_National.json for an example. Does not reuse that function's helper methods
    b/c the latter is limited to 1) reading rows from CSV (not the db), and 2) only handling the three types of
    predictions in CDC CSV files. Does include the 'meta' section in the returned dict.

    :param forecast: a Forecast whose predictions are to be outputted
    :return a "JSON IO dict" (aka 'json_io_dict' by callers) that contains forecast's predictions. sorted by unit
        and target for visibility. see docs for details
    """
    unit_names, target_names, prediction_dicts = _units_targets_pred_dicts_from_forecast(forecast)
    return {
        'meta': {
            'forecast': _forecast_dict_for_forecast(forecast),
            'units': sorted([{'name': unit_names} for unit_names in unit_names],
                                key=lambda _: (_['name'])),
            'targets': sorted(
                [_target_dict_for_target(target) for target in forecast.forecast_model.project.targets.all()],
                key=lambda _: (_['name'])),
        },
        'predictions': sorted(prediction_dicts, key=lambda _: (_['unit'], _['target']))}


def _units_targets_pred_dicts_from_forecast(forecast):
    """
    json_io_dict_from_forecast() helper

    :param forecast: the Forecast to read predictions from
    :return: a 3-tuple: (unit_names, target_names, prediction_dicts) where the first two are sets of the Unit
        names and Target names in forecast's Predictions, and the last is list of "prediction dicts" as documented
        elsewhere
    """
    # recall Django's limitations in handling abstract classes and polymorphic models - asking for all of a Forecast's
    # Predictions returns base Prediction instances (forecast, unit, and target) without subclass fields (e.g.,
    # PointPrediction.value). so we have to handle each Prediction subclass individually. this implementation loads
    # all instances of each concrete subclass into memory, ordered by (unit, target) for groupby(). note: b/c the
    # code for each class is so similar, I had implemented an abstraction, but it turned out to be longer and more
    # complicated, and IMHO didn't warrant eliminating the duplication
    target_name_to_obj = {target.name: target for target in forecast.forecast_model.project.targets.all()}

    unit_names = set()
    target_names = set()
    prediction_dicts = []  # filled next for each Prediction subclass

    # PointPrediction
    point_qs = forecast.point_prediction_qs() \
        .order_by('pk') \
        .values_list('unit__name', 'target__name', 'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
    for unit_name, target_values_grouper in groupby(point_qs, key=lambda _: _[0]):
        unit_names.add(unit_name)
        for target_name, values_grouper in groupby(target_values_grouper, key=lambda _: _[1]):
            is_date_target = target_name_to_obj[target_name].data_type() == Target.DATE_DATA_TYPE
            target_names.add(target_name)
            for _, _, value_i, value_f, value_t, value_d, value_b in values_grouper:  # recall that exactly one will be non-NULL
                # note that we create a separate dict for each row b/c there is supposed to be 0 or 1 PointPredictions
                # per Forecast. validation should take care of enforcing this, but this code here is general
                point_value = PointPrediction.first_non_none_value(value_i, value_f, value_t, value_d, value_b)
                if is_date_target:
                    point_value = point_value.strftime(YYYY_MM_DD_DATE_FORMAT)
                prediction_dicts.append({"unit": unit_name, "target": target_name,
                                         "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction],
                                         "prediction": {"value": point_value}})

    # NamedDistribution
    named_qs = forecast.named_distribution_qs() \
        .order_by('pk') \
        .values_list('unit__name', 'target__name', 'family', 'param1', 'param2', 'param3')
    for unit_name, target_family_params_grouper in groupby(named_qs, key=lambda _: _[0]):
        unit_names.add(unit_name)
        for target_name, family_params_grouper in groupby(target_family_params_grouper, key=lambda _: _[1]):
            target_names.add(target_name)
            for _, _, family, param1, param2, param3 in family_params_grouper:
                # note that we create a separate dict for each row b/c there is supposed to be 0 or 1 NamedDistributions
                # per Forecast. validation should take care of enforcing this, but this code here is general
                family_abbrev = NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION[family]
                pred_dict_pred = {"family": family_abbrev}  # add non-null param* values next
                if param1 is not None:
                    pred_dict_pred["param1"] = param1
                if param2 is not None:
                    pred_dict_pred["param2"] = param2
                if param3 is not None:
                    pred_dict_pred["param3"] = param3
                prediction_dicts.append({"unit": unit_name, "target": target_name,
                                         "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[NamedDistribution],
                                         "prediction": pred_dict_pred})

    # BinDistribution. ordering by 'cat_*' for testing, but it's a slower query:
    bincat_qs = forecast.bin_distribution_qs() \
        .order_by('pk') \
        .values_list('unit__name', 'target__name', 'prob', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')
    for unit_name, target_cat_prob_grouper in groupby(bincat_qs, key=lambda _: _[0]):
        unit_names.add(unit_name)
        for target_name, cat_prob_grouper in groupby(target_cat_prob_grouper, key=lambda _: _[1]):
            is_date_target = target_name_to_obj[target_name].data_type() == Target.DATE_DATA_TYPE
            target_names.add(target_name)
            bin_cats, bin_probs = [], []
            for _, _, prob, cat_i, cat_f, cat_t, cat_d, cat_b in cat_prob_grouper:
                cat_value = PointPrediction.first_non_none_value(cat_i, cat_f, cat_t, cat_d, cat_b)
                if is_date_target:
                    cat_value = cat_value.strftime(YYYY_MM_DD_DATE_FORMAT)
                bin_cats.append(cat_value)
                bin_probs.append(prob)
            prediction_dicts.append({'unit': unit_name, 'target': target_name,
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution],
                                     'prediction': {'cat': bin_cats, 'prob': bin_probs}})

    # SampleDistribution
    sample_qs = forecast.sample_distribution_qs() \
        .order_by('pk') \
        .values_list('unit__name', 'target__name', 'sample_i', 'sample_f', 'sample_t', 'sample_d', 'sample_b')
    for unit_name, target_sample_grouper in groupby(sample_qs, key=lambda _: _[0]):
        unit_names.add(unit_name)
        for target_name, sample_grouper in groupby(target_sample_grouper, key=lambda _: _[1]):
            is_date_target = target_name_to_obj[target_name].data_type() == Target.DATE_DATA_TYPE
            target_names.add(target_name)
            sample_cats, sample_probs = [], []
            for _, _, sample_i, sample_f, sample_t, sample_d, sample_b in sample_grouper:
                sample_value = PointPrediction.first_non_none_value(sample_i, sample_f, sample_t, sample_d, sample_b)
                if is_date_target:
                    sample_value = sample_value.strftime(YYYY_MM_DD_DATE_FORMAT)
                sample_cats.append(sample_value)
            prediction_dicts.append({'unit': unit_name, 'target': target_name,
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleDistribution],
                                     'prediction': {'sample': sample_cats}})

    return unit_names, target_names, prediction_dicts


def _forecast_dict_for_forecast(forecast):
    """
    json_io_dict_from_forecast() helper that returns a dict for the 'forecast' section of the exported json.
    See cdc-predictions.json for an example.
    """
    return {'id': forecast.pk,
            'forecast_model_id': forecast.forecast_model.pk,
            'source': forecast.source,
            'created_at': forecast.created_at.isoformat(),
            'time_zero': {
                'timezero_date': forecast.time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT),
                'data_version_date': forecast.time_zero.data_version_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                if forecast.time_zero.data_version_date else None
            }}


#
# load_predictions_from_json_io_dict()
#

BIN_SUM_REL_TOL = 0.001  # hard-coded magic number for prediction probability sums


@transaction.atomic
def load_predictions_from_json_io_dict(forecast, json_io_dict, is_validate_cats=True):
    """
    Loads the prediction data into forecast from json_io_dict. Validates the forecast data. Note that we ignore the
    'meta' portion of json_io_dict. Errors if any referenced Units and Targets do not exist in forecast's Project.

    :param is_validate_cats: True if bin cat values should be validated against their Target.cats. used for testing
    :param forecast: a Forecast to load json_io_dict's predictions into
    :param json_io_dict: a "JSON IO dict" to load from. see docs for details
    """
    # validate predictions, convert them to class-specific quickly-loadable rows, and then load them by class
    if 'predictions' not in json_io_dict:
        raise RuntimeError(f"json_io_dict had no 'predictions' key: {json_io_dict}")

    prediction_dicts = json_io_dict['predictions']
    bin_rows, named_rows, point_rows, sample_rows = \
        _prediction_dicts_to_validated_db_rows(forecast, prediction_dicts, is_validate_cats)
    target_pk_to_object = {target.pk: target for target in forecast.forecast_model.project.targets.all()}

    _load_bin_rows(forecast, bin_rows, target_pk_to_object)
    _load_named_rows(forecast, named_rows)
    _load_point_rows(forecast, point_rows, target_pk_to_object)
    _load_sample_rows(forecast, sample_rows, target_pk_to_object)


def _prediction_dicts_to_validated_db_rows(forecast, prediction_dicts, is_validate_cats):
    """
    Validates prediction_dicts and returns a 4-tuple of rows suitable for bulk-loading into a database:
        bin_rows, named_rows, point_rows, sample_rows
    Each row is Prediction class-specific. Skips zero-prob BinDistribution rows.

    :param is_validate_cats: same as load_predictions_from_json_io_dict()
    :param forecast: a Forecast that's used to validate against
    :param prediction_dicts: the 'predictions' portion of a "JSON IO dict" as returned by
        json_io_dict_from_cdc_csv_file()
    """
    unit_name_to_obj = {unit.name: unit for unit in forecast.forecast_model.project.units.all()}
    target_name_to_obj = {target.name: target for target in forecast.forecast_model.project.targets.all()}
    family_abbrev_to_int = {abbreviation: family_int for family_int, abbreviation
                            in NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.items()}
    # this variable helps to do "prediction"-level validations at the end of this function. it maps 2-tuples to a list
    # of prediction classes (strs)
    loc_targ_to_pred_classes = defaultdict(list)  # (unit_name, target_name) -> [prediction_class1, ...]
    bin_rows, named_rows, point_rows, sample_rows = [], [], [], []  # return values. set next
    for prediction_dict in prediction_dicts:
        unit_name = prediction_dict['unit']
        target_name = prediction_dict['target']
        prediction_class = prediction_dict['class']
        prediction_data = prediction_dict['prediction']
        loc_targ_to_pred_classes[(unit_name, target_name)].append(prediction_class)

        # validate unit and target names (applies to all prediction classes)
        if unit_name not in unit_name_to_obj:
            raise RuntimeError(f"prediction_dict referred to an undefined Unit. unit_name={unit_name!r}. "
                               f"existing_unit_names={unit_name_to_obj.keys()}")
        elif target_name not in target_name_to_obj:
            raise RuntimeError(f"prediction_dict referred to an undefined Target. target_name={target_name!r}. "
                               f"existing_target_names={target_name_to_obj.keys()}")

        # do class-specific validation and row collection
        target = target_name_to_obj[target_name]
        if prediction_class == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution]:
            _validate_bin_rows(is_validate_cats, prediction_data, prediction_dict, target)  # raises o/w
            for cat, prob in zip(prediction_data['cat'], prediction_data['prob']):
                if prob != 0:  # skip cat values with zero probability (saves database space and doesn't affect scoring)
                    bin_rows.append([unit_name, target_name, cat, prob])
        elif prediction_class == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[NamedDistribution]:
            family_abbrev = prediction_data['family']
            _validate_named_rows(family_abbrev, family_abbrev_to_int, prediction_data, prediction_dict,
                                 target)  # raises o/w
            named_rows.append([unit_name, target_name, family_abbrev,
                               prediction_data.get('param1', None),
                               prediction_data.get('param2', None),
                               prediction_data.get('param3', None)])
        elif prediction_class == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction]:
            value = prediction_data['value']
            _validate_point_rows(prediction_data, prediction_dict, target, value)  # raises o/w
            point_rows.append([unit_name, target_name, value])
        elif prediction_class == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleDistribution]:
            _validate_sample_rows(prediction_data, prediction_dict, target)  # raises o/w
            for sample in prediction_data['sample']:
                sample_rows.append([unit_name, target_name, sample])
        else:
            raise RuntimeError(f"invalid prediction_class: {prediction_class!r}. must be one of: "
                               f"{list(PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS.values())}. "
                               f"prediction_dict={prediction_dict}")

    # finally, do "prediction"-level validation. recall that "prediction" is defined as "a group of a prediction
    # elements(s) specific to a unit and target"

    # validate: "Within a Prediction, there cannot be more than 1 Prediction Element of the same type".
    duplicate_unit_target_tuples = [(unit, target, pred_classes) for (unit, target), pred_classes
                                        in loc_targ_to_pred_classes.items()
                                        if len(pred_classes) != len(set(pred_classes))]
    if duplicate_unit_target_tuples:
        raise RuntimeError(f"Within a Prediction, there cannot be more than 1 Prediction Element of the same class. "
                           f"Found these duplicate unit/target tuples: {duplicate_unit_target_tuples}")

    # validate: (for both continuous and discrete target types): Within one prediction, there can be at most one of the
    # following prediction elements, but not both: {`Named`, `Bin`}.
    named_bin_conflict_tuples = [(unit, target, pred_classes) for (unit, target), pred_classes
                                 in loc_targ_to_pred_classes.items()
                                 if (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution] in pred_classes)
                                 and (PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[NamedDistribution] in pred_classes)]
    if named_bin_conflict_tuples:
        raise RuntimeError(f"Within one prediction, there can be at most one of the following prediction elements, "
                           f"but not both: `Named`, `Bin`. Found these conflicting unit/target tuples: "
                           f"{named_bin_conflict_tuples}")

    # done!
    return bin_rows, named_rows, point_rows, sample_rows


def _validate_bin_rows(is_validate_cats, prediction_data, prediction_dict, target):
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
    prob_types = set(map(type, prediction_data['prob']))
    if (prob_types != {int, float}) and (len(prob_types) != 1):
        raise RuntimeError(f"there was more than one data type in `prob` column, which should only contain "
                           f"numbers. prob column={prediction_data['prob']}, prob_types={prob_types}, "
                           f"prediction_dict={prediction_dict}")
    prob_type = next(iter(prob_types))  # vs. pop()
    if (prob_type != int) and (prob_type != float):
        raise RuntimeError(f"wrong data type in `prob` column, which should only contain "
                           f"numbers. prob column={prediction_data['prob']}, prob_type={prob_type}, "
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


def _validate_named_rows(family_abbrev, family_abbrev_to_int, prediction_data, prediction_dict, target):
    # validate: "`family`: must be one of the abbreviations shown in the table below"
    family_abbrevs = NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.values()
    if family_abbrev not in family_abbrevs:
        raise RuntimeError(f"family must be one of the abbreviations shown in the table below. "
                           f"family_abbrev={family_abbrev!r}, family_abbrevs={family_abbrevs}, "
                           f"prediction_dict={prediction_dict}")

    # validate: "The Prediction's class must be valid for its target's type". note that only NamedDistributions
    # are constrained; all other target_type/prediction_class combinations are valid
    if family_abbrev_to_int[family_abbrev] not in Target.valid_named_families(target.type):
        raise RuntimeError(f"family {family_abbrev!r} is not valid for {target.type_as_str()!r} "
                           f"target types. prediction_dict={prediction_dict}")

    # validate: "The number of param columns with non-NULL entries count must match family definition"
    param_to_exp_count = {'norm': 2, 'lnorm': 2, 'gamma': 2, 'beta': 2, 'pois': 1, 'nbinom': 2, 'nbinom2': 2}
    num_params = 0
    if 'param1' in prediction_data:
        num_params += 1
    if 'param2' in prediction_data:
        num_params += 1
    if 'param3' in prediction_data:
        num_params += 1
    if num_params != param_to_exp_count[family_abbrev]:
        raise RuntimeError(f"The number of param columns with non-NULL entries count must match family "
                           f"definition. family_abbrev={family_abbrev!r}, num_params={num_params}, "
                           f"expected count={param_to_exp_count[family_abbrev]}, "
                           f"prediction_dict={prediction_dict}")
    # validate: Parameters for each distribution must be within valid ranges, which, if constraints exist, are
    # specified in the table below
    ge_0, gt_0, bw_0_1 = '>=0', '>0', '0<=&>=0'
    family_abbrev_to_param1_2_constraint_type = {
        'norm': (None, ge_0),  # | mean | sd>=0 | - |
        'lnorm': (None, ge_0),  # | mean | sd>=0 | - |
        'gamma': (gt_0, gt_0),  # | shape>0 |rate>0 | - |
        'beta': (gt_0, gt_0),  # | a>0 | b>0 | - |
        'pois': (gt_0, None),  # | rate>0 |  - | - |
        'nbinom': (gt_0, bw_0_1),  # | r>0 | 0<=p<=1 | - |
        'nbinom2': (gt_0, gt_0)  # | mean>0 | disp>0 | - |
    }
    p1_constr, p2_constr = family_abbrev_to_param1_2_constraint_type[family_abbrev]
    if ((p1_constr == gt_0) and not (prediction_data['param1'] > 0)) or \
            ((p2_constr == ge_0) and not (prediction_data['param2'] >= 0)) or \
            ((p2_constr == gt_0) and not (prediction_data['param2'] > 0)) or \
            ((p2_constr == bw_0_1) and not (0 <= prediction_data['param2'] <= 1)):
        raise RuntimeError(f"Parameters for each distribution must be within valid ranges: "
                           f"prediction_dict={prediction_dict}")


def _validate_point_rows(prediction_data, prediction_dict, target, value):
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


def _validate_sample_rows(prediction_data, prediction_dict, target):
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


def _load_bin_rows(forecast, rows, target_pk_to_object):
    """
    Loads the rows in prediction_data_dict as BinCatDistributions.
    """
    # incoming rows: [unit_name, target_name, cat, prob]

    # after this, rows will be: [unit_id, target_id, cat, prob]:
    _replace_unit_target_names_with_pks(forecast, rows)

    # after this, rows will be: [unit_id, target_id, cat_i, cat_f, cat_t, cat_d, cat_b, prob]:
    _replace_value_with_five_types(rows, target_pk_to_object, is_exclude_last=True)

    # after this, rows will be: [unit_id, target_id, cat_i, cat_f, cat_t, cat_d, cat_b, prob, self_pk]:
    _add_forecast_pks(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = BinDistribution
    columns_names = [prediction_class._meta.get_field('unit').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('cat_i').column,
                     prediction_class._meta.get_field('cat_f').column,
                     prediction_class._meta.get_field('cat_t').column,
                     prediction_class._meta.get_field('cat_d').column,
                     prediction_class._meta.get_field('cat_b').column,
                     prediction_class._meta.get_field('prob').column,
                     Forecast._meta.model_name + '_id']
    _insert_prediction_rows(prediction_class, columns_names, rows)


def _load_named_rows(forecast, rows):
    """
    Loads the rows in rows as NamedDistribution concrete subclasses. Recall that each subclass has different IVs,
    so we use a hard-coded mapping to decide the subclass based on the `family` column.
    """
    # incoming rows: [unit_name, target_name, family, param1, param2, param3]

    # after this, rows will be: [unit_id, target_id, family, param1, param2, param3]:
    _replace_unit_target_names_with_pks(forecast, rows)

    # after this, rows will be: [unit_id, target_id, family_id, param1, param2, param3]:
    _replace_family_abbrev_with_id(rows)

    # after this, rows will be: [unit_id, target_id, family_id, param1_or_0, param2_or_0, param3_or_0]:
    # _replace_null_params_with_zeros(rows)  # todo xx temp!

    # after this, rows will be: [unit_id, target_id, family_id, param1, param2, param3, self_pk]:
    _add_forecast_pks(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = NamedDistribution
    columns_names = [prediction_class._meta.get_field('unit').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('family').column,
                     prediction_class._meta.get_field('param1').column,
                     prediction_class._meta.get_field('param2').column,
                     prediction_class._meta.get_field('param3').column,
                     Forecast._meta.model_name + '_id']
    _insert_prediction_rows(prediction_class, columns_names, rows)


def _load_point_rows(forecast, rows, target_pk_to_object):
    """
    Validates and loads the rows in rows as PointPredictions.
    """
    # incoming rows: [unit_name, target_name, value]

    # after this, rows will be: [unit_id, target_id, value]:
    _replace_unit_target_names_with_pks(forecast, rows)

    # # validate rows
    # unit_id_to_obj = {unit.pk: unit for unit in forecast.forecast_model.project.units.all()}
    # target_id_to_obj = {target.pk: target for target in forecast.forecast_model.project.targets.all()}
    # for unit_id, target_id, value in rows:
    #     target = target_id_to_obj[target_id]
    #     if (not target.is_date) and (value is None):
    #         raise RuntimeError(f"Point value was non-numeric. forecast={forecast}, "
    #                            f"unit={unit_id_to_obj[unit_id]}, target={target}")

    # after this, rows will be: [unit_id, target_id, value_i, value_f, value_t]:
    _replace_value_with_five_types(rows, target_pk_to_object, is_exclude_last=False)

    # after this, rows will be: [unit_id, target_id, value_i, value_f, value_t, self_pk]:
    _add_forecast_pks(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = PointPrediction
    columns_names = [prediction_class._meta.get_field('unit').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('value_i').column,
                     prediction_class._meta.get_field('value_f').column,
                     prediction_class._meta.get_field('value_t').column,
                     prediction_class._meta.get_field('value_d').column,
                     prediction_class._meta.get_field('value_b').column,
                     Forecast._meta.model_name + '_id']
    _insert_prediction_rows(prediction_class, columns_names, rows)


def _load_sample_rows(forecast, rows, target_pk_to_object):
    """
    Loads the rows in rows as SampleDistribution. See SAMPLE_DISTRIBUTION_HEADER.
    """
    # incoming rows: [unit_name, target_name, sample]

    # after this, rows will be: [unit_id, target_id, sample]:
    _replace_unit_target_names_with_pks(forecast, rows)

    # after this, rows will be: [unit_id, target_id, sample_i, sample_f, sample_t, sample_d, sample_b]:
    _replace_value_with_five_types(rows, target_pk_to_object, is_exclude_last=False)

    # after this, rows will be: [unit_id, target_id, sample, self_pk]:
    _add_forecast_pks(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = SampleDistribution
    columns_names = [prediction_class._meta.get_field('unit').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('sample_i').column,
                     prediction_class._meta.get_field('sample_f').column,
                     prediction_class._meta.get_field('sample_t').column,
                     prediction_class._meta.get_field('sample_d').column,
                     prediction_class._meta.get_field('sample_b').column,
                     Forecast._meta.model_name + '_id']
    _insert_prediction_rows(prediction_class, columns_names, rows)


def _replace_unit_target_names_with_pks(forecast, rows):
    """
    Does an in-place rows replacement of target and unit names with PKs.
    """
    project = forecast.forecast_model.project

    # todo xx pass in:
    unit_name_to_pk = {unit.name: unit.id for unit in project.units.all()}

    target_name_to_pk = {target.name: target.id for target in project.targets.all()}
    for row in rows:  # unit_name, target_name, value, self_pk
        row[0] = unit_name_to_pk[row[0]]
        row[1] = target_name_to_pk[row[1]]


def _replace_value_with_five_types(rows, target_pk_to_object, is_exclude_last):
    """
    Does an in-place rows replacement of values with the five type-specific values based on each row's Target's
    data_type. The values: value_i, value_f, value_t, value_d, value_b. Recall that exactly one will be non-NULL (i.e.,
    not None).

    :param rows: a list of lists of the form: [unit_id, target_id, value, [last_item]], where last_item is optional
        and is indicated by is_exclude_last
    :param is_exclude_last: True if the last item should be preserved, and False o/w
    :return: rows, but with the value_idx replaced with the above five type-specific values, i.e.,
        [unit_id, target_id, value_i, value_f, value_t, value_d, value_b, [last_item]]
    """
    value_idx = 2
    for row in rows:
        target_pk = row[1]
        data_type = target_pk_to_object[target_pk].data_type()
        value = row[value_idx]
        value_i = value if data_type == Target.INTEGER_DATA_TYPE else None
        value_f = value if data_type == Target.FLOAT_DATA_TYPE else None
        value_t = value if data_type == Target.TEXT_DATA_TYPE else None
        value_d = value if data_type == Target.DATE_DATA_TYPE else None
        value_b = value if data_type == Target.BOOLEAN_DATA_TYPE else None
        if is_exclude_last:
            row[value_idx:-1] = [value_i, value_f, value_t, value_d, value_b]
        else:
            row[value_idx:] = [value_i, value_f, value_t, value_d, value_b]


def _replace_family_abbrev_with_id(rows):
    """
    Does an in-place rows replacement of family abbreviations with ids in NamedDistribution.FAMILY_CHOICES (ints).
    """
    for row in rows:
        abbreviation = row[2]
        if abbreviation in NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.values():
            row[2] = [choice for choice, abbrev in NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.items()
                      if abbrev == abbreviation][0]
        else:
            raise RuntimeError(f"invalid family. abbreviation={abbreviation!r}, "
                               f"abbreviations={NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.values()}")


def _replace_null_params_with_zeros(rows):
    """
    Does an in-place rows replacement of empty params with zeros."
    """
    for row in rows:
        row[3] = row[3] or 0  # param1
        row[4] = row[4] or 0  # param2
        row[5] = row[5] or 0  # param3


def _add_forecast_pks(forecast, rows):
    """
    Does an in-place rows addition of my pk to the end.
    """
    for row in rows:
        row.append(forecast.pk)


def _insert_prediction_rows(prediction_class, columns_names, rows):
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
                unit_id, target_id = row[0], row[1]
                prediction_items = row[2:-1]
                self_pk = row[-1]

                for idx in range(len(prediction_items)):
                    # value_i if value_i is not None else POSTGRES_NULL_VALUE
                    prediction_item = prediction_items[idx]
                    prediction_items[idx] = prediction_item if prediction_item is not None else POSTGRES_NULL_VALUE

                csv_writer.writerow([unit_id, target_id] + prediction_items + [self_pk])
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
