import datetime
from itertools import groupby

from forecast_app.models.prediction_element import PRED_CLASS_INT_TO_NAME, PredictionElement
from utils.project_queries import CSV_HEADER
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


RETRACT_VAL = 'NULL'  # value in CSV files that represents a retraction


#
# csv_rows_from_json_io_dict()
#

def csv_rows_from_json_io_dict(json_io_dict):
    """
    A utility that converts a "JSON IO dict" as returned by zoltar to a list rows in zoltar-specific CSV format. The
    columns are: 'unit', 'target', 'class', 'value', 'cat', 'prob', 'sample', 'quantile', 'family', 'param1', 'param2',
    'param3'. They are documented at https://docs.zoltardata.com/fileformats/#forecast-data-format-csv .

    notes:
    - retractions: represented in csv_rows by placing RETRACT_VAL in *all* pred_class-required column(s)

    :param json_io_dict: a "JSON IO dict" to load from. see docs for details. the "meta" section is ignored
    :return: a list of CSV rows including header - see CSV_HEADER
    """
    # do some initial validation
    if 'predictions' not in json_io_dict:
        raise RuntimeError("no predictions section found in json_io_dict")

    rows = [CSV_HEADER]  # return value. filled next
    for prediction_dict in json_io_dict['predictions']:
        prediction_class = prediction_dict['class']
        if prediction_class not in list(PRED_CLASS_INT_TO_NAME.values()):  # 'bin', 'named', 'point', 'sample', quantile
            raise RuntimeError(f"invalid prediction_dict class: {prediction_class}")

        is_bin_class = prediction_class == PRED_CLASS_INT_TO_NAME[PredictionElement.BIN_CLASS]
        is_named_class = prediction_class == PRED_CLASS_INT_TO_NAME[PredictionElement.NAMED_CLASS]
        is_point_class = prediction_class == PRED_CLASS_INT_TO_NAME[PredictionElement.POINT_CLASS]
        is_sample_class = prediction_class == PRED_CLASS_INT_TO_NAME[PredictionElement.SAMPLE_CLASS]
        unit = prediction_dict['unit']
        target = prediction_dict['target']
        prediction_data = prediction_dict['prediction']
        is_retraction = prediction_data is None

        # prediction_class-specific columns all default to empty:
        value, cat, prob, sample, quantile, family, param1, param2, param3 = '', '', '', '', '', '', '', '', ''
        if is_retraction:
            # retractions are prediction_class-specific. we put RETRACT_VAL in all required columns, and we only need to
            # output one row in these cases
            if is_bin_class:
                cat, prob = RETRACT_VAL, RETRACT_VAL
            elif is_named_class:
                family, param1, param2, param3 = RETRACT_VAL, RETRACT_VAL, RETRACT_VAL, RETRACT_VAL
            elif is_point_class:
                value = RETRACT_VAL
            elif is_sample_class:
                sample = RETRACT_VAL
            else:  # PRED_CLASS_INT_TO_NAME[PredictionElement.QUANTILE_CLASS]
                value, quantile = RETRACT_VAL, RETRACT_VAL
            rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                         family, param1, param2, param3])
        elif is_bin_class:
            for cat, prob in zip(prediction_data['cat'], prediction_data['prob']):
                rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                             family, param1, param2, param3])
        elif is_named_class:
            rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                         prediction_data['family'],
                         prediction_data['param1'] if 'param1' in prediction_data else '',
                         prediction_data['param2'] if 'param2' in prediction_data else '',
                         prediction_data['param3'] if 'param3' in prediction_data else ''])
        elif is_point_class:
            rows.append([unit, target, prediction_class, prediction_data['value'], cat, prob, sample, quantile,
                         family, param1, param2, param3])
        elif is_sample_class:
            for sample in prediction_data['sample']:
                rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                             family, param1, param2, param3])
        else:  # PRED_CLASS_INT_TO_NAME[PredictionElement.QUANTILE_CLASS]
            for quantile, value in zip(prediction_data['quantile'], prediction_data['value']):
                rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                             family, param1, param2, param3])
    return rows


#
# json_io_dict_from_csv_rows()
#

def json_io_dict_from_csv_rows(csv_rows):
    """
    The opposite of `csv_rows_from_json_io_dict()`, this function converts a list rows in zoltar-specific CSV format to
    a "JSON IO dict". See `csv_rows_from_json_io_dict()` for doc links.

    notes:
    - error handling: this function terminates on the first error
    - DB-level validation (units, targets, etc.) is left to the caller to do with returned json_io_dict
    - retractions: represented in csv_rows by placing RETRACT_VAL in *all* pred_class-required column(s)

    :param csv_rows: a list of rows in zoltar-specific CSV format. columns: 12 (see CSV_HEADER)
    :return: a "JSON IO dict"
    """
    row0 = csv_rows.pop(0) if csv_rows else []
    if row0 != CSV_HEADER:
        raise RuntimeError(f"first row was not the proper header. row0 = {row0}, header={CSV_HEADER}")

    pred_class_to_pred_dict_fcn = {'bin': _pred_dict_for_bin_rows,
                                   'named': _pred_dict_for_named_rows,
                                   'point': _pred_dict_for_point_rows,
                                   'sample': _pred_dict_for_sample_rows,
                                   'quantile': _pred_dict_for_quantile_rows}
    prediction_dicts = []
    csv_rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby(): unit, target, pred_class
    for (unit, target, pred_class), values_grouper in groupby(csv_rows, key=lambda _: (_[0], _[1], _[2])):
        prediction_dicts.append(pred_class_to_pred_dict_fcn[pred_class](unit, target, pred_class, list(values_grouper)))
    return {'meta': {}, 'predictions': prediction_dicts}


def _pred_dict_for_bin_rows(unit, target, pred_class, values_rows):
    # bin rows: columns: cat, prob
    cats, probs = [], []
    for _, _, _, value, cat, prob, sample, quantile, family, param1, param2, param3 in values_rows:
        validate_empties([value, cat, prob, sample, quantile, family, param1, param2, param3], [1, 2])
        cats.append(_parse_value_csv(cat))
        probs.append(_parse_value_csv(prob))
    is_retraction = all(map(lambda _: _ == RETRACT_VAL, cats)) and all(map(lambda _: _ == RETRACT_VAL, probs))
    pred_data = None if is_retraction else {"cat": cats, "prob": probs}
    return {"unit": unit, "target": target, "class": pred_class, "prediction": pred_data}


def _pred_dict_for_named_rows(unit, target, pred_class, values_rows):
    # named rows: columns: family, param1, param2, param3
    if len(values_rows) != 1:
        raise RuntimeError(f"not exactly one row for named class. values_rows={values_rows}")

    _, _, _, value, cat, prob, sample, quantile, family, param1, param2, param3 = values_rows[0]
    validate_empties([value, cat, prob, sample, quantile, family, param1, param2, param3], [5, 6], [7, 8])

    family, param1, param2, param3 = \
        _parse_value_csv(family), _parse_value_csv(param1), _parse_value_csv(param2), _parse_value_csv(param3)
    pred_data = {"family": family, "param1": param1}
    if param2 != '':
        pred_data['param2'] = param2
    if param3 != '':
        pred_data['param3'] = param3
    is_retraction = all(map(lambda _: _ == RETRACT_VAL, [family, param1, param2, param3]))
    pred_data = None if is_retraction else pred_data
    return {"unit": unit, "target": target, "class": pred_class, "prediction": pred_data}


def _pred_dict_for_point_rows(unit, target, pred_class, values_rows):
    # point rows: columns: value
    if len(values_rows) != 1:
        raise RuntimeError(f"not exactly one row for point class. values_rows={values_rows}")

    _, _, _, value, cat, prob, sample, quantile, family, param1, param2, param3 = values_rows[0]
    validate_empties([value, cat, prob, sample, quantile, family, param1, param2, param3], [0])

    value = _parse_value_csv(value)
    is_retraction = value == RETRACT_VAL
    pred_data = None if is_retraction else {"value": value}
    return {"unit": unit, "target": target, "class": pred_class, "prediction": pred_data}


def _pred_dict_for_sample_rows(unit, target, pred_class, values_rows):
    # sample rows: columns: sample
    samples = []
    for _, _, _, value, cat, prob, sample, quantile, family, param1, param2, param3 in values_rows:
        validate_empties([value, cat, prob, sample, quantile, family, param1, param2, param3], [3])
        samples.append(_parse_value_csv(sample))
    is_retraction = all(map(lambda _: _ == RETRACT_VAL, samples))
    pred_data = None if is_retraction else {"sample": samples}
    return {"unit": unit, "target": target, "class": pred_class, "prediction": pred_data}


def _pred_dict_for_quantile_rows(unit, target, pred_class, values_rows):
    # quantile rows: columns: value, quantile
    values, quantiles = [], []
    for _, _, _, value, cat, prob, sample, quantile, family, param1, param2, param3 in values_rows:
        validate_empties([value, cat, prob, sample, quantile, family, param1, param2, param3], [0, 4])
        values.append(_parse_value_csv(value))
        quantiles.append(_parse_value_csv(quantile))
    is_retraction = all(map(lambda _: _ == RETRACT_VAL, values)) and all(map(lambda _: _ == RETRACT_VAL, quantiles))
    pred_data = None if is_retraction else {"quantile": quantiles, "value": values}
    return {"unit": unit, "target": target, "class": pred_class, "prediction": pred_data}


#
#  _pred_dict_for_*_rows() helpers
#

def validate_empties(row, required_idxs, optional_idxs=()):
    """
    Helper for _pred_dict_for_*_rows() functions. Raises if the passed args are valid for the `required_idxs`, which
    contains indexes of values that should be non-empty (i.e., non-''). `optional_idxs` are indexes of values that
    may or may not be empty.
    """
    for idx, value in enumerate(row):
        if idx in optional_idxs:
            continue

        is_empty = len(value) == 0
        if is_empty and (idx in required_idxs):
            raise RuntimeError(f"row missing required value. row={row}, idx={idx}")
        elif (not is_empty) and (idx not in required_idxs):
            raise RuntimeError(f"row has unexpected non-empty value. row={row}, idx={idx}, value={value}")


def _parse_value_csv(value_str):
    """
    Helper for _pred_dict_for_*_rows() functions. Similar to utils.cdc_io._parse_value(), but returns strings as-is if
    they can't be parsed as a boolean, number or date, and returns date strings as-is.
    """
    if value_str == 'True':
        return True
    elif value_str == 'False':
        return False

    try:
        return int(value_str)
    except ValueError:
        pass

    try:
        return float(value_str)
    except ValueError:
        pass

    try:
        datetime.datetime.strptime(value_str, YYYY_MM_DD_DATE_FORMAT).date()  # validates date format
        return value_str
    except ValueError:
        pass

    return value_str
