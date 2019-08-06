import csv
import io
import math
from itertools import groupby

from django.db import connection, transaction

from forecast_app.models import BinCatDistribution, BinLwrDistribution, BinaryDistribution, NamedDistribution, \
    PointPrediction, SampleDistribution, SampleCatDistribution, Forecast, Target
from forecast_app.models.project import POSTGRES_NULL_VALUE
from utils.utilities import YYYYMMDD_DATE_FORMAT


#
# JSON input/output format documentation
# - todo xx move these docs elsewhere!
#
# For prediction input and output we use a dictionary structure suitable for JSON I/O. The dict is called a
# "JSON IO dict" in code documentation. See predictions-example.json for an example. Functions accept a json_io_dict
# include: load_predictions_from_json_io_dict(). Functions that return a json_io_dict include:
# json_io_dict_from_forecast() and json_io_dict_from_cdc_csv_file(). This format is closely inspired by
# https://github.com/cdcepi/predx/blob/master/predx_classes.md
#
# Briefly, the dict has four top level keys:
#
# - forecast: a metadata dict about the file's forecast. has these keys: 'id', 'forecast_model_id', 'csv_filename',
#   'created_at', and 'time_zero'. Some or all of these keys might be ignored by functions that accept a JSON IO dict.
#
# - locations: a list of "location dicts", each of which has just a 'name' key whose value is the name of a location
#   in the below 'predictions' section.
#
# - targets: a list of "target dicts", each of which has the following fields. The fields are: 'name', 'description',
#   'unit', 'is_date', 'is_step_ahead', and 'step_ahead_increment'.
#
# - predictions: a list of "prediction dicts" that contains the prediction data. Each dict has these fields:
#   = 'location': name of the Location
#   = 'target': "" the Target
#   = 'class': the type of prediction this is. it is an abbreviation of the corresponding Prediction subclass - see
#     PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS for the names
#   = 'prediction': a class-specific dict containing the prediction data itself. the format varies according to class.
#     See https://github.com/cdcepi/predx/blob/master/predx_classes.md for details. Here is a summary:
#     + 'BinCat': Binned distribution with a category for each bin. is a two-column table represented by two keys, one
#                 per column: 'cat' and 'prob'. They are paired, i.e., have the same number of rows
#     + 'BinLwr': Binned distribution defined by inclusive lower bounds for each bin. Similar to 'BinCat', but has these
#                 two keys: 'lwr' and 'prob'.
#     + 'Binary': Binary distribution with a single 'prob' key.
#     + 'Named': A named distribution with four fields: 'family' and 'param1' through 'param3'. family must be listed in
#                FAMILY_CHOICE_TO_ABBREVIATION.
#     + 'Point': A numeric point prediction with a single 'value' key.
#     + 'Sample': Numeric samples represented as a table with one column that is found in the 'sample' key.
#     + 'SampleCat': Character string samples from categories. Similar to 'BinCat', but has these two keys: 'cat' and
#                    'sample'.
#


PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS = {
    BinCatDistribution: 'BinCat',
    BinLwrDistribution: 'BinLwr',
    BinaryDistribution: 'Binary',
    NamedDistribution: 'Named',
    PointPrediction: 'Point',
    SampleDistribution: 'Sample',
    SampleCatDistribution: 'SampleCat',
}


#
# json_io_dict_from_forecast
#

def json_io_dict_from_forecast(forecast):
    """
    The database equivalent of json_io_dict_from_cdc_csv_file(), returns a "JSON IO dict" for exporting json (for
    example). See predictions-example.json for an example. Does not reuse that function's helper methods b/c the latter
    is limited to 1) reading rows from CSV (not the db), and 2) only handling the three types of predictions in CDC CSV
    files. Does include the 'meta' section in the returned dict.

    :param forecast: a Forecast whose predictions are to be outputted
    :return a "JSON IO dict" (aka 'json_io_dict' by callers) that contains forecast's predictions. sorted by location
        and target for visibility. see docs for details
    """
    location_names, target_names, prediction_dicts = _locations_targets_pred_dicts_from_forecast(forecast)
    return {
        'meta': {
            'forecast': _forecast_dict_for_forecast(forecast),
            'locations': sorted([{'name': location_names} for location_names in location_names],
                                key=lambda _: (_['name'])),
            'targets': sorted(_target_dicts_for_project(forecast.forecast_model.project, target_names),
                              key=lambda _: (_['name'])),
        },
        'predictions': sorted(prediction_dicts, key=lambda _: (_['location'], _['target']))}


def _locations_targets_pred_dicts_from_forecast(forecast):
    """
    json_io_dict_from_forecast() helper that returns

    :param forecast: the Forecast to read predictions from
    :return: a 3-tuple: (location_names, target_names, prediction_dicts) where the first two are sets of the Location
        names and Target names in forecast's Predictions, and the last is list of "prediction dicts" as documented
        elsewhere
    """
    # recall Django's limitations in handling abstract classes and polymorphic models - asking for all of a Forecast's
    # Predictions returns base Prediction instances (forecast, location, and target) without subclass fields (e.g.,
    # PointPrediction.value). so we have to handle each Prediction subclass individually. this implementation loads
    # all instances of each concrete subclass into memory, ordered by (location, target) for groupby(). note: b/c the
    # code for each class is so similar, I implemented an abstraction, but it turned out to be longer and more
    # complicated, and didn't warrant eliminating the duplication

    location_names = set()
    target_names = set()
    prediction_dicts = []  # filled next for each Prediction subclass

    # BinCatDistribution
    bincat_qs = forecast.bincat_distribution_qs() \
        .order_by('location__id', 'target__id', 'cat') \
        .values_list('location__name', 'target__name', 'cat', 'prob')  # ordering by 'cat' for testing - slower query
    for location_name, target_cat_prob_grouper in groupby(bincat_qs, key=lambda _: _[0]):
        location_names.add(location_name)
        for target_name, cat_prob_grouper in groupby(target_cat_prob_grouper, key=lambda _: _[1]):
            target_names.add(target_name)
            bincat_cats, bincat_probs = [], []
            for _, _, cat, prob in cat_prob_grouper:
                bincat_cats.append(cat)
                bincat_probs.append(prob)
            prediction_dicts.append({"location": location_name, "target": target_name,
                                     "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinCatDistribution],
                                     "prediction": {"cat": bincat_cats, "prob": bincat_probs}})

    # BinLwrDistribution
    binlwr_qs = forecast.binlwr_distribution_qs() \
        .order_by('location__id', 'target__id', 'lwr') \
        .values_list('location__name', 'target__name', 'lwr', 'prob')  # ordering by 'lwr'
    for location_name, target_lwr_prob_grouper in groupby(binlwr_qs, key=lambda _: _[0]):
        location_names.add(location_name)
        for target_name, lwr_prob_grouper in groupby(target_lwr_prob_grouper, key=lambda _: _[1]):
            target_names.add(target_name)
            binlwr_lwrs, binlwr_probs = [], []
            for _, _, lwr, prob in lwr_prob_grouper:
                binlwr_lwrs.append(lwr)
                binlwr_probs.append(prob)
            prediction_dicts.append({"location": location_name, "target": target_name,
                                     "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinLwrDistribution],
                                     "prediction": {"lwr": binlwr_lwrs, "prob": binlwr_probs}})

    # BinaryDistribution
    binary_qs = forecast.binary_distribution_qs() \
        .order_by('location__id', 'target__id') \
        .values_list('location__name', 'target__name', 'prob')
    for location_name, target_prob_grouper in groupby(binary_qs, key=lambda _: _[0]):
        location_names.add(location_name)
        for target_name, prob_grouper in groupby(target_prob_grouper, key=lambda _: _[1]):
            target_names.add(target_name)
            for _, _, prob in prob_grouper:
                # note that we create a separate dict for each row b/c there is supposed to be 0 or 1
                # BinaryDistributions per Forecast. validation should take care of enforcing this, but this code here is
                # general
                prediction_dicts.append({"location": location_name, "target": target_name,
                                         "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinaryDistribution],
                                         "prediction": {"prob": prob}})

    # NamedDistribution
    named_qs = forecast.named_distribution_qs() \
        .order_by('location__id', 'target__id') \
        .values_list('location__name', 'target__name', 'family', 'param1', 'param2', 'param3')
    for location_name, target_family_params_grouper in groupby(named_qs, key=lambda _: _[0]):
        location_names.add(location_name)
        for target_name, family_params_grouper in groupby(target_family_params_grouper, key=lambda _: _[1]):
            target_names.add(target_name)
            for _, _, family, param1, param2, param3 in family_params_grouper:
                # note that we create a separate dict for each row b/c there is supposed to be 0 or 1 NamedDistributions
                # per Forecast. validation should take care of enforcing this, but this code here is general
                famil_abbrev = NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION[family]
                prediction_dicts.append({"location": location_name, "target": target_name,
                                         "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[NamedDistribution],
                                         "prediction": {"family": famil_abbrev,
                                                        "param1": param1, "param2": param2, "param3": param3}})

    # PointPrediction
    point_qs = forecast.point_prediction_qs() \
        .order_by('location__id', 'target__id') \
        .values_list('location__name', 'target__name', 'value_i', 'value_f', 'value_t')
    for location_name, target_values_grouper in groupby(point_qs, key=lambda _: _[0]):
        location_names.add(location_name)
        for target_name, values_grouper in groupby(target_values_grouper, key=lambda _: _[1]):
            target_names.add(target_name)
            for _, _, value_i, value_f, value_t in values_grouper:  # recall that exactly one will be non-NULL
                # note that we create a separate dict for each row b/c there is supposed to be 0 or 1 PointPredictions
                # per Forecast. validation should take care of enforcing this, but this code here is general
                point_value = [_ for _ in [value_i, value_f, value_t] if _ is not None][0]
                prediction_dicts.append({"location": location_name, "target": target_name,
                                         "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction],
                                         "prediction": {"value": point_value}})

    # SampleDistribution
    sample_qs = forecast.sample_distribution_qs() \
        .order_by('location__id', 'target__id') \
        .values_list('location__name', 'target__name', 'sample')
    for location_name, target_sample_grouper in groupby(sample_qs, key=lambda _: _[0]):
        location_names.add(location_name)
        for target_name, sample_grouper in groupby(target_sample_grouper, key=lambda _: _[1]):
            target_names.add(target_name)
            samples = []
            for _, _, sample in sample_grouper:
                samples.append(sample)
            prediction_dicts.append({"location": location_name, "target": target_name,
                                     "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleDistribution],
                                     "prediction": {"sample": samples}})

    # SampleCatDistribution
    samplecat_qs = forecast.samplecat_distribution_qs() \
        .order_by('location__id', 'target__id', 'cat') \
        .values_list('location__name', 'target__name', 'cat', 'sample')  # ordering by 'cat' for testing - slower query
    for location_name, target_cat_sample_grouper in groupby(samplecat_qs, key=lambda _: _[0]):
        location_names.add(location_name)
        for target_name, cat_sample_grouper in groupby(target_cat_sample_grouper, key=lambda _: _[1]):
            target_names.add(target_name)
            samplecat_cats, samplecat_samples = [], []
            for _, _, cat, sample in cat_sample_grouper:
                samplecat_cats.append(cat)
                samplecat_samples.append(sample)
            prediction_dicts.append({"location": location_name, "target": target_name,
                                     "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleCatDistribution],
                                     "prediction": {"cat": samplecat_cats, "sample": samplecat_samples}})

    return location_names, target_names, prediction_dicts


def _forecast_dict_for_forecast(forecast):
    """
    json_io_dict_from_cdc_csv_file() helper that returns a dict for the 'forecast' section of the exported json.
    See predictions-example.json for an example.
    """
    return {"id": forecast.pk,
            "forecast_model_id": forecast.forecast_model.pk,
            "csv_filename": forecast.csv_filename,
            "created_at": forecast.created_at.isoformat(),
            "time_zero": {
                "timezero_date": forecast.time_zero.timezero_date.strftime(YYYYMMDD_DATE_FORMAT),
                "data_version_date": forecast.time_zero.data_version_date.strftime(YYYYMMDD_DATE_FORMAT)
                if forecast.time_zero.data_version_date else None
            }}


def _target_dicts_for_project(project, target_names):
    """
    json_io_dict_from_cdc_csv_file() helper that returns a list of target dicts for the 'targets' section of the exported
    json. See predictions-example.json for an example. only those in target_names are included
    """
    return [{"name": target.name,
             "description": target.description,
             "unit": target.unit,
             "is_date": target.is_date,
             "is_step_ahead": target.is_step_ahead,
             "step_ahead_increment": target.step_ahead_increment}
            for target in project.targets.all() if target.name in target_names]


#
# load_predictions_from_json_io_dict()
#

@transaction.atomic
def load_predictions_from_json_io_dict(forecast, json_io_dict):
    """
    Loads the prediction data into forecast from json_io_dict. Validates the forecast data. Note that we ignore the
    'meta' portion of json_io_dict. Errors if any referenced Locations and Targets do not exist in forecast's Project.

    :param forecast a Forecast to load json_io_dict's predictions into
    :param json_io_dict a "JSON IO dict" to load from. see docs for details
    """
    # validate predictions, convert them to class-specific quickly-loadable rows, and then load them by class
    prediction_dicts = json_io_dict['predictions']
    bincat_rows, binlwr_rows, binary_rows, named_rows, point_rows, sample_rows, samplecat_rows = \
        _prediction_dicts_to_validated_db_rows(forecast, prediction_dicts)
    _load_bincat_rows(forecast, bincat_rows)
    _load_binlwr_rows(forecast, binlwr_rows)
    _load_binary_rows(forecast, binary_rows)
    _load_named_rows(forecast, named_rows)
    _load_point_rows(forecast, point_rows)
    _load_sample_rows(forecast, sample_rows)
    _load_samplecat_rows(forecast, samplecat_rows)


def _prediction_dicts_to_validated_db_rows(forecast, prediction_dicts):
    """
    Validates prediction_dicts and returns a 7-tuple of rows suitable for bulk-loading into a database:
        bincat_rows, binlwr_rows, binary_rows, named_rows, point_rows, sample_rows, samplecat_rows
    Each row is Prediction class-specific.

    :param forecast: a Forecast that's used to validate against
    :param prediction_dicts: the 'predictions' portion of a "JSON IO dict" as returned by
        json_io_dict_from_cdc_csv_file()
    """
    location_name_to_obj = {location.name: location for location in forecast.forecast_model.project.locations.all()}
    target_name_to_obj = {target.name: target for target in forecast.forecast_model.project.targets.all()}
    bincat_rows, binlwr_rows, binary_rows, named_rows, point_rows, sample_rows, samplecat_rows = \
        [], [], [], [], [], [], []  # return values. filled next
    for prediction_dict in prediction_dicts:
        location_name = prediction_dict['location']
        target_name = prediction_dict['target']
        prediction_class = prediction_dict['class']
        prediction_data = prediction_dict['prediction']

        # validate location and target names (applies to all prediction classes)
        if location_name not in location_name_to_obj:
            raise RuntimeError(f"prediction_dict referred to an undefined Location. location_name={location_name}. "
                               f"existing_location_names={location_name_to_obj.keys()}")

        if target_name not in target_name_to_obj:
            raise RuntimeError(f"prediction_dict referred to an undefined Target. target_name={target_name}. "
                               f"existing_target_names={target_name_to_obj.keys()}")

        # do class-specific validation and row collection
        target = target_name_to_obj[target_name]
        location = location_name_to_obj[location_name]
        if prediction_class == 'BinCat':
            # validation: xx
            _validate_bin_prob(forecast, location, target, prediction_data['prob'])
            for cat, prob in zip(prediction_data['cat'], prediction_data['prob']):
                bincat_rows.append([location_name, target_name, cat, prob])
        elif prediction_class == 'BinLwr':
            # validation: xx
            _validate_bin_prob(forecast, location, target, prediction_data['prob'])
            for lwr, prob in zip(prediction_data['lwr'], prediction_data['prob']):
                binlwr_rows.append([location_name, target_name, lwr, prob])
        elif prediction_class == 'Binary':
            # validation: xx
            binary_rows.append([location_name, target_name, prediction_data['prob']])
        elif prediction_class == 'Named':
            # family name validated in _replace_family_abbrev_with_id_rows()
            named_rows.append([location_name, target_name, prediction_data['family'],
                               prediction_data['param1'], prediction_data['param2'], prediction_data['param3']])
        elif prediction_class == 'Point':
            value = prediction_data['value']
            if (not target.is_date) and (value is None):
                raise RuntimeError(f"Point value was non-numeric. forecast={forecast}, location={location}, "
                                   f"target={target}")

            point_rows.append([location_name, target_name, value])
        elif prediction_class == 'Sample':
            # validation: xx
            for sample in prediction_data['sample']:
                sample_rows.append([location_name, target_name, sample])
        elif prediction_class == 'SampleCat':
            # validation: xx
            for cat, sample in zip(prediction_data['cat'], prediction_data['sample']):
                samplecat_rows.append([location_name, target_name, cat, sample])
        else:
            raise RuntimeError(f"invalid prediction_class: {prediction_class!r}. must be one of: "
                               f"{list(PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS.values())}")
    return bincat_rows, binlwr_rows, binary_rows, named_rows, point_rows, sample_rows, samplecat_rows


def _validate_bin_prob(forecast, location, target, bin_probs):
    # todo xx validate: [0, 1]. No NAs

    # validate probs sum to 1.0
    # note that the default rel_tol of 1e-09 failed for EW17-KoTstable-2017-05-09.csv
    # (forecast_bin_sum=0.9614178215505512 -> 0.04 fixed it), and for EW17-KoTkcde-2017-05-09.csv
    # (0.9300285798758262 -> 0.07 fixed it)
    forecast_bin_sum = sum([prob if prob is not None else 0 for prob in bin_probs])
    if not math.isclose(1.0, forecast_bin_sum, rel_tol=0.07):  # todo hard-coded magic number
        raise RuntimeError(f"Bin did not sum to 1.0. bin_probs={bin_probs}, forecast_bin_sum={forecast_bin_sum}, "
                           f"forecast={forecast}, location={location}, target={target}")


def _load_bincat_rows(forecast, rows):
    """
    Loads the rows in prediction_data_dict as BinCatDistributions.
    """
    # incoming rows: [location_name, target_name, cat, prob]

    # after this, rows will be: [location_id, target_id, cat, prob]:
    _replace_location_target_names_with_pks_rows(forecast, rows)

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


def _load_binlwr_rows(forecast, rows):
    """
    Loads the rows in rows as BinLwrDistributions.
    """
    # incoming rows: [location_name, target_name, lwr, prob]

    # after this, rows will be: [location_id, target_id, lwr, prob]:
    _replace_location_target_names_with_pks_rows(forecast, rows)

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


def _load_binary_rows(forecast, rows):
    """
    Loads the rows in rows as BinaryDistributions.
    """
    # incoming rows: [location_name, target_name, prob]

    # after this, rows will be: [location_id, target_id, prob]:
    _replace_location_target_names_with_pks_rows(forecast, rows)

    # after this, rows will be: [location_id, target_id, prob, self_pk]:
    _add_forecast_pk_rows(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = BinaryDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('prob').column,
                     Forecast._meta.model_name + '_id']
    _insert_rows(prediction_class, columns_names, rows)


def _load_named_rows(forecast, rows):
    """
    Loads the rows in rows as NamedDistribution concrete subclasses. Recall that each subclass has different IVs,
    so we use a hard-coded mapping to decide the subclass based on the `family` column.
    """
    # incoming rows: [location_name, target_name, family, param1, param2, param3]

    # after this, rows will be: [location_id, target_id, family, param1, param2, param3]:
    _replace_location_target_names_with_pks_rows(forecast, rows)

    # after this, rows will be: [location_id, target_id, family_id, param1, param2, param3]:
    _replace_family_abbrev_with_id_rows(rows)

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


def _load_point_rows(forecast, rows):
    """
    Validates and loads the rows in rows as PointPredictions.
    """
    # incoming rows: [location_name, target_name, value]

    # after this, rows will be: [location_id, target_id, value]:
    _replace_location_target_names_with_pks_rows(forecast, rows)

    # # validate rows
    # location_id_to_obj = {location.pk: location for location in forecast.forecast_model.project.locations.all()}
    # target_id_to_obj = {target.pk: target for target in forecast.forecast_model.project.targets.all()}
    # for location_id, target_id, value in rows:
    #     target = target_id_to_obj[target_id]
    #     if (not target.is_date) and (value is None):
    #         raise RuntimeError(f"Point value was non-numeric. forecast={forecast}, "
    #                            f"location={location_id_to_obj[location_id]}, target={target}")

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


def _load_sample_rows(forecast, rows):
    """
    Loads the rows in rows as SampleDistribution. See SAMPLE_DISTRIBUTION_HEADER.
    """
    # incoming rows: [location_name, target_name, sample]

    # after this, rows will be: [location_id, target_id, sample]:
    _replace_location_target_names_with_pks_rows(forecast, rows)

    # after this, rows will be: [location_id, target_id, sample, self_pk]:
    _add_forecast_pk_rows(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = SampleDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('sample').column,
                     Forecast._meta.model_name + '_id']
    _insert_rows(prediction_class, columns_names, rows)


def _load_samplecat_rows(forecast, rows):
    """
    Loads the rows in rows as SampleCatDistributions.
    """
    # incoming rows: [location_name, target_name, cat, sample]

    # after this, rows will be: [location_id, target_id, cat, sample]:
    _replace_location_target_names_with_pks_rows(forecast, rows)

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


def _replace_location_target_names_with_pks_rows(forecast, rows):
    """
    Does an in-place rows replacement of target and location names with PKs.
    """
    project = forecast.forecast_model.project

    # todo xx pass in:
    location_name_to_pk = {location.name: location.id for location in project.locations.all()}

    target_name_to_pk = {target.name: target.id for target in project.targets.all()}
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


def _replace_family_abbrev_with_id_rows(rows):
    """
    Does an in-place rows replacement of family abbreviations with ids in NamedDistribution.FAMILY_CHOICES (ints).
    """
    for row in rows:
        abbreviation = row[2]
        if abbreviation in NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.values():
            row[2] = [choice for choice, abbrev in NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.items()
                      if abbrev == abbreviation][0]
        else:
            raise RuntimeError(f"invalid family. abbreviation='{abbreviation}', "
                               f"abbreviations={NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.values()}")


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
