import csv
import io
from itertools import groupby

from django.db import connection, transaction

from forecast_app.models import NamedDistribution, PointPrediction, Forecast, Target, BinDistribution, \
    SampleDistribution
from forecast_app.models.project import POSTGRES_NULL_VALUE
from utils.utilities import YYYYMMDD_DATE_FORMAT


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
    example). See cdc-predictions.json for an example. Does not reuse that function's helper methods b/c the latter
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
    json_io_dict_from_forecast() helper

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
                point_value = PointPrediction.first_non_none_value(value_i, value_f, value_t)
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
    See cdc-predictions.json for an example.
    """
    return {"id": forecast.pk,
            "forecast_model_id": forecast.forecast_model.pk,
            "source": forecast.source,
            "created_at": forecast.created_at.isoformat(),
            "time_zero": {
                "timezero_date": forecast.time_zero.timezero_date.strftime(YYYYMMDD_DATE_FORMAT),
                "data_version_date": forecast.time_zero.data_version_date.strftime(YYYYMMDD_DATE_FORMAT)
                if forecast.time_zero.data_version_date else None
            }}


def _target_dicts_for_project(project, target_names):
    """
    json_io_dict_from_cdc_csv_file() helper that returns a list of target dicts for the 'targets' section of the exported
    json. See cdc-predictions.json for an example. only those in target_names are included
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

BIN_SUM_REL_TOL = 0.01  # hard-coded magic number for prediction probability sums


@transaction.atomic
def load_predictions_from_json_io_dict(forecast, json_io_dict):
    """
    Loads the prediction data into forecast from json_io_dict. Validates the forecast data. Note that we ignore the
    'meta' portion of json_io_dict. Errors if any referenced Locations and Targets do not exist in forecast's Project.

    :param forecast: a Forecast to load json_io_dict's predictions into
    :param json_io_dict: a "JSON IO dict" to load from. see docs for details
    """
    # validate predictions, convert them to class-specific quickly-loadable rows, and then load them by class
    if 'predictions' not in json_io_dict:
        raise RuntimeError(f"json_io_dict had no 'predictions' key: {json_io_dict}")

    prediction_dicts = json_io_dict['predictions']
    bin_rows, named_rows, point_rows, sample_rows = _prediction_dicts_to_validated_db_rows(forecast, prediction_dicts)
    target_pk_to_object = {target.pk: target for target in forecast.forecast_model.project.targets.all()}

    _load_bin_rows(forecast, bin_rows, target_pk_to_object)
    _load_named_rows(forecast, named_rows)
    _load_point_rows(forecast, point_rows, target_pk_to_object)
    _load_sample_rows(forecast, sample_rows, target_pk_to_object)


def _prediction_dicts_to_validated_db_rows(forecast, prediction_dicts):
    """
    Validates prediction_dicts and returns a 4-tuple of rows suitable for bulk-loading into a database:
        bin_rows, named_rows, point_rows, sample_rows
    Each row is Prediction class-specific. Skips zero-prob BinDistribution rows.

    :param forecast: a Forecast that's used to validate against
    :param prediction_dicts: the 'predictions' portion of a "JSON IO dict" as returned by
        json_io_dict_from_cdc_csv_file()
    """
    location_name_to_obj = {location.name: location for location in forecast.forecast_model.project.locations.all()}
    target_name_to_obj = {target.name: target for target in forecast.forecast_model.project.targets.all()}
    bin_rows, named_rows, point_rows, sample_rows = [], [], [], []  # return values. filled next
    for prediction_dict in prediction_dicts:
        location_name = prediction_dict['location']
        target_name = prediction_dict['target']
        prediction_class = prediction_dict['class']
        prediction_data = prediction_dict['prediction']

        # validate location and target names (applies to all prediction classes)
        if location_name not in location_name_to_obj:
            raise RuntimeError(f"prediction_dict referred to an undefined Location. location_name={location_name!r}. "
                               f"existing_location_names={location_name_to_obj.keys()}")

        if target_name not in target_name_to_obj:
            raise RuntimeError(f"prediction_dict referred to an undefined Target. target_name={target_name!r}. "
                               f"existing_target_names={target_name_to_obj.keys()}")

        # do class-specific validation and row collection
        # target = target_name_to_obj[target_name]
        # location = location_name_to_obj[location_name]
        if prediction_class == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution]:
            # _validate_bin_prob(forecast, location, target, prediction_data['prob'])
            for cat, prob in zip(prediction_data['cat'], prediction_data['prob']):
                if prob != 0:
                    bin_rows.append([location_name, target_name, cat, prob])
        elif prediction_class == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[NamedDistribution]:
            # family name validated in _replace_family_abbrev_with_id()
            named_rows.append([location_name, target_name, prediction_data['family'],
                               prediction_data.get('param1', None),
                               prediction_data.get('param2', None),
                               prediction_data.get('param3', None)])
        elif prediction_class == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction]:
            value = prediction_data['value']
            # if (not target.is_date) and (value is None):
            #     raise RuntimeError(f"Point value was non-numeric. forecast={forecast}, location={location}, "
            #                        f"target={target}")

            point_rows.append([location_name, target_name, value])
        elif prediction_class == PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleDistribution]:
            for sample in prediction_data['sample']:
                sample_rows.append([location_name, target_name, sample])
        else:
            raise RuntimeError(f"invalid prediction_class: {prediction_class!r}. must be one of: "
                               f"{list(PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS.values())}")
    return bin_rows, named_rows, point_rows, sample_rows


# def _validate_bin_prob(forecast, location, target, bin_probs):
#     # todo other validations!
#
#     # validate probs sum to 1.0
#     # note that the default rel_tol of 1e-09 failed for EW17-KoTstable-2017-05-09.csv
#     # (forecast_bin_sum=0.9614178215505512 -> 0.04 fixed it), and for EW17-KoTkcde-2017-05-09.csv
#     # (0.9300285798758262 -> 0.07 fixed it)
#     forecast_bin_sum = sum([prob if prob is not None else 0 for prob in bin_probs])
#     if not math.isclose(1.0, forecast_bin_sum, rel_tol=BIN_SUM_REL_TOL):
#         raise RuntimeError(f"Bin did not sum to 1.0. bin_probs={bin_probs}, forecast_bin_sum={forecast_bin_sum}, "
#                            f"forecast={forecast}, location={location}, target={target}")


# def _validate_bin_lwr(forecast, location, target, bin_lwrs):
#     # ensure bin_lwrs are a subset of the target's TargetBinLwrs. note that we test subsets and not lists b/c some
#     # forecasts do not generate bins with values of zero
#     target_binlwrs = target.binlwrs.all().values_list('lwr', flat=True)
#     if not (set(bin_lwrs) <= set(target_binlwrs)):
#         raise RuntimeError(f"BinLwr lwrs did not match Target. bin_lwrs={bin_lwrs}, target_binlwrs={target_binlwrs}"
#                            f"forecast={forecast}, location={location}, target={target}")


def _load_bin_rows(forecast, rows, target_pk_to_object):
    """
    Loads the rows in prediction_data_dict as BinCatDistributions.
    """
    # incoming rows: [location_name, target_name, cat, prob]

    # after this, rows will be: [location_id, target_id, cat, prob]:
    _replace_location_target_names_with_pks(forecast, rows)

    # after this, rows will be: [location_id, target_id, cat_i, cat_f, cat_t, cat_d, cat_b, prob]:
    _replace_value_with_five_types(rows, target_pk_to_object, is_exclude_last=True)

    # after this, rows will be: [location_id, target_id, cat_i, cat_f, cat_t, cat_d, cat_b, prob, self_pk]:
    _add_forecast_pks(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = BinDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
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
    # incoming rows: [location_name, target_name, family, param1, param2, param3]

    # after this, rows will be: [location_id, target_id, family, param1, param2, param3]:
    _replace_location_target_names_with_pks(forecast, rows)

    # after this, rows will be: [location_id, target_id, family_id, param1, param2, param3]:
    _replace_family_abbrev_with_id(rows)

    # after this, rows will be: [location_id, target_id, family_id, param1_or_0, param2_or_0, param3_or_0]:
    _replace_null_params_with_zeros(rows)

    # after this, rows will be: [location_id, target_id, family_id, param1, param2, param3, self_pk]:
    _add_forecast_pks(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = NamedDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
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
    # incoming rows: [location_name, target_name, value]

    # after this, rows will be: [location_id, target_id, value]:
    _replace_location_target_names_with_pks(forecast, rows)

    # # validate rows
    # location_id_to_obj = {location.pk: location for location in forecast.forecast_model.project.locations.all()}
    # target_id_to_obj = {target.pk: target for target in forecast.forecast_model.project.targets.all()}
    # for location_id, target_id, value in rows:
    #     target = target_id_to_obj[target_id]
    #     if (not target.is_date) and (value is None):
    #         raise RuntimeError(f"Point value was non-numeric. forecast={forecast}, "
    #                            f"location={location_id_to_obj[location_id]}, target={target}")

    # after this, rows will be: [location_id, target_id, value_i, value_f, value_t]:
    _replace_value_with_five_types(rows, target_pk_to_object, is_exclude_last=False)

    # after this, rows will be: [location_id, target_id, value_i, value_f, value_t, self_pk]:
    _add_forecast_pks(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = PointPrediction
    columns_names = [prediction_class._meta.get_field('location').column,
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
    # incoming rows: [location_name, target_name, sample]

    # after this, rows will be: [location_id, target_id, sample]:
    _replace_location_target_names_with_pks(forecast, rows)

    # after this, rows will be: [location_id, target_id, sample_i, sample_f, sample_t, sample_d, sample_b]:
    _replace_value_with_five_types(rows, target_pk_to_object, is_exclude_last=False)

    # after this, rows will be: [location_id, target_id, sample, self_pk]:
    _add_forecast_pks(forecast, rows)

    # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use ForecastData._meta.fields ?
    prediction_class = SampleDistribution
    columns_names = [prediction_class._meta.get_field('location').column,
                     prediction_class._meta.get_field('target').column,
                     prediction_class._meta.get_field('sample_i').column,
                     prediction_class._meta.get_field('sample_f').column,
                     prediction_class._meta.get_field('sample_t').column,
                     prediction_class._meta.get_field('sample_d').column,
                     prediction_class._meta.get_field('sample_b').column,
                     Forecast._meta.model_name + '_id']
    _insert_prediction_rows(prediction_class, columns_names, rows)


def _replace_location_target_names_with_pks(forecast, rows):
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


def _replace_value_with_five_types(rows, target_pk_to_object, is_exclude_last):
    """
    Does an in-place rows replacement of values with the five type-specific values based on each row's Target's
    data_type. The values: value_i, value_f, value_t, value_d, value_b. Recall that exactly one will be non-NULL (i.e.,
    not None).

    :param rows: a list of lists of the form: [location_id, target_id, value, [last_item]], where last_item is optional
        and is indicated by is_exclude_last
    :param is_exclude_last: True if the last item should be preserved, and False o/w
    :return: rows, but with the value_idx replaced with the above five type-specific values, i.e.,
        [location_id, target_id, value_i, value_f, value_t, value_d, value_b, [last_item]]
    """
    value_idx = 2
    for row in rows:
        target_pk = row[1]
        target = target_pk_to_object[target_pk]
        data_type = Target.data_type(target.type)
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
            raise RuntimeError(f"invalid family. abbreviation='{abbreviation}', "
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
