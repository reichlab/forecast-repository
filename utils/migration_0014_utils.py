import logging
import timeit
from itertools import groupby

import django
from django.db import transaction, connection
from django.shortcuts import get_object_or_404

# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
from forecast_app.models.prediction_element import PRED_CLASS_NAME_TO_INT

django.setup()

from forecast_app.models import PredictionData, Prediction, BinDistribution, NamedDistribution, PointPrediction, \
    SampleDistribution, QuantileDistribution, PredictionElement, Target, Forecast, ForecastModel
from utils.forecast import load_predictions_from_json_io_dict, json_io_dict_from_forecast
from utils.utilities import YYYY_MM_DD_DATE_FORMAT

logger = logging.getLogger(__name__)


#
# This file defines (dangerous!) functions that are used to assist the migration 0014_predictionelement_1_of_2.py.
#


#
# copy_old_data_to_new_tables() and helpers
#

def copy_old_data_to_new_tables(forecast):
    """
    Top-level migration function that copies data from all of function's concrete Prediction tables to its concrete
    PredictionData tables. For convenience (it's a little complicated - see load_predictions_from_json_io_dict()'s docs
    about the two passes required) we use existing json_io_dict-orientedfunctions instead of using SQL. (We will see if
    this is a performance issue.) This function operates at the forecast level, and can be re-started to pick up at the
    last forecast where it left off, in case of failures.

    :param forecast: a Forecast
    """
    if _num_rows_new_data(forecast) != 0:
        logger.info(f"copy_old_data_to_new_tables(): {forecast}: skipping (has data)")
        return  # already loaded

    start_time = timeit.default_timer()
    logger.info(f"copy_old_data_to_new_tables(): {forecast}: started")
    json_io_dict = {'meta': {}, 'predictions': _pred_dicts_from_forecast_old(forecast)}
    load_predictions_from_json_io_dict(forecast, json_io_dict, is_skip_validation=True,
                                       is_validate_cats=False)  # atomic
    logger.info(f"copy_old_data_to_new_tables(): {forecast}: done. time: {timeit.default_timer() - start_time}")


def _num_rows_old_data(forecast):
    """
    :param forecast: a Forecast
    :return: number of rows in all of forecast's concrete Prediction tables
    """
    return sum(concrete_prediction_class.objects.filter(forecast=forecast).count()
               for concrete_prediction_class in Prediction.concrete_subclasses())


def _num_rows_new_data(forecast):
    """
    Computed dynamically and does not use ForecastMetaPrediction, so might be slow.

    :param forecast: a Forecast
    :return: number of rows in all of project's PredictionData
    """
    num_rows = 0
    pred_data_qs = PredictionData.objects.filter(pred_ele__forecast=forecast) \
        .values_list('pred_ele__pred_class', 'data')
    for pred_class, pred_data in pred_data_qs:
        if pred_class == PredictionElement.BIN_CLASS:
            num_rows += len(pred_data['cat'])
        elif pred_class == PredictionElement.NAMED_CLASS:
            num_rows += 1
        elif pred_class == PredictionElement.POINT_CLASS:
            num_rows += 1
        elif pred_class == PredictionElement.QUANTILE_CLASS:
            num_rows += len(pred_data['quantile'])
        elif pred_class == PredictionElement.SAMPLE_CLASS:
            num_rows += len(pred_data['sample'])
    return num_rows


def delete_old_data(forecast):
    """
    Deletes all rows from all of forecast's concrete Prediction tables.

    :param forecast: a Forecast
    """
    logger.info(f"delete_old_data(): {forecast}: started")
    for concrete_prediction_class in Prediction.concrete_subclasses():
        concrete_prediction_class.objects.filter(forecast=forecast).delete()
    logger.info(f"delete_old_data(): {forecast}: done")


def _delete_new_data(project):
    """
    Deletes all rows from all of project's PredictionElement and concrete PredictionData tables.

    :param project: a Project
    """
    # cascade deletes PredictionData subclass tables' data
    PredictionElement.objects.filter(forecast__forecast_model__project=project).delete()


#
# _copy_new_data_to_old_tables()
#

def _copy_new_data_to_old_tables(project):
    """
    Test helper function that copies data from all of project's concrete PredictionData tables to its concrete
    Prediction tables.

    :param project: a Project
    """
    unit_name_to_obj = {unit.name: unit for unit in project.units.all()}
    target_name_to_obj = {target.name: target for target in project.targets.all()}
    family_abbrev_to_int = {abbreviation: family_int for family_int, abbreviation
                            in NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.items()}
    for forecast in Forecast.objects.filter(forecast_model__project=project):
        json_io_dict_new = json_io_dict_from_forecast(forecast, None)
        for pred_ele in json_io_dict_new['predictions']:
            unit = unit_name_to_obj[pred_ele['unit']]
            target = target_name_to_obj[pred_ele['target']]
            pred_class = PRED_CLASS_NAME_TO_INT[pred_ele['class']]
            pred_data = pred_ele['prediction']
            if pred_class == PredictionElement.BIN_CLASS:
                for cat, prob in zip(pred_data['cat'], pred_data['prob']):
                    cat_i = cat if target.type == Target.DISCRETE_TARGET_TYPE else None
                    cat_f = cat if target.type == Target.CONTINUOUS_TARGET_TYPE else None
                    cat_t = cat if target.type == Target.NOMINAL_TARGET_TYPE else None
                    cat_d = cat if target.type == Target.DATE_TARGET_TYPE else None
                    cat_b = cat if target.type == Target.BINARY_TARGET_TYPE else None
                    BinDistribution.objects.create(forecast=forecast, unit=unit, target=target,
                                                   prob=prob, cat_i=cat_i, cat_f=cat_f, cat_t=cat_t, cat_d=cat_d,
                                                   cat_b=cat_b)
            elif pred_class == PredictionElement.NAMED_CLASS:
                NamedDistribution.objects.create(forecast=forecast, unit=unit, target=target,
                                                 family=family_abbrev_to_int[pred_data['family']],
                                                 param1=pred_data.get('param1'), param2=pred_data.get('param2'),
                                                 param3=pred_data.get('param3'))
            elif pred_class == PredictionElement.POINT_CLASS:
                value_i = pred_data['value'] if target.type == Target.DISCRETE_TARGET_TYPE else None
                value_f = pred_data['value'] if target.type == Target.CONTINUOUS_TARGET_TYPE else None
                value_t = pred_data['value'] if target.type == Target.NOMINAL_TARGET_TYPE else None
                value_d = pred_data['value'] if target.type == Target.DATE_TARGET_TYPE else None
                value_b = pred_data['value'] if target.type == Target.BINARY_TARGET_TYPE else None
                PointPrediction.objects.create(forecast=forecast, unit=unit, target=target,
                                               value_i=value_i, value_f=value_f, value_t=value_t, value_d=value_d,
                                               value_b=value_b)
            elif pred_class == PredictionElement.QUANTILE_CLASS:
                for quantile, value in zip(pred_data['quantile'], pred_data['value']):
                    value_i = value if target.type == Target.DISCRETE_TARGET_TYPE else None
                    value_f = value if target.type == Target.CONTINUOUS_TARGET_TYPE else None
                    value_d = value if target.type == Target.DATE_TARGET_TYPE else None
                    QuantileDistribution.objects.create(forecast=forecast, unit=unit, target=target,
                                                        quantile=quantile, value_i=value_i, value_f=value_f,
                                                        value_d=value_d)
            elif pred_class == PredictionElement.SAMPLE_CLASS:
                for sample in pred_data['sample']:
                    sample_i = sample if target.type == Target.DISCRETE_TARGET_TYPE else None
                    sample_f = sample if target.type == Target.CONTINUOUS_TARGET_TYPE else None
                    sample_t = sample if target.type == Target.NOMINAL_TARGET_TYPE else None
                    sample_d = sample if target.type == Target.DATE_TARGET_TYPE else None
                    sample_b = sample if target.type == Target.BINARY_TARGET_TYPE else None
                    SampleDistribution.objects.create(forecast=forecast, unit=unit, target=target,
                                                      sample_i=sample_i, sample_f=sample_f, sample_t=sample_t,
                                                      sample_d=sample_d, sample_b=sample_b)


#
# _pred_dicts_from_forecast_old()
#

PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS = {
    BinDistribution: 'bin',
    NamedDistribution: 'named',
    PointPrediction: 'point',
    SampleDistribution: 'sample',
    QuantileDistribution: 'quantile',
}


def _pred_dicts_from_forecast_old(forecast):
    """
    json_io_dict_from_forecast() helper

    :param forecast: the Forecast to read predictions from
    :return: prediction_dicts
    """
    # recall Django's limitations in handling abstract classes and polymorphic models - asking for all of a Forecast's
    # Predictions returns base Prediction instances (forecast, unit, and target) without subclass fields (e.g.,
    # PointPrediction.value). so we have to handle each Prediction subclass individually. this implementation loads
    # all instances of each concrete subclass into memory, ordered by (unit, target) for groupby(). note: b/c the
    # code for each class is so similar, I had implemented an abstraction, but it turned out to be longer and more
    # complicated, and IMHO didn't warrant eliminating the duplication
    target_name_to_obj = {target.name: target for target in forecast.forecast_model.project.targets.all()}
    prediction_dicts = []  # filled next for each Prediction subclass

    # PointPrediction
    point_qs = PointPrediction.objects.filter(forecast=forecast) \
        .order_by('pk') \
        .values_list('unit__name', 'target__name', 'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
    for unit_name, target_values_grouper in groupby(point_qs, key=lambda _: _[0]):
        for target_name, values_grouper in groupby(target_values_grouper, key=lambda _: _[1]):
            is_date_target = (Target.DATE_DATA_TYPE in target_name_to_obj[target_name].data_types())
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
    named_qs = NamedDistribution.objects.filter(forecast=forecast) \
        .order_by('pk') \
        .values_list('unit__name', 'target__name', 'family', 'param1', 'param2', 'param3')
    for unit_name, target_family_params_grouper in groupby(named_qs, key=lambda _: _[0]):
        for target_name, family_params_grouper in groupby(target_family_params_grouper, key=lambda _: _[1]):
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
    bin_qs = BinDistribution.objects.filter(forecast=forecast) \
        .order_by('pk') \
        .values_list('unit__name', 'target__name', 'prob', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')
    for unit_name, target_prob_cat_grouper in groupby(bin_qs, key=lambda _: _[0]):
        for target_name, prob_cat_grouper in groupby(target_prob_cat_grouper, key=lambda _: _[1]):
            is_date_target = target_name_to_obj[target_name].type == Target.DATE_TARGET_TYPE
            bin_cats, bin_probs = [], []
            for _, _, prob, cat_i, cat_f, cat_t, cat_d, cat_b in prob_cat_grouper:
                cat_value = PointPrediction.first_non_none_value(cat_i, cat_f, cat_t, cat_d, cat_b)
                if is_date_target:
                    cat_value = cat_value.strftime(YYYY_MM_DD_DATE_FORMAT)
                bin_cats.append(cat_value)
                bin_probs.append(prob)
            prediction_dicts.append({'unit': unit_name, 'target': target_name,
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution],
                                     'prediction': {'cat': bin_cats, 'prob': bin_probs}})

    # SampleDistribution
    sample_qs = SampleDistribution.objects.filter(forecast=forecast) \
        .order_by('pk') \
        .values_list('unit__name', 'target__name', 'sample_i', 'sample_f', 'sample_t', 'sample_d', 'sample_b')
    for unit_name, target_sample_grouper in groupby(sample_qs, key=lambda _: _[0]):
        for target_name, sample_grouper in groupby(target_sample_grouper, key=lambda _: _[1]):
            is_date_target = target_name_to_obj[target_name].type == Target.DATE_TARGET_TYPE
            sample_cats, sample_probs = [], []
            for _, _, sample_i, sample_f, sample_t, sample_d, sample_b in sample_grouper:
                sample_value = PointPrediction.first_non_none_value(sample_i, sample_f, sample_t, sample_d, sample_b)
                if is_date_target:
                    sample_value = sample_value.strftime(YYYY_MM_DD_DATE_FORMAT)
                sample_cats.append(sample_value)
            prediction_dicts.append({'unit': unit_name, 'target': target_name,
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[SampleDistribution],
                                     'prediction': {'sample': sample_cats}})

    # QuantileDistribution
    quantile_qs = QuantileDistribution.objects.filter(forecast=forecast) \
        .order_by('pk') \
        .values_list('unit__name', 'target__name', 'quantile', 'value_i', 'value_f', 'value_d')
    for unit_name, target_quant_val_grouper in groupby(quantile_qs, key=lambda _: _[0]):
        for target_name, quant_val_grouper in groupby(target_quant_val_grouper, key=lambda _: _[1]):
            is_date_target = target_name_to_obj[target_name].type == Target.DATE_TARGET_TYPE
            quant_quantiles, quant_values = [], []
            for _, _, quantile, value_i, value_f, value_d in quant_val_grouper:
                quantile_value = PointPrediction.first_non_none_value(value_i, value_f, value_d, None, None)
                if is_date_target:
                    quantile_value = quantile_value.strftime(YYYY_MM_DD_DATE_FORMAT)
                quant_quantiles.append(quantile)
                quant_values.append(quantile_value)
            prediction_dicts.append({'unit': unit_name, 'target': target_name,
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[QuantileDistribution],
                                     'prediction': {'quantile': quant_quantiles, 'value': quant_values}})

    # done
    return prediction_dicts


#
# _migrate_forecast_worker()
#

def _migrate_forecast_worker(forecast_pks):
    """
    enqueue() helper function

    :param forecast_pks: list of Forecast.pk ids to process in order
    """
    for forecast_pk in forecast_pks:
        forecast = get_object_or_404(Forecast, pk=forecast_pk)
        try:
            with transaction.atomic():
                copy_old_data_to_new_tables(forecast)
                # delete_old_data(forecast)  # todo xx uncomment!
        except Exception as ex:
            logger.error(f"_migrate_forecast_worker(): error: {ex!r}. forecast={forecast}")


#
# _migrate_correctness_worker()
#

def _migrate_correctness_worker(forecast_pk):
    """
    enqueue() helper function
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    is_different = is_different_old_new_json(forecast)
    if is_different:
        logger.error(f"forecast={forecast}. {is_different}")


def is_different_old_new_json(forecast):
    """
    Acts as a boolean by returning an error string if the json exported from old and new versions of `forecast` are
    equal, or None not. NB: Does not take into account the subtleties of forecast versions, so output needs to be
    studied with that in mind.
    """

    def sort_key(pred_dict):
        return pred_dict['unit'], pred_dict['target'], pred_dict['class']

    prediction_dicts_new = sorted(json_io_dict_from_forecast(forecast, None)['predictions'], key=sort_key)
    prediction_dicts_old = sorted(_pred_dicts_from_forecast_old(forecast), key=sort_key)
    if prediction_dicts_new == prediction_dicts_old:
        return None

    set_old = {(pred_dict['unit'], pred_dict['target'], pred_dict['class']) for pred_dict in prediction_dicts_old}
    set_new = {(pred_dict['unit'], pred_dict['target'], pred_dict['class']) for pred_dict in prediction_dicts_new}
    # total_old, total_new, set_old, set_new, old-new, old>new, old<new
    return f"{_num_rows_old_data(forecast)}, {_num_rows_new_data(forecast)}, {len(set_old)}, {len(set_new)}, " \
           f"{len(set_old - set_new)}, {set_old > set_new}, {set_old < set_new}"


#
# _grouped_version_rows()
#

def _grouped_version_rows(project, is_versions_only):
    """
    Returns rows corresponding to forecast versions in `project`, suitable for groupby().

    :param project: a Project
    :param is_versions_only: True if only forecasts with versions should be returned. o/w returns all
    :return rows: list of 7-tuples: (fm_id, tz_id, issue_date, f_id, f_source, f_created_at, rank)
    """
    where_count = f"WHERE count > 1" if is_versions_only else ""
    sql = f"""
        WITH ranked_rows AS (
            SELECT f.forecast_model_id                       AS fm_id,
                   f.time_zero_id                            AS tz_id,
                   f.issue_date                              AS issue_date,
                   f.id                                      AS f_id,
                   f.source                                  AS f_source,
                   f.created_at                              AS f_created_at,
                   RANK() OVER (PARTITION BY f.forecast_model_id,
                       f.time_zero_id ORDER BY f.issue_date) AS rank,
                   COUNT(*) OVER (PARTITION BY f.forecast_model_id,
                       f.time_zero_id)                       AS count
            FROM {Forecast._meta.db_table} AS f
                     JOIN {ForecastModel._meta.db_table} fm ON f.forecast_model_id = fm.id
            WHERE fm.project_id = %s
        )
        SELECT fm_id, tz_id, issue_date, f_id, f_source, f_created_at, rank
        FROM ranked_rows AS rr
        {where_count}
        ORDER BY fm_id, tz_id, issue_date;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        return cursor.fetchall()


#
# pred_dicts_with_implicit_retractions_old() and friends
#

def pred_dicts_with_implicit_retractions_old(f1, f2):
    """
    :param f1: a Forecast with old data
    :param f2: "" that is a subset of f1
    :return: a json_io_dict constructed from f1 and f2 that contains implicit retractions in f2. NB: limited to points
        and quantiles only
    """
    from utils.version_info_app import _forecast_diff_old  # avoid circular imports

    unit_id_to_obj = {unit.pk: unit for unit in f1.forecast_model.project.units.all()}
    target_id_to_obj = {target.pk: target for target in f1.forecast_model.project.targets.all()}

    # is_point, is_intersect, is_pred_eles, is_count
    pred_eles_f1_not_in_f2_p = _forecast_diff_old(f1.pk, f2.pk, True, False, True, False)
    pred_eles_f1_not_in_f2_q = _forecast_diff_old(f1.pk, f2.pk, False, False, True, False)
    f2_predictions = _pred_dicts_from_forecast_old(f2)
    for unit_id, target_id in pred_eles_f1_not_in_f2_p:
        f2_predictions.append({"unit": unit_id_to_obj[unit_id].name,
                               "target": target_id_to_obj[target_id].name,
                               "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction],
                               "prediction": None})

    for unit_id, target_id in pred_eles_f1_not_in_f2_q:
        f2_predictions.append({"unit": unit_id_to_obj[unit_id].name,
                               "target": target_id_to_obj[target_id].name,
                               "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[QuantileDistribution],
                               "prediction": None})

    return f2_predictions


def _forecast_previous_version(forecast):
    """
    :param forecast:
    :return: the forecast immediately preceding `forecast`, or None if `forecast` is the first version
    """
    # recall forecast version tuples: (forecast_model_id, timezero_id, issue_date)
    forecast_versions = Forecast.objects \
        .filter(forecast_model=forecast.forecast_model,
                time_zero=forecast.time_zero,
                issue_date__lt=forecast.issue_date) \
        .order_by('-issue_date')
    return forecast_versions[0] if forecast_versions else None
