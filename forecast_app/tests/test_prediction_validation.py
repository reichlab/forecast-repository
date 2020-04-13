import datetime
from pathlib import Path

from django.test import TestCase

from forecast_app.models import ForecastModel, TimeZero, Forecast, NamedDistribution, Target
from forecast_app.models.target import TargetRange
from utils.forecast import load_predictions_from_json_io_dict
from utils.project import create_project_from_json, load_truth_data
from utils.utilities import get_or_create_super_po_mo_users


#
# tests the validations in docs/Validation.md at https://github.com/reichlab/docs.zoltardata/
#

# the following variable helps test named distributions by associating applicable docs-project.json targets that have
# valid types for each family with a tuple of ok_params (the correct count and valid values), plus a list of bad_params
# that have correct counts but one or more out-of-range values. we have one tuple of bad params for each combination of
# variables. the two targets we use are the only two that NamedDistributions are valid for: 'pct next week' (continuous)
# and 'cases next week' (discrete). comments before each family/key indicate params ('-' means no paramN)

FAMILY_TO_TARGET_OK_BAD_PARAMS = {
    'norm': ('pct next week', (0.0, 0.0), [(0.0, -0.1)]),  # | mean | sd>=0 | - |
    'lnorm': ('pct next week', (0.0, 0.0), [(0.0, -0.1)]),  # | mean | sd>=0 | - |
    'gamma': ('pct next week', (0.1, 0.1), [(0.0, 0.1), (0.1, 0.0), (0.0, 0.0)]),  # | shape>0 |rate>0 | - |
    'beta': ('pct next week', (0.1, 0.1), [(0.0, 0.1), (0.1, 0.0), (0.0, 0.0)]),  # | a>0 | b>0 | - |
    'pois': ('cases next week', (0.1,), [(0,)]),  # | rate>0 |  - | - |
    'nbinom': ('cases next week', (0.1, 0.1), [(0.0, 0.1), (0.1, -0.1), (0.0, -0.1)]),  # | r>0 | 0<=p<=1 | - |
    'nbinom2': ('cases next week', (0.1, 0.1), [(0.0, 0.1), (0.1, 0.0), (0.0, 0.0)])  # | mean>0 | disp>0 | - |
}


class PredictionValidationTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        cls.project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=cls.project)
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=datetime.date(2017, 1, 1))
        cls.forecast = Forecast.objects.create(forecast_model=forecast_model, time_zero=time_zero)


    # ----
    # Tests for all Prediction Elements
    # ----

    def test_the_predictions_class_must_be_valid_for_its_targets_type(self):
        # note: we do not test docs-predictions.json here (it's tested elsewhere), which takes care of testing all
        # target_type/prediction_class combinations. here we just test the valid NamedDistribution family/target_type
        # combinations, which are the only ones that are constrained

        #   target type   | docs-project.json target | valid named distributions
        #   ------------- | ------------------------ | --------------------------------
        #   continuous    | "pct next week"          | `norm`, `lnorm`, `gamma`, `beta`
        #   discrete      | "cases next week"        | `pois`, `nbinom`, `nbinom2`
        #   nominal       | "season severity"        | none
        #   binary        | "above baseline"         | none
        #   date          | "Season peak week"       | none

        target_name_to_is_valid_family_tuple = {  # is_valid: 7-tuples: t/f in below order ('norm', 'lnorm', ...)
            "pct next week": (True, True, True, True, False, False, False),
            "cases next week": (False, False, False, False, True, True, True),
            "season severity": (False, False, False, False, False, False, False),
            "above baseline": (False, False, False, False, False, False, False),
            "Season peak week": (False, False, False, False, False, False, False)}
        for target_name, is_valid_family_tuple in target_name_to_is_valid_family_tuple.items():
            for family_int, is_valid in zip([NamedDistribution.NORM_DIST, NamedDistribution.LNORM_DIST,
                                             NamedDistribution.GAMMA_DIST, NamedDistribution.BETA_DIST,
                                             NamedDistribution.POIS_DIST, NamedDistribution.NBINOM_DIST,
                                             NamedDistribution.NBINOM2_DIST],
                                            is_valid_family_tuple):
                family_abbrev = NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION[family_int]
                prediction_dict = {"unit": "location1", "target": target_name, "class": "named",
                                   "prediction": {"family": family_abbrev}}  # add paramN next based on ok_params:
                ok_params = FAMILY_TO_TARGET_OK_BAD_PARAMS[family_abbrev][1]
                prediction_dict['prediction']["param1"] = ok_params[0]  # all families have param1
                if len(ok_params) > 1:
                    prediction_dict['prediction']["param2"] = ok_params[1]
                if len(ok_params) > 2:
                    prediction_dict['prediction']["param3"] = ok_params[2]
                if is_valid:  # valid: should not raise
                    try:
                        load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
                    except Exception as ex:
                        self.fail(f"unexpected exception: {ex}")
                else:  # invalid: should raise
                    with self.assertRaises(RuntimeError) as context:
                        load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
                    self.assertIn(f"family {family_abbrev!r} is not valid for",
                                  str(context.exception))


    def test_within_a_prediction_there_cannot_be_no_more_than_1_prediction_element_of_the_same_class(self):
        prediction_dict = {"unit": "location1", "target": "pct next week", "class": "point",
                           "prediction": {"value": 1.1}}  # duplicated unit/target pair
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict] * 2})
        self.assertIn(f"Within a Prediction, there cannot be more than 1 Prediction Element of the same class",
                      str(context.exception))

        prediction_dict2 = dict(prediction_dict)  # copy, but with different unit/target pair
        prediction_dict2['unit'] = 'location2'
        try:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict, prediction_dict2]})
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")


    # ----
    # Tests for Prediction Elements by Prediction Class
    # ----

    #
    # `Bin` Prediction Elements
    #

    # `|cat| = |prob|`
    def test_the_number_of_elements_in_cat_and_prob_should_be_identical(self):
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "bin",
                           "prediction": {"cat": [2.2, 3.3],
                                          "prob": [0.3, 0.2, 0.5]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The number of elements in the 'cat' and 'prob' vectors should be identical.",
                      str(context.exception))


    # `cat` (i, f, t, d, b)
    def test_entries_in_the_database_rows_in_the_cat_column_cannot_be_empty_na_or_null(self):
        prediction_dict = {"unit": "location1", "target": "season severity", "class": "bin",
                           "prediction": {"cat": ["mild", "", "severe"],  # empty
                                          "prob": [0.0, 0.1, 0.9]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        prediction_dict = {"unit": "location1", "target": "season severity", "class": "bin",
                           "prediction": {"cat": ["mild", "NA", "severe"],  # NA
                                          "prob": [0.0, 0.1, 0.9]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        prediction_dict = {"unit": "location1", "target": "season severity", "class": "bin",
                           "prediction": {"cat": ["mild", None, "severe"],  # null
                                          "prob": [0.0, 0.1, 0.9]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))


    # `cat` (i, f, t, d, b)
    def test_entries_in_cat_must_be_a_subset_of_target_cats_from_the_target_definition(self):
        # "pct next week": continuous. cats: [0.0, 1.0, 1.1, 2.0, 2.2, 3.0, 3.3, 5.0, 10.0, 50.0]
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "bin",
                           "prediction": {"cat": [1.1, 2.2, -1.0],  # -1.0 not in cats
                                          "prob": [0.3, 0.2, 0.5]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `cat` must be a subset of `Target.cats` from the target definition",
                      str(context.exception))

        # "cases next week": discrete. cats: [0, 2, 50]
        prediction_dict = {"unit": "location3", "target": "cases next week", "class": "bin",
                           "prediction": {"cat": [-1, 1, 2],  # -1 not in cats
                                          "prob": [0.0, 0.1, 0.9]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `cat` must be a subset of `Target.cats` from the target definition",
                      str(context.exception))

        # "season severity": nominal. cats: ["high", "mild", "moderate", "severe"]
        prediction_dict = {"unit": "location1", "target": "season severity", "class": "bin",
                           "prediction": {"cat": ["mild", "-1", "severe"],  # '-1" not in cats
                                          "prob": [0.0, 0.1, 0.9]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `cat` must be a subset of `Target.cats` from the target definition",
                      str(context.exception))

        # "Season peak week": date. cats: ["2019-12-15", "2019-12-22", "2019-12-29", "2020-01-05"]
        prediction_dict = {"unit": "location1", "target": "Season peak week", "class": "bin",
                           "prediction": {
                               "cat": ["2019-12-15", "2019-12-22", "2020-01-11"],  # "2020-01-11" not in cats
                               "prob": [0.01, 0.1, 0.89]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `cat` must be a subset of `Target.cats` from the target definition",
                      str(context.exception))


    # `prob` (f): [0, 1]
    def test_entries_in_the_database_rows_in_the_prob_column_must_be_numbers_in_0_1(self):
        # test precursor: prob column can only contain ints or floats
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "bin",
                           "prediction": {"cat": [1.1, 2.2, 3.3],
                                          "prob": [0.3, '0.2', 0.5]}}  # '0.2' not int or float
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"wrong data type in `prob` column, which should only contain ints or floats",
                      str(context.exception))

        # "pct next week": continuous. cats: [0.0, 1.0, 1.1, 2.0, 2.2, 3.0, 3.3, 5.0, 10.0, 50.0]
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "bin",
                           "prediction": {"cat": [1.1, 2.2, 3.3],
                                          "prob": [-1.1, 0.2, 0.5]}}  # -1.1 not in [0, 1]
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `prob` column must be numbers in [0, 1]",
                      str(context.exception))

        # "cases next week": discrete. cats: [0, 2, 50]
        prediction_dict = {"unit": "location3", "target": "cases next week", "class": "bin",
                           "prediction": {"cat": [0, 2, 50],
                                          "prob": [1.1, 0.1, 0.9]}}  # 1.1 not in [0, 1]
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `prob` column must be numbers in [0, 1]",
                      str(context.exception))


    # `prob` (f): [0, 1]
    def test_for_one_prediction_element_the_values_within_prob_must_sum_to_1_0(self):
        # Note that for binary targets that by definition need only have one row, this validation does not apply.
        # "season severity": nominal. cats: ["high", "mild", "moderate", "severe"].
        # recall: BIN_SUM_REL_TOL
        prediction_dict = {"unit": "location1", "target": "season severity", "class": "bin",
                           "prediction": {"cat": ["mild", "moderate", "severe"],
                                          "prob": [1.0, 0.1, 0.9]}}  # sums to 2.0, not 1.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"For one prediction element, the values within prob must sum to 1.0",
                      str(context.exception))

        # "Season peak week": date. cats: ["2019-12-15", "2019-12-22", "2019-12-29", "2020-01-05"]
        prediction_dict = {"unit": "location1", "target": "Season peak week", "class": "bin",
                           "prediction": {"cat": ["2019-12-15", "2019-12-22", "2019-12-29"],
                                          "prob": [0.01, 0.1, 0.8]}}  # sums to 0.91, not 1.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"For one prediction element, the values within prob must sum to 1.0",
                      str(context.exception))


    def test_data_format_of_cat_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition(self):
        # 'pct next week': continuous
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "bin",
                           "prediction": {"cat": [1.1, '2.2', 3.3],
                                          "prob": [0.3, 0.2, 0.5]}}  # cat not float
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `cat` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'cases next week: discrete
        prediction_dict = {"unit": "location3", "target": "cases next week", "class": "bin",
                           "prediction": {"cat": [0, 2.2, 50],
                                          "prob": [0.0, 0.1, 0.9]}}  # cat not translatable to int
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `cat` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'season severity: nominal
        prediction_dict = {"unit": "location1", "target": "season severity", "class": "bin",
                           "prediction": {"cat": ["mild", True, "severe"],
                                          "prob": [0.0, 0.1, 0.9]}}  # cat not str
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `cat` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'above baseline: binary
        prediction_dict = {"unit": "location2", "target": "above baseline", "class": "bin",
                           "prediction": {"cat": [True, "not boolean"],
                                          "prob": [0.9, 0.1]}}  # cat not boolean
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `cat` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'Season peak week: date
        prediction_dict = {"unit": "location1", "target": "Season peak week", "class": "bin",
                           "prediction": {"cat": ["2019-12-15", 1.1, "2019-12-29"],
                                          "prob": [0.01, 0.1, 0.89]}}  # cat not date
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `cat` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'Season peak week: date
        prediction_dict = {"unit": "location1", "target": "Season peak week", "class": "bin",
                           "prediction": {"cat": ["2019-12-15", "x 2019-12-22", "2019-12-29"],
                                          "prob": [0.01, 0.1, 0.89]}}  # cat not date format
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `cat` should correspond or be translatable to the `type` as in the",
                      str(context.exception))


    #
    # `Named` Prediction Elements
    #

    # `family`
    def test_family_must_be_one_of_the_abbreviations_shown_in_the_table_below(self):
        # note that test_target_type_to_valid_named_families() test the underlying Target.valid_named_families()
        # function, but here we test that indirectly via load_predictions_from_json_io_dict().
        prediction_dict = {"unit": "location1", "target": "pct next week", "class": "named",
                           "prediction": {"family": "bad family",
                                          "param1": 1.1,
                                          "param2": 2.2}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"family must be one of the abbreviations shown in the table below",
                      str(context.exception))


    # # this is tested by test_parameters_for_each_distribution_must_be_within_valid_ranges()
    # def test_the_number_of_param_columns_with_non_null_entries_count_must_match_family_definition(self):
    #     pass


    # `param1`, `param2`, `param3` (f)
    def test_parameters_for_each_distribution_must_be_within_valid_ranges_1(self):
        for family_abbrev, (target_name, ok_params, bad_params_list) in FAMILY_TO_TARGET_OK_BAD_PARAMS.items():
            # test valid params using ok_params
            try:
                prediction_dict = {"unit": "location1", "target": target_name, "class": "named",
                                   "prediction": {"family": family_abbrev,
                                                  "param1": ok_params[0]}}  # all have param1. add 2&3 next if needed
                if len(ok_params) > 1:
                    prediction_dict['prediction']["param2"] = ok_params[1]
                if len(ok_params) > 2:
                    prediction_dict['prediction']["param3"] = ok_params[2]
                load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")


    # `param1`, `param2`, `param3` (f)
    def test_parameters_for_each_distribution_must_be_within_valid_ranges_2(self):
        for family_abbrev, (target_name, ok_params, bad_params_list) in FAMILY_TO_TARGET_OK_BAD_PARAMS.items():
            # test invalid param count by removing one param from ok_params
            ok_params = ok_params[:-1]  # discard the first. list may now be [], so no default 'param1' in dict
            prediction_dict = {"unit": "location1", "target": target_name, "class": "named",
                               "prediction": {"family": family_abbrev}}  # no param1
            with self.assertRaises(RuntimeError) as context:
                if len(ok_params) > 0:
                    prediction_dict['prediction']["param1"] = ok_params[0]
                if len(ok_params) > 1:
                    prediction_dict['prediction']["param2"] = ok_params[1]
                if len(ok_params) > 2:
                    prediction_dict['prediction']["param3"] = ok_params[2]
                load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
            self.assertIn(f"The number of param columns with non-NULL entries count must match family definition",
                          str(context.exception))


    # `param1`, `param2`, `param3` (f)
    def test_parameters_for_each_distribution_must_be_within_valid_ranges_3(self):
        for family_abbrev, (target_name, ok_params, bad_params_list) in FAMILY_TO_TARGET_OK_BAD_PARAMS.items():
            # test invalid param range (but correct count) using bad_params
            for bad_params in bad_params_list:
                prediction_dict = {"unit": "location1", "target": target_name, "class": "named",
                                   "prediction": {"family": family_abbrev,
                                                  "param1": bad_params[0]}}  # all have param1. add 2&3 next
                with self.assertRaises(RuntimeError) as context:
                    if len(bad_params) > 1:
                        prediction_dict['prediction']["param2"] = bad_params[1]
                    if len(bad_params) > 2:
                        prediction_dict['prediction']["param3"] = bad_params[2]
                    load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
                self.assertIn(f"Parameters for each distribution must be within valid ranges",
                              str(context.exception))


    #
    # `Point` Prediction Elements
    #

    # `value` (i, f, t, d, b)
    def test_entries_in_the_database_rows_in_the_value_column_cannot_be_empty_na_or_null_point(self):
        prediction_dict = {"unit": "location1", "target": "season severity", "class": "point",
                           "prediction": {"value": ""}}  # empty
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `value` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        prediction_dict = {"unit": "location1", "target": "season severity", "class": "point",
                           "prediction": {"value": "NA"}}  # NA
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `value` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        prediction_dict = {"unit": "location1", "target": "season severity", "class": "point",
                           "prediction": {"value": None}}  # null
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `value` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))


    def test_data_format_of_value_should_correspond_or_be_translatable_to_the_type_as_in_the_target_def_point(self):
        # 'pct next week': continuous
        prediction_dict = {"unit": "location1", "target": "pct next week", "class": "point",
                           "prediction": {"value": '1.2'}}  # value not float
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'cases next week: discrete
        prediction_dict = {"unit": "location2", "target": "cases next week", "class": "point",
                           "prediction": {"value": 1.1}}  # value not int
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'season severity: nominal
        prediction_dict = {"unit": "location1", "target": "season severity", "class": "point",
                           "prediction": {"value": -1}}  # value not str
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'above baseline: binary
        prediction_dict = {"unit": "location1", "target": "above baseline", "class": "point",
                           "prediction": {"value": "not boolean"}}  # value not boolean
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'Season peak week: date
        prediction_dict = {"unit": "location1", "target": "Season peak week", "class": "point",
                           "prediction": {"value": "x 2019-12-22"}}  # value not date
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        prediction_dict = {"unit": "location1", "target": "Season peak week", "class": "point",
                           "prediction": {"value": "20191222"}}  # value wrong date format
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))


    #
    # `Sample` Prediction Elements
    #

    # `sample` (i, f, t, d, b)
    def test_entries_in_the_database_rows_in_the_sample_column_cannot_be_empty_na_or_null(self):
        prediction_dict = {"unit": "location2", "target": "season severity", "class": "sample",
                           "prediction": {"sample": ["moderate", "", "high", "moderate", "mild"]}}  # empty
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `sample` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        prediction_dict = {"unit": "location2", "target": "season severity", "class": "sample",
                           "prediction": {"sample": ["moderate", "NA", "high", "moderate", "mild"]}}  # NA
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `sample` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        prediction_dict = {"unit": "location2", "target": "season severity", "class": "sample",
                           "prediction": {"sample": ["moderate", None, "high", "moderate", "mild"]}}  # null
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `sample` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))


    def test_data_format_of_sample_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition(self):
        # 'pct next week': continuous
        prediction_dict = {"unit": "location3", "target": "pct next week", "class": "sample",
                           "prediction": {"sample": [2.3, '6.5', 0.0, 10.0234, 0.0001]}}  # sample not float
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'cases next week: discrete
        prediction_dict = {"unit": "location2", "target": "cases next week", "class": "sample",
                           "prediction": {"sample": [0, 2.0, 5]}}  # sample not int
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'season severity: nominal
        prediction_dict = {"unit": "location2", "target": "season severity", "class": "sample",
                           "prediction": {"sample": ["moderate", 1]}}  # sample not str
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'above baseline: binary
        prediction_dict = {"unit": "location2", "target": "above baseline", "class": "sample",
                           "prediction": {"sample": [True, 'False', True]}}  # sample not boolean
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'Season peak week: date
        prediction_dict = {"unit": "location1", "target": "Season peak week", "class": "sample",
                           "prediction": {"sample": ["2020-01-05", 2.2]}}  # sample not date
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'Season peak week: date
        prediction_dict = {"unit": "location1", "target": "Season peak week", "class": "sample",
                           "prediction": {"sample": ["2020-01-05", "x 2019-12-15"]}}  # sample not date format
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))


    #
    # `Quantile` Prediction Elements
    #

    # `|quantile| = |value|`
    def test_the_number_of_elements_in_quantile_and_value_should_be_identical(self):
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "quantile",
                           "prediction": {"quantile": [0.25, 0.5, 0.75, 0.975],  # one fewer
                                          "value": [1.0, 2.2, 2.2, 5.0, 50.0]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The number of elements in the `quantile` and `value` vectors should be identical.",
                      str(context.exception))

        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "quantile",
                           "prediction": {"quantile": [0.025, 0.25, 0.5, 0.75, 0.975],
                                          "value": [1.0, 2.2, 5.0, 50.0]}}  # one fewer
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The number of elements in the `quantile` and `value` vectors should be identical.",
                      str(context.exception))


    # `quantile` (f)
    def test_entries_in_the_database_rows_in_the_quantile_column_must_be_numbers_in_0_1(self):
        # test precursor: prob column can only contain ints or floats
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "quantile",
                           "prediction": {"quantile": [0.025, '0.25', 0.5, 0.75, 0.975],  # '0.25' not int or float
                                          "value": [1.0, 2.2, 2.2, 5.0, 50.0]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"wrong data type in `quantile` column, which should only contain ints or floats",
                      str(context.exception))

        # "pct next week": continuous. cats: [0.0, 1.0, 1.1, 2.0, 2.2, 3.0, 3.3, 5.0, 10.0, 50.0]
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "quantile",
                           "prediction": {"quantile": [-1.1, 0.25, 0.5, 0.75, 0.975],  # -1.1 not in [0, 1]
                                          "value": [1.0, 2.2, 2.2, 5.0, 50.0]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `quantile` column must be numbers in [0, 1].",
                      str(context.exception))

        # "cases next week": discrete. cats: [0, 2, 50]
        prediction_dict = {"unit": "location3", "target": "cases next week", "class": "quantile",
                           "prediction": {"quantile": [0.25, 1.1],  # 1.1 not in [0, 1]
                                          "value": [0, 50]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `quantile` column must be numbers in [0, 1].",
                      str(context.exception))


    # `quantile` (f)
    def test_quantiles_must_be_unique(self):
        prediction_dict = {"unit": "location2", "target": "Season peak week", "class": "quantile",
                           "prediction": {"quantile": [0.5, 0.75, 0.75],  # 0.75 not unique
                                          "value": ["2019-12-22", "2019-12-29", "2020-01-05"]}}
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"`quantile`s must be unique.", str(context.exception))


    # `value` (i, f, d)
    def test_entries_in_value_must_be_non_decreasing_as_quantiles_increase(self):
        # continuous target
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "quantile",
                           "prediction": {"quantile": [0.025, 0.25, 0.5, 0.75, 0.975],
                                          "value": [1.0, 1.0, 2.2, 5.0, 4.9]}}  # last decreases
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `value` must be non-decreasing as quantiles increase.", str(context.exception))

        # discrete target
        prediction_dict = {"unit": "location3", "target": "cases next week", "class": "quantile",
                           "prediction": {"quantile": [0.25, 0.75],
                                          "value": [50, 0]}}  # last decreases
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `value` must be non-decreasing as quantiles increase.", str(context.exception))

        # date target
        prediction_dict = {"unit": "location2", "target": "Season peak week", "class": "quantile",
                           "prediction": {"quantile": [0.5, 0.75, 0.975],
                                          "value": ["2019-12-22", "2019-12-21", "2020-01-05"]}}  # second decreases
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `value` must be non-decreasing as quantiles increase.", str(context.exception))


    # `value` (i, f, d)
    def test_entries_in_value_must_obey_existing_ranges_for_targets(self):
        # 'pct next week': continuous. range: [0.0, 100.0]
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "quantile",
                           "prediction": {"quantile": [0.025, 0.25, 0.5, 0.75, 0.975],
                                          "value": [-0.1, 2.2, 2.2, 5.0, 50.0]}}  # first < 0.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `value` must obey existing ranges for targets.", str(context.exception))

        # 'pct next week': continuous. range: [0.0, 100.0]
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "quantile",
                           "prediction": {"quantile": [0.025, 0.25, 0.5, 0.75, 0.975],
                                          "value": [1.0, 2.2, 2.2, 5.0, 101.0]}}  # last > 100.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `value` must obey existing ranges for targets.", str(context.exception))

        # cases next week: discrete. range: [0, 100000]
        prediction_dict = {"unit": "location3", "target": "cases next week", "class": "quantile",
                           "prediction": {"quantile": [0.25, 0.75],
                                          "value": [-1, 50]}}  # first < 0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `value` must obey existing ranges for targets.", str(context.exception))

        # cases next week: discrete. range: [0, 100000]
        prediction_dict = {"unit": "location3", "target": "cases next week", "class": "quantile",
                           "prediction": {"quantile": [0.25, 0.75],
                                          "value": [0, 100001]}}  # last > 100000
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `value` must obey existing ranges for targets.", str(context.exception))


    def test_data_format_of_value_should_correspond_or_be_translatable_to_the_type_as_in_the_target_def_quant(self):
        # 'pct next week': continuous
        prediction_dict = {"unit": "location2", "target": "pct next week", "class": "quantile",
                           "prediction": {"quantile": [0.025, 0.25, 0.5, 0.75, 0.975],
                                          "value": [1.0, '2.2', 2.2, 5.0, 50.0]}}  # value not float
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'cases next week: discrete
        prediction_dict = {"unit": "location3", "target": "cases next week", "class": "quantile",
                           "prediction": {"quantile": [0.25, 0.75],
                                          "value": [0.1, 50]}}  # value not translatable to int
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'Season peak week: date
        prediction_dict = {"unit": "location2", "target": "Season peak week", "class": "quantile",
                           "prediction": {"quantile": [0.5, 0.75, 0.975],
                               "value": ["2019-12-22", 20191229, "2020-01-05"]}}  # value not date
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'Season peak week: date
        prediction_dict = {"unit": "location2", "target": "Season peak week", "class": "quantile",
                           "prediction": {"quantile": [0.5, 0.75, 0.975],
                                          "value": ["2019-12-22", "x 2019-12-29", "2020-01-05"]}
                           }  # value not date format
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))


    # ----
    # Tests for Predictions by Target Type
    # ----

    def test_within_one_prediction_there_can_be_at_most_one_of_the_following_prediction_elements_but_not_both(self):
        # 'pct next week': continuous
        prediction_dicts = [{"unit": "location1", "target": "pct next week", "class": "named",
                             "prediction": {"family": "norm", "param1": 1.1, "param2": 2.2}},
                            {"unit": "location1", "target": "pct next week", "class": "bin",
                             "prediction": {"cat": [1.1, 2.2, 3.3],
                                            "prob": [0.3, 0.2, 0.5]}}]
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': prediction_dicts})
        self.assertIn(f"Within one prediction, there can be at most one of the following prediction elements, but",
                      str(context.exception))

        # 'cases next week': discrete
        prediction_dicts = [{"unit": "location1", "target": "cases next week", "class": "named",
                             "prediction": {"family": "pois", "param1": 1.1}},
                            {"unit": "location1", "target": "cases next week", "class": "bin",
                             "prediction": {"cat": [0, 2, 50],
                                            "prob": [0.0, 0.1, 0.9]}}]
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': prediction_dicts})
        self.assertIn(f"Within one prediction, there can be at most one of the following prediction elements, but",
                      str(context.exception))


    # ----
    # Tests for Prediction Elements by Target Type
    # ----

    #
    # "continuous"
    #

    # # tested in test_data_format_of_value_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition():
    # def test_any_values_in_point_or_sample_prediction_elements_should_be_numeric(self):
    #     pass


    def test_if_range_is_specified_any_values_in_point_or_sample_prediction_elements_should_be_contained(self):
        # 'pct next week': continuous. range: [0.0, 100.0]
        prediction_dict = {"unit": "location1", "target": "pct next week", "class": "point",
                           "prediction": {"value": -1}}  # -1 <= 0.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Point` Prediction Elements should be contained within",
                      str(context.exception))

        prediction_dict = {"unit": "location1", "target": "pct next week", "class": "point",
                           "prediction": {"value": 101.0}}  # 100.0 < 101.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Point` Prediction Elements should be contained within",
                      str(context.exception))

        prediction_dict = {"unit": "location3", "target": "pct next week", "class": "sample",
                           "prediction": {"sample": [2.3, 6.5, -1, 10.0234, 0.0001]}}  # -1 <= 0.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Sample` Prediction Elements should be contained within",
                      str(context.exception))

        prediction_dict = {"unit": "location3", "target": "pct next week", "class": "sample",
                           "prediction": {"sample": [2.3, 6.5, 101.0, 10.0234, 0.0001]}}  # 100.0 < 101.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Sample` Prediction Elements should be contained within",
                      str(context.exception))

        # cases next week: discrete. range: [0, 100000]
        prediction_dict = {"unit": "location2", "target": "cases next week", "class": "point",
                           "prediction": {"value": -1}}  # -1 <= 0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Point` Prediction Elements should be contained within",
                      str(context.exception))

        prediction_dict = {"unit": "location2", "target": "cases next week", "class": "point",
                           "prediction": {"value": 100001}}  # 100000 < 100001
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Point` Prediction Elements should be contained within",
                      str(context.exception))

        prediction_dict = {"unit": "location2", "target": "cases next week", "class": "sample",
                           "prediction": {"sample": [0, -1, 5]}}  # -1 <= 0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Sample` Prediction Elements should be contained within",
                      str(context.exception))

        prediction_dict = {"unit": "location2", "target": "cases next week", "class": "sample",
                           "prediction": {"sample": [0, 100001, 5]}}  # 100000 < 100001
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Sample` Prediction Elements should be contained within",
                      str(context.exception))


    def test_if_range_is_specified_any_named_prediction_element_should_have_negligible_probability_density(self):
        #  - if `range` is specified, any `Named` Prediction Element should have negligible probability density (no more than 0.001 density) outside of the range.
        # 'pct next week': continuous
        self.fail()  # todo xx

        # cases next week: discrete
        self.fail()  # todo xx


    # # tested in test_entries_in_cat_must_be_a_subset_of_target_cats_from_the_target_definition():
    # def test_for_bin_prediction_elements_the_submitted_set_of_cat_values_must_be_a_subset_of_the_cats(self):
    #     pass


    # # tested in test_the_predictions_class_must_be_valid_for_its_targets_type():
    # def test_for_named_prediction_elements_the_distribution_must_be_one_of_norm_lnorm_gamma_beta(self):
    #     pass


    #
    # "discrete"
    #

    # most tested above in "continuous"

    # # tested in test_the_predictions_class_must_be_valid_for_its_targets_type():
    # def test_for_named_prediction_elements_the_distribution_must_be_one_of_pois_nbinom_nbinom2(self):
    #     pass


    #
    # "nominal"
    #

    # tested above in "continuous"


    #
    # "binary"
    #

    # # tested in test_data_format_of_value_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition():
    # def test_any_values_in_point_or_sample_prediction_elements_should_be_either_true_or_false(self):
    #     pass


    def test_for_bin_prediction_elements_there_must_be_exactly_two_cat_values_labeled_true_and_false(self):
        #  - for `Bin` Prediction Elements, there must be exactly two `cat` values labeled `true` and `false`. These are the two `cats` that are implied (but not allowed to be specified) by binary target types.
        # above baseline: binary

        # case: cat value not being boolean. Note: this is tested in:
        # test_data_format_of_cat_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition()

        # case: not exactly two booleans
        prediction_dict = {"unit": "location2", "target": "above baseline", "class": "bin",
                           "prediction": {"cat": [True, False, True],
                                          "prob": [0.8, 0.1, 0.1]}}  # cat not exactly two booleans
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"for `Bin` Prediction Elements, there must be exactly two `cat` values labeled `true` and",
                      str(context.exception))


    #
    # "date"
    #

    # # tested in:
    # # - test_data_format_of_cat_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition()
    # # - test_data_format_of_value_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition()
    # # - test_data_format_of_sample_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition()
    # def test_any_values_in_point_or_sample_prediction_elements_should_be_string_that_can_be_interpreted_as_a_date_in_yyyy_mm_dd(self):
    #     pass


    # # tested in:
    # # - test_entries_in_cat_must_be_a_subset_of_target_cats_from_the_target_definition()
    # def test_for_bin_prediction_elements_the_submitted_set_of_cats_must_be_a_subset_of_the_valid_outcomes(self):
    #     pass


    # ----
    # Tests for target definitions by Target Type
    # ----

    #
    # these are all tested in test_target.py:
    # - "continuous"
    # - "discrete"
    # - "nominal"
    # - "binary"
    # - "date"


    # ----
    # Tests for ground truth data tables
    # ----

    #
    # For all ground truth files
    #

    def test_the_columns_are_timezero_unit_target_and_value(self):
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-no-header.csv'))
        self.assertIn(f"invalid header", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-bad-header.csv'))
        self.assertIn(f"invalid header", str(context.exception))


    # # this is tested elsewhere:
    # def test_for_every_unique_target_unit_timezero_combination_there_should_be_either_1_or_0_rows_of_truth(self):
    #     pass


    # # this is tested elsewhere:
    # def test_every_value_of_timezero_target_unit_must_be_in_list_of_valid_values_defined_by_project_config(self):
    #     pass


    def test_the_value_of_the_truth_data_cannot_be_empty_na_or_null(self):
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-null-value.csv'))
        self.assertIn(f"value was not compatible with target data type", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-na-value.csv'))
        self.assertIn(f"value was not compatible with target data type", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-empty-value.csv'))
        self.assertIn(f"value was not compatible with target data type", str(context.exception))


    def test_the_value_of_truth_data_should_be_interpretable_as_the_corresponding_data_type_of_specified_target(self):
        load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth.csv'))  # 14 rows
        self.assertEqual(14, self.project.truth_data_qs().count())

        exp_rows = [(datetime.date(2011, 10, 2), 'location1', 'pct next week', None, 4.5432, None, None, None),
                    (datetime.date(2011, 10, 2), 'location1', 'cases next week', 10, None, None, None, None),
                    (datetime.date(2011, 10, 2), 'location1', 'season severity', None, None, 'moderate', None, None),
                    (datetime.date(2011, 10, 2), 'location1', 'above baseline', None, None, None, None, True),
                    (datetime.date(2011, 10, 2), 'location1', 'Season peak week', None, None, None,
                     datetime.date(2019, 12, 15), None),
                    (datetime.date(2011, 10, 9), 'location2', 'pct next week', None, 99.9, None, None, None),
                    (datetime.date(2011, 10, 9), 'location2', 'cases next week', 3, None, None, None, None),
                    (datetime.date(2011, 10, 9), 'location2', 'season severity', None, None, 'severe', None, None),
                    (datetime.date(2011, 10, 9), 'location2', 'above baseline', None, None, None, None, True),
                    (datetime.date(2011, 10, 9), 'location2', 'Season peak week', None, None, None,
                     datetime.date(2019, 12, 29), None),
                    (datetime.date(2011, 10, 16), 'location1', 'pct next week', None, 0.0, None, None, None),
                    (datetime.date(2011, 10, 16), 'location1', 'cases next week', 0, None, None, None, None),
                    (datetime.date(2011, 10, 16), 'location1', 'above baseline', None, None, None, None, False),
                    (datetime.date(2011, 10, 16), 'location1', 'Season peak week', None, None, None,
                     datetime.date(2019, 12, 22), None)]
        act_rows_qs = self.project.truth_data_qs() \
            .values_list('time_zero__timezero_date', 'unit__name', 'target__name',
                         'value_i', 'value_f', 'value_t', 'value_d', 'value_b')
        self.assertEqual(exp_rows, list(act_rows_qs))


    def test_truth_value_not_compatible_with_target_data_type(self):
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-bad-date.csv'))
        self.assertIn(f"value was not compatible with target data type", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-bad-date.csv'))
        self.assertIn(f"value was not compatible with target data type", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-bad-continuous.csv'))
        self.assertIn(f"value was not compatible with target data type", str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-bad-discrete.csv'))
        self.assertIn(f"value was not compatible with target data type", str(context.exception))


    #
    # Range-check for ground truth data
    #

    # For `binary` targets
    def test_entry_in_value_column_for_a_specific_target_unit_timezero_must_be_either_true_or_false(self):
        # "good" binary values are checked when loading docs-ground-truth.csv elsewhere
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data/docs-ground-truth-bad-binary.csv'))
        self.assertIn(f"value was not compatible with target data type", str(context.exception))


    # For `discrete` and `continuous` targets (if `range` is specified)
    def test_entry_in_value_column_for_a_specific_target_unit_timezero_must_be_contained_within_the_range(self):
        # For `discrete` and `continuous` targets (if `range` is specified):
        # - The entry in the `value` column for a specific `target`-`unit`-`timezero` combination must be contained
        #   within the `range` of valid values for the target. If `cats` is specified but `range` is not, then there is
        #   an implicit range for the ground truth value, and that is between min(`cats`) and \infty.
        # recall: "The range is assumed to be inclusive on the lower bound and open on the upper bound, # e.g. [a, b)."

        # pct next week: continuous. range: [0.0, 100.0]. cats: [0.0, 1.0, 1.1, 2.0, 2.2, 3.0, 3.3, 5.0, 10.0, 50.0]
        # - "good" continuous values are checked when loading docs-ground-truth.csv elsewhere
        # - the case of the value being equal to the lower range value is also in that file
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-continuous-range-lt-lower.csv'))
        self.assertIn(f"The entry in the `value` column for a specific `target`-`unit`-`timezero` combination",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-continuous-range-equal-upper.csv'))
        self.assertIn(f"The entry in the `value` column for a specific `target`-`unit`-`timezero` combination",
                      str(context.exception))

        # change pct next week (continuous): remove range, leave cats -> effective range is [0.0, \infty), which means:
        # - docs-ground-truth-bad-continuous-range-lt-lower.csv:    still invalid
        # - docs-ground-truth-bad-continuous-range-equal-upper.csv: is now valid
        pct_next_week_target = Target.objects.filter(name='pct next week').first()
        TargetRange.objects.filter(target=pct_next_week_target).delete()
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-continuous-range-lt-lower.csv'))
        self.assertIn(f"The entry in the `value` column for a specific `target`-`unit`-`timezero` combination",
                      str(context.exception))

        try:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-continuous-range-equal-upper.csv'))
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        # cases next week: discrete. range: [0, 100000]. cats: [0, 2, 50]
        # - "good" continuous values are checked when loading docs-ground-truth.csv elsewhere
        # - the case of the value being equal to the lower range value is also in that file
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-discrete-range-lt-lower.csv'))
        self.assertIn(f"The entry in the `value` column for a specific `target`-`unit`-`timezero` combination",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-discrete-range-equal-upper.csv'))
        self.assertIn(f"The entry in the `value` column for a specific `target`-`unit`-`timezero` combination",
                      str(context.exception))

        # change cases next week (discrete): remove range, leave cats -> effective range is [0, \infty), which means:
        # - docs-ground-truth-bad-discrete-range-lt-lower.csv:    still invalid
        # - docs-ground-truth-bad-discrete-range-equal-upper.csv: is now valid
        cases_next_week_target = Target.objects.filter(name='cases next week').first()
        TargetRange.objects.filter(target=cases_next_week_target).delete()
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-discrete-range-lt-lower.csv'))
        self.assertIn(f"The entry in the `value` column for a specific `target`-`unit`-`timezero` combination",
                      str(context.exception))

        try:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-discrete-range-equal-upper.csv'))
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")


    # For `nominal` and `date` target_types
    def test_entry_in_cat_column_for_a_specific_target_unit_timezero_must_be_contained_within_the_set_of(self):
        # The entry in the `cat` column for a specific `target`-`unit`-`timezero` combination must be contained
        # within the set of valid values for the target, as defined by the project config file.

        # season severity: nominal. cats: ["high", "mild", "moderate", "severe"]
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-nominal-not-in-cats.csv'))
        self.assertIn(f"The entry in the `cat` column for a specific `target`-`unit`-`timezero` combination must",
                      str(context.exception))

        # Season peak week: date. cats: ["2019-12-15", "2019-12-22", "2019-12-29", "2020-01-05"]
        with self.assertRaises(RuntimeError) as context:
            load_truth_data(self.project, Path('forecast_app/tests/truth_data',
                                               'docs-ground-truth-bad-date-not-in-cats.csv'))
        self.assertIn(f"The entry in the `cat` column for a specific `target`-`unit`-`timezero` combination must",
                      str(context.exception))
