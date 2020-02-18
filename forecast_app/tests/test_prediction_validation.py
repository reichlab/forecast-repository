import datetime
from pathlib import Path

from django.test import TestCase

from forecast_app.models import ForecastModel, TimeZero, Forecast, NamedDistribution
from utils.forecast import load_predictions_from_json_io_dict
from utils.project import create_project_from_json
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
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project)
        time_zero = TimeZero.objects.create(project=project, timezero_date=datetime.date(2017, 1, 1))
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
                prediction_dict = {"location": "location1", "target": target_name, "class": "named",
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
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "pct next week", "class": "point",
                               "prediction": {"value": 1.1}}  # duplicated location/target pair
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict] * 2})
        self.assertIn(f"Within a Prediction, there cannot be more than 1 Prediction Element of the same class",
                      str(context.exception))

        try:
            prediction_dict2 = dict(prediction_dict)  # copy, but with different location/target pair
            prediction_dict2['location'] = 'location2'
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
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "pct next week", "class": "bin",
                               "prediction": {"cat": [2.2, 3.3],
                                              "prob": [0.3, 0.2, 0.5]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The number of elements in the 'cat' and 'prob' vectors should be identical.",
                      str(context.exception))


    # `cat` (i, f, t, d, b)
    def test_entries_in_the_database_rows_in_the_cat_column_cannot_be_empty_na_or_null(self):
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "bin",
                               "prediction": {"cat": ["mild", "", "severe"],  # empty
                                              "prob": [0.0, 0.1, 0.9]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "bin",
                               "prediction": {"cat": ["mild", "NA", "severe"],  # NA
                                              "prob": [0.0, 0.1, 0.9]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "bin",
                               "prediction": {"cat": ["mild", None, "severe"],  # null
                                              "prob": [0.0, 0.1, 0.9]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))


    # `cat` (i, f, t, d, b)
    def test_entries_in_cat_must_be_a_subset_of_target_cats_from_the_target_definition(self):
        # "pct next week": continuous. cats: [0.0, 1.0, 1.1, 2.0, 2.2, 3.0, 3.3, 5.0, 10.0, 50.0]
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "pct next week", "class": "bin",
                               "prediction": {"cat": [1.1, 2.2, -1.0],  # -1.0 not in cats
                                              "prob": [0.3, 0.2, 0.5]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `cat` must be a subset of `Target.cats` from the target definition",
                      str(context.exception))

        # "cases next week": discrete. cats: [0, 2, 50]
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location3", "target": "cases next week", "class": "bin",
                               "prediction": {"cat": [-1, 1, 2],  # -1 not in cats
                                              "prob": [0.0, 0.1, 0.9]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `cat` must be a subset of `Target.cats` from the target definition",
                      str(context.exception))

        # "season severity": nominal. cats: ["high", "mild", "moderate", "severe"]
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "bin",
                               "prediction": {"cat": ["mild", "-1", "severe"],  # '-1" not in cats
                                              "prob": [0.0, 0.1, 0.9]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `cat` must be a subset of `Target.cats` from the target definition",
                      str(context.exception))

        # "Season peak week": date. cats: ["2019-12-15", "2019-12-22", "2019-12-29", "2020-01-05"]
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "Season peak week", "class": "bin",
                               "prediction": {
                                   "cat": ["2019-12-15", "2019-12-22", "2020-01-11"],  # "2020-01-11" not in cats
                                   "prob": [0.01, 0.1, 0.89]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in `cat` must be a subset of `Target.cats` from the target definition",
                      str(context.exception))


    # `prob` (f): [0, 1]
    def test_entries_in_the_database_rows_in_the_prob_column_must_be_numbers_in_0_1(self):
        # "pct next week": continuous. cats: [0.0, 1.0, 1.1, 2.0, 2.2, 3.0, 3.3, 5.0, 10.0, 50.0]
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "pct next week", "class": "bin",
                               "prediction": {"cat": [1.1, 2.2, 3.3],
                                              "prob": [-1.1, 0.2, 0.5]}}  # -1.1 not in [0, 1]
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `prob` column must be numbers in [0, 1]",
                      str(context.exception))

        # "cases next week": discrete. cats: [0, 2, 50]
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location3", "target": "cases next week", "class": "bin",
                               "prediction": {"cat": [0, 2, 50],
                                              "prob": [1.1, 0.1, 0.9]}}  # 1.1 not in [0, 1]
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `prob` column must be numbers in [0, 1]",
                      str(context.exception))


    # `prob` (f): [0, 1]
    def test_for_one_prediction_element_the_values_within_prob_must_sum_to_1_0(self):
        # Note that for binary targets that by definition need only have one row, this validation does not apply.
        # "season severity": nominal. cats: ["high", "mild", "moderate", "severe"].
        # recall: BIN_SUM_REL_TOL
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "bin",
                               "prediction": {"cat": ["mild", "moderate", "severe"],
                                              "prob": [1.0, 0.1, 0.9]}}  # sums to 2.0, not 1.0
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"For one prediction element, the values within prob must sum to 1.0",
                      str(context.exception))

        # "Season peak week": date. cats: ["2019-12-15", "2019-12-22", "2019-12-29", "2020-01-05"]
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "Season peak week", "class": "bin",
                               "prediction": {"cat": ["2019-12-15", "2019-12-22", "2019-12-29"],
                                              "prob": [0.01, 0.1, 0.8]}}  # sums to 0.91, not 1.0
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"For one prediction element, the values within prob must sum to 1.0",
                      str(context.exception))


    # # these cases are tested implicitly by: "Entries in `cat` must be a subset of `Target.cats`".
    # def test_data_format_of_cat_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition(self):
    #     pass


    #
    # `Named` Prediction Elements
    #

    # `family`
    def test_family_must_be_one_of_the_abbreviations_shown_in_the_table_below(self):
        # note that test_target_type_to_valid_named_families() test the underlying Target.valid_named_families()
        # function, but here we test that indirectly via load_predictions_from_json_io_dict().
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "pct next week", "class": "named",
                               "prediction": {"family": "bad family",
                                              "param1": 1.1,
                                              "param2": 2.2}}
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
                prediction_dict = {"location": "location1", "target": target_name, "class": "named",
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
            with self.assertRaises(RuntimeError) as context:
                prediction_dict = {"location": "location1", "target": target_name, "class": "named",
                                   "prediction": {"family": family_abbrev}}  # no param1
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
                with self.assertRaises(RuntimeError) as context:
                    prediction_dict = {"location": "location1", "target": target_name, "class": "named",
                                       "prediction": {"family": family_abbrev,
                                                      "param1": bad_params[0]}}  # all have param1. add 2&3 next
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
    def test_entries_in_the_database_rows_in_the_value_column_cannot_be_empty_na_or_null(self):
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "point",
                               "prediction": {"value": ""}}  # empty
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `value` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "point",
                               "prediction": {"value": "NA"}}  # NA
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `value` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "point",
                               "prediction": {"value": None}}  # null
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `value` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))


    def test_data_format_of_value_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition(self):
        # 'pct next week': continuous
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "pct next week", "class": "point",
                               "prediction": {"value": '1.2'}}  # value not float
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'cases next week: discrete
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "cases next week", "class": "point",
                               "prediction": {"value": 1.1}}  # value not int
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'season severity: nominal
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "point",
                               "prediction": {"value": -1}}  # value not str
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'above baseline: binary
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "above baseline", "class": "point",
                               "prediction": {"value": "not boolean"}}  # value not boolean
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'Season peak week: date
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "Season peak week", "class": "point",
                               "prediction": {"value": "x 2019-12-22"}}  # value not date
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "Season peak week", "class": "point",
                               "prediction": {"value": "20191222"}}  # value wrong date format
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))


    #
    # `Sample` Prediction Elements
    #

    # `sample` (i, f, t, d, b)
    def test_entries_in_the_database_rows_in_the_sample_column_cannot_be_empty_na_or_null(self):
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "season severity", "class": "sample",
                               "prediction": {"sample": ["moderate", "", "high", "moderate", "mild"]}}  # empty
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `sample` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "season severity", "class": "sample",
                               "prediction": {"sample": ["moderate", "NA", "high", "moderate", "mild"]}}  # NA
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `sample` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "season severity", "class": "sample",
                               "prediction": {"sample": ["moderate", None, "high", "moderate", "mild"]}}  # null
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `sample` column cannot be `“”`, `“NA”` or `NULL`",
                      str(context.exception))


    def test_data_format_of_sample_should_correspond_or_be_translatable_to_the_type_as_in_the_target_definition(self):
        # 'pct next week': continuous
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location3", "target": "pct next week", "class": "sample",
                               "prediction": {"sample": [2.3, '6.5', 0.0, 10.0234, 0.0001]}}  # sample not float
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'cases next week: discrete
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "cases next week", "class": "sample",
                               "prediction": {"sample": [0, 2.0, 5]}}  # sample not int
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'season severity: nominal
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "season severity", "class": "sample",
                               "prediction": {"sample": ["moderate", 1]}}  # sample not str
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'above baseline: binary
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "above baseline", "class": "sample",
                               "prediction": {"sample": [True, 'False', True]}}  # sample not boolean
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        # 'Season peak week: date
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "Season peak week", "class": "sample",
                               "prediction": {"sample": ["2020-01-05", "x 2019-12-15"]}}  # sample not date
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))


    # ----
    # Tests for Predictions by Target Type
    # ----

    def test_within_one_prediction_there_can_be_at_most_one_of_the_following_prediction_elements_but_not_both(self):
        # 'pct next week': continuous
        prediction_dicts = [{"location": "location1", "target": "pct next week", "class": "named",
                             "prediction": {"family": "norm", "param1": 1.1, "param2": 2.2}},
                            {"location": "location1", "target": "pct next week", "class": "bin",
                             "prediction": {"cat": [1.1, 2.2, 3.3], "prob": [0.3, 0.2, 0.5]}}]
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': prediction_dicts})
        self.assertIn(f"Within one prediction, there can be at most one of the following prediction elements, but",
                      str(context.exception))

        # 'cases next week': discrete
        prediction_dicts = [{"location": "location1", "target": "cases next week", "class": "named",
                             "prediction": {"family": "pois", "param1": 1.1}},
                            {"location": "location1", "target": "cases next week", "class": "bin",
                             "prediction": {"cat": [0, 2, 50], "prob": [0.0, 0.1, 0.9]}}]
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

    def test_any_values_in_point_or_sample_prediction_elements_should_be_numeric(self):
        # 'pct next week': continuous
        prediction_dict = {"location": "location1", "target": "pct next week", "class": "point",
                           "prediction": {"value": '1.2'}}  # value not float
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `value` should correspond or be translatable to the `type` as in the",
                      str(context.exception))

        prediction_dict = {"location": "location3", "target": "pct next week", "class": "sample",
                           "prediction": {"sample": [2.3, 6.5, 0.0, '10.0234', 0.0001]}}  # value not float
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The data format of `sample` should correspond or be translatable to the `type` as in the",
                      str(context.exception))


    # if `range` is specified, any values in `Point` or `Sample` Prediction Elements should be contained within `range`
    def test_any_values_in_point_or_sample_prediction_elements_should_be_contained_within_range(self):
        # 'pct next week': continuous. range: [0.0, 100.0]
        prediction_dict = {"location": "location1", "target": "pct next week", "class": "point",
                           "prediction": {"value": 101.0}}  # 100.0 < 101.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Point`Prediction Elements should be contained within",
                      str(context.exception))

        prediction_dict = {"location": "location1", "target": "pct next week", "class": "point",
                           "prediction": {"value": -1}}  # -1 < 0.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Point`Prediction Elements should be contained within",
                      str(context.exception))

        prediction_dict = {"location": "location3", "target": "pct next week", "class": "sample",
                           "prediction": {"sample": [2.3, 6.5, 101.0, 10.0234, 0.0001]}}  # 100.0 < 101.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Sample` Prediction Elements should be contained within",
                      str(context.exception))

        prediction_dict = {"location": "location3", "target": "pct next week", "class": "sample",
                           "prediction": {"sample": [2.3, 6.5, -1, 10.0234, 0.0001]}}  # -1 < 0.0
        with self.assertRaises(RuntimeError) as context:
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"if `range` is specified, any values in `Sample` Prediction Elements should be contained within",
                      str(context.exception))


    #
    # "discrete"
    #

    def test_xx(self):
        self.fail()  # todo xx


    #
    # "nominal"
    #

    def test_xx(self):
        self.fail()  # todo xx


    #
    # "binary"
    #

    def test_xx(self):
        self.fail()  # todo xx


    #
    # "date"
    #

    def test_xx(self):
        self.fail()  # todo xx


    # ----
    # Tests for target definitions by Target Type
    # ----

    #
    # "continuous"
    #

    def test_xx(self):
        self.fail()  # todo xx


    #
    # "discrete"
    #

    def test_xx(self):
        self.fail()  # todo xx


    #
    # "nominal"
    #

    def test_xx(self):
        self.fail()  # todo xx


    #
    # "binary"
    #

    def test_xx(self):
        self.fail()  # todo xx


    #
    # "date"
    #

    def test_xx(self):
        self.fail()  # todo xx


    # ----
    # Tests for ground truth data tables
    # ----

    #
    # For all ground truth files
    #

    def test_xx(self):
        self.fail()  # todo xx


    #
    # Range-check for ground truth data
    #

    # For `binary` targets
    def test_xx(self):
        self.fail()  # todo xx


    # For `discrete` and `continuous` targets (if `range` is specified)
    def test_xx(self):
        self.fail()  # todo xx


    # For `nominal` and `date` target_types
    def test_xx(self):
        self.fail()  # todo xx
