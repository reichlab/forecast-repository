import datetime
from pathlib import Path

from django.test import TestCase

from forecast_app.models import ForecastModel, TimeZero, Forecast, NamedDistribution, Target
from utils.forecast import load_predictions_from_json_io_dict
from utils.project import create_project_from_json
from utils.utilities import get_or_create_super_po_mo_users


#
# tests the validations in docs/Validation.md at https://github.com/reichlab/docs.zoltardata/
#

PARAM_TO_TARGET_EXP_COUNT = {'norm': ('pct next week', 2), 'lnorm': ('pct next week', 2),  # continuous
                             'gamma': ('pct next week', 2), 'beta': ('pct next week', 2),
                             'binom': ('pct next week', 2), 'pois': ('cases next week', 2),
                             'nbinom': ('cases next week', 2), 'nbinom2': ('cases next week', 2)}  # discrete


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
        target_name_to_type_is_valid_family_tuple = {  # 7-tuples: t/f for families in below order
            "pct next week": (Target.CONTINUOUS_TARGET_TYPE, (True, True, True, True, False, False, False)),
            "cases next week": (Target.DISCRETE_TARGET_TYPE, (False, False, False, False, True, True, True)),
            "season severity": (Target.NOMINAL_TARGET_TYPE, (False, False, False, False, False, False, False)),
            "above baseline": (Target.BINARY_TARGET_TYPE, (False, False, False, False, False, False, False)),
            "Season peak week": (Target.DATE_TARGET_TYPE, (False, False, False, False, False, False, False))}
        for target_name, (target_type_int, is_valid_family_tuple) \
                in target_name_to_type_is_valid_family_tuple.items():
            for family_int, is_valid in zip([NamedDistribution.NORM_DIST, NamedDistribution.LNORM_DIST,
                                             NamedDistribution.GAMMA_DIST, NamedDistribution.BETA_DIST,
                                             NamedDistribution.POIS_DIST, NamedDistribution.NBINOM_DIST,
                                             NamedDistribution.NBINOM2_DIST],
                                            is_valid_family_tuple):
                family_name = NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION[family_int]
                # NB: this one-size-fits-all param will fail in future tests:
                prediction_dict = {"location": "location1", "target": target_name, "class": "named",
                                   "prediction": {"family": family_name, "param1": 1.1}}

                if PARAM_TO_TARGET_EXP_COUNT[family_name][1] > 1:
                    prediction_dict['prediction']["param2"] = 2.2
                if PARAM_TO_TARGET_EXP_COUNT[family_name][1] > 2:
                    prediction_dict['prediction']["param3"] = 3.3

                if is_valid:  # valid: should not raise
                    try:
                        load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
                    except Exception as ex:
                        self.fail(f"unexpected exception: {ex}")
                else:  # invalid: should raise
                    with self.assertRaises(RuntimeError) as context:
                        load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
                    self.assertIn(f"family {family_name!r} is not valid for "
                                  f"{Target.str_for_target_type(target_type_int)!r} target types",
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
        self.assertIn(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `null`",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "bin",
                               "prediction": {"cat": ["mild", "NA", "severe"],  # NA
                                              "prob": [0.0, 0.1, 0.9]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `null`",
                      str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "season severity", "class": "bin",
                               "prediction": {"cat": ["mild", None, "severe"],  # null
                                              "prob": [0.0, 0.1, 0.9]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"Entries in the database rows in the `cat` column cannot be `“”`, `“NA”` or `null`",
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


    # `param1`, `param2`, `param3` (f)
    def test_the_number_of_param_columns_with_non_null_entries_count_must_match_family_definition(self):
        #  abbreviation | param1    | param2   | param3
        #  ------------ | --------- | -------- | ------
        #  `norm`       | mean      | sd>=0    |    -
        #  `lnorm`      | mean      | sd>=0    |    -
        #  `gamma`      | shape>0   | rate>0   |    -
        #  `beta`       | a>0       | b>0      |    -
        #  `binom`      | p??       | n??      |    -
        #  `pois`       | mean??    | -        |    -
        #  `nbinom`     | r>0       | 0<=p<=1  |    -
        #  `nbinom2`    | mean>0    | disp>0   |    -

        # recall that all params are floats, and all families require at least param1
        for abbrev, (target, exp_count) in PARAM_TO_TARGET_EXP_COUNT.items():
            # test valid parameter counts
            try:
                prediction_dict = {"location": "location1", "target": target, "class": "named",
                                   "prediction": {"family": abbrev, "param1": 1.1}}
                if exp_count > 1:
                    prediction_dict['prediction']["param2"] = 2.2
                if exp_count > 2:
                    prediction_dict['prediction']["param3"] = 3.3
                load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")

            # test invalid parameter counts: simply exp_count-1 :-) . this also tests the case where "param1" is omitted
            with self.assertRaises(RuntimeError) as context:
                prediction_dict = {"location": "location1", "target": target, "class": "named",
                                   "prediction": {"family": abbrev}}  # no "param1"
                if (exp_count - 1) > 0:
                    prediction_dict['prediction']["param1"] = 1.1
                if (exp_count - 1) > 1:
                    prediction_dict['prediction']["param2"] = 2.2
                if (exp_count - 1) > 2:
                    prediction_dict['prediction']["param3"] = 3.3
                load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
            self.assertIn(f"The number of param columns with non-NULL entries count must match family definition",
                          str(context.exception))


    #
    # `Point` Prediction Elements
    #

    def test_xx(self):
        self.fail()  # todo xx


    #
    # `Sample` Prediction Elements
    #

    def test_xx(self):
        self.fail()  # todo xx


    # ----
    # Tests for Predictions by Target Type
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


    # ----
    # Tests for Prediction Elements by Target Type
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
