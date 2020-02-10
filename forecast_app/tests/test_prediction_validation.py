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


class PredictionsTestCase(TestCase):
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

    def test_named_prediction_family_must_be_valid_for_target_type(self):
        # note: we do not test docs-predictions.json here (it's tested elsewhere), which takes care of testing all
        # target_type/prediction_class combinations. here we just test the valid NamedDistribution family/target_type
        # combinations

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
                if is_valid:  # valid: should not raise
                    # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
                    with self.assertRaises(Exception):
                        try:
                            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
                        except:
                            pass
                        else:
                            raise Exception
                else:  # invalid: should raise
                    with self.assertRaises(RuntimeError) as context:
                        load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
                    self.assertIn(f"family {family_name!r} is not valid for "
                                  f"{Target.str_for_target_type(target_type_int)!r} target types",
                                  str(context.exception))


    def test_no_more_than_1_prediction_element_of_same_type(self):
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location1", "target": "pct next week", "class": "point",
                               "prediction": {"value": 1.1}}  # duplicated location/target pair
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict] * 2})
        self.assertIn(f"Within a Prediction, there cannot be more than 1 Prediction Element of the same class",
                      str(context.exception))

        # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
        with self.assertRaises(Exception):
            try:
                prediction_dict2 = dict(prediction_dict)  # copy, but with different location/target pair
                prediction_dict2['location'] = 'location2'
                load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict, prediction_dict2]})
            except:
                pass
            else:
                raise Exception


    # ----
    # Tests for Prediction Elements by Prediction Class
    # ----

    def test_number_of_elements_in_cat_and_prob_should_be_identical(self):
        with self.assertRaises(RuntimeError) as context:
            prediction_dict = {"location": "location2", "target": "pct next week", "class": "bin",
                               "prediction": {
                                   "cat": [2.2, 3.3],
                                   "prob": [0.3, 0.2, 0.5]}}
            load_predictions_from_json_io_dict(self.forecast, {'predictions': [prediction_dict]})
        self.assertIn(f"The number of elements in the 'cat' and 'prob' vectors should be identical.",
                      str(context.exception))


    # ----
    # Tests for Predictions by Target Type
    # ----

    def test_xx(self):
        self.fail()  # todo xx


    # ----
    # Tests for Prediction Elements by Target Type
    # ----

    def test_xx(self):
        self.fail()  # todo xx


    # ----
    # Tests for target definitions by Target Type
    # ----

    def test_xx(self):
        self.fail()  # todo xx


    # ----
    # Tests for ground truth data tables
    # ----

    def test_xx(self):
        self.fail()  # todo xx
