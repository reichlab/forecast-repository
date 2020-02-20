import datetime
import json
import logging
from pathlib import Path

import django
from django.core.exceptions import ValidationError
from django.db import transaction
from django.test import TestCase

from forecast_app.models import Target, PointPrediction, BinDistribution, SampleDistribution, NamedDistribution, Project
from forecast_app.models.target import TargetRange, TargetCat, TargetLwr
from utils.project import create_project_from_json
from utils.utilities import get_or_create_super_po_mo_users


logging.getLogger().setLevel(logging.ERROR)


class TargetTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()


    def test_all_required(self):
        # b/c I'm getting confused about which tests are testing which required fields, this tests steps through each
        # missing field to ensure it errors if missing. notice that TextFields default to '': name, description, unit
        # and therefore cannot be tested for being passed
        # no type
        model_init = {}
        with self.assertRaises(RuntimeError) as context:
            Target.objects.create(**model_init)
        self.assertIn('target has no type', str(context.exception))

        # no is_step_ahead
        model_init = {'type': Target.CONTINUOUS_TARGET_TYPE}
        with self.assertRaises(RuntimeError) as context:
            Target.objects.create(**model_init)
        self.assertIn('is_step_ahead not found but is required', str(context.exception))

        # no step_ahead_increment
        model_init = {'type': Target.CONTINUOUS_TARGET_TYPE, 'unit': 'biweek', 'is_step_ahead': True}
        with self.assertRaises(RuntimeError) as context:
            Target.objects.create(**model_init)
        self.assertIn('step_ahead_increment not found but is required when is_step_ahead', str(context.exception))

        # no project (raises django.db.utils.IntegrityError)
        model_init = {'type': Target.CONTINUOUS_TARGET_TYPE, 'unit': 'biweek', 'is_step_ahead': False}
        with self.assertRaises(django.db.utils.IntegrityError) as context:
            Target.objects.create(**model_init)
        # self.assertIn('NOT NULL constraint failed: forecast_app_target.project_id', str(context.exception))  # sqlite3
        # self.assertIn('null value in column "project_id" violates not-null constraint', str(context.exception))  # postgres


    def test_step_ahead_increment_if_is_step_ahead(self):
        # target type: any. step_ahead_increment required if is_step_ahead

        # case: is_step_ahead=True, step_ahead_increment: missing
        model_init = {'project': self.project,
                      'type': Target.NOMINAL_TARGET_TYPE,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': True}  # missing step_ahead_increment
        with self.assertRaises(RuntimeError) as context:
            Target.objects.create(**model_init)
        self.assertIn('step_ahead_increment not found but is required when is_step_ahead is', str(context.exception))

        # case: is_step_ahead=True, step_ahead_increment: 0
        model_init['step_ahead_increment'] = 0
        try:
            Target.objects.create(**model_init)
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        # case: is_step_ahead=False, step_ahead_increment: missing
        model_init['is_step_ahead'] = False
        del (model_init['step_ahead_increment'])
        with self.assertRaises(Exception):
            try:
                Target.objects.create(**model_init)
            except:
                pass
            else:
                raise Exception


    def test_unit_required(self):
        # target type: continuous, discrete, date
        model_init = {'project': self.project,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False}  # missing type and unit
        for target_type in [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE, Target.DATE_TARGET_TYPE]:
            model_init['type'] = target_type
            with self.assertRaises(RuntimeError) as context:
                Target.objects.create(**model_init)
            self.assertIn("'unit' not passed but is required", str(context.exception))


    def test_range_required(self):
        # target type: continuous and discrete accept an optional 'range' list via Target.set_range(). here we test that
        # that function checks the target type
        model_init = {'project': self.project,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False,
                      'unit': 'month'}  # missing type
        # case: valid types
        for target_type, the_range in [(Target.CONTINUOUS_TARGET_TYPE, (3.3, 4.4)),
                                       (Target.DISCRETE_TARGET_TYPE, (1, 2))]:  # unit valid for both
            model_init['type'] = target_type
            target = Target.objects.create(**model_init)
            try:
                target.set_range(*the_range)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")

        # case: invalid types
        for target_type in [Target.NOMINAL_TARGET_TYPE, Target.BINARY_TARGET_TYPE, Target.DATE_TARGET_TYPE]:
            model_init['type'] = target_type
            if target_type in [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE, Target.DATE_TARGET_TYPE]:
                model_init['unit'] = 'month'
            else:
                model_init.pop('unit', None)
            target = Target.objects.create(**model_init)
            with self.assertRaises(ValidationError) as context:
                target.set_range(0, 0)
            self.assertIn('invalid target type', str(context.exception))


    def test_cats_required(self):
        # the target types continuous, discrete, nominal, and date accept an optional or required 'cats'
        # list via Target.set_cats(). here we test that that function checks the target type
        model_init = {'project': self.project,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False}  # missing type
        # case: valid types
        for target_type, cats in [(Target.CONTINUOUS_TARGET_TYPE, [1.1, 2.2, 3.3]),
                                  (Target.DISCRETE_TARGET_TYPE, [1, 20, 35]),
                                  (Target.NOMINAL_TARGET_TYPE, ['cat1', 'cat2', 'cat3']),
                                  (Target.DATE_TARGET_TYPE, ['2019-01-09', '2019-01-19'])]:
            model_init['type'] = target_type
            if target_type in [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE, Target.DATE_TARGET_TYPE]:
                model_init['unit'] = 'month'
            else:
                model_init.pop('unit', None)
            target = Target.objects.create(**model_init)
            try:
                target.set_cats(cats)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")

        # case: invalid type
        model_init['type'] = Target.BINARY_TARGET_TYPE
        model_init.pop('unit', None)
        target = Target.objects.create(**model_init)
        with self.assertRaises(ValidationError) as context:
            target.set_cats(['2017-01-02', '2017-01-09'])
        self.assertIn('cats data type did not match target data type', str(context.exception))


    def test_data_type_for_target_type(self):
        target_type_to_exp_data_type = {
            Target.CONTINUOUS_TARGET_TYPE: Target.FLOAT_DATA_TYPE,
            Target.DISCRETE_TARGET_TYPE: Target.INTEGER_DATA_TYPE,
            Target.NOMINAL_TARGET_TYPE: Target.TEXT_DATA_TYPE,
            Target.BINARY_TARGET_TYPE: Target.BOOLEAN_DATA_TYPE,
            Target.DATE_TARGET_TYPE: Target.DATE_DATA_TYPE,
        }
        for target_type, exp_data_type in target_type_to_exp_data_type.items():
            self.assertEqual(exp_data_type, Target.data_type_for_target_type(target_type))


    def test_is_value_compatible_with_target_type(self):
        target_type_value_is_compatibles = [
            (Target.CONTINUOUS_TARGET_TYPE, 1, (True, 1.0)),
            (Target.CONTINUOUS_TARGET_TYPE, 1.0, (True, 1.0)),
            (Target.CONTINUOUS_TARGET_TYPE, 'nan', (False, False)),
            (Target.DISCRETE_TARGET_TYPE, 1, (True, True)),
            (Target.DISCRETE_TARGET_TYPE, 1.0, (False, False)),
            (Target.DISCRETE_TARGET_TYPE, 'a str', (False, False)),
            (Target.NOMINAL_TARGET_TYPE, 'a str', (True, 'a str')),
            (Target.NOMINAL_TARGET_TYPE, 1, (False, False)),
            (Target.BINARY_TARGET_TYPE, True, (True, True)),
            (Target.BINARY_TARGET_TYPE, False, (True, False)),
            (Target.BINARY_TARGET_TYPE, 'a str', (False, False)),
            (Target.DATE_TARGET_TYPE, '2020-01-05', (True, datetime.date(2020, 1, 5))),
            (Target.DATE_TARGET_TYPE, '20200105', (False, False)),
            (Target.DATE_TARGET_TYPE, datetime.date(2020, 1, 5), (False, False)),
            (Target.DATE_TARGET_TYPE, 'x 2020-01-05', (False, False))]
        for target_type, value, is_compatible_tuple in target_type_value_is_compatibles:
            self.assertEqual(is_compatible_tuple, Target.is_value_compatible_with_target_type(target_type, value))


    def test_target_type_to_valid_named_families(self):
        target_type_to_exp_valid_named_families = {
            Target.CONTINUOUS_TARGET_TYPE: [NamedDistribution.NORM_DIST, NamedDistribution.LNORM_DIST,
                                            NamedDistribution.GAMMA_DIST, NamedDistribution.BETA_DIST],
            Target.DISCRETE_TARGET_TYPE: [NamedDistribution.POIS_DIST, NamedDistribution.NBINOM_DIST,
                                          NamedDistribution.NBINOM2_DIST],
            Target.NOMINAL_TARGET_TYPE: [],  # n/a
            Target.BINARY_TARGET_TYPE: [],  # n/a
            Target.DATE_TARGET_TYPE: [],  # n/a
        }
        for target_type, exp_valid_named_families in target_type_to_exp_valid_named_families.items():
            self.assertEqual(exp_valid_named_families, Target.valid_named_families(target_type))


    def test_target_type_to_valid_prediction_types(self):
        target_type_to_exp_pred_types = {
            Target.CONTINUOUS_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution, NamedDistribution],
            Target.DISCRETE_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution, NamedDistribution],
            Target.NOMINAL_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution],
            Target.BINARY_TARGET_TYPE: [PointPrediction, SampleDistribution, NamedDistribution],
            Target.DATE_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution],
        }
        for target_type, exp_prediction_types in target_type_to_exp_pred_types.items():
            self.assertEqual(exp_prediction_types, Target.valid_prediction_types(target_type))


    def test_target_set_range(self):
        # tests that exactly two TargetRange rows of the correct type are created (continuous: f, discrete: t).
        # recall both are optional
        model_init = {'project': self.project,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False,
                      'unit': 'the_unit'}  # missing type
        for target_type, the_range, exp_range in [(Target.CONTINUOUS_TARGET_TYPE, (3.3, 4.4),
                                                   [(None, 3.3), (None, 4.4)]),
                                                  (Target.DISCRETE_TARGET_TYPE, (1, 2),
                                                   [(1, None), (2, None)])]:
            model_init['type'] = target_type
            target = Target.objects.create(**model_init)
            for _ in range(2):  # twice to make sure old are deleted
                target.set_range(*the_range)
                target_ranges = sorted(list(TargetRange.objects
                                            .filter(target=target)
                                            .values_list('value_i', 'value_f')))
                self.assertEqual(exp_range, target_ranges)

        # test lower and upper types match - both each other, and the target type's data_type
        model_init['type'] = Target.CONTINUOUS_TARGET_TYPE
        target = Target.objects.create(**model_init)
        with self.assertRaises(ValidationError) as context:
            target.set_range(1, 2)  # should be floats
        self.assertIn('lower and upper data type did not match target data type', str(context.exception))

        model_init['type'] = Target.DISCRETE_TARGET_TYPE
        target = Target.objects.create(**model_init)
        with self.assertRaises(ValidationError) as context:
            target.set_range(3.3, 4.4)  # should be ints
        self.assertIn('lower and upper data type did not match target data type', str(context.exception))

        with self.assertRaises(ValidationError) as context:
            target.set_range(1.1, 2)  # should be same type
        self.assertIn('lower and upper were of different data types', str(context.exception))


    def test_target_range_cats_lwr_relationship(self):
        # test this relationship: "if `range` had been specified as [0, 100] in addition to the above `cats`, then the
        # final bin would be [2.2, 100]."
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            input_project_dict = json.load(fp)
            create_project_from_json(input_project_dict, po_user)
        # "pct next week":
        #   "range": [0.0, 100.0]                                         -> TargetRange: 2 value_f
        #   "cats": [0.0, 1.0, 1.1, 2.0, 2.2, 3.0, 3.3, 5.0, 10.0, 50.0]  -> TargetCat:  10 value_f
        #   -> TargetLwr: 10: lwr/upper: [(0.0, 1.0), (1.0, 1.1), (1.1, 2.0), (2.0, 2.2), (2.2, 3.0), (3.0, 3.3),
        #                                 (3.3, 5.0), (5.0, 10.0), (10.0, 50.0), (50.0, 100.0)]
        pct_next_week_target = Target.objects.filter(name='pct next week').first()
        ranges_qs = pct_next_week_target.ranges.all() \
            .order_by('value_f') \
            .values_list('target__name', 'value_i', 'value_f')
        self.assertEqual([('pct next week', None, 0.0), ('pct next week', None, 100.0)], list(ranges_qs))

        cats_qs = pct_next_week_target.cats.all() \
            .order_by('cat_f') \
            .values_list('target__name', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')
        exp_cats = [('pct next week', None, 0.0, None, None, None), ('pct next week', None, 1.0, None, None, None),
                    ('pct next week', None, 1.1, None, None, None), ('pct next week', None, 2.0, None, None, None),
                    ('pct next week', None, 2.2, None, None, None), ('pct next week', None, 3.0, None, None, None),
                    ('pct next week', None, 3.3, None, None, None), ('pct next week', None, 5.0, None, None, None),
                    ('pct next week', None, 10.0, None, None, None), ('pct next week', None, 50.0, None, None, None)]
        self.assertEqual(exp_cats, list(cats_qs))

        lwrs_qs = pct_next_week_target.lwrs.all() \
            .order_by('lwr') \
            .values_list('target__name', 'lwr', 'upper')
        exp_lwrs = [('pct next week', 0.0, 1.0), ('pct next week', 1.0, 1.1), ('pct next week', 1.1, 2.0),
                    ('pct next week', 2.0, 2.2), ('pct next week', 2.2, 3.0), ('pct next week', 3.0, 3.3),
                    ('pct next week', 3.3, 5.0), ('pct next week', 5.0, 10.0), ('pct next week', 10.0, 50.0),
                    ('pct next week', 50.0, 100.0), ('pct next week', 100.0, float('inf'))]
        self.assertEqual(exp_lwrs, list(lwrs_qs))

        # "cases next week":
        #   "range": [0, 100000]  -> TargetRange: 2 value_i
        #   "cats": [0, 2, 50]    -> TargetCat:   3 value_i
        #   -> TargetLwr: 3: lwr/upper: [(0, 2), (2, 50), (50, 100000)]
        cases_next_week_target = Target.objects.filter(name='cases next week').first()
        ranges_qs = cases_next_week_target.ranges.all() \
            .order_by('value_i') \
            .values_list('target__name', 'value_i', 'value_f')
        self.assertEqual([('cases next week', 0, None), ('cases next week', 100000, None)], list(ranges_qs))

        cats_qs = cases_next_week_target.cats.all() \
            .order_by('cat_i') \
            .values_list('target__name', 'cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')
        exp_cats = [('cases next week', 0, None, None, None, None),
                    ('cases next week', 2, None, None, None, None),
                    ('cases next week', 50, None, None, None, None)]
        self.assertEqual(exp_cats, list(cats_qs))

        lwrs_qs = cases_next_week_target.lwrs.all() \
            .order_by('lwr') \
            .values_list('target__name', 'lwr', 'upper')
        exp_lwrs = [('cases next week', 0.0, 2.0), ('cases next week', 2.0, 50.0), ('cases next week', 50.0, 100000.0),
                    ('cases next week', 100000.0, float('inf'))]
        self.assertEqual(exp_lwrs, list(lwrs_qs))


    def test_range_tuple(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            input_project_dict = json.load(fp)
            project = create_project_from_json(input_project_dict, po_user)
        act_range_tuples = [(target.name, target.range_tuple()) for target in project.targets.all().order_by('pk')]
        self.assertEqual([('pct next week', (0.0, 100.0)),
                          ('cases next week', (0, 100000)),
                          ('season severity', None),
                          ('above baseline', None),
                          ('Season peak week', None)],
                         act_range_tuples)


    def test_target_range_cat_validation(self):
        # tests this relationship: "If `cats` are specified, then the min(`cats`) must equal the lower bound of `range`
        # and max(`cats`) must be less than the upper bound of `range`."
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        with open(Path('forecast_app/tests/projects/docs-project.json')) as fp:
            input_project_dict = json.load(fp)

        # test: "the min(`cats`) must equal the lower bound of `range`":
        # for the "cases next week" target, change min(cats) to != min(range)
        #   "range": [0, 100000]
        #   "cats": [0, 2, 50]  -> change to [1, 2, 50]
        input_project_dict['targets'][1]['cats'] = [1, 2, 50]
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(input_project_dict, po_user)
        self.assertIn("the minimum cat (1) did not equal the range's lower bound (0)", str(context.exception))

        # test: "max(`cats`) must be less than the upper bound of `range`":
        # for the "cases next week" target, change max(cats) to == max(range)
        #   "range": [0, 100000]
        #   "cats": [0, 2, 50]  -> change to [0, 2, 100000]
        input_project_dict['targets'][1]['cats'] = [0, 2, 100000]
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(input_project_dict, po_user)
        self.assertIn("the maximum cat (100000) was not less than the range's upper bound", str(context.exception))

        # also test max(cats) to > max(range)
        input_project_dict['targets'][1]['cats'] = [0, 2, 100001]
        with self.assertRaises(RuntimeError) as context:
            create_project_from_json(input_project_dict, po_user)
        self.assertIn("the maximum cat (100001) was not less than the range's upper bound ", str(context.exception))


    def test_target_set_cats(self):
        # tests that TargetCat rows of the correct type are created
        model_init = {'project': self.project,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False}  # missing type
        for target_type, cats, exp_cats in [(Target.CONTINUOUS_TARGET_TYPE,
                                             [0.0, 1.0],
                                             [(None, 0.0, None, None, None), (None, 1.0, None, None, None)]),
                                            (Target.DISCRETE_TARGET_TYPE,
                                             [0, 2],
                                             [(0, None, None, None, None), (2, None, None, None, None)]),
                                            (Target.NOMINAL_TARGET_TYPE,
                                             ["high", "mild"],
                                             [(None, None, 'high', None, None), (None, None, 'mild', None, None)]),
                                            (Target.DATE_TARGET_TYPE,
                                             ["2019-12-15", "2019-12-22"],
                                             [(None, None, None, datetime.date(2019, 12, 15), None),
                                              (None, None, None, datetime.date(2019, 12, 22), None)])]:
            model_init['type'] = target_type
            if target_type in [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE, Target.DATE_TARGET_TYPE]:
                model_init['unit'] = 'month'
            else:
                model_init.pop('unit', None)
            target = Target.objects.create(**model_init)
            for _ in range(2):  # twice to make sure old are deleted
                target.set_cats(cats)
                target_cats = sorted(list(TargetCat.objects
                                          .filter(target=target)
                                          .values_list('cat_i', 'cat_f', 'cat_t', 'cat_d', 'cat_b')))
                self.assertEqual(exp_cats, target_cats)

        # test cat types must match - both within the list, and the target type's data_type
        model_init['type'] = Target.CONTINUOUS_TARGET_TYPE
        model_init['unit'] = 'month'
        target = Target.objects.create(**model_init)
        with self.assertRaises(ValidationError) as context:
            target.set_cats(['cat4', 'cat5', 'cat6'])  # should be floats
        self.assertIn('cats data type did not match target data type', str(context.exception))

        model_init['type'] = Target.NOMINAL_TARGET_TYPE
        model_init.pop('unit', None)
        target = Target.objects.create(**model_init)
        with self.assertRaises(ValidationError) as context:
            target.set_cats([1.1, 2.2, 3.3])  # should be strings
        self.assertIn('cats data type did not match target data type', str(context.exception))

        with self.assertRaises(ValidationError) as context:
            target.set_cats([1.1, 'cat5', 'cat6'])  # should be same type
        self.assertIn('there was more than one data type in cats', str(context.exception))


    def test_target_date_unit(self):
        # date target type: unit must be one of Target.DATE_UNITS
        model_init = {'project': self.project,
                      'type': Target.DATE_TARGET_TYPE,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False}  # missing unit

        # case: valid unit
        for ok_unit in Target.DATE_UNITS:
            model_init['unit'] = ok_unit
            try:
                Target.objects.create(**model_init)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")

        # case: invalid unit
        model_init['unit'] = 'bad_unit'
        with self.assertRaises(RuntimeError) as context:
            Target.objects.create(**model_init)
        self.assertIn("'unit' passed for date target but was not valid", str(context.exception))


    def test_target_date_format(self):
        # date target type: dates must be YYYY_MM_DD_DATE_FORMAT
        model_init = {'project': self.project,
                      'type': Target.DATE_TARGET_TYPE,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False,
                      'unit': 'month'}
        target = Target.objects.create(**model_init)

        # case: valid format
        try:
            target.set_cats(['2019-01-09', '2019-01-19'])
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        # case: invalid format
        with self.assertRaises(ValidationError) as context:
            target.set_cats(['bad-date-format', '2019-01-19'])
        self.assertIn('one or more cats were not in YYYY-MM-DD format', str(context.exception))


    def test_target_date_cats_created(self):
        # tests that TargetCat rows of the correct type are created (date: d)
        model_init = {'project': self.project,
                      'type': Target.DATE_TARGET_TYPE,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False,
                      'unit': 'month'}
        target = Target.objects.create(**model_init)
        target.set_cats(['2019-01-09', '2019-01-19'])
        target_cats = sorted(list(TargetCat.objects.filter(target=target).values_list('cat_d', flat=True)))
        self.assertEqual([datetime.date(2019, 1, 9), datetime.date(2019, 1, 19)], target_cats)


    def test_target_lwrs_created(self):
        # tests that TargetLwr rows are created for continuous targets
        model_init = {'project': self.project,
                      'type': Target.CONTINUOUS_TARGET_TYPE,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False,
                      'unit': 'the_unit'}
        target = Target.objects.create(**model_init)
        target.set_cats([1.1, 2.2, 3.3])
        lwrs = sorted(list(TargetLwr.objects.filter(target=target).values_list('lwr', 'upper')))
        self.assertEqual([(1.1, 2.2), (2.2, 3.3), (3.3, float('inf'))], lwrs)
