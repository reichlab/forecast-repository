import datetime
import logging

from django.core.exceptions import ValidationError
from django.test import TestCase

from forecast_app.models import Target, PointPrediction, BinDistribution, SampleDistribution, NamedDistribution, Project
from forecast_app.models.target import TargetRange, TargetCat, TargetDate, TargetLwr


logging.getLogger().setLevel(logging.ERROR)


class TargetTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()


    def test_all_required(self):
        # target type: any. required fields type, name, description, is_step_ahead. (step_ahead_increment tested
        # separately)

        # all required fields. swapped out one-by-one next:
        model_init = {'project': self.project,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False}  # missing type

        # type missing raises django.db.utils.IntegrityError, not ValidationError
        with self.assertRaises(Exception) as context:
            Target.objects.create(**model_init)
        self.assertIn('NOT NULL constraint failed', str(context.exception))
        model_init['type'] = Target.DISCRETE_TARGET_TYPE

        # these all raise ValidationError
        for field_name in ['name', 'description', 'is_step_ahead']:
            old_field_value = model_init[field_name]
            del (model_init[field_name])
            with self.assertRaises(ValidationError) as context:
                Target.objects.create(**model_init)
            self.assertIn(f"{field_name} is required", str(context.exception))
            model_init[field_name] = old_field_value


    def test_step_ahead_increment_if_is_step_ahead(self):
        # target type: any. step_ahead_increment required if is_step_ahead

        # case: is_step_ahead=True, step_ahead_increment: missing
        model_init = {'project': self.project,
                      'type': Target.NOMINAL_TARGET_TYPE,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': True}  # missing step_ahead_increment
        with self.assertRaises(ValidationError) as context:
            Target.objects.create(**model_init)
        self.assertIn('passed is_step_ahead with no step_ahead_increment', str(context.exception))

        # case: is_step_ahead=True, step_ahead_increment: 0
        model_init['step_ahead_increment'] = 0
        # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
        with self.assertRaises(Exception):
            try:
                Target.objects.create(**model_init)
            except:
                pass
            else:
                raise Exception

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
            with self.assertRaises(ValidationError) as context:
                Target.objects.create(**model_init)
            self.assertIn('unit is required', str(context.exception))


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
                                       (Target.DISCRETE_TARGET_TYPE, (1, 2))]:
            model_init['type'] = target_type
            target = Target.objects.create(**model_init)
            # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
            with self.assertRaises(Exception):
                try:
                    target.set_range(*the_range)
                except:
                    pass
                else:
                    raise Exception

        # case: invalid types
        for target_type in [Target.NOMINAL_TARGET_TYPE, Target.BINARY_TARGET_TYPE, Target.DATE_TARGET_TYPE,
                            Target.COMPOSITIONAL_TARGET_TYPE]:
            model_init['type'] = target_type
            target = Target.objects.create(**model_init)
            with self.assertRaises(ValidationError) as context:
                target.set_range(0, 0)
            self.assertIn('invalid target type', str(context.exception))


    def test_cat_required(self):
        # target type: continuous, nominal, compositional accept an optional 'cat' list via Target.set_cats(). here we
        # test that that function checks the target type
        model_init = {'project': self.project,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False,
                      'unit': 'month'}  # missing type
        # case: valid types
        for target_type, cats in [(Target.CONTINUOUS_TARGET_TYPE, [1.1, 2.2, 3.3]),
                                  (Target.NOMINAL_TARGET_TYPE, ['cat1', 'cat2', 'cat3']),
                                  (Target.COMPOSITIONAL_TARGET_TYPE, ['cat4', 'cat5', 'cat6'])]:
            model_init['type'] = target_type
            target = Target.objects.create(**model_init)
            # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
            with self.assertRaises(Exception):
                try:
                    target.set_cats(cats)
                except:
                    pass
                else:
                    raise Exception

        # case: invalid types
        for target_type in [Target.DISCRETE_TARGET_TYPE, Target.BINARY_TARGET_TYPE, Target.DATE_TARGET_TYPE]:
            model_init['type'] = target_type
            target = Target.objects.create(**model_init)
            with self.assertRaises(ValidationError) as context:
                target.set_cats(['cat1', 'cat2'])
            self.assertIn('invalid target type', str(context.exception))


    def test_date_required(self):
        # target type: continuous and discrete accept an optional 'date' list via Target.set_dates(). here we test that
        # that function checks the target type
        model_init = {'project': self.project,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False,
                      'unit': 'month'}  # missing type
        # case: valid types
        for target_type in [Target.DATE_TARGET_TYPE]:
            model_init['type'] = target_type
            target = Target.objects.create(**model_init)
            # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
            with self.assertRaises(Exception):
                try:
                    target.set_dates(['2019-01-09', '2019-01-19'])
                except:
                    pass
                else:
                    raise Exception

        # case: invalid types
        for target_type in [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE, Target.NOMINAL_TARGET_TYPE,
                            Target.BINARY_TARGET_TYPE, Target.COMPOSITIONAL_TARGET_TYPE]:
            model_init['type'] = target_type
            target = Target.objects.create(**model_init)
            with self.assertRaises(ValidationError) as context:
                target.set_dates(['2019-01-09', '2019-01-19'])
            self.assertIn('invalid target type', str(context.exception))


    def test_target_type_to_data_type(self):
        target_type_to_exp_data_type = {
            Target.CONTINUOUS_TARGET_TYPE: Target.FLOAT_DATA_TYPE,
            Target.DISCRETE_TARGET_TYPE: Target.INTEGER_DATA_TYPE,
            Target.NOMINAL_TARGET_TYPE: Target.TEXT_DATA_TYPE,
            Target.BINARY_TARGET_TYPE: Target.BOOLEAN_DATA_TYPE,
            Target.DATE_TARGET_TYPE: Target.DATE_DATA_TYPE,
            Target.COMPOSITIONAL_TARGET_TYPE: Target.TEXT_DATA_TYPE,
        }
        for target_type, exp_data_type in target_type_to_exp_data_type.items():
            self.assertEqual(exp_data_type, Target.data_type(target_type))


    def test_target_type_to_valid_named_families(self):
        target_type_to_exp_valid_named_families = {
            Target.CONTINUOUS_TARGET_TYPE: [NamedDistribution.NORM_DIST, NamedDistribution.LNORM_DIST,
                                            NamedDistribution.GAMMA_DIST, NamedDistribution.BETA_DIST],
            Target.DISCRETE_TARGET_TYPE: [NamedDistribution.POIS_DIST, NamedDistribution.NBINOM_DIST,
                                          NamedDistribution.NBINOM2_DIST],
            Target.NOMINAL_TARGET_TYPE: [],  # n/a
            Target.BINARY_TARGET_TYPE: [NamedDistribution.BERN_DIST],
            Target.DATE_TARGET_TYPE: [],  # n/a
            Target.COMPOSITIONAL_TARGET_TYPE: [],  # n/a
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
            Target.COMPOSITIONAL_TARGET_TYPE: [BinDistribution],
        }
        for target_type, exp_prediction_types in target_type_to_exp_pred_types.items():
            self.assertEqual(exp_prediction_types, Target.valid_prediction_types(target_type))


    def test_target_ranges_created(self):
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
                target_ranges = sorted(
                    list(TargetRange.objects.filter(target=target).values_list('value_i', 'value_f')))
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


    def test_target_cats_created(self):
        # tests that TargetCat rows of the correct type are created (continuous: f, nominal: t, compositional: t).
        # recall that continuous is optional.
        model_init = {'project': self.project,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False,
                      'unit': 'the_unit'}  # missing type
        for target_type, cats, exp_cats in [(Target.CONTINUOUS_TARGET_TYPE, [1.1, 2.2, 3.3],
                                             [(1.1, None), (2.2, None), (3.3, None)]),
                                            (Target.NOMINAL_TARGET_TYPE, ['cat1', 'cat2', 'cat3'],
                                             [(None, 'cat1'), (None, 'cat2'), (None, 'cat3')]),
                                            (Target.COMPOSITIONAL_TARGET_TYPE, ['cat4', 'cat5', 'cat6'],
                                             [(None, 'cat4'), (None, 'cat5'), (None, 'cat6')])]:
            model_init['type'] = target_type
            target = Target.objects.create(**model_init)
            for _ in range(2):  # twice to make sure old are deleted
                target.set_cats(cats)
                target_cats = sorted(list(TargetCat.objects.filter(target=target).values_list('cat_f', 'cat_t')))
                self.assertEqual(exp_cats, target_cats)

        # test cat types must match - both within the list, and the target type's data_type
        model_init['type'] = Target.CONTINUOUS_TARGET_TYPE
        target = Target.objects.create(**model_init)
        with self.assertRaises(ValidationError) as context:
            target.set_cats(['cat4', 'cat5', 'cat6'])  # should be floats
        self.assertIn('cats type did not match target data type', str(context.exception))

        model_init['type'] = Target.NOMINAL_TARGET_TYPE
        target = Target.objects.create(**model_init)
        with self.assertRaises(ValidationError) as context:
            target.set_cats([1.1, 2.2, 3.3])  # should be strings
        self.assertIn('cats type did not match target data type', str(context.exception))

        model_init['type'] = Target.COMPOSITIONAL_TARGET_TYPE
        target = Target.objects.create(**model_init)
        with self.assertRaises(ValidationError) as context:
            target.set_cats([1.1, 2.2, 3.3])  # should be strings
        self.assertIn('cats type did not match target data type', str(context.exception))

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
            # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
            with self.assertRaises(Exception):
                try:
                    Target.objects.create(**model_init)
                except:
                    pass
                else:
                    raise Exception

        # case: invalid unit
        model_init['unit'] = 'bad_unit'
        with self.assertRaises(ValidationError) as context:
            Target.objects.create(**model_init)
        self.assertIn('unit was not one of', str(context.exception))


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
        # via https://stackoverflow.com/questions/647900/python-test-that-succeeds-when-exception-is-not-raised
        with self.assertRaises(Exception):
            try:
                target.set_dates(['2019-01-09', '2019-01-19'])
            except:
                pass
            else:
                raise Exception

        # case: invalid format
        with self.assertRaises(ValidationError) as context:
            target.set_dates(['bad-date-format', '2019-01-19'])
        self.assertIn('date was not in YYYY-MM-DD format', str(context.exception))


    def test_target_dates_created(self):
        # tests that TargetDate rows of the correct type are created (date: d)
        model_init = {'project': self.project,
                      'type': Target.DATE_TARGET_TYPE,
                      'name': 'target_name',
                      'description': 'target_description',
                      'is_step_ahead': False,
                      'unit': 'month'}
        target = Target.objects.create(**model_init)
        target.set_dates(['2019-01-09', '2019-01-19'])
        target_dates = sorted(list(TargetDate.objects.filter(target=target).values_list('date', flat=True)))
        self.assertEqual([datetime.date(2019, 1, 9), datetime.date(2019, 1, 19)], target_dates)


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
        bin_lwrs = sorted(list(TargetLwr.objects.filter(target=target).values_list('lwr', 'upper')))
        self.assertEqual([(1.1, 2.2), (2.2, 3.3), (3.3, float('inf'))], bin_lwrs)
