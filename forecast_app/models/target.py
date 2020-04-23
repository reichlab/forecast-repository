import datetime
import itertools

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import BooleanField, IntegerField
from rest_framework.test import APIRequestFactory

from forecast_app.models import Project, PointPrediction, BinDistribution, SampleDistribution, NamedDistribution, \
    QuantileDistribution
from utils.utilities import basic_str, YYYY_MM_DD_DATE_FORMAT


#
# ---- Target ----
#

class Target(models.Model):
    """
    Represents one of a Project's targets. See https://github.com/reichlab/docs.zoltardata/ for details about
    target_type and related information.
    """
    # database-level data_types - leveraging Python's built-in types
    BOOLEAN_DATA_TYPE = bool
    DATE_DATA_TYPE = datetime.date
    FLOAT_DATA_TYPE = float
    INTEGER_DATA_TYPE = int
    TEXT_DATA_TYPE = str

    # date unit choices
    DATE_UNITS = ['month', 'week', 'biweek', 'day']

    project = models.ForeignKey(Project, related_name='targets', on_delete=models.CASCADE)

    # target_type choices
    CONTINUOUS_TARGET_TYPE = 0
    DISCRETE_TARGET_TYPE = 1
    NOMINAL_TARGET_TYPE = 2
    BINARY_TARGET_TYPE = 3
    DATE_TARGET_TYPE = 4
    TARGET_TYPE_CHOICES = (
        (CONTINUOUS_TARGET_TYPE, 'continuous'),
        (DISCRETE_TARGET_TYPE, 'discrete'),
        (NOMINAL_TARGET_TYPE, 'nominal'),
        (BINARY_TARGET_TYPE, 'binary'),
        (DATE_TARGET_TYPE, 'date'),
    )
    # required fields for all types
    type = models.IntegerField(choices=TARGET_TYPE_CHOICES,
                               help_text="The Target's type. The choices are 'continuous', 'discrete', 'nominal', "
                                         "'binary', and 'date'.")
    name = models.TextField(help_text="A brief name for the target.")
    description = models.TextField(help_text="A verbose description of what the target is.")
    is_step_ahead = BooleanField(help_text="True if the target is one of a sequence of targets that predict values at "
                                           "different points in the future.")
    step_ahead_increment = IntegerField(help_text="An integer, indicating the forecast horizon represented by this "
                                                  "target. It is required if `is_step_ahead` is True.",
                                        null=True, default=None)

    # type-specific fields
    unit = models.TextField(help_text="This target's units, e.g., 'percentage', 'week', 'cases', etc.", null=True)


    # 'list' type-specific fields: see TargetLwr.lwrs, TargetCat.cats, and TargetDate.range


    def __repr__(self):
        return str((self.pk, self.name, Target.str_for_target_type(self.type),
                    self.is_step_ahead, self.step_ahead_increment))


    def __str__(self):  # todo
        return basic_str(self)


    def type_as_str(self):
        return Target.str_for_target_type(self.type)


    @classmethod
    def str_for_target_type(cls, the_type_int):
        for type_int, type_name in cls.TARGET_TYPE_CHOICES:
            if type_int == the_type_int:
                return type_name

        return '!?'


    def save(self, *args, **kwargs):
        """
        Validates is_step_ahead and step_ahead_increment, and is_date and is_step_ahead.
        """
        from utils.project import _target_dict_for_target, _validate_target_dict  # avoid circular imports


        # validate by serializing to a dict so we can use _validate_target_dict(). note that Targets created without
        # a name, description. request is required for TargetSerializer's 'id' field, but that field is ignored, so as
        # a hack we use APIRequestFactory. the other way around this is to make the 'id' field dynamic, but that looks
        # like it could get complicated - see rest_framework.relations.HyperlinkedIdentityField,
        # rest_framework.serializers.ModelSerializer.build_url_field(), etc. so we deal with the hack for now :-)
        request = APIRequestFactory().request()
        target_dict = _target_dict_for_target(self, request)

        type_name_to_type_int = {type_name: type_int for type_int, type_name in Target.TARGET_TYPE_CHOICES}
        _validate_target_dict(target_dict, type_name_to_type_int)  # raises RuntimeError if invalid

        # done
        super().save(*args, **kwargs)


    def data_types(self):
        return Target.data_types_for_target_type(self.type)


    @classmethod
    def data_types_for_target_type(cls, target_type):
        """
        :param target_type: one of my *_TARGET_TYPE values
        :return: a list of database data_types for target_type. a list rather than a single type b/c continuous can be
            either int OR float (no loss of information coercing int to float), but not vice versa. the first type in
            the list is the preferred one, say for casting
        """
        return {
            Target.CONTINUOUS_TARGET_TYPE: [Target.FLOAT_DATA_TYPE, Target.INTEGER_DATA_TYPE],
            Target.DISCRETE_TARGET_TYPE: [Target.INTEGER_DATA_TYPE],
            Target.NOMINAL_TARGET_TYPE: [Target.TEXT_DATA_TYPE],
            Target.BINARY_TARGET_TYPE: [Target.BOOLEAN_DATA_TYPE],
            Target.DATE_TARGET_TYPE: [Target.DATE_DATA_TYPE],
        }[target_type]


    @classmethod
    def is_value_compatible_with_target_type(cls, target_type, value, is_coerce=False, is_convert_na_none=False):
        """
        Returns a 2-tuple indicating if value's type is compatible with target_type: (is_compatible, parsed_value).
        parsed_value is None if not is_compatible, and is o/w the Python object resulting from parsing value as
        target_type. is_coerce controls whether value is checked based on its Python data type (is_coerce=False) or on
        whether it can be coerced into the correct type (is_coerce=True). Use is_coerce=False when you know the data
        type is correct (such as when loading json), and use is_coerce=True when inputs are strs, such as when loading
        csv. In either case, DATE_TARGET_TYPE is treated as a str and parsed in YYYY_MM_DD_DATE_FORMAT.

        :param target_type: one of my *_TARGET_TYPE values
        :param value: an int, float, str, or boolean
        :param is_coerce: True if value is a str that should be parsed as the correct data type before checking
            compatibility
        :param is_convert_na_none: True if value should be converted to None for these cases: `""`, `NA` or `NULL`
            (case does not matter). in that case (True, None) is returned
        :return: 2-tuple indicating if value's type is compatible with target_type: (is_compatible, parsed_value)
        """
        if is_convert_na_none and ((value == '') or (value.lower() == 'na') or (value.lower() == 'null')):
            return True, None
        elif is_coerce:
            try:
                if target_type == Target.CONTINUOUS_TARGET_TYPE:
                    return True, float(value)
                elif target_type == Target.DISCRETE_TARGET_TYPE:
                    return True, int(value)
                elif target_type == Target.NOMINAL_TARGET_TYPE:
                    return True, str(value)
                elif target_type == Target.BINARY_TARGET_TYPE:
                    # recall that any non-empty string parses as True, e.g., '0' or 'False', or 'None'. we handle
                    # only these two cases, per docs: `true` or `false`
                    if value == 'true':
                        return True, True
                    elif value == 'false':
                        return True, False
                    else:
                        return False, False
                elif target_type == Target.DATE_TARGET_TYPE:
                    return True, datetime.datetime.strptime(value, YYYY_MM_DD_DATE_FORMAT).date()
                else:
                    raise RuntimeError(f"invalid target_type={target_type!r}")
            except ValueError:
                return False, False
        else:  # not is_coerce
            value_type = type(value)
            try:
                if (target_type == Target.CONTINUOUS_TARGET_TYPE) and \
                        (value_type in Target.data_types_for_target_type(Target.CONTINUOUS_TARGET_TYPE)):
                    return True, float(value)  # coerce in case int
                elif (target_type == Target.DISCRETE_TARGET_TYPE) and \
                        (value_type in Target.data_types_for_target_type(Target.DISCRETE_TARGET_TYPE)):
                    return True, value
                elif (target_type == Target.NOMINAL_TARGET_TYPE) and \
                        (value_type in Target.data_types_for_target_type(Target.NOMINAL_TARGET_TYPE)):
                    return True, value
                elif (target_type == Target.BINARY_TARGET_TYPE) and \
                        (value_type in Target.data_types_for_target_type(Target.BINARY_TARGET_TYPE)):
                    return True, value
                elif (target_type == Target.DATE_TARGET_TYPE) and (value_type == str):
                    return True, datetime.datetime.strptime(value, YYYY_MM_DD_DATE_FORMAT).date()
                else:
                    return False, False
            except ValueError:
                return False, False


    def set_cats(self, cats, extra_lwr=None):
        """
        Creates TargetCat and optional TargetLwr entries for each cat in cats, first deleting all current ones.

        :param cats: a list of categories. they are either all ints, floats, or strs depending on my data_type. strs
            will be converted to datetime.date objects for date targets.
        :param extra_lwr: an optional final upper lwr to use when creating TargetLwrs. used when a Target has both cats
            and range
        """
        # before validating data type compatibility, try to replace date strings with actual date objects
        data_types_set = set(self.data_types())
        try:
            if data_types_set == {Target.DATE_DATA_TYPE}:  # Target.DATE_TARGET_TYPE
                cats = [datetime.datetime.strptime(cat_str, YYYY_MM_DD_DATE_FORMAT).date() for cat_str in cats]
        except ValueError as ve:
            raise ValidationError(f"one or more cats were not in YYYY-MM-DD format. cats={cats}. ve={ve}")

        # validate compatible data type(s)
        cats_type_set = set(map(type, cats))
        if not (cats_type_set <= data_types_set):
            raise ValidationError(f"cats_type_set was not a subset of data_types_set. cats_type_set={cats_type_set}, "
                                  f"data_types_set={data_types_set}")

        # delete and save the new TargetCats
        TargetCat.objects.filter(target=self).delete()
        preferred_data_type = self.data_types()[0]
        for cat in cats:
            TargetCat.objects.create(target=self,
                                     cat_i=cat if (preferred_data_type == Target.INTEGER_DATA_TYPE) else None,
                                     cat_f=cat if (preferred_data_type == Target.FLOAT_DATA_TYPE) else None,
                                     cat_t=cat if (preferred_data_type == Target.TEXT_DATA_TYPE) else None,
                                     cat_d=cat if (preferred_data_type == Target.DATE_DATA_TYPE) else None,
                                     cat_b=cat if (preferred_data_type == Target.BOOLEAN_DATA_TYPE) else None)

        # ditto for TargetLwrs for continuous and discrete cases (required for scoring), calculating `upper` via zip().
        # NB: we use infinity for the last bin's upper!
        if self.type in [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE]:
            cats = sorted(cats)
            if extra_lwr:
                cats.append(extra_lwr)
            for lwr, upper in itertools.zip_longest(cats, cats[1:], fillvalue=float('inf')):
                TargetLwr.objects.create(target=self, lwr=lwr, upper=upper)


    def set_range(self, lower, upper):
        """
        Creates two TargetRange entries for lower and upper, first deleting all current ones.

        :param lower: an int or float, depending on my data_type
        :param upper: ""
        """
        # validate target type
        valid_target_types = [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE]
        if self.type not in valid_target_types:
            raise ValidationError(f"invalid target type '{self.type}'. range must be one of: {valid_target_types}")

        # validate lower, upper
        data_types = self.data_types()  # the first is the preferred one
        if type(lower) != type(upper):
            raise ValidationError(f"lower and upper were of different data types: {type(lower)} != {type(upper)}")
        elif type(lower) not in data_types:  # arbitrarily test lower
            raise ValidationError(f"lower and upper data type did not match target data type. "
                                  f"lower/upper type={type(lower)}, data_types={data_types}. lower, "
                                  f"upper={lower, upper}")

        # delete and save the new TargetRanges
        TargetRange.objects.filter(target=self).delete()
        TargetRange.objects.create(target=self,
                                   value_i=lower if (data_types[0] == Target.INTEGER_DATA_TYPE) else None,
                                   value_f=lower if (data_types[0] == Target.FLOAT_DATA_TYPE) else None)
        TargetRange.objects.create(target=self,
                                   value_i=upper if (data_types[0] == Target.INTEGER_DATA_TYPE) else None,
                                   value_f=upper if (data_types[0] == Target.FLOAT_DATA_TYPE) else None)


    def range_tuple(self):
        """
        :return: either a 2-tuple () if I have a ranges, or None o/w. ordered by min, max
        """
        ranges_qs = self.ranges.all()
        if not ranges_qs.count():
            return None

        ranges_list = list(ranges_qs)
        ranges0 = ranges_list[0]
        ranges1 = ranges_list[1]
        ranges0_val = PointPrediction.first_non_none_value(ranges0.value_i, ranges0.value_f, None, None, None)
        ranges1_val = PointPrediction.first_non_none_value(ranges1.value_i, ranges1.value_f, None, None, None)
        return min(ranges0_val, ranges1_val), max(ranges0_val, ranges1_val)


    def cats_values(self):
        """
        A utility function used for validation. Returns a list of my cat values based on my data_types(), similar to
        what PointPrediction.first_non_none_value() might do, except instead of retrieving all cat_* fields we only get
        the field corresponding to my type.
        """
        data_type = self.data_types()[0]  # the first is the preferred one
        if data_type == Target.INTEGER_DATA_TYPE:
            values = self.cats.values_list('cat_i', flat=True)
        elif data_type == Target.FLOAT_DATA_TYPE:
            values = self.cats.values_list('cat_f', flat=True)
        elif data_type == Target.TEXT_DATA_TYPE:
            values = self.cats.values_list('cat_t', flat=True)
        elif data_type == Target.DATE_DATA_TYPE:
            values = self.cats.values_list('cat_d', flat=True)
        else:  # data_type == Target.BINARY_TARGET_TYPE
            values = self.cats.values_list('cat_b', flat=True)
        return list(values)


    @classmethod
    def valid_named_families(cls, target_type):
        """
        :param target_type: one of my *_TARGET_TYPE values
        :return: a list of valid NamedDistribution families for target_type
        """
        return {
            Target.CONTINUOUS_TARGET_TYPE: [NamedDistribution.NORM_DIST, NamedDistribution.LNORM_DIST,
                                            NamedDistribution.GAMMA_DIST, NamedDistribution.BETA_DIST],
            Target.DISCRETE_TARGET_TYPE: [NamedDistribution.POIS_DIST, NamedDistribution.NBINOM_DIST,
                                          NamedDistribution.NBINOM2_DIST],
            Target.NOMINAL_TARGET_TYPE: [],  # n/a
            Target.BINARY_TARGET_TYPE: [],  # n/a
            Target.DATE_TARGET_TYPE: [],  # n/a
        }[target_type]


    @classmethod
    def valid_prediction_types(cls, target_type):
        """
        :param target_type: one of my *_TARGET_TYPE values
        :return: a list of valid concrete Prediction subclasses for target_type
        """
        return {
            Target.CONTINUOUS_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution,
                                            NamedDistribution, QuantileDistribution],
            Target.DISCRETE_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution,
                                          NamedDistribution, QuantileDistribution],
            Target.NOMINAL_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution],
            Target.BINARY_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution],
            Target.DATE_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution,
                                      QuantileDistribution]
        }[target_type]


#
# ---- TargetCat ----
#

class TargetCat(models.Model):
    """
    Associates a 'list' of cat values with Targets of type Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE,
    Target.NOMINAL_TARGET_TYPE, or Target.DATE_TARGET_TYPE.
    """
    target = models.ForeignKey('Target', blank=True, null=True, related_name='cats', on_delete=models.CASCADE)
    cat_i = models.IntegerField(null=True)  # NULL if any others non-NULL
    cat_f = models.FloatField(null=True)  # ""
    cat_t = models.TextField(null=True)  # ""
    cat_d = models.DateField(null=True)  # ""
    cat_b = models.NullBooleanField(null=True)  # ""


    def __repr__(self):
        return str((self.pk, self.target.pk, self.cat_i, self.cat_f, self.cat_t, self.cat_d, self.cat_b))


    def __str__(self):  # todo
        return basic_str(self)


#
# ---- TargetLwr ----
#

class TargetLwr(models.Model):
    """
    Associates a 'list' of lwr values with Targets of type Target.CONTINUOUS_TARGET_TYPE that have 'cats'. These act as
    a "template" against which forecast TargetLwr predictions can be validated against. Note that only lwr is typically
    passed by the user (as `cat`). upper is typically calculated from lwr by the caller.

    Regarding upper: It is currently used only for scoring, when the true bin is queried for. In that case we test
    truth >= lwr AND truth < upper. Therefore it is currently calculated by utils.project._validate_and_create_targets()
    based on lwr. That function has to infer the final bin's upper, and uses float('inf') for that
    """

    target = models.ForeignKey('Target', blank=True, null=True, related_name='lwrs', on_delete=models.CASCADE)
    lwr = models.FloatField(null=True)  # nullable b/c some bins have non-numeric values, e.g., 'NA'
    upper = models.FloatField(null=True)  # "". possibly float('inf')


    def __repr__(self):
        return str((self.pk, self.target.pk, self.lwr, self.upper))


    def __str__(self):  # todo
        return basic_str(self)


#
# ---- TargetRange ----
#

class TargetRange(models.Model):
    """
    Associates a 'list' of range values with Targets of type Target.CONTINUOUS_TARGET_TYPE or Target.DISCRETE_TARGET_TYPE. Note that
    unlike other 'list' Models relating to Target, this one should have exactly two rows per target, where the first
    one's value is the lower range number, and the second row's value is the upper range number.
    """
    target = models.ForeignKey('Target', blank=True, null=True, related_name='ranges', on_delete=models.CASCADE)
    value_i = models.IntegerField(null=True)  # NULL if value_f is non-NULL
    value_f = models.FloatField(null=True)  # "" value_i ""


    def __repr__(self):
        return str((self.pk, self.target.pk, self.value_i, self.value_f))


    def __str__(self):  # todo
        return basic_str(self)
