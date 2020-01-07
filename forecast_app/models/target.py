import itertools
from datetime import datetime

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import BooleanField, IntegerField

from forecast_app.models import Project, PointPrediction, BinDistribution, SampleDistribution, NamedDistribution
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
    COMPOSITIONAL_TARGET_TYPE = 5
    TARGET_TYPE_CHOICES = (
        (CONTINUOUS_TARGET_TYPE, 'continuous'),
        (DISCRETE_TARGET_TYPE, 'discrete'),
        (NOMINAL_TARGET_TYPE, 'nominal'),
        (BINARY_TARGET_TYPE, 'binary'),
        (DATE_TARGET_TYPE, 'date'),
        (COMPOSITIONAL_TARGET_TYPE, 'compositional'),
    )
    # required fields for all types
    type = models.IntegerField(choices=TARGET_TYPE_CHOICES,
                               help_text="The Target's type. The choices are 'continuous', 'discrete', 'nominal', "
                                         "'binary', 'date', and 'compositional'.")
    name = models.TextField(help_text="A brief name for the target.")
    description = models.TextField(help_text="A verbose description of what the target is.")
    is_step_ahead = BooleanField(help_text="True if the target is one of a sequence of targets that predict values at "
                                           "different points in the future.")
    step_ahead_increment = IntegerField(help_text="An integer, indicating the forecast horizon represented by this "
                                                  "target. It is required if `is_step_ahead` is True.",
                                        null=True, default=None)

    # type-specific fields
    unit = models.TextField(help_text="This target's units, e.g., 'percentage', 'week', 'cases', etc.", null=True)


    # 'list' type-specific fields: see TargetLwr.lwrs, TargetCat.cats, TargetDate.date, and TargetDate.range


    def __repr__(self):
        return str((self.pk, self.name, Target.type_as_str(self.type), self.is_step_ahead, self.step_ahead_increment))


    def __str__(self):  # todo
        return basic_str(self)


    # def is_date(self):
    #     return self.type == Target.CONTINUOUS_TARGET_TYPE


    @classmethod
    def type_as_str(cls, the_type_int):
        for type_int, type_name in cls.TARGET_TYPE_CHOICES:
            if type_int == the_type_int:
                return type_name

        return '!?'


    def save(self, *args, **kwargs):
        """
        Validates is_step_ahead and step_ahead_increment, and is_date and is_step_ahead.
        """
        if not self.name:
            raise ValidationError("name is required")
        elif not self.description:
            raise ValidationError("description is required")
        elif getattr(self, 'is_step_ahead', None) is None:
            raise ValidationError("is_step_ahead is required")
        elif self.is_step_ahead and (self.step_ahead_increment is None):
            raise ValidationError('passed is_step_ahead with no step_ahead_increment')
        elif (self.type in [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE, Target.DATE_TARGET_TYPE]) \
                and (getattr(self, 'unit', None) is None):
            raise ValidationError("unit is required")
        elif (self.type == Target.DATE_TARGET_TYPE) and (self.unit not in Target.DATE_UNITS):
            raise ValidationError(f"unit was not one of: {Target.DATE_UNITS!r}")

        # done
        super().save(*args, **kwargs)


    @classmethod
    def data_type(cls, target_type):
        """
        :param target_type: one of my *_TARGET_TYPE values
        :return: the database data_type for target_type
        """
        return {
            Target.CONTINUOUS_TARGET_TYPE: Target.FLOAT_DATA_TYPE,
            Target.DISCRETE_TARGET_TYPE: Target.INTEGER_DATA_TYPE,
            Target.NOMINAL_TARGET_TYPE: Target.TEXT_DATA_TYPE,
            Target.BINARY_TARGET_TYPE: Target.BOOLEAN_DATA_TYPE,
            Target.DATE_TARGET_TYPE: Target.DATE_DATA_TYPE,
            Target.COMPOSITIONAL_TARGET_TYPE: Target.TEXT_DATA_TYPE,
        }[target_type]


    def set_cats(self, cats):
        """
        Creates TargetCat entries for each cat in cats, first deleting all current ones.

        :param cats: a list of either all floats or all strings, depending on my data_type
        """
        # validate target type
        valid_target_types = [Target.CONTINUOUS_TARGET_TYPE, Target.NOMINAL_TARGET_TYPE,
                              Target.COMPOSITIONAL_TARGET_TYPE]
        if self.type not in valid_target_types:
            raise ValidationError(f"invalid target type  {self.type}. must be one of: {valid_target_types}")

        # validate cats
        data_type = Target.data_type(self.type)
        types_set = set(map(type, cats))
        if len(types_set) != 1:
            raise ValidationError(f"there was more than one data type in cats={cats}: {types_set}")

        cats_type = list(types_set)[0]
        if data_type != cats_type:
            raise ValidationError(f"cats type did not match target data type. cats type={cats_type}, "
                                  f"data_type={data_type}")

        # delete and save the new TargetCats
        TargetCat.objects.filter(target=self).delete()
        for cat in cats:
            TargetCat.objects.create(target=self,
                                     cat_f=cat if (data_type == Target.FLOAT_DATA_TYPE) else None,
                                     cat_t=cat if (data_type == Target.TEXT_DATA_TYPE) else None)

        # ditto for TargetLwrs for the continuous case (required for scoring), calculating `upper` via zip().
        # NB: we use infinity for the last bin's upper!
        if self.type == Target.CONTINUOUS_TARGET_TYPE:
            cats = sorted(cats)
            for lwr, upper in itertools.zip_longest(cats, cats[1:], fillvalue=float('inf')):
                TargetLwr.objects.create(target=self, lwr=lwr, upper=upper)


    def set_dates(self, dates):
        """
        Creates TargetDate entries for each date in dates, first deleting all current ones.

        :param dates: a list of date strings in YYYY_MM_DD_DATE_FORMAT
        """
        # validate target type
        valid_target_types = [Target.DATE_TARGET_TYPE]
        if self.type not in valid_target_types:
            raise ValidationError(f"invalid target type  {self.type}. must be one of: {valid_target_types}")

        # validate dates
        for date in dates:
            try:
                datetime.strptime(date, YYYY_MM_DD_DATE_FORMAT)
            except ValueError as ve:
                raise ValidationError(f"date was not in YYYY-MM-DD format: {date}")

        # delete and save the new TargetDates
        TargetDate.objects.filter(target=self).delete()
        for date in dates:
            TargetDate.objects.create(target=self, date=date)


    def set_range(self, lower, upper):
        """
        Creates two TargetRange entries for lower and upper, first deleting all current ones.

        :param lower: a float or string, depending on my data_type
        :param upper: ""
        """
        # validate target type
        valid_target_types = [Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE]
        if self.type not in valid_target_types:
            raise ValidationError(f"invalid target type  {self.type}. must be one of: {valid_target_types}")

        # validate lower, upper
        data_type = Target.data_type(self.type)
        if type(lower) != type(upper):
            raise ValidationError(f"lower and upper were of different data types: {type(lower)} != {type(upper)}")
        elif data_type != type(lower):
            raise ValidationError(f"lower and upper data type did not match target data type. "
                                  f"lower/upper type={type(lower)}, data_type={data_type}")

        # delete and save the new TargetRanges
        TargetRange.objects.filter(target=self).delete()
        TargetRange.objects.create(target=self,
                                   value_i=lower if (data_type == Target.INTEGER_DATA_TYPE) else None,
                                   value_f=lower if (data_type == Target.FLOAT_DATA_TYPE) else None)
        TargetRange.objects.create(target=self,
                                   value_i=upper if (data_type == Target.INTEGER_DATA_TYPE) else None,
                                   value_f=upper if (data_type == Target.FLOAT_DATA_TYPE) else None)


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
            Target.BINARY_TARGET_TYPE: [NamedDistribution.BERN_DIST],
            Target.DATE_TARGET_TYPE: [],  # n/a
            Target.COMPOSITIONAL_TARGET_TYPE: [],  # n/a
        }[target_type]


    @classmethod
    def valid_prediction_types(cls, target_type):
        """
        :param target_type: one of my *_TARGET_TYPE values
        :return: a list of valid concrete Prediction subclasses for target_type
        """
        return {
            Target.CONTINUOUS_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution, NamedDistribution],
            Target.DISCRETE_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution, NamedDistribution],
            Target.NOMINAL_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution],
            Target.BINARY_TARGET_TYPE: [PointPrediction, SampleDistribution, NamedDistribution],
            Target.DATE_TARGET_TYPE: [PointPrediction, BinDistribution, SampleDistribution],
            Target.COMPOSITIONAL_TARGET_TYPE: [BinDistribution]
        }[target_type]


#
# ---- TargetCat ----
#

class TargetCat(models.Model):
    """
    Associates a 'list' of cat values with Targets of type Target.NOMINAL_TARGET_TYPE and Target.COMPOSITIONAL.
    """
    target = models.ForeignKey('Target', blank=True, null=True, related_name='cats', on_delete=models.CASCADE)
    cat_f = models.FloatField(null=True)  # NULL if cat_t non-NULL
    cat_t = models.TextField(null=True)  # NULL if cat_f non-NULL


    def __repr__(self):
        return str((self.pk, self.target.pk, self.cat_f, self.cat_t))


    def __str__(self):  # todo
        return basic_str(self)


#
# ---- TargetLwr ----
#

class TargetLwr(models.Model):
    """
    Associates a 'list' of lwr values with Targets of type Target.CONTINUOUS_TARGET_TYPE that have 'cats'

    . These act as a "template"
    against which forecast BinLwr predictions can be validated against. Note that only lwr is typically passed by the
    user (as `cat`). upper is typically calculated from lwr by the caller.

    Regarding upper: It is currently used only for scoring, when the true bin is queried for. In that case we test
    truth >= lwr AND truth < upper. Therefore it is currently calculated by utils.project.validate_and_create_targets()
    based on lwr. That function has to infer the final bin's upper, and uses float('inf') for that
    """
    target = models.ForeignKey('Target', blank=True, null=True, related_name='lwrs', on_delete=models.CASCADE)
    lwr = models.FloatField()
    upper = models.FloatField()


    def __repr__(self):
        return str((self.pk, self.target.pk, self.lwr, self.upper))


    def __str__(self):  # todo
        return basic_str(self)


#
# ---- TargetDate ----
#

class TargetDate(models.Model):
    """
    Associates a 'list' of date values with Targets of type Target.DATE.
    """
    target = models.ForeignKey('Target', blank=True, null=True, related_name='dates', on_delete=models.CASCADE)
    date = models.DateField()


    def __repr__(self):
        return str((self.pk, self.target.pk, self.date))


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
