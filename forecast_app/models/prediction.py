from django.db import models

from utils.utilities import basic_str


# This file contains classes that represent a variety of forecast data formats. It is inspired by those of
# https://github.com/cdcepi/predx - see https://github.com/cdcepi/predx/blob/master/predx_classes.md .

#
# ---- Prediction ----
#

class Prediction(models.Model):
    """
    Abstract base class representing a prediction of any type, e.g., point, binomial distribution, samples, etc.
    """


    class Meta:
        abstract = True


    forecast = models.ForeignKey('Forecast', on_delete=models.CASCADE, null=True)
    location = models.ForeignKey('Location', blank=True, null=True, on_delete=models.SET_NULL)
    target = models.ForeignKey('Target', blank=True, null=True, on_delete=models.SET_NULL)


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.location.pk, self.target.pk))


    def __str__(self):  # todo
        return basic_str(self)


    @classmethod
    def concrete_subclasses(cls):
        """
        Utility that returns a set of Prediction subclasses where Meta.abstract is not True.
        """


        # https://stackoverflow.com/questions/3862310/how-to-find-all-the-subclasses-of-a-class-given-its-name
        def all_subclasses(cls):
            return set(cls.__subclasses__()).union(
                [s for c in cls.__subclasses__() for s in all_subclasses(c)])


        # https://stackoverflow.com/questions/1772841/django-how-to-determine-if-model-class-is-abstract
        return {subclass for subclass in (all_subclasses(cls)) if not subclass._meta.abstract}


#
# ---- PointPrediction ----
#

class PointPrediction(Prediction):
    """
    Concrete class representing point predictions. Note that point values can be integers, floats, or text, depending on
    the Target.point_value_type associated with the prediction. We chose to implement this as a sparse table where two
    of the three columns is NULL in every row.
    """

    value_i = models.IntegerField(null=True)  # NULL if any others non-NULL
    value_f = models.FloatField(null=True)  # ""
    value_t = models.TextField(null=True)  # ""
    value_d = models.DateField(null=True)  # ""
    value_b = models.NullBooleanField(null=True)  # ""


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.location.pk, self.target.pk, '.',
                    self.value_i, self.value_f, self.value_t, self.value_d, self.value_b))


    def __str__(self):  # todo
        return basic_str(self)


    @staticmethod
    def first_non_none_value(value_i, value_f, value_t, value_d, value_b):
        """
        Simple utility that returns the first of the passed value_* args that is not None. NB: you cannot simply use
        'or' b/c 0 values fail. Returns None if all are None.
        """
        non_non_values = [_ for _ in [value_i, value_f, value_t, value_d, value_b] if _ is not None]
        return non_non_values[0] if non_non_values else None


#
# ---- NamedDistribution ----
#

class NamedDistribution(Prediction):
    """
    Concrete class representing named distributions like normal, log normal, gamma, etc. These are essentially named
    functions (the function's `family`) with up to general-purpose three parameter fields - `param1`, `param2`, etc.
    Each parameter's semantics and calculation are defined by the family.

    We chose to use a single sparse table to represent all families of distributions, rather than specific Django models
    for each. Further, we use float for all three parameters including ones that are properly ints (e.g., binomial's
    `n`). We simply let the database cast ints to float, and then rely on the family's `calculate()` function to cast
    back to int as needed. This seemed more reasonable than having separate int and float fields (would have been 6
    fields instead of 3).

    Each family has a definition that describes parameter semantics, parameter types, abbreviations, and `calculate()`
    implementations - see FAMILY_DEFINITIONS. FAMILY_CHOICES below defines the family_id for each family, which is
    referenced in FAMILY_DEFINITIONS.

    To add a new named distribution: todo xx details - ala the Score class docs
    """

    NORM_DIST = 0
    LNORM_DIST = 1
    GAMMA_DIST = 2
    BETA_DIST = 3
    POIS_DIST = 4
    NBINOM_DIST = 5
    NBINOM2_DIST = 6
    FAMILY_CHOICES = (  # also defines family long_name
        (NORM_DIST, 'Normal'),
        (LNORM_DIST, 'Log Normal'),
        (GAMMA_DIST, 'Gamma'),
        (BETA_DIST, 'Beta'),
        (POIS_DIST, 'Poisson'),
        (NBINOM_DIST, 'Negative Binomial'),
        (NBINOM2_DIST, 'Negative Binomial 2'),
    )
    family = models.IntegerField(choices=FAMILY_CHOICES)

    param1 = models.FloatField(null=True)  # the first parameter
    param2 = models.FloatField(null=True)  # second
    param3 = models.FloatField(null=True)  # third

    # maps named distribution abbreviations to their FAMILY_CHOICES value. note that csv files use abbreviations for the
    # 'family' column
    FAMILY_CHOICE_TO_ABBREVIATION = {
        NORM_DIST: 'norm',
        LNORM_DIST: 'lnorm',
        GAMMA_DIST: 'gamma',
        BETA_DIST: 'beta',
        POIS_DIST: 'pois',
        NBINOM_DIST: 'nbinom',
        NBINOM2_DIST: 'nbinom2',
    }


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.location.pk, self.target.pk,
                    self.family, '.', self.param1, self.param2, self.param3))


    def __str__(self):  # todo
        return basic_str(self)


def calc_named_distribution(abbreviation, param1, param2, param3):
    """
    Does the actual NamedDistribution function calculation based on abbreviation and the passed parameters.
    abbreviation must be a FAMILY_DEFINI4TIONS key.
    """
    if abbreviation not in NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.values():
        raise RuntimeError(f"invalid family. abbreviation='{abbreviation}', "
                           f"abbreviations={NamedDistribution.FAMILY_CHOICE_TO_ABBREVIATION.values()}")

    if abbreviation == 'norm':
        raise NotImplementedError()  # todo xx
    elif abbreviation == 'lnorm':
        raise NotImplementedError()
    elif abbreviation == 'gamma':
        raise NotImplementedError()
    elif abbreviation == 'beta':
        raise NotImplementedError()
    elif abbreviation == 'pois':
        raise NotImplementedError()
    elif abbreviation == 'nbinom':
        raise NotImplementedError()
    else:  # elif abbreviation == 'nbinom2':
        raise NotImplementedError()


#
# ---- EmpiricalDistribution ----
#

class EmpiricalDistribution(Prediction):
    """
    Abstract base class representing empirical distributions like bins and samples. This class has no instance
    variables.
    """


    class Meta:
        abstract = True


#
# ---- BinDistribution ----
#

class BinDistribution(EmpiricalDistribution):
    """
    Concrete class representing binned distribution with a category for each bin. Like PointPrediction, we compromise
    database design by having multiple fields/columns for required data/field types. For a particular object/record, all
    but one are NULL.
    """

    cat_i = models.IntegerField(null=True)  # NULL if any others non-NULL
    cat_f = models.FloatField(null=True)  # ""
    cat_t = models.TextField(null=True)  # ""
    cat_d = models.DateField(null=True)  # ""
    cat_b = models.NullBooleanField(null=True)  # ""
    prob = models.FloatField()


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.location.pk, self.target.pk, '.',
                    self.cat_i, self.cat_f, self.cat_t, self.cat_d, self.cat_b, '.', self.prob))


#
# ---- SampleDistribution ----
#

class SampleDistribution(EmpiricalDistribution):
    """
    Concrete class representing character string samples from categories. Like PointPrediction, we compromise
    database design by having multiple fields/columns for required data/field types. For a particular object/record, all
    but one are NULL.
    """

    sample_i = models.IntegerField(null=True)  # NULL if any others non-NULL
    sample_f = models.FloatField(null=True)  # ""
    sample_t = models.TextField(null=True)  # ""
    sample_d = models.DateField(null=True)  # ""
    sample_b = models.NullBooleanField(null=True)  # ""


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.location.pk, self.target.pk, '.',
                    self.sample_i, self.sample_f, self.sample_t, self.sample_d, self.sample_b))
