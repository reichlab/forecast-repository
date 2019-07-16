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


#
# ---- PointPrediction ----
#

class PointPrediction(Prediction):
    """
    Concrete class representing point predictions. Note that point values can be integers, floats, or text, depending on
    the Target associated with the prediction. We chose to implement this as a sparse table where two of the three
    columns is NULL in every row.
    """

    value_i = models.IntegerField(null=True)
    value_f = models.FloatField(null=True)
    value_t = models.TextField(null=True)


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.location.pk, self.target.pk,
                    self.value_i, self.value_f, self.value_t))


    def __str__(self):  # todo
        return basic_str(self)


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
    BERN_DIST = 4
    BINOM_DIST = 5
    POIS_DIST = 6
    NBINOM_DIST = 7
    NBINOM2_DIST = 8
    FAMILY_CHOICES = (  # also defines family long_name
        (NORM_DIST, 'Normal'),
        (LNORM_DIST, 'Log Normal'),
        (GAMMA_DIST, 'Gamma'),
        (BETA_DIST, 'Beta'),
        (BERN_DIST, 'Bernoulli'),
        (BINOM_DIST, 'Binomial'),
        (POIS_DIST, 'Poisson'),
        (NBINOM_DIST, 'Negative Binomial'),
        (NBINOM2_DIST, 'Negative Binomial 2'),
    )
    family = models.IntegerField(choices=FAMILY_CHOICES)

    param1 = models.FloatField(null=True)  # the first parameter
    param2 = models.FloatField(null=True)  # second
    param3 = models.FloatField(null=True)  # third


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.location.pk, self.target.pk,
                    self.family, '.', self.param1, self.param2, self.param3))


    def __str__(self):  # todo
        return basic_str(self)


# maps named distribution abbreviations to their FAMILY_CHOICES value. note that csv files use abbreviations for the
# 'family' column
FAMILY_ABBREVIATION_TO_FAMILY_ID = {
    'norm': NamedDistribution.NORM_DIST,
    'lnorm': NamedDistribution.LNORM_DIST,
    'gamma': NamedDistribution.GAMMA_DIST,
    'beta': NamedDistribution.BETA_DIST,
    'bern': NamedDistribution.BERN_DIST,
    'binom': NamedDistribution.BINOM_DIST,
    'pois': NamedDistribution.POIS_DIST,
    'nbinom': NamedDistribution.NBINOM_DIST,
    'nbinom2': NamedDistribution.NBINOM2_DIST,
}


def calc_named_distribution(abbreviation, param1, param2, param3):
    """
    Does the actual NamedDistribution function calculation based on abbreviation and the passed parameters.
    abbreviation must be a FAMILY_DEFINITIONS key.
    """
    if abbreviation not in FAMILY_ABBREVIATION_TO_FAMILY_ID:
        raise RuntimeError(f"Invalid abbreviation '{abbreviation}' - wasn't one of: "
                           f"{FAMILY_ABBREVIATION_TO_FAMILY_ID.keys()}")

    if abbreviation == 'norm':
        raise NotImplementedError()  # todo xx
    elif abbreviation == 'lnorm':
        raise NotImplementedError()
    elif abbreviation == 'gamma':
        raise NotImplementedError()
    elif abbreviation == 'beta':
        raise NotImplementedError()
    elif abbreviation == 'bern':
        raise NotImplementedError()
    elif abbreviation == 'binom':
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
# ---- BinLwrDistribution ----
#

class BinLwrDistribution(EmpiricalDistribution):
    """
    Concrete class representing binned distribution defined by inclusive lower bounds for each bin.
    """

    lwr = models.FloatField()
    prob = models.FloatField()


#
# ---- SampleDistribution ----
#

class SampleDistribution(EmpiricalDistribution):
    """
    Concrete class representing numeric samples.
    """

    sample = models.FloatField()


#
# ---- BinCatDistribution ----
#

class BinCatDistribution(EmpiricalDistribution):
    """
    Concrete class representing binned distribution with a category for each bin.
    """

    cat = models.TextField()
    prob = models.FloatField()


#
# ---- SampleCatDistribution ----
#

class SampleCatDistribution(EmpiricalDistribution):
    """
    Concrete class representing character string samples from categories.
    """

    cat = models.TextField()
    sample = models.TextField()


#
# ---- BinaryDistribution ----
#

class BinaryDistribution(EmpiricalDistribution):
    """
    Concrete class representing binary distributions.
    Validation: The arg cannot be null.
    """

    prob = models.FloatField()
