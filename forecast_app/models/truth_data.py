from django.db import models

from forecast_app.models.project import TimeZero
from utils.utilities import basic_str


class TruthData(models.Model):
    """
    Represents one line of truth data for a Project, i.e., a data point. Truth data is a project-agnostic way to capture
    actual values that models predicted for. Each Project is responsible to generating a truth table csv file, which is
    then loaded via Project.load_truth_data() - see. Note that we do not store a link to the owning Project b/c that can
    be obtained via time_zero.project - see truth_data_qs(). also note that we have a sparse table with value columns
    covering all possible target types, similar to PointPrediction.
    """

    time_zero = models.ForeignKey(TimeZero, on_delete=models.CASCADE)
    unit = models.ForeignKey('Unit', blank=True, null=True, on_delete=models.CASCADE)
    target = models.ForeignKey('Target', blank=True, null=True, on_delete=models.CASCADE)
    value_i = models.IntegerField(null=True)  # NULL if any others non-NULL
    value_f = models.FloatField(null=True)  # ""
    value_t = models.TextField(null=True)  # ""
    value_d = models.DateField(null=True)  # ""
    value_b = models.NullBooleanField(null=True)  # ""


    def __repr__(self):
        return str((self.pk, self.time_zero.pk, self.unit.pk, self.target.pk, '.',
                    self.value_i, self.value_f, self.value_t, self.value_d, self.value_b))


    def __str__(self):  # todo
        return basic_str(self)
