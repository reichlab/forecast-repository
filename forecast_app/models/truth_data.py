from django.db import models

from forecast_app.models.project import TimeZero
from utils.utilities import basic_str


class TruthData(models.Model):
    """
    Represents one line of truth data for a Project, i.e., a data point. Truth data is a project-agnostic way to capture
    actual values that models predicted for. Each Project is responsible to generating a truth table csv file, which is
    then loaded via Project.load_truth_data() - see.
    """

    time_zero = models.ForeignKey(TimeZero, on_delete=models.CASCADE)
    # the standard CDC format columns:
    location = models.ForeignKey('Location', blank=True, null=True, on_delete=models.CASCADE)
    target = models.ForeignKey('Target', blank=True, null=True, on_delete=models.CASCADE)
    value = models.FloatField(null=True)


    def __repr__(self):
        return str((self.pk, self.time_zero, self.location, self.target, self.value))


    def __str__(self):  # todo
        return basic_str(self)
