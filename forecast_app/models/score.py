from django.db import models

from forecast_app.models import ForecastModel
from utils.utilities import basic_str


class Score(models.Model):
    """
    Represents the definition of a score. In our terminology, a `Score` has corresponding `ScoreValue` objects.
    Example scores: `Error`, `Absolute Error`, `Log Score`, and `Multi Bin Log Score`.
    """
    name = models.CharField(max_length=200, help_text="The score's name, e.g., 'Absolute Error'.")

    description = models.CharField(max_length=2000, help_text="A paragraph describing the score.")


    def __repr__(self):
        return str((self.pk, self.name, self.description))


    def __str__(self):  # todo
        return basic_str(self)


class ScoreValue(models.Model):
    """
    Represents a single value of a Score, e.g., an 'Absolute Error' (the Score) of 0.1 (the ScoreValue).
    """
    score = models.ForeignKey(Score, related_name='values', on_delete=models.CASCADE)

    forecast_model = models.ForeignKey(ForecastModel, on_delete=models.CASCADE)

    location = models.ForeignKey('Location', blank=True, null=True, on_delete=models.SET_NULL)

    target = models.ForeignKey('Target', blank=True, null=True, on_delete=models.SET_NULL)

    value = models.FloatField(null=False)


    def __repr__(self):
        return str((self.pk, self.score.pk, self.forecast_model.pk, self.location.pk, self.target.pk, self.value))


    def __str__(self):  # todo
        return basic_str(self)
