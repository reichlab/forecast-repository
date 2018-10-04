from django.db import models

from forecast_app.models import Forecast, Project
from utils.utilities import basic_str


#
# Score
#

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


    def last_update_for_project(self, project):
        """
        :return: my ScoreLastUpdate for project, or None if no entry
        """
        return ScoreLastUpdate.objects.filter(project=project, score=self).first()  # None o/w


    def set_last_update_for_project(self, project):
        """
        Updates my ScoreLastUpdate for project, creating it if necessary.
        """
        score_last_update, is_created = ScoreLastUpdate.objects.get_or_create(project=project, score=self)
        score_last_update.save()  # triggers last_update's auto_now


    def num_score_values(self):
        """
        Returns # ScoreValues for me.
        """
        return ScoreValue.objects.filter(score=self).count()


    def num_score_values_for_project(self, project):
        """
        Returns # ScoreValues for me related to project.
        """
        return ScoreValue.objects.filter(score=self, forecast__forecast_model__project=project).count()


#
# ScoreValue
#

class ScoreValue(models.Model):
    """
    Represents a single value of a Score, e.g., an 'Absolute Error' (the Score) of 0.1 (the ScoreValue).
    """
    score = models.ForeignKey(Score, related_name='values', on_delete=models.CASCADE)

    forecast = models.ForeignKey(Forecast, on_delete=models.CASCADE)

    location = models.ForeignKey('Location', blank=True, null=True, on_delete=models.SET_NULL)

    target = models.ForeignKey('Target', blank=True, null=True, on_delete=models.SET_NULL)

    value = models.FloatField(null=False)


    def __repr__(self):
        return str((self.pk, self.score.pk, self.forecast.pk, self.location.pk, self.target.pk, self.value))


    def __str__(self):  # todo
        return basic_str(self)


#
# ScoreLastUpdate
#

class ScoreLastUpdate(models.Model):
    """
    Similar to RowCountCache, records the last time a particular Score was updated for a particular Project.
    """

    project = models.ForeignKey(Project, on_delete=models.CASCADE)

    # datetime at the last update. auto_now: automatically set the field to now every time the object is saved.
    last_update = models.DateTimeField(auto_now=True)

    score = models.ForeignKey(Score, on_delete=models.CASCADE)


    def __repr__(self):
        return str((self.pk, self.project, self.last_update, self.score))


    def __str__(self):  # todo
        return basic_str(self)
