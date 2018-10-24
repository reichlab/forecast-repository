import logging
import timeit

import django_rq
from django.db import models
from django.shortcuts import get_object_or_404

from forecast_app.models import Forecast, Project, ForecastModel
from utils.utilities import basic_str


logger = logging.getLogger(__name__)


#
# Score
#

class Score(models.Model):
    """
    Represents the definition of a score. In our terminology, a `Score` has corresponding `ScoreValue` objects.
    Example scores: `Error`, `Absolute Error`, `Log Score`, and `Multi Bin Log Score`.

    Design notes: We needed a way to represent arbitrary scores in a way that was fairly clean and generalizable, but
    with minimal impact to migrations, etc. After looking at many options, including the inbuilt ones
    ( https://docs.djangoproject.com/en/1.11/topics/db/models/#model-inheritance ) and third party ones like
    https://django-polymorphic.readthedocs.io/ and
    https://django-model-utils.readthedocs.io/en/latest/managers.html#inheritancemanager , we decided to simply store
    a text abbreviation for each Score and use that to look up the corresponding function to call in the hard-coded
    forecast_app.scores.definitions module.

    To add a new score:
    1. Add a new item to forecast_app.scores.definitions.SCORE_ABBREV_TO_NAME_AND_DESCR. The abbreviation key has some
       constraints and a special use:
       - The abbreviation is used as the column name in exported CSV files and therefore should be csv-compatible, i.e.,
         no commas, tabs, etc. We recommend using a short lower case name with only underscores, such as 'error' or
         'abs_error'.
       - The abbreviation is used to obtain the function name to call that calculates it by creating ScoreValue entries
         for a particular Score and Project. The naming convention for these functions is documented in #2 next.
    2. Define a function in the forecast_app.scores.definitions module named `calc_<abbreviation>`, where <abbreviation>
       is the one from your score definition in #1. The function will be passed two arguments: score and forecast_model.
       It should create ScoreValues for every Forecast in the model.
    3. Call Score.ensure_all_scores_exist().

    Notes:
    - To update a particular score for a particular ForecastModel, call update_score_for_model() rather than directly
       calling the 'calc_*()' method (see update_score_for_model() docs for details).
    - Newly-created scores will require restarting the web and any worker processes so they have the definitions
      available to them.
    """

    abbreviation = models.CharField(max_length=200, help_text="Short name used as a column header for this score in "
                                                              "downloaded CSV score files. Also used to look up the "
                                                              "Score's calculation function name.")

    name = models.CharField(max_length=200, help_text="The score's name, e.g., 'Absolute Error'.")

    description = models.CharField(max_length=2000, help_text="A paragraph describing the score.")


    def __repr__(self):
        return str((self.pk, self.abbreviation, self.name, self.description))


    def __str__(self):  # todo
        return basic_str(self)


    def num_score_values(self):
        """
        Returns # ScoreValues for me.
        """
        return ScoreValue.objects.filter(score=self).count()


    def num_score_values_for_model(self, forecast_model):
        """
        Returns # ScoreValues for me related to project.
        """
        return ScoreValue.objects.filter(score=self, forecast__forecast_model=forecast_model).count()


    def num_score_values_for_project(self, project):
        """
        Returns # ScoreValues for me related to project.
        """
        return ScoreValue.objects.filter(score=self, forecast__forecast_model__project=project).count()


    def last_update_for_forecast_model(self, forecast_model):
        """
        :return: my ScoreLastUpdate for forecast_model, or None if no entry
        """
        return ScoreLastUpdate.objects.filter(forecast_model=forecast_model, score=self).first()  # None o/w


    def last_update_for_project(self, project):
        """
        :return: my ScoreLastUpdate for project, or None if no entry
        """
        return ScoreLastUpdate.objects.filter(forecast_model__project=project, score=self).first()  # None o/w


    def set_last_update_for_forecast_model(self, forecast_model):
        """
        Updates my ScoreLastUpdate for project, creating it if necessary.
        """
        score_last_update, is_created = ScoreLastUpdate.objects.get_or_create(forecast_model=forecast_model, score=self)
        score_last_update.save()  # triggers last_update's auto_now


    def csv_column_name(self):
        """
        :return: the column name to use in the header when outputting CSV data. uses my abbreviation
        """
        return self.abbreviation


    #
    # calculation helper methods
    #

    @classmethod
    def ensure_all_scores_exist(cls):
        """
        Utility that ensures all Scores corresponding to SCORE_ABBREV_TO_NAME_AND_DESCR exist.
        """
        import forecast_app.scores.definitions


        for abbreviation, (name, description) in forecast_app.scores.definitions.SCORE_ABBREV_TO_NAME_AND_DESCR.items():
            score, is_created = Score.objects.get_or_create(abbreviation=abbreviation,
                                                            name=name, description=description)
            if is_created:
                logger.debug("ensure_all_scores_exist(): created Score: {}".format(score))


    def update_score_for_model(self, forecast_model):
        """
        The top-level method for updating this score for forecast_model. You should call this rather than directly
        calling the 'calc_*()' methods b/c this method does some important pre- and post-calculation housekeeping.
        Runs in the calling thread and therefore blocks.
        """
        import forecast_app.scores.definitions


        start_time = timeit.default_timer()
        logger.debug("update_score_for_model(): entered. score={}, forecast_model={}".format(self, forecast_model))

        logger.debug("update_score_for_model(): deleting existing ScoreValues for model")
        ScoreValue.objects.filter(score=self, forecast__forecast_model=forecast_model).delete()

        # e.g., 'calc_error' or 'calc_abs_error':
        calc_function = getattr(forecast_app.scores.definitions, 'calc_' + self.abbreviation)
        logger.debug("update_score_for_model(): calling calculation function: {}".format(calc_function))
        calc_function(self, forecast_model)

        self.set_last_update_for_forecast_model(forecast_model)
        logger.debug("update_score_for_model(): done. -> {} total ScoreValues. time: {}"
                     .format(self.num_score_values(), timeit.default_timer() - start_time))


    def clear(self):
        """
        Deletes all my ScoreValues and ScoreLastUpdates.
        """
        ScoreValue.objects.filter(score=self).delete()
        ScoreLastUpdate.objects.filter(score=self).delete()


    @classmethod
    def enqueue_update_scores_for_all_projects(cls):
        """
        Top-level method for enqueuing the update of all scores for all projects.
        """
        Score.ensure_all_scores_exist()
        for score in cls.objects.all():
            for project in Project.objects.all():
                for forecast_model in project.models.all():
                    logger.debug("enqueuing update project scores. score={}, forecast_model={}"
                                 .format(score, forecast_model))
                    django_rq.enqueue(_update_model_scores, score.pk, forecast_model.pk)


def _update_model_scores(score_pk, forecast_model_pk):
    """
    Enqueue helper function.
    """
    score = get_object_or_404(Score, pk=score_pk)
    forecast_model = get_object_or_404(ForecastModel, pk=forecast_model_pk)
    score.update_score_for_model(forecast_model)


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
    Similar to RowCountCache, records the last time a particular Score was updated for a particular ForecastModel.
    """

    forecast_model = models.ForeignKey(ForecastModel, on_delete=models.CASCADE)

    # datetime at the last update. auto_now: automatically set the field to now every time the object is saved.
    updated_at = models.DateTimeField(auto_now=True)

    score = models.ForeignKey(Score, on_delete=models.CASCADE)


    def __repr__(self):
        return str((self.pk, self.forecast_model, self.updated_at, self.score))


    def __str__(self):  # todo
        return basic_str(self)
