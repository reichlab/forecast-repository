import logging
import timeit

import django_rq
from django.db import models
from django.shortcuts import get_object_or_404
from rq.timeouts import JobTimeoutException

from forecast_app.models import Forecast, ForecastModel
from forecast_repo.settings.base import UPDATE_MODEL_SCORES_QUEUE_NAME
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
    abbreviation = models.TextField(help_text="Short name used as a column header for this score in downloaded CSV " \
                                              "score files. Also used to look up the Score's calculation function " \
                                              "name.")
    name = models.TextField(help_text="The score's name, e.g., 'Absolute Error'.")
    description = models.TextField(help_text="A paragraph describing the score.")


    def __repr__(self):
        return str((self.pk, self.abbreviation, self.name))


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
                logger.debug("ensure_all_scores_exist(): created: {}".format(score))


    def update_score_for_model(self, forecast_model):
        """
        The top-level method for updating this score for forecast_model. You should call this rather than directly
        calling the 'calc_*()' methods b/c this method does some important pre- and post-calculation housekeeping.
        Runs in the calling thread and therefore blocks.
        """
        import forecast_app.scores.definitions


        start_time = timeit.default_timer()
        logger.debug(f"update_score_for_model(): 1/4 entered. score={self}, forecast_model={forecast_model}")

        logger.debug(f"update_score_for_model(): 2/4 deleting existing ScoreValues for model. "
                     f"score={self}, forecast_model={forecast_model}")
        forecast_model_score_value_qs = ScoreValue.objects.filter(score=self, forecast__forecast_model=forecast_model)
        forecast_model_score_value_qs.delete()

        # e.g., 'calc_error' or 'calc_abs_error':
        calc_function = getattr(forecast_app.scores.definitions, 'calc_' + self.abbreviation)
        logger.debug(f"update_score_for_model(): 3/4 calling calculation function: {calc_function}. score={self}, "
                     f"forecast_model={forecast_model}")
        calc_function(self, forecast_model)

        self.set_last_update_for_forecast_model(forecast_model)
        logger.debug(f"update_score_for_model(): 4/4 done. score={self}, forecast_model={forecast_model} -> "
                     f"count={forecast_model_score_value_qs.count()} "
                     f"total ScoreValues. time: {timeit.default_timer() - start_time}")


    def clear(self):
        """
        Deletes all my ScoreValues and ScoreLastUpdates.
        """
        ScoreValue.objects.filter(score=self).delete()
        ScoreLastUpdate.objects.filter(score=self).delete()


    @classmethod
    def enqueue_update_scores_for_all_models(cls, is_only_changed, dry_run=False):
        """
        Utility method that enqueues updates of all scores for all models in all projects.

        :param is_only_changed: True if should exclude enqueuing models that have not changed since the last score
            update.
        :param dry_run: True means just print a report of Score/ForecastModel pairs that would be updated
        :return list of enqueued 2-tuples: (score, forecast_model)
        """
        logger.debug(f"enqueue_update_scores_for_all_models: entered. is_only_changed={is_only_changed}, "
                     f"dry_run={dry_run}")
        Score.ensure_all_scores_exist()
        queue = django_rq.get_queue(UPDATE_MODEL_SCORES_QUEUE_NAME)
        enqueued_score_models = []  # 2-tuples: (score, forecast_model)
        for score in cls.objects.all():
            for forecast_model in ForecastModel.objects.all():
                model_score_change = forecast_model.score_change
                score_last_update = score.last_update_for_forecast_model(forecast_model)  # None o/w
                is_out_of_date = (score_last_update is None) or \
                                 (model_score_change.changed_at > score_last_update.updated_at)
                if is_only_changed and (not is_out_of_date):
                    continue

                logger.debug(f"enqueuing score update. {score}, {forecast_model} "
                             f"{model_score_change.changed_at} > "
                             f"{score_last_update.updated_at if score_last_update else '(no score_last_update)'}")
                if not dry_run:
                    queue.enqueue(_update_model_scores_worker, score.pk, forecast_model.pk)
                enqueued_score_models.append((score, forecast_model))
        logger.debug(
            f"enqueue_update_scores_for_all_models: done. # enqueued_score_models={len(enqueued_score_models)}")
        return enqueued_score_models


def _update_model_scores_worker(score_pk, forecast_model_pk):
    """
    enqueue() helper function
    """
    score = get_object_or_404(Score, pk=score_pk)
    forecast_model = get_object_or_404(ForecastModel, pk=forecast_model_pk)
    try:
        score.update_score_for_model(forecast_model)
    except JobTimeoutException as jte:
        logger.error(f"_update_model_scores_worker(): Job timeout: {jte!r}. score={score}, "
                     f"forecast_model={forecast_model}")
    except Exception as ex:
        logger.error(f"_update_model_scores_worker(): error: {ex!r}. score={score}, forecast_model={forecast_model}")


#
# ScoreValue
#

class ScoreValue(models.Model):
    """
    Represents a single value of a Score, e.g., an 'Absolute Error' (the Score) of 0.1 (the ScoreValue).
    """
    score = models.ForeignKey(Score, related_name='values', on_delete=models.CASCADE)
    forecast = models.ForeignKey(Forecast, on_delete=models.CASCADE)
    unit = models.ForeignKey('Unit', blank=True, null=True, on_delete=models.SET_NULL)
    target = models.ForeignKey('Target', blank=True, null=True, on_delete=models.SET_NULL)
    value = models.FloatField(null=False)


    def __repr__(self):
        return str((self.pk, self.score.pk, self.forecast.pk, self.unit.pk, self.target.pk, self.value))


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
    # datetime at the last update. auto_now: automatically set the field to now every time the object is saved:
    updated_at = models.DateTimeField(auto_now=True)
    score = models.ForeignKey(Score, on_delete=models.CASCADE)


    def __repr__(self):
        return str((self.pk, self.forecast_model, str(self.updated_at), self.score))


    def __str__(self):  # todo
        return basic_str(self)
