import logging

import django_rq
from django.db import models
from django.shortcuts import get_object_or_404

from forecast_app.models import Forecast, Project
from utils.utilities import basic_str


logger = logging.getLogger(__name__)


#
# Score
#

class Score(models.Model):
    """
    Represents the definition of a score. In our terminology, a `Score` has corresponding `ScoreValue` objects.
    Example scores: `Error`, `Absolute Error`, `Log Score`, and `Multi Bin Log Score`.

    Design notes: We needed a way to represent arbitrary scores in a way that was clean and generalizable, but with
    minimal impact to migrations, etc. After looking at many options, including the inbuilt ones
    ( https://docs.djangoproject.com/en/1.11/topics/db/models/#model-inheritance ) and third party ones like
    https://django-polymorphic.readthedocs.io/ and
    https://django-model-utils.readthedocs.io/en/latest/managers.html#inheritancemanager , we decided to simply store
    a text abbreviation for each Score, which is used to look up the corresponding function to call in the (hard-coded)
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
       is the one from your score definition in #1. The function will be passed two arguments: score and project. It
       should create ScoreValues for every Forecast in the Project.
    3. Call Score.ensure_all_scores_exist().

    Notes:
    - To update a particular score for a particular project, call update_score() rather than directly calling the
      'calc_*()' method (see update_score() docs for details)
    - Newly-created scores will require restarting the web and any worker processes so they have the definitions
      available to them.
    """

    abbreviation = models.CharField(max_length=200, help_text="Short name used as a column header for this score in "
                                                              "downloaded CSV score files.")

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


    def num_score_values_for_project(self, project):
        """
        Returns # ScoreValues for me related to project.
        """
        return ScoreValue.objects.filter(score=self, forecast__forecast_model__project=project).count()


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


    def update_score(self, project):
        """
        The top-level method for updating a project's score. You should call this rather than directly calling the
        'calc_*()' methods b/c this method does some important pre- and post-calculation housekeeping. Runs in the
        calling thread and therefore blocks. Use score_type_to_score_and_is_created() to map a *_SCORE_TYPE int to the
        corresponding Score instance.
        """
        import forecast_app.scores.definitions


        logger.debug("update_score(): entered. score={}, project={}".format(self, project))

        logger.debug("update_score(): deleting existing ScoreValues for project")
        ScoreValue.objects.filter(score=self, forecast__forecast_model__project=project).delete()

        calc_function = getattr(forecast_app.scores.definitions, 'calc_' + self.abbreviation)
        logger.debug("update_score(): calling calculation function: {}".format(calc_function))
        calc_function(self, project)

        self.set_last_update_for_project(project)
        logger.debug("update_score(): done. created {} ScoreValues".format(self.num_score_values()))


    @classmethod
    def update_scores_for_all_projects(cls):
        """
        Update all scores for all projects. Limited usefulness b/c runs in the calling thread and therefore blocks.
        """
        Score.ensure_all_scores_exist()
        for score in cls.objects.all():
            for project in Project.objects.all():
                score.update_score(project)


    @classmethod
    def enqueue_update_scores_for_all_projects(cls):
        """
        Top-level method for enqueuing the update of all scores for all projects.
        """
        Score.ensure_all_scores_exist()
        for score in cls.objects.all():
            for project in Project.objects.all():
                logger.debug("enqueuing update project scores. score={}, project={}".format(score, project))
                django_rq.enqueue(_update_project_scores, score.pk, project.pk)


def _update_project_scores(score_pk, project_pk):
    """
    Enqueue helper function.
    """
    score = get_object_or_404(Score, pk=score_pk)
    project = get_object_or_404(Project, pk=project_pk)
    score.update_score(project)


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
