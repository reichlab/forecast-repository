from itertools import groupby

from django.db import models, connection
from django.utils.text import get_valid_filename

from forecast_app.models import Forecast, Project, ForecastModel
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


#
# CSV-related functions
#

SCORE_CSV_HEADER_PREFIX = ['model', 'timezero', 'season', 'location', 'target']


def _write_csv_score_data_for_project(csv_writer, project):
    """
    Writes all ScoreValue data for project into csv_writer. There is one column per ScoreValue BUT: all Scores are on
    one line. Thus, the row 'key' is the (fixed) first five columns:

        `ForecastModel.name, TimeZero.timezero_date, season, Location.name, Target.name`

    Followed on the same line by a variable number of ScoreValue.value columns, one for each Score. Score names are in
    the header. An example header and first few rows:

        model,           timezero,    season,    location,  target,          constant score,  Absolute Error
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      1_biweek_ahead,  1                <blank>
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      1_biweek_ahead,  <blank>          2
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      2_biweek_ahead,  <blank>          1
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      3_biweek_ahead,  <blank>          9
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      4_biweek_ahead,  <blank>          6
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH01,      5_biweek_ahead,  <blank>          8
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      1_biweek_ahead,  <blank>          6
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      2_biweek_ahead,  <blank>          6
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      3_biweek_ahead,  <blank>          37
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      4_biweek_ahead,  <blank>          25
        gam_lag1_tops3,  2017-04-23,  2017-2018  TH02,      5_biweek_ahead,  <blank>          62

    Notes:
    - `season` is each TimeZero's containing season_name, similar to Project.timezeros_in_season().
    - we use get_valid_filename() to ensure values are CSV-compliant, i.e., no commas, returns, tabs, etc. Using that
      function is as good as any :-)
    - we use groupby to group row 'keys' so that all score values are together
    """
    # re: scores order: it is crucial that order matches query ORDER BY ... sv.score_id so that columns match values
    scores = Score.objects.all().order_by('pk')

    # write hearder
    SCORE_CSV_HEADER = SCORE_CSV_HEADER_PREFIX + [get_valid_filename(score.name) for score in scores]
    csv_writer.writerow(SCORE_CSV_HEADER)

    # get the raw rows - sorted so groupby() will work
    sql = """
        SELECT f.forecast_model_id, f.time_zero_id, sv.location_id, sv.target_id, sv.score_id, sv.value
        FROM {scorevalue_table_name} AS sv
               INNER JOIN {score_table_name} s ON sv.score_id = s.id
               INNER JOIN {forecast_table_name} AS f ON sv.forecast_id = f.id
               INNER JOIN {forecastmodel_table_name} AS fm ON f.forecast_model_id = fm.id
        WHERE fm.project_id = %s
        ORDER BY f.forecast_model_id, f.time_zero_id, sv.location_id, sv.target_id, sv.score_id;
    """.format(scorevalue_table_name=ScoreValue._meta.db_table,
               score_table_name=Score._meta.db_table,
               forecast_table_name=Forecast._meta.db_table,
               forecastmodel_table_name=ForecastModel._meta.db_table)
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.pk,))
        rows = cursor.fetchall()

    # write grouped rows
    forecast_model_id_to_obj = {forecast_model.pk: forecast_model for forecast_model in project.models.all()}
    timezero_id_to_obj = {timezero.pk: timezero for timezero in project.timezeros.all()}
    location_id_to_obj = {location.pk: location for location in project.locations.all()}
    target_id_to_obj = {target.pk: target for target in project.targets.all()}
    for (forecast_model_id, time_zero_id, location_id, target_id), score_id_value_grouper \
            in groupby(rows, key=lambda _: (_[0], _[1], _[2], _[3])):
        forecast_model = forecast_model_id_to_obj[forecast_model_id]
        timezero = timezero_id_to_obj[time_zero_id]
        location = location_id_to_obj[location_id]
        target = target_id_to_obj[target_id]
        season = '?'  # todo xx timezero.containing_season_name()
        # ex score_groups: [(1, 18, 1, 1, 1, 1.0), (1, 18, 1, 1, 2, 2.0)]  # multiple scores per group
        #                  [(1, 18, 1, 2, 2, 0.0)]                         # single score
        score_groups = list(score_id_value_grouper)
        # NB: if a score is missing then we need to use '' for it so that scores align with the header:
        score_id_to_value = {score_group[-2]: score_group[-1] for score_group in score_groups}
        score_values = [score_id_to_value[score.id] if score.id in score_id_to_value else None for score in scores]
        csv_writer.writerow(
            [get_valid_filename(forecast_model.name), timezero.timezero_date, get_valid_filename(season),
             get_valid_filename(location.name), get_valid_filename(target.name)]
            + score_values)
