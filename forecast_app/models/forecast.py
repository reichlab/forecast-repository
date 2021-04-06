from django.db import models, connection
from django.db.models.signals import pre_save, pre_delete
from django.dispatch import receiver
from django.urls import reverse

from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.project import TimeZero
from utils.utilities import basic_str


class Forecast(models.Model):
    """
    Represents a model's forecasted data. There are one or more Forecasts for each of my ForecastModel's Project's
    TimeZeros. Supports versioning via this 3-tuple: (forecast_model__id, time_zero__id, issue_date). That is, a
    Forecast's "version" is the combination of those three. Put another way, within a ForecastModel, a forecast's
    version is the (time_zero, issue_date) 2-tuple.
    """

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['forecast_model', 'time_zero', 'issue_date'], name='unique_version'),
        ]

    forecast_model = models.ForeignKey(ForecastModel, related_name='forecasts', on_delete=models.CASCADE)

    source = models.TextField(help_text="file name of the source of this forecast's prediction data")

    # NB: these TimeZeros must be the exact objects as the ones in my ForecastModel's Project, b/c there is no __eq__()
    time_zero = models.ForeignKey(TimeZero, on_delete=models.CASCADE,
                                  help_text="TimeZero that this forecast is in relation to.")

    # when this instance was created. basically the post-validation save date:
    created_at = models.DateTimeField(auto_now_add=True)

    # this Forecast's version - Forecast versions are named/identified by `issue_date`. defaults to the date at time of
    # creation. only special users can edit this due wanting to implement some scientific integrity controls
    issue_date = models.DateField(auto_now_add=True, db_index=True)

    # arbitrary information about this forecast
    notes = models.TextField(null=True, blank=True,
                             help_text="Text describing anything slightly different about a given forecast, e.g., a "
                                       "changed set of assumptions or a comment about when the forecast was created. "
                                       "Notes should be brief, typically less than 50 words.")

    def __repr__(self):
        return str((self.pk, self.time_zero, self.issue_date, self.source, self.created_at))

    def __str__(self):  # todo
        return basic_str(self)

    def get_absolute_url(self):
        return reverse('forecast-detail', args=[str(self.pk)])

    def get_class(self):
        """
        :return: view utility that simply returns a my class as a string. used by delete_modal_snippet.html
        """
        return self.__class__.__name__

    def html_id(self):
        """
        :return: view utility that returns a unique HTML id for this object. used by delete_modal_snippet.html
        """
        return self.__class__.__name__ + '_' + str(self.pk)

    @property
    def name(self):
        """
        We define the name property so that delete_modal_snippet.html can show something identifiable when asking to
        confirm deleting a Forecast. All other deletable models have 'name' fields (Project and ForecastModel).
        """
        return self.source


#
# set up signals to implement some of the rules from `load_predictions_from_json_io_dict()`:
#
# todo should probably move signals to a new signals.py file. see "Where should this code live?":
#  https://docs.djangoproject.com/en/3.1/topics/signals/
#

@receiver(pre_save, sender=Forecast)
def pre_validate_new_or_edited_forecast(instance, **kwargs):
    if instance.pk is None:  # creating a Forecast
        # validate the rule: "you cannot position a new forecast before any existing versions"
        newest_version = _newest_forecast_version(instance.forecast_model, instance.time_zero)
        if newest_version and instance.issue_date and (instance.issue_date < newest_version.issue_date):
            raise RuntimeError(f"you cannot position a new forecast before any existing versions. forecast={instance}, "
                               f"earlier_version={newest_version}")

    else:  # instance.pk is not None -> editing a Forecast
        # validate the rule: "editing a version's issue_date cannot reposition it before any existing forecasts". do
        # so by comparing the db's version list to what the list would be after the edit
        db_forecasts = list(Forecast.objects.filter(forecast_model=instance.forecast_model,
                                                    time_zero=instance.time_zero) \
                            .order_by('issue_date'))  # includes `instance`'s pre-saved state
        new_forecasts = sorted([forecast for forecast in db_forecasts if forecast.pk != instance.pk] + [instance],
                               key=lambda forecast: forecast.issue_date)
        if db_forecasts != new_forecasts:  # edited forecast's position changed
            raise RuntimeError(f"editing a version's issue_date cannot reposition it before any existing forecasts. "
                               f"forecast={instance}, db_forecasts={db_forecasts}, new_forecasts={new_forecasts}")


@receiver(pre_delete, sender=Forecast)
def pre_validate_deleted_forecast(instance, **kwargs):
    # validate the rule: "you cannot delete a forecast that has any newer versions"
    is_newer_forecasts = Forecast.objects.filter(forecast_model=instance.forecast_model,
                                                 time_zero=instance.time_zero,
                                                 issue_date__gt=instance.issue_date).exists()
    if is_newer_forecasts:
        raise RuntimeError(f"you cannot delete a forecast that has any newer versions. forecast={instance}")


#
# _newest_forecast_version()
#

def _newest_forecast_version(forecast_model, time_zero):
    """
    :param forecast_model: a ForecastModel
    :param time_zero: a TimeZero
    :return: the newest Forecast for the version indicated by (forecast_model, time_zero), based on issue_date,
        or None if there were no non-empty versions
    """

    sql = f"""
        WITH ranked_issue_dates AS (
            SELECT f.id AS f_id, f.issue_date AS issue_date, RANK() OVER (ORDER BY f.issue_date DESC) AS rank
            FROM {Forecast._meta.db_table} AS f
            WHERE f.forecast_model_id = %s
              AND f.time_zero_id = %s)
        SELECT cte.f_id
        FROM ranked_issue_dates AS cte
        WHERE cte.rank = 1;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (forecast_model.pk, time_zero.pk,))
        f_id_max_issue_date = cursor.fetchone()
        if f_id_max_issue_date is None:
            return None

        f_id_max_issue_date = f_id_max_issue_date[0]
        return Forecast.objects.get(pk=f_id_max_issue_date) if f_id_max_issue_date is not None else None
