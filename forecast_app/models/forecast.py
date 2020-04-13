from django.db import models
from django.db.models.signals import pre_delete, post_save
from django.dispatch import receiver
from django.urls import reverse

from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.project import TimeZero
from utils.utilities import basic_str


class Forecast(models.Model):
    """
    Represents a model's forecasted data. There is one Forecast for each of my ForecastModel's Project's TimeZeros.
    """

    forecast_model = models.ForeignKey(ForecastModel, related_name='forecasts', on_delete=models.CASCADE)

    source = models.TextField(help_text="file name of the source of this forecast's prediction data")

    # NB: these TimeZeros must be the exact objects as the ones in my ForecastModel's Project, b/c there is no __eq__()
    time_zero = models.ForeignKey(TimeZero, on_delete=models.CASCADE,
                                  help_text="TimeZero that this forecast is in relation to.")

    # when this instance was created. basically the post-validation save date:
    created_at = models.DateTimeField(auto_now_add=True)


    def __repr__(self):
        return str((self.pk, self.time_zero, self.source))


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


    def is_user_ok_to_delete(self, user):
        return user.is_superuser or (user == self.forecast_model.project.owner) or (user == self.forecast_model.owner)


    #
    # prediction-specific accessors
    #

    def get_num_rows(self):
        """
        :return: the total of number of data rows in me, for all types of Predictions
        """
        from forecast_app.models import Prediction  # avoid circular imports


        return sum(concrete_prediction_class.objects.filter(forecast=self).count()
                   for concrete_prediction_class in Prediction.concrete_subclasses())


    def bin_distribution_qs(self):
        from forecast_app.models import BinDistribution


        return self._predictions_qs(BinDistribution)


    def named_distribution_qs(self):
        from forecast_app.models import NamedDistribution


        return self._predictions_qs(NamedDistribution)


    def point_prediction_qs(self):
        from forecast_app.models import PointPrediction


        return self._predictions_qs(PointPrediction)


    def sample_distribution_qs(self):
        from forecast_app.models import SampleDistribution


        return self._predictions_qs(SampleDistribution)


    def quantile_prediction_qs(self):
        from forecast_app.models import QuantileDistribution


        return self._predictions_qs(QuantileDistribution)


    def _predictions_qs(self, prediction_subclass):
        # *_prediction_qs() helper that returns a QuerySet for all of my Predictions of type prediction_subclass
        return prediction_subclass.objects.filter(forecast=self)


#
# set up save and delete signals to manage my ForecastModel's forecasts_changed_at.
#
# NB: post_save will NOT be called by bulk operations: Overridden model methods are not called on bulk operations: ...
# To ensure customized delete logic gets executed, you can use pre_delete and/or post_delete signals. Unfortunately,
# there isn’t a workaround when creating or updating objects in bulk, since none of save(), pre_save, and post_save are
# called. https://docs.djangoproject.com/en/1.11/topics/db/models/#overriding-model-methods
#

@receiver(pre_delete, sender=Forecast)  # pre_delete not post_delete so that instance.forecast_model is available
@receiver(post_save, sender=Forecast)
def set_model_forecasts_changed_at(sender, instance, using, **kwargs):
    instance.forecast_model.score_change.update_changed_at()
