from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.timezone import now

from forecast_app.models import ForecastModel
from utils.utilities import basic_str


class ModelScoreChange(models.Model):
    """
    Stores when the last score-impacting change to a ForecastModel took place. Used to decide if the model's scores need
    updating by comparing its ModelScoreChange.changed_at to corresponding ScoreLastUpdate.updated_at fields.
    """

    forecast_model = models.OneToOneField(ForecastModel, related_name='score_change', on_delete=models.CASCADE,
                                          primary_key=True)
    changed_at = models.DateTimeField(auto_now_add=True)


    def __repr__(self):
        return str((self.pk, self.forecast_model.pk, self.changed_at))


    def __str__(self):  # todo
        return basic_str(self)


    def update_changed_at(self):
        self.changed_at = now()
        self.save()


#
# post_save signal
#

# make sure new ForecastModels have a ModelScoreChange. see create_project_row_count_cache() for rationale
@receiver(post_save, sender=ForecastModel)
def create_project_row_count_cache(sender, instance, created, **kwargs):
    if created:
        if not hasattr(instance, 'score_change'):
            ModelScoreChange.objects.create(forecast_model=instance)
