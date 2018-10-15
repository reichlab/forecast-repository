from django.db import models
from django.db.models import IntegerField
from django.db.models.signals import post_save
from django.dispatch import receiver

from forecast_app.models import Project
from utils.utilities import basic_str


class RowCountCache(models.Model):
    """
    Stores a cached value of Project.get_num_forecast_rows(), which can be a time-consuming operation.
    """

    project = models.OneToOneField(
        Project,
        related_name='row_count_cache',
        on_delete=models.CASCADE,
        primary_key=True)

    # datetime at the last update. auto_now: automatically set the field to now every time the object is saved.
    updated_at = models.DateTimeField(auto_now=True)

    row_count = IntegerField(default=None, null=True)  # count at the last update. None -> has not be updated at all yet


    def __repr__(self):
        return str((self.pk, self.project, self.updated_at, self.row_count))


    def __str__(self):  # todo
        return basic_str(self)


# per https://stackoverflow.com/questions/1652550/can-django-automatically-create-a-related-one-to-one-model
# NB: because this is the only place a RowCountCache is created, it means existing projects will not have one added,
# which will cause problems b/c RowCountCache-related code assumes one exists
@receiver(post_save, sender=Project)
def create_row_count_cache_for_project(sender, instance, created, **kwargs):
    if created:
        if not hasattr(instance, 'row_count_cache'):
            RowCountCache.objects.create(project=instance)
