from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import models
from django.urls import reverse

from forecast_app.models.project import Project
from utils.utilities import basic_str


class ForecastModel(models.Model):
    """
    Represents a project's model entry by a competing team, including metadata, model-specific auxiliary data beyond
    core data, and a list of the actual forecasts.
    """
    owner = models.ForeignKey(User, blank=True, null=True, help_text="The model's owner.", on_delete=models.SET_NULL)
    project = models.ForeignKey(Project, related_name='models', on_delete=models.CASCADE)
    name = models.TextField()
    abbreviation = models.TextField(help_text="Short name for the model. Used in the 'model' column in downloaded " \
                                              "CSV score files.")
    team_name = models.TextField()
    description = models.TextField(help_text="A few paragraphs describing the model. Please see documentation for " \
                                             "what should be included here - information on reproducing the model’s " \
                                             "results, etc.")
    home_url = models.URLField(help_text="The model's home site.")
    aux_data_url = models.URLField(
        null=True, blank=True,
        help_text="Optional model-specific auxiliary data directory or Zip file containing data files (e.g., "
                  "CSV files) beyond Project.core_data that were used by this model.")


    def __repr__(self):
        return str((self.pk, self.name, self.abbreviation))


    def __str__(self):  # todo
        return basic_str(self)


    def save(self, *args, **kwargs):
        """
        Validates my abbreviation for uniqueness within my project.
        """
        if (not self.name) or (not self.abbreviation):
            raise ValidationError(f"both name and abbreviation are required. one or both was not found. "
                                  f"name={self.name!r}, abbreviation={self.abbreviation!r}")

        for forecast_model in self.project.models.all():
            if (forecast_model != self) and (self.abbreviation == forecast_model.abbreviation):
                raise ValidationError(f"abbreviation must be unique but was a duplicate of this model: "
                                      f"{forecast_model}. name={self.name!r} abbreviation={self.abbreviation!r}")

        # done
        super().save(*args, **kwargs)


    def get_absolute_url(self):
        return reverse('model-detail', args=[str(self.pk)])


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


    def forecast_for_time_zero(self, time_zero):
        """
        :return: the first Forecast in me corresponding to time_zero. returns None o/w. NB: tests for object equality
        """
        return self.forecasts.filter(time_zero=time_zero).first()
