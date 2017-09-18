from django.db import models
from django.urls import reverse

from utils.utilities import basic_str


class Project(models.Model):
    """
    The main class representing a forecast challenge, including metadata, core data, targets, and model entries.
    """
    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000,
                                   help_text="A few paragraphs describing the project. Includes info about "
                                             "'real-time-ness' of data, i.e., revised/unrevised")

    url = models.URLField(help_text="The project's site")

    core_data = models.URLField(help_text="Zip file containing data files (e.g., CSV files) made made available to "
                                          "everyone in the challenge, including supplemental data like Google "
                                          "queries or weather")

    def __repr__(self):
        return str((self.pk, self.name))

    def __str__(self):  # todo
        return basic_str(self)

    def get_absolute_url(self):
        return reverse('project-detail', args=[str(self.id)])


class Target(models.Model):
    """
    Represents a project's target - a description of the desired data in the each forecast's data file.
    """
    project = models.ForeignKey(Project, on_delete=models.CASCADE, null=True)

    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000, help_text="A few paragraphs describing the target")

    def __repr__(self):
        return str((self.pk, self.name))

    def __str__(self):  # todo
        return basic_str(self)


class TimeZero(models.Model):
    """
    A date that a target is relative to. Additionally, contains an optional data_version_date the specifies the database
    date at which models should work with for this timezero_date date. Akin to rolling back (versioning) the database
    to that date.
     
    Assumes dates from any project can be converted to actual dates, e.g., from Dengue biweeks or CDC MMWR weeks
    ( https://ibis.health.state.nm.us/resource/MMWRWeekCalendar.html ).
    """
    project = models.ForeignKey(Project, on_delete=models.CASCADE, null=True)

    timezero_date = models.DateField(null=True, blank=True, help_text="A date that a target is relative to")

    data_version_date = models.DateField(
        null=True, blank=True,
        help_text="the database date at which models should work with for the timezero_date")  # nullable

    def __repr__(self):
        return str((self.pk, self.timezero_date, self.data_version_date))

    def __str__(self):  # todo
        return basic_str(self)
