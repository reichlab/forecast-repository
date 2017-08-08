from django.db import models


class DataFile(models.Model):
    """
    A data file located somewhere - server, cloud, document store, etc.
    """
    # location = xx  # todo - server, etc. maybe: https://docs.djangoproject.com/en/1.11/ref/models/fields/#filefield

    FILE_TYPES = (
        ('z', 'Zip File'),
        ('c', 'CDC Forecast File'),  # CSV data file in CDC standard format (points and binned distributions)
    )
    file_type = models.CharField(max_length=1, choices=FILE_TYPES, blank=True, help_text='Data File Type')


class Project(models.Model):
    """
    The main class representing a forecast challenge, including metadata, core data, targets, and model entries.
    """
    name = models.CharField(max_length=200)

    # ~3 paragraphs. includes info about 'real-time-ness' of data, i.e., revised/unrevised
    description = models.CharField(max_length=2000)

    url = models.URLField()

    # documents (e.g., CSV files) in one zip file. includes all data sets made available to everyone in the challenge,
    # including supplemental data like google queries or weather. constraint: file_type = 'z'
    core_data = models.ForeignKey(DataFile, on_delete=models.SET_NULL, null=True)


class ForecastDate(models.Model):
    """
    Associates a project and a list of its forecast dates. Assumes dates from any project can be converted to actual
    dates, e.g., from Dengue biweeks or CDC MMWR weeks (https://ibis.health.state.nm.us/resource/MMWRWeekCalendar.html).
    
    Project <-- 1:M -- ForecastsDate
    """
    project = models.ForeignKey(Project, on_delete=models.SET_NULL, null=True)

    date = models.DateField(null=True, blank=True)


class Target(models.Model):
    """
    Represents a project's target - a description of the data in the each forecast's data file.
    
    Project <-- 1:M -- Target
    """
    project = models.ForeignKey(Project, on_delete=models.SET_NULL, null=True)

    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000)  # ~3 paragraphs


class ForecastModel(models.Model):
    """
    Represents a project's model entry by a competing team, including metadata, model-specific auxiliary data beyond
    core data, and the actual forecasts.

    Project <-- 1:M -- ForecastModel
    """
    project = models.ForeignKey(Project, on_delete=models.SET_NULL, null=True)

    name = models.CharField(max_length=200)

    # should include information on reproducing the model’s results
    description = models.CharField(max_length=2000)  # ~3 paragraphs

    url = models.URLField()

    # (optional) model-specific documents in one zip file beyond Project.core_data that were used by the this model.
    # constraint: file_type = 'z'
    core_data = models.ForeignKey(DataFile, on_delete=models.SET_NULL, null=True)


class Forecast(models.Model):
    """
    Represents a model's forecasted data. There is one Forecast for each of my ForecastModel's Project's ForecastsDates.

    ForecastModel <-- 1:M -- Forecast
    """
    # my model. constraint: there must be exactly one Forecast per Project.forecast_dates
    forecast_model = models.ForeignKey(ForecastModel, on_delete=models.SET_NULL, null=True)

    # date the forecast was actually generated. TODO see my note re: date terminology:
    # https://reichlab.slack.com/archives/C57HNDFN0/p1501595341689125?thread_ts=1501171869.540526&cid=C57HNDFN0
    date_generated = models.DateField(null=True, blank=True)

    # Project.forecast_date that this forecast applies to
    forecast_date = models.ForeignKey(ForecastDate, on_delete=models.SET_NULL, null=True)

    # CSV data file in CDC standard format (points and binned distributions)
    # constraint: file_type = 'c'. must have rows matching Project.targets
    data = models.ForeignKey(DataFile, on_delete=models.SET_NULL, null=True)
