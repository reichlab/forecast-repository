from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.db.models.signals import pre_save, post_save
from django.dispatch import receiver
from django.urls import reverse
from jsonfield import JSONField

from forecast_app.models.data import ProjectTemplateData, ModelWithCDCData
from utils.utilities import basic_str


#
# ---- Project class ----
#

class Project(ModelWithCDCData):
    """
    The main class representing a forecast challenge, including metadata, core data, targets, and model entries.
    """

    cdc_data_class = ProjectTemplateData  # the CDCData class I'm paired with. used by ModelWithCDCData

    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000,
                                   help_text="A few paragraphs describing the project. Includes info about "
                                             "'real-time-ness' of data, i.e., revised/unrevised")

    url = models.URLField(help_text="The project's site")

    core_data = models.URLField(help_text="Zip file containing data files (e.g., CSV files) made made available to "
                                          "everyone in the challenge, including supplemental data like Google "
                                          "queries or weather")

    # config_dict: specifies project-specific information with these keys:
    #  - 'target_to_week_increment': a dict that maps week-related target names to ints, such as '1 wk ahead' -> 1 .
    #     also, this dict's keys are used by mean_abs_error_rows_for_project() to decide which targets to use
    # - 'location_to_delphi_region': a dict that maps all my locations to Delphi region names - see
    #     delphi_wili_for_epi_week()
    config_dict = JSONField(help_text="JSON dict containing these two keys, each of which is a dict: "
                                      "'target_to_week_increment' and 'location_to_delphi_region'. Please see "
                                      "documentation for details.")


    def __init__(self, *args, **kwargs):
        """
        Handles the custom (i.e., non-field) 'template' argument. Note that this kind of handling has to be done
        carefully due to Django's object lifecycle, specifically [1]:

            You may be tempted to customize the model by overriding the __init__ method. If you do so, however, take
            care not to change the calling signature as any change may prevent the model instance from being saved.

        There are different solutions available; we chose to use these two interconnected pieces:

        a) __init__() extracts the 'template' arg from keywords, saves it into an IV, and validates it
        b) a post_save [2] signal loads the template's data if the object was created (i.e., it has a PK). (A PK is
           required b/c the data must have a FK to the project.)

        [1] https://docs.djangoproject.com/en/1.11/ref/models/instances/
        [2] https://docs.djangoproject.com/en/1.11/ref/signals/#post-save

        :param template: a Path to a template CSV file. for an example, see about.html for this CDC flue challenge
            template: https://github.com/cdcepi/FluSight-forecasts/blob/master/2016-2017_submission_template.csv
        :param args: standard Model args
        :param kwargs: ""
        """
        self.template = kwargs.pop('template', None)
        super(Project, self).__init__(*args, **kwargs)

        # check for the required passed template and then save it so project_post_save() can do load_template_data()
        if (not self.pk) and (not self.template):  # pk is None if not saved to the database
            # we have a new/non-saved instance with no template arg
            raise RuntimeError("unsaved instance is missing the required 'template' key: {!r}".format(self))


    def __repr__(self):
        return str((self.pk, self.name))


    def __str__(self):  # todo
        return basic_str(self)


    def get_absolute_url(self):
        return reverse('project-detail', args=[str(self.id)])


    @transaction.atomic
    def load_template_data(self, csv_template_file_path):
        """
        Called from constructor, loads the data in the passed template file into data for this project.

        :param csv_template_file_path: a Path to a template CSV file as described above
        """
        self.load_csv_data(csv_template_file_path)


    def get_week_increment_for_target_name(self, target_name):
        """
        :return: returns an incremented week value based on the future specified by target_name
        """
        return self.config_dict['target_to_week_increment'][target_name]


    def get_region_for_location_name(self, location_name):
        """
        :return: Delphi region name corresponding to location_name. see here for valid ones:
        https://github.com/cmu-delphi/delphi-epidata/blob/master/labels/regions.txt

        """
        return self.config_dict['location_to_delphi_region'][location_name]


    def get_targets_for_mean_absolute_error(self):
        """
        :return: list of targets that can be used for ForecastModel.mean_absolute_error() calls, i.e., those that are
        week-relative (?) ones
        """
        return list(self.config_dict['target_to_week_increment'].keys())


#
# ---- Target class ----
#

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


#
# ---- TimeZone class ----
#

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


#
# ---- signal handlers ----
#

@receiver(pre_save, sender=Project)
def model_pre_save(instance, **kwargs):
    # validate config_dict field to check for keys: 'target_to_week_increment' and 'location_to_delphi_region'
    if ('target_to_week_increment' not in instance.config_dict) or \
            ('location_to_delphi_region' not in instance.config_dict):
        raise ValidationError("config_dict did not contain both require keys: 'target_to_week_increment' and "
                              "'location_to_delphi_region': {}".format(instance.config_dict))


@receiver(post_save, sender=Project)
def project_post_save(instance, created, **kwargs):
    if created:  # o/w no pk
        instance.load_template_data(instance.template)
