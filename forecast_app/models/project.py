import itertools
import math

from django.db import models, transaction
from django.db.models.signals import post_save
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
        self.template_path = kwargs.pop('template', None)  # saved for project_post_save() to load_template_data()
        super(Project, self).__init__(*args, **kwargs)

        # check for the required passed template
        if (not self.pk) and (not self.template_path):  # pk is None if not saved to the database
            raise RuntimeError("Unsaved instance is missing the required 'template' key: {!r}".format(self))

        # set csv_filename based on template if not already saved
        if not self.csv_filename:
            self.csv_filename = self.template_path.name

        # validate config_dict
        if (not self.config_dict) or ('target_to_week_increment' not in self.config_dict) or \
                ('location_to_delphi_region' not in self.config_dict):
            raise RuntimeError("config_dict did not contain both required keys: 'target_to_week_increment' and "
                               "'location_to_delphi_region': {}".format(self.config_dict))


    def __repr__(self):
        return str((self.pk, self.name))


    def __str__(self):  # todo
        return basic_str(self)


    def get_absolute_url(self):
        return reverse('project-detail', args=[str(self.id)])


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


    def validate_template_data(self):
        """
        Validates my template's structure. Raises RuntimeError if any tests fail. Note that basic structure is tested in
        load_csv_data(). Also note that validate_forecast_data() does not test the following because it compares against
        a validated template, thus 'inheriting' these validations due to equality testing.
        """
        template_locations = self.get_locations()
        if not template_locations:
            raise RuntimeError("Template has no locations. csv_filename={}".format(self.csv_filename))

        location_template_pairs = set()  # 2-tuples used for testing targets existing in every location
        found_targets = set()  # also used for ""
        for template_location in template_locations:
            template_targets = self.get_targets(template_location)
            for template_target in template_targets:
                location_template_pairs.add((template_location, template_target))
                found_targets.add(template_target)
                if not self.get_target_point_value(template_location, template_target):
                    raise RuntimeError("Target has no point value. csv_filename={}, template_location={}, "
                                       "template_target={}"
                                       .format(self.csv_filename, template_location, template_target))

                template_bins = self.get_target_bins(template_location, template_target,
                                                     include_values=False, include_unit=True)
                if not template_bins:
                    raise RuntimeError("Target has no bins. csv_filename={}, template_location={}, "
                                       "template_target={}"
                                       .format(self.csv_filename, template_location, template_target))

                template_unit = self.get_target_unit(template_location, template_target)
                for template_bin in template_bins:
                    bin_unit = template_bin[2]
                    if template_unit != bin_unit:
                        raise RuntimeError("Target point and bin have different unit. csv_filename={}, "
                                           "template_location={}, template_target={}, template_unit={}, bin_unit={}"
                                           .format(self.csv_filename, template_location, template_target, template_unit,
                                                   bin_unit))

                template_bin_sum = self.get_target_bin_sum(template_location, template_target)
                # note that the default of 1e-09 failed for 2016-2017_submission_template.csv
                if not math.isclose(1.0, template_bin_sum, rel_tol=1e-07):
                    raise RuntimeError("Bin did not sum to 1.0. csv_filename={}, template_location={}, "
                                       "template_target={}, template_bin_sum={}"
                                       .format(self.csv_filename, template_location, template_target, template_bin_sum))

        # test that every target exists in every location
        expected_location_template_pairs = set(itertools.product(template_locations, found_targets))
        if location_template_pairs != expected_location_template_pairs:
            raise RuntimeError("Target(s) was not found in every location. csv_filename={}, "
                               "missing location, target: {}"
                               .format(self.csv_filename, location_template_pairs ^ expected_location_template_pairs))


    def validate_forecast_data(self, forecast):
        """
        Validates forecast's data against my template. Raises if invalid.
        """
        template_locations = self.get_locations()
        forecast_locations = forecast.get_locations()
        if template_locations != forecast_locations:
            raise RuntimeError("Locations did not match template. csv_filename={}, template_locations={}, "
                               "forecast_locations={}"
                               .format(forecast.csv_filename, template_locations, forecast_locations))

        for template_location in template_locations:
            template_targets = self.get_targets(template_location)
            forecast_targets = forecast.get_targets(template_location)
            if template_targets != forecast_targets:
                raise RuntimeError("Targets did not match template. csv_filename={}, template_location={},"
                                   " template_targets={}, forecast_targets={}"
                                   .format(forecast.csv_filename, template_location, template_targets,
                                           forecast_targets))

            for template_target in template_targets:
                template_bins = self.get_target_bins(template_location, template_target, include_values=False)
                forecast_bins = forecast.get_target_bins(template_location, template_target, include_values=False)

                # per https://stackoverflow.com/questions/18411560/python-sort-list-with-none-at-the-end
                template_bins = sorted(template_bins, key=lambda x: (x[0] is None or x[1] is None, x))
                forecast_bins = sorted(forecast_bins, key=lambda x: (x[0] is None or x[1] is None, x))

                if template_bins != forecast_bins:
                    raise RuntimeError("Bins did not match template. csv_filename={}, "
                                       "template_location={}, template_target={}, # template_bins={}, "
                                       "# forecast_bins={}"
                                       .format(forecast.csv_filename, template_location, template_target,
                                               len(template_bins), len(forecast_bins)))

                forecast_bin_sum = forecast.get_target_bin_sum(template_location, template_target)
                # note that the default of 1e-09 failed for EW17-KoTstable-2017-05-09.csv
                # (forecast_bin_sum=0.9614178215505512 -> 0.04 fixed it), and for EW17-KoTkcde-2017-05-09.csv
                # (0.9300285798758262 -> 0.07 fixed it)
                if not math.isclose(1.0, forecast_bin_sum, rel_tol=0.07):
                    raise RuntimeError("Bin did not sum to 1.0. csv_filename={}, "
                                       "template_location={}, template_target={}, forecast_bin_sum={}"
                                       .format(forecast.csv_filename, template_location, template_target,
                                               forecast_bin_sum))

                # test unit. recall that get_target_unit() arbitrarily uses the point row's unit. this means that the
                # following test also handles when a point line is missing as well
                template_unit = self.get_target_unit(template_location, template_target)
                forecast_unit = forecast.get_target_unit(template_location, template_target)
                if (not forecast_unit) or (template_unit != forecast_unit):
                    raise RuntimeError("Target unit not found or didn't match template. csv_filename={}, "
                                       "template_location={}, template_target={}, template_unit={}, forecast_unit={}"
                                       .format(forecast.csv_filename, template_location, template_target,
                                               template_unit, forecast_unit))


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

@transaction.atomic
@receiver(post_save, sender=Project)
def project_post_save(instance, created, **kwargs):
    if created:  # o/w no pk
        instance.load_csv_data(instance.template_path)
        instance.validate_template_data()  # must be done here in post_save because data must first be loaded
