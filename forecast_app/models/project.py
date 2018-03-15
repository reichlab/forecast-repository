import itertools
import math

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.db.models import ManyToManyField
from django.urls import reverse
from jsonfield import JSONField

from forecast_app.models.data import ProjectTemplateData, ModelWithCDCData, ForecastData
from utils.utilities import basic_str


#
# ---- Project class ----
#

class Project(ModelWithCDCData):
    """
    The main class representing a forecast challenge, including metadata, core data, targets, and model entries.
    NB: The inherited 'csv_filename' field from ModelWithCDCData is used as a flag to indicate that a valid template
    was loaded - see is_template_loaded().
    """

    # w/out related_name we get: forecast_app.Project.model_owners:
    #   (fields.E304) Reverse accessor for 'Project.model_owners' clashes with reverse accessor for 'Project.owner'.
    owner = models.ForeignKey(User,
                              related_name='project_owner',
                              on_delete=models.SET_NULL,
                              blank=True, null=True,
                              help_text="The project's owner.")

    is_public = models.BooleanField(default=True,
                                    help_text="Controls project visibility. False means the project is private and "
                                              "can only be accessed by the project's owner or any of its model_owners. "
                                              "True means it is publicly accessible.")

    model_owners = ManyToManyField(User, blank=True,  # blank=True allows omitting in forms
                                   help_text="Users who are allowed to create, edit, and delete ForecastModels "
                                             "in this project. Or: non-editing users who simply need access "
                                             "to a private project. Use control/command click to add/remove from "
                                             "the list. ")

    cdc_data_class = ProjectTemplateData  # the CDCData class I'm paired with. used by ModelWithCDCData

    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000,
                                   help_text="A few paragraphs describing the project. Please see documentation for"
                                             "what should be included here - 'real-time-ness', time_zeros, etc.")

    home_url = models.URLField(help_text="The project's home site.")

    logo_url = models.URLField(blank=True, null=True, help_text="The project's optional logo image.")

    core_data = models.URLField(
        help_text="Directory or Zip file containing data files (e.g., CSV files) made made available to everyone in "
                  "the challenge, including supplemental data like Google queries or weather.")

    # config_dict: specifies project-specific information using these keys:
    #  - 'target_to_week_increment': a dict that maps week-related target names to ints, such as '1 wk ahead' -> 1 .
    #     also, this dict's keys are used by mean_abs_error_rows_for_project() to decide which targets to use
    # - 'location_to_delphi_region': a dict that maps all my locations to Delphi region names - see
    #     delphi_wili_for_mmwr_year_week()
    config_dict = JSONField(null=True, blank=True,
                            help_text="JSON dict containing these two keys, each of which is a dict: "
                                      "'target_to_week_increment' and 'location_to_delphi_region'. Please see "
                                      "documentation for details.")


    def __repr__(self):
        return str((self.pk, self.name))


    def __str__(self):  # todo
        return basic_str(self)


    def save(self, *args, **kwargs):
        """
        Validates my config_dict if provided, and my TimeZero.timezero_dates for uniqueness.
        """
        if self.config_dict:
            if ('target_to_week_increment' not in self.config_dict) or \
                    ('location_to_delphi_region' not in self.config_dict):
                raise ValidationError("config_dict did not contain both required keys: 'target_to_week_increment' and "
                                      "'location_to_delphi_region': {}".format(self.config_dict))

        # validate my TimeZero.timezero_dates
        found_timezero_dates = []
        for timezero in self.timezeros.all():
            if timezero.timezero_date not in found_timezero_dates:
                found_timezero_dates.append(timezero.timezero_date)
            else:
                raise ValidationError("found duplicate TimeZero.timezero_date: {}".format(timezero.timezero_date))

        # done
        super().save(*args, **kwargs)


    def is_user_allowed_to_view(self, user):
        """
        :return: True if user is allowed to view my pages based on my is_public, owner, and model_owners.
            returns False o/w
        """
        return user.is_superuser or self.is_public or (user == self.owner) or (user in self.model_owners.all())


    def get_absolute_url(self):
        return reverse('project-detail', args=[str(self.pk)])


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


    def get_summary_counts(self):
        """
        :return: a 3-tuple summarizing total counts in me: (num_models, num_forecasts, num_rows)
        """
        from .forecast import Forecast  # avoid circular imports
        from .forecast_model import ForecastModel  # ""


        return ForecastModel.objects.filter(project=self).count(), \
               Forecast.objects.filter(forecast_model__project=self).count(), \
               self.get_num_forecast_rows_estimated()


    def get_num_forecast_rows(self):
        """
        :return: the total number of data rows across all my models' forecasts
        """
        return ForecastData.objects.filter(forecast__forecast_model__project=self).count()


    def get_num_forecast_rows_estimated(self):
        """
        :return: like get_num_forecast_rows(), but returns an estimate that is much faster to calculate. the estimate
            is based on getting the number of rows for an arbitrary Forecast and then multiplying by the number of
            forecasts times the number of models in me. it will be exact for projects whose models all have the same
            number of rows
        """
        first_model = self.models.first()
        first_forecast = first_model.forecasts.first() if first_model else None
        first_forecast_num_rows = first_forecast.get_num_rows() if first_forecast else None
        return (self.models.count() * first_model.forecasts.count() * first_forecast_num_rows) \
            if first_forecast_num_rows else 0


    def forecasts_for_timezero(self, timezero):
        """
        :param timezero: a TimeZero
        :return: a list of Forecasts for timezero for each of my models
        """
        return [forecast_model.forecast_for_time_zero(timezero) for forecast_model in self.models.all()]


    def get_region_for_location_name(self, location_name):
        """
        :return: Delphi region name corresponding to location_name. see here for valid ones:
            https://github.com/cmu-delphi/delphi-epidata/blob/master/labels/regions.txt. returns None if no config_dict.
        """
        return self.config_dict and self.config_dict['location_to_delphi_region'][location_name]


    def get_targets_for_mean_absolute_error(self):
        """
        :return: list of targets that can be used for ForecastModel.mean_absolute_error() calls, i.e., those that are
        week-relative (?) ones. returns None if no config_dict.
        """
        return self.config_dict and list(self.config_dict['target_to_week_increment'].keys())


    def get_distribution_preview(self):
        """
        :return: returns an arbitrary Forecast bin for this project as a 3-tuple: (Forecast, location, target). returns
            None if I have no targets, locations, models, or forecasts
        """
        first_model = self.models.first()
        first_forecast = first_model.forecasts.first() if first_model else None
        locations = self.get_locations()
        first_location = next(iter(sorted(locations))) if locations else None  # sort to make deterministic
        targets = self.get_targets(first_location)
        first_target = next(iter(sorted(targets))) if targets else None  # sort to make deterministic
        return (first_forecast, first_location, first_target) if (first_forecast and first_location and first_target) \
            else None


    def get_week_increment_for_target_name(self, target_name):
        """
        :return: returns an incremented week value based on the future specified by target_name. returns None if no
        config_dict.
        """
        return self.config_dict and self.config_dict['target_to_week_increment'][target_name]


    @transaction.atomic
    def load_template(self, template_path, file_name=None):
        """
        Loads the data from the passed Path into my corresponding ForecastData. First validates the data against my
        Project's template.

        :param template_path: Path to a CDC CSV forecast file
        :param file_name: optional name to use for the file. if None (default), uses template_path. helpful b/c uploaded
            files have random template_path file names, so original ones must be extracted and passed separately
        """
        file_name = file_name if file_name else template_path.name
        self.csv_filename = file_name
        self.load_csv_data(template_path)
        self.validate_template_data()
        self.save()


    def delete_template(self):
        """
        Clears my csv_filename and deletes my template data.
        """
        self.csv_filename = ''
        self.save()
        ProjectTemplateData.objects.filter(project=self).delete()


    def is_template_loaded(self):
        return self.csv_filename != ''


    def validate_forecast_data(self, forecast, validation_template=None, forecast_bin_map=None):
        """
        Validates forecast's data against my template. Raises if invalid.

        :param forecast: a Forecast
        :param validation_template: optional validation template (a Path) to override mine. useful in cases
            (like the CDC Flu Ensemble) where multiple templates could apply, depending on the year of the forecast
        :param forecast_bin_map: a function of one arg (forecast_bin) that returns a modified version of the bin to use
            in the validation against the template. forecast_bin is a 3-tuple: bin_start_incl, bin_end_notincl, value
        """
        if not self.is_template_loaded():
            raise RuntimeError("Cannot validate forecast data because project has no template loaded. Project={}, "
                               "forecast={}".format(forecast.csv_filename, self, forecast))

        # instead of working with ModelWithCDCData.get*() data access calls, we use these dicts as caches to speedup bin
        # lookup b/c get_target_bins() was slow. this has the added benefit of enabling us to easily override my
        # template if validation_template is passed
        if validation_template:
            template_location_dicts = self.get_location_dicts_internal_format_for_cdc_csv_file(validation_template)
        else:
            template_location_dicts = self.get_location_dicts_internal_format()
        forecast_location_dicts = forecast.get_location_dicts_internal_format()

        template_name = validation_template.name if validation_template else self.csv_filename
        template_locations = list(template_location_dicts.keys())
        forecast_locations = list(forecast_location_dicts.keys())
        if template_locations != forecast_locations:
            raise RuntimeError("Locations did not match template. csv_filename={}, template_locations={}, "
                               "forecast_locations={}"
                               .format(forecast.csv_filename, template_locations, forecast_locations))

        for template_location in template_locations:
            template_target_dicts = template_location_dicts[template_location]
            forecast_target_dicts = forecast_location_dicts[template_location]
            template_targets = list(template_target_dicts.keys())
            forecast_targets = list(forecast_target_dicts.keys())
            if template_targets != forecast_targets:
                raise RuntimeError("Targets did not match template. csv_filename={}, template_location={},"
                                   " template_targets={}, forecast_targets={}"
                                   .format(forecast.csv_filename, template_location, template_targets,
                                           forecast_targets))

            for template_target in template_targets:
                template_bins = template_target_dicts[template_target]['bins']
                forecast_bins = forecast_target_dicts[template_target]['bins']
                if forecast_bin_map:
                    forecast_bins = list(map(forecast_bin_map, forecast_bins))

                # per https://stackoverflow.com/questions/18411560/python-sort-list-with-none-at-the-end
                template_bins_sorted = sorted([b[:2] for b in template_bins],
                                              key=lambda x: (x[0] is None or x[1] is None, x))
                forecast_bins_sorted = sorted([b[:2] for b in forecast_bins],
                                              key=lambda x: (x[0] is None or x[1] is None, x))

                if template_bins_sorted != forecast_bins_sorted:  # compare bin_start_incl and bin_end_notincl
                    raise RuntimeError("Bins did not match template. template={}, csv_filename={}, "
                                       "template_location={}, template_target={}, # template_bins={}, forecast_bins={}"
                                       .format(template_name,
                                               forecast.csv_filename, template_location,
                                               template_target, len(template_bins), len(forecast_bins)))

                # note that the default rel_tol of 1e-09 failed for EW17-KoTstable-2017-05-09.csv
                # (forecast_bin_sum=0.9614178215505512 -> 0.04 fixed it), and for EW17-KoTkcde-2017-05-09.csv
                # (0.9300285798758262 -> 0.07 fixed it)
                forecast_bin_sum = sum([b[-1] if b[-1] is not None else 0 for b in forecast_bins])
                if not math.isclose(1.0, forecast_bin_sum, rel_tol=0.07):
                    raise RuntimeError("Bin did not sum to 1.0. template={}, csv_filename={}, "
                                       "template_location={}, template_target={}, forecast_bin_sum={}"
                                       .format(template_name, forecast.csv_filename, template_location, template_target,
                                               forecast_bin_sum))

                # test unit. recall that get_target_unit() arbitrarily uses the point row's unit. this means that the
                # following test also handles when a point line is missing as well
                template_unit = template_target_dicts[template_target]['unit']
                forecast_unit = forecast_target_dicts[template_target]['unit']
                if (not forecast_unit) or (template_unit != forecast_unit):
                    raise RuntimeError("Target unit not found or didn't match template. template={}, csv_filename={}, "
                                       "template_location={}, template_target={}, template_unit={}, forecast_unit={}"
                                       .format(template_name, forecast.csv_filename, template_location, template_target,
                                               template_unit, forecast_unit))


    def validate_template_data(self):
        """
        Validates my template's structure. Raises RuntimeError if any tests fail. Note that basic structure is tested in
        load_csv_data(). Also note that validate_forecast_data() does not test the following because it compares against
        a validated template, thus 'inheriting' these validations due to equality testing.
        """
        # instead of working with ModelWithCDCData.get*() data access calls, we use these dicts as caches to speedup bin
        # lookup b/c get_target_bins() was slow
        template_location_dicts = self.get_location_dicts_internal_format()
        template_locations = list(template_location_dicts.keys())
        if not template_locations:
            raise RuntimeError("Template has no locations. csv_filename={}".format(self.csv_filename))

        location_template_pairs = set()  # 2-tuples used for testing targets existing in every location
        found_targets = set()  # also used for ""
        for template_location in template_locations:
            template_target_dicts = template_location_dicts[template_location]
            template_targets = list(template_target_dicts.keys())
            for template_target in template_targets:
                location_template_pairs.add((template_location, template_target))
                found_targets.add(template_target)

                # note that we do not have to test for a missing point value:
                # 'template_target_dicts[template_target]['point']' b/c get_location_dicts_internal_format() verifies a point
                # row exists, which is all we care about in templates.

                # also note that we do not validate that template_bins sum to ~1.0 b/c specifying actual values in
                # templates is not required, partly b/c there is no standard for what values to use. however, do note
                # that validate_forecast_data() does check bin sums

                template_bins = template_target_dicts[template_target]['bins']
                if not template_bins:
                    raise RuntimeError("Target has no bins. csv_filename={}, template_location={}, "
                                       "template_target={}"
                                       .format(self.csv_filename, template_location, template_target))

        # test that every target exists in every location
        expected_location_template_pairs = set(itertools.product(template_locations, found_targets))
        if location_template_pairs != expected_location_template_pairs:
            raise RuntimeError("Target(s) was not found in every location. csv_filename={}, "
                               "missing location, target: {}"
                               .format(self.csv_filename, location_template_pairs ^ expected_location_template_pairs))


    def time_zero_for_timezero_date(self, timezero_date_str):
        """
        :return: the first TimeZero in me that has a timezero_date matching timezero_date_str
        """
        return self.timezeros.filter(timezero_date=timezero_date_str).first()


# NB: only works for abstract superclasses. per https://stackoverflow.com/questions/927729/how-to-override-the-verbose-name-of-a-superclass-model-field-in-django
Project._meta.get_field('csv_filename').help_text = "CSV file name of this project's template file."


#
# ---- Target class ----
#

class Target(models.Model):
    """
    Represents a project's target - a description of the desired data in the each forecast's data file.
    """
    project = models.ForeignKey(Project, related_name='targets', on_delete=models.CASCADE)

    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000, help_text="A few paragraphs describing the target.")


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
    project = models.ForeignKey(Project, related_name='timezeros', on_delete=models.CASCADE)

    timezero_date = models.DateField(help_text="A date that a target is relative to.")

    data_version_date = models.DateField(
        null=True, blank=True,
        help_text="The optional database date at which models should work with for the timezero_date.")  # nullable


    def __repr__(self):
        return str((self.pk, self.timezero_date, self.data_version_date))


    def __str__(self):  # todo
        return basic_str(self)
