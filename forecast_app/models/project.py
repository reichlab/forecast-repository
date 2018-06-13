import csv
import datetime
import io
import logging
import math
from collections import defaultdict
from itertools import groupby, product

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import models, transaction, connection
from django.db.models import ManyToManyField, Max, BooleanField, IntegerField
from django.urls import reverse
from jsonfield import JSONField

from forecast_app.models.data import ProjectTemplateData, ModelWithCDCData, ForecastData, POSTGRES_NULL_VALUE, CDCData
from utils.utilities import basic_str, parse_value, YYYYMMDD_DATE_FORMAT


logger = logging.getLogger(__name__)

#
# ---- Project class ----
#

TRUTH_CSV_HEADER = ['timezero', 'location', 'target', 'value']


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

    WEEK_TIME_INTERVAL_TYPE = 'w'
    BIWEEK_TIME_INTERVAL_TYPE = 'b'
    MONTH_TIME_INTERVAL_TYPE = 'm'
    TIME_INTERVAL_TYPE_CHOICES = ((WEEK_TIME_INTERVAL_TYPE, 'Week'),
                                  (BIWEEK_TIME_INTERVAL_TYPE, 'Biweek'),
                                  (MONTH_TIME_INTERVAL_TYPE, 'Month'))
    time_interval_type = models.CharField(max_length=1,
                                          choices=TIME_INTERVAL_TYPE_CHOICES, default=WEEK_TIME_INTERVAL_TYPE)

    truth_csv_filename = models.CharField(max_length=200, help_text="Name of the truth csv file that was uploaded.")

    description = models.CharField(max_length=2000,
                                   help_text="A few paragraphs describing the project. Please see documentation for"
                                             "what should be included here - 'real-time-ness', time_zeros, etc.")

    home_url = models.URLField(help_text="The project's home site.")

    logo_url = models.URLField(blank=True, null=True, help_text="The project's optional logo image.")

    core_data = models.URLField(
        help_text="Directory or Zip file containing data files (e.g., CSV files) made made available to everyone in "
                  "the challenge, including supplemental data like Google queries or weather.")

    # config_dict: specifies project-specific information
    config_dict = JSONField(null=True, blank=True,
                            help_text="JSON dict containing these keys: 'visualization-y-label'. "
                                      "Please see documentation for details.")


    def __repr__(self):
        return str((self.pk, self.name))


    def __str__(self):  # todo
        return basic_str(self)


    def save(self, *args, **kwargs):
        """
        Validates my config_dict if provided, and my TimeZero.timezero_dates for uniqueness.
        """
        config_dict_keys = {'visualization-y-label'}  # definitive list
        if self.config_dict and (set(self.config_dict.keys()) != config_dict_keys):
            raise ValidationError("config_dict did not contain the required keys. expected keys: {}, actual keys: {}"
                                  .format(config_dict_keys, self.config_dict.keys()))

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


    #
    # distribution-related utilities
    #

    def get_distribution_preview(self):
        """
        :return: returns an arbitrary Forecast bin for this project as a 3-tuple: (Forecast, location, target). returns
            None if I have no targets, locations, models, or forecasts
        """
        first_model = self.models.first()
        first_forecast = first_model.forecasts.first() if first_model else None
        locations = self.get_locations()
        first_location = next(iter(sorted(locations))) if locations else None  # sort to make deterministic
        targets = self.get_targets_for_location(first_location)
        first_target = next(iter(sorted(targets))) if targets else None  # sort to make deterministic
        return (first_forecast, first_location, first_target) if (first_forecast and first_location and first_target) \
            else None


    #
    # time-related utilities
    #

    def forecasts_for_timezero(self, timezero):
        """
        :param timezero: a TimeZero
        :return: a list of Forecasts for timezero for each of my models
        """
        return [forecast_model.forecast_for_time_zero(timezero) for forecast_model in self.models.all()]


    def time_zero_for_timezero_date(self, timezero_date_str):
        """
        :return: the first TimeZero in me that has a timezero_date matching timezero_date_str
        """
        return self.timezeros.filter(timezero_date=timezero_date_str).first()


    def seasons(self):
        """
        :return: list of season names for this project based on my timezeros
        """
        return list(self.timezeros.filter(is_season_start=True).values_list('season_name', flat=True))


    def timezeros_in_season(self, season_name):
        """
        Utility that returns a sorted list of TimeZeros for season_name.

        :param: season_name: a valid season name (see seasons()) or None, which is used to access TimeZeros that have
            no season. For the latter, there are two cases:
            1) there are no seasons at all
            2) there are seasons, but the first starts after the first TimeZero, i.e., my TimeZeros start with some
               non-season ones that are followed by some seasons
        :return: two cases based on whether season_name is None. 1) If not None: returns a list of TimeZeros that are
            within season_name, i.e., those that start with the TimeZero named season_name and go TO the next season,
            or to the end if season_name is the last season. 2) If None: returns based on the two cases listed above
            for season_name: 1) no seasons at all: return all TimeZeros. 2) starts with some non-seasons: return those
            up TO the first season.
        """
        # start with all TimeZeros - case #1 (no seasons at all), and filter as needed
        season_timezeros = self.timezeros.all()
        if season_name:
            season_tz = season_timezeros.filter(season_name=season_name).first()
            if not season_tz:
                raise RuntimeError("Invalid season_name. season_name={}, seasons={}"
                                   .format(season_name, self.seasons()))

            season_timezeros = season_timezeros.filter(timezero_date__gte=season_tz.timezero_date)
            next_season_tz = season_timezeros \
                .filter(is_season_start=True,
                        timezero_date__gt=season_tz.timezero_date) \
                .first()
            if next_season_tz:
                season_timezeros = season_timezeros.filter(timezero_date__lt=next_season_tz.timezero_date)
        else:  # no season_name
            first_season_tz = season_timezeros.filter(is_season_start=True).first()
            if first_season_tz:  # case #2 (seasons after initial TZs)
                season_timezeros = season_timezeros.filter(timezero_date__lt=first_season_tz.timezero_date)
        return list(season_timezeros.order_by('timezero_date'))


    def start_end_dates_for_season(self, season_name):
        """
        :param: season_name: same as timezeros_in_season() - can be None
        :return: 2-tuple: (start_date, end_date) for season_name. this is a closed interval - both are included.
            Note that start_date == end_date if there is only one TimeZero. returns None if no TimeZeros found
        """
        timezeros = self.timezeros_in_season(season_name)
        if len(timezeros) == 0:
            return None

        return timezeros[0].timezero_date, timezeros[-1].timezero_date


    def time_interval_type_to_foresight(self):
        """
        :return: my time_interval_type formatted for D3-Foresight's pointType
        """
        return dict(Project.TIME_INTERVAL_TYPE_CHOICES)[self.time_interval_type].lower()


    #
    # count-related functions
    #

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


    def location_to_max_val(self, season_name, targets):
        """
        :return: a dict mapping each location to the maximum value across all my forecasts for season_name and targets
        """
        season_start_date, season_end_date = self.start_end_dates_for_season(season_name)
        loc_max_val_qs = ForecastData.objects \
            .filter(forecast__forecast_model__project=self,
                    row_type=CDCData.POINT_ROW_TYPE,
                    target__in=targets,
                    forecast__time_zero__timezero_date__gte=season_start_date,
                    forecast__time_zero__timezero_date__lte=season_end_date) \
            .values('location') \
            .annotate(max_val=Max('value'))
        # [{'location': 'TH01', 'max_val': 15.0}, ...]
        return {location_max_val['location']: location_max_val['max_val'] for location_max_val in loc_max_val_qs}


    #
    # visualization-related functions
    #

    def visualization_targets(self):
        """
        :return: list of Target names that can be used for flusight_location_to_data_dict() and mean_absolute_error()
            (for the meantime) calls. returns None if no config_dict
        """
        return Target.objects.filter(project=self).filter(is_step_ahead=True).values_list('name', flat=True)


    def visualization_y_label(self):
        """
        :return: Y axis label used by flusight_location_to_data_dict(). returns None if no config_dict
        """
        return self.config_dict and self.config_dict['visualization-y-label']


    #
    # truth data-related functions
    #

    def is_truth_data_loaded(self):
        """
        :return: True if I have truth data loaded via load_truth_data(). Actually, returns the count, which acts as a
            boolean.
        """
        return self.truth_data_qs().count()


    def get_truth_data_preview(self):
        """
        :return: view helper function that returns a preview of my truth data in the form of a table that's represented
            as a nested list of rows
        """
        return list(self.truth_data_qs()
                    .values_list('time_zero__timezero_date', 'location', 'target', 'value')[:10])


    def get_truth_data_rows(self):
        """
        Returns all of my data as a a list of rows, excluding any PKs and FKs columns, and ordered by PK.
        """
        return list(self.truth_data_qs()
                    .order_by('id')
                    .values_list('time_zero__timezero_date', 'location', 'target', 'value'))


    def truth_data_qs(self):
        """
        :return: A QuerySet of my TruthData.
        """
        from forecast_app.models import TruthData  # avoid circular imports


        return TruthData.objects.filter(time_zero__project=self)


    def delete_truth_data(self):
        """
        Deletes all of my truth data.
        """
        self.truth_data_qs().delete()
        self.truth_csv_filename = ''
        self.save()


    def reference_target_for_actual_values(self):
        """
        Returns the target in me that should act as the one to use when computing an 'actual' step-ahead value from
        loaded truth data. We try to use the one that is the fewest step ahead steps available, starting with zero and
        going up from there. Returns None if no appropriate targets were found, say if there are no targets, or only
        negative ones.

        _About calculating 'actual' step-head values from truth data_: Loaded truth data contains actual values by way
        of the project's 'step ahead' targets. Some projects provide a zero step ahead target (whose
        step_ahead_increment is 0), which is what we need to get the an actual value for a particular
        [location][timezero_date] combination: Just index in to the desired timezero_date. However, other projects
        provide only non-zero targets, e.g., '1 wk ahead' (whose step_ahead_increment is 1). In these cases we need a
        'reference' target to use, which we then apply to move that many steps ahead in the project's TimeZeros (sorted
        by date) to get the actual (0 step ahead) value for that timezero_date. For example, if we wan the actual value
        for this truth data:

            timezero   location       target       value
            20170723   HHS Region 1   1 wk ahead   0.303222
            20170730   HHS Region 1   1 wk ahead   0.286054

        And if we are using '1 wk ahead' as our reference target, then to get the actual step-ahead value for the
        [location][timezero_date] combination of ['20170730']['HHS Region 1'] we need to work backwards 1
        step_ahead_increment to ['20170723']['HHS Region 1'] and use the '1 wk ahead' target's value, i.e., 0.303222. In
        our example above, there is actual step-ahead value for 20170723.

        Generally, the definition is:
            actual[location][timezero_date] = truth[location][ref_target][timezero_date - ref_target_incr]
        """
        return Target.objects.filter(project=self, is_step_ahead=True, step_ahead_increment__gte=0) \
            .order_by('step_ahead_increment') \
            .first()


    def location_target_timezero_date_to_truth(self, season_name=None):
        """
        Returns my truth values as a dict that's organized for easy access, as in:
        location_target_timezero_date_to_truth[location][target][timezero_date]. Only includes data from season_name,
        which is None if I have no seasons.
        """
        loc_target_tz_date_to_truth = {}
        query_set = self.truth_data_qs() \
            .order_by('location', 'target') \
            .values_list('location', 'target', 'time_zero__timezero_date', 'value')
        if season_name:
            season_start_date, season_end_date = self.start_end_dates_for_season(season_name)
            query_set = query_set.filter(time_zero__timezero_date__gte=season_start_date,
                                         time_zero__timezero_date__lte=season_end_date)
        for location, loc_target_tz_grouper in groupby(query_set, key=lambda _: _[0]):
            target_tz_date_to_truth = {}
            loc_target_tz_date_to_truth[location] = target_tz_date_to_truth
            for target, target_tz_grouper in groupby(loc_target_tz_grouper, key=lambda _: _[1]):
                tz_date_to_truth = defaultdict(list)
                target_tz_date_to_truth[target] = tz_date_to_truth
                for _, _, tz_date, value in target_tz_grouper:
                    tz_date_to_truth[tz_date].append(value)
        return loc_target_tz_date_to_truth


    @transaction.atomic
    def load_truth_data(self, truth_file_path, file_name=None):
        """
        Similar to load_template(), loads the data in truth_file_path (see below for file format docs). Like
        load_csv_data(), uses direct SQL for performance, using a fast Postgres-specific routine if connected to it.
        Note that this method should be called after all TimeZeros are created b/c truth data is validated against
        them. Notes:

        - Validates against the Project's template, which is therefore required to be set before this call.
        - One csv file/project, which includes timezeros across all seasons.
        - Columns: timezero, location, target, value . NB: There is no season information (see below). timezeros are
          formatted “yyyymmdd”. A header must be included.
        - Missing timezeros: If the program generating the csv file does not have information for a particular project
          timezero, then it should not generate a value for it. (The alternative would be to require the program to
          generate placeholder values for missing dates.)
        - Non-numeric values: Some targets will have no value, such as season onset when a baseline is not met. In those
          cases, the value should be “NA”, per
          https://predict.phiresearchlab.org/api/v1/attachments/flusight/flu_challenge_2016-17_update.docx .
        - For date-based onset or peak targets, values must be dates in the same format as timezeros, rather than
            project-specific time intervals such as an epidemic week.
        - Validation:
            - Every timezero in the csv file must have a matching one in the project. Note that the inverse is not
              necessarily true, such as in the case above of missing timezeros.
            - Every location in the csv file must a matching one in the Project.
            - Ditto for every target.

        :param truth_file_path: Path to csv file with the truth data, one line per timezero|location|target combination
        :param file_name: optional name to use for the file. if None (default), uses template_path. helpful b/c uploaded
            files have random template_path file names, so original ones must be extracted and passed separately
        """
        from forecast_app.models import TruthData  # avoid circular imports


        # the template needs to be loaded b/c _load_truth_data_rows() validates truth data against template locations
        # and targets
        if not self.is_template_loaded():
            raise RuntimeError("Template not loaded")

        if not self.pk:
            raise RuntimeError("Instance is not saved the the database, so can't insert data: {!r}".format(self))

        with open(str(truth_file_path)) as csv_file_fp, \
                connection.cursor() as cursor:
            rows = self._load_truth_data_rows(csv_file_fp)  # validates
            if not rows:
                return

            truth_data_table_name = TruthData._meta.db_table
            columns = [TruthData._meta.get_field('time_zero').column, 'location', 'target', 'value']
            if connection.vendor == 'postgresql':
                string_io = io.StringIO()
                csv_writer = csv.writer(string_io, delimiter=',')
                for timezero, location, target, value in rows:
                    # note that we translate None -> POSTGRES_NULL_VALUE for the nullable column
                    csv_writer.writerow([timezero, location, target,
                                         value if value is not None else POSTGRES_NULL_VALUE])
                string_io.seek(0)
                cursor.copy_from(string_io, truth_data_table_name, columns=columns, sep=',', null=POSTGRES_NULL_VALUE)
            else:  # 'sqlite', etc.
                sql = """
                    INSERT INTO {truth_data_table_name} ({column_names})
                    VALUES (%s, %s, %s, %s);
                """.format(truth_data_table_name=truth_data_table_name, column_names=(', '.join(columns)))
                cursor.executemany(sql, rows)

        # done!
        self.truth_csv_filename = file_name or truth_file_path.name
        self.save()


    def _load_truth_data_rows(self, csv_file_fp):
        """
        Similar to ModelWithCDCData.read_cdc_csv_file_rows(), loads, validates, and cleans the rows in csv_file_fp.
        """
        csv_reader = csv.reader(csv_file_fp, delimiter=',')

        # validate header
        try:
            orig_header = next(csv_reader)
        except StopIteration:
            raise RuntimeError("Empty file")

        header = orig_header
        header = [h.lower() for h in [i.replace('"', '') for i in header]]
        if header != TRUTH_CSV_HEADER:
            raise RuntimeError("Invalid header: {}".format(', '.join(orig_header)))

        # collect the rows. first we load them all into memory (processing and validating them as we go)
        locations = self.get_locations()  # template data
        targets = self.get_targets()  # ""
        rows = []
        timezero_to_missing_count = defaultdict(int)  # to minimize warnings
        location_to_missing_count = defaultdict(int)
        target_to_missing_count = defaultdict(int)
        for row in csv_reader:
            if len(row) != 4:
                raise RuntimeError("Invalid row (wasn't 4 columns): {!r}".format(row))

            timezero_date, location, target, value = row

            # validate timezero_date
            # todo cache: time_zero_for_timezero_date() results - expensive?
            timezero = self.time_zero_for_timezero_date(datetime.datetime.strptime(timezero_date, YYYYMMDD_DATE_FORMAT))
            if not timezero:
                timezero_to_missing_count[timezero_date] += 1
                continue

            # validate location and target
            if location not in locations:
                location_to_missing_count[location] += 1
                continue

            if target not in targets:
                target_to_missing_count[target] += 1
                continue

            value = parse_value(value)  # parse_value() handles non-numeric cases like 'NA' and 'none'
            rows.append((timezero.pk, location, target, value))

        # report warnings
        for timezero, count in timezero_to_missing_count.items():
            logger.warning("_load_truth_data_rows(): timezero not found in project: {}: {} row(s)"
                           .format(timezero, count))
        for location, count in location_to_missing_count.items():
            logger.warning("_load_truth_data_rows(): Location not found in project: {!r}: {} row(s)"
                           .format(location, count))
        for target, count in target_to_missing_count.items():
            logger.warning("_load_truth_data_rows(): Target not found in project: {!r}: {} row(s)"
                           .format(target, count))

        # done
        return rows


    #
    # actual data-related functions
    #

    @staticmethod
    def location_to_actual_points(loc_tz_date_to_actual_vals):
        """
        :return: view function that returns a dict mapping location to a list of actual values found in
            loc_tz_date_to_actual_vals, which is as returned by location_timezero_date_to_actual_vals(). it is what the D3
            component expects: "[a JavaScript] array of the same length as timePoints"
        """


        def actual_list_from_tz_date_to_actual_dict(tz_date_to_actual):
            return [tz_date_to_actual[tz_date][0] if isinstance(tz_date_to_actual[tz_date], list) else None
                    for tz_date in sorted(tz_date_to_actual.keys())]


        location_to_actual_points = {location: actual_list_from_tz_date_to_actual_dict(tz_date_to_actual)
                                     for location, tz_date_to_actual in loc_tz_date_to_actual_vals.items()}
        return location_to_actual_points


    @staticmethod
    def location_to_actual_max_val(loc_tz_date_to_actual_vals):
        """
        :return: view function that returns a dict mapping each location to the maximum value found in
            loc_tz_date_to_actual_vals, which is as returned by location_timezero_date_to_actual_vals()
        """


        def max_from_tz_date_to_actual_dict(tz_date_to_actual):
            flat_values = [item for sublist in tz_date_to_actual.values() if sublist for item in sublist]
            return max(flat_values) if flat_values else None  # NB: None is arbitrary


        location_to_actual_max = {location: max_from_tz_date_to_actual_dict(tz_date_to_actual)
                                  for location, tz_date_to_actual in loc_tz_date_to_actual_vals.items()}
        return location_to_actual_max


    def location_timezero_date_to_actual_vals(self, season_name):
        """
        Returns 'actual' step-ahead values from loaded truth data as a dict that's organized for easy access, as in:
        location_timezero_date_to_actual_vals[location][timezero_date] . Returns {} if no
        reference_target_for_actual_values().
        """


        def is_tz_date_in_season(timezero_date):
            return (timezero_date >= season_start_date) and (timezero_date <= season_end_date)


        ref_target = self.reference_target_for_actual_values()
        if not ref_target:
            return {}

        if season_name:
            season_start_date, season_end_date = self.start_end_dates_for_season(season_name)

        # build tz_date_to_next_tz_date by zipping ordered TimeZeros, staggered by the ref_target's step_ahead_increment
        tz_dates = TimeZero.objects.filter(project=self) \
            .order_by('timezero_date') \
            .values_list('timezero_date', flat=True)
        tz_date_to_next_tz_date = dict(zip(tz_dates, tz_dates[ref_target.step_ahead_increment:]))

        # get loc_target_tz_date_to_truth(). we use all seasons b/c might need TimeZero from a previous season to get
        # this one. recall: [location][target][timezero_date] -> truth
        loc_target_tz_date_to_truth = self.location_target_timezero_date_to_truth()
        loc_tz_date_to_actual_vals = {}  # [location][timezero_date] -> actual
        for location in loc_target_tz_date_to_truth:
            # default to None so that any TimeZeros missing from loc_target_tz_date_to_truth are present:
            location_dict = {}
            for timezero in tz_dates:
                if not season_name or is_tz_date_in_season(timezero):
                    location_dict[timezero] = None
            loc_tz_date_to_actual_vals[location] = location_dict
            for truth_tz_date in loc_target_tz_date_to_truth[location][ref_target.name]:
                if truth_tz_date not in tz_date_to_next_tz_date:  # trying to project beyond last truth date
                    continue

                actual_tz_date = tz_date_to_next_tz_date[truth_tz_date]
                truth_value = loc_target_tz_date_to_truth[location][ref_target.name][truth_tz_date]
                is_actual_in_season = is_tz_date_in_season(actual_tz_date) if season_name else True
                if is_actual_in_season:
                    location_dict[actual_tz_date] = truth_value
        return loc_tz_date_to_actual_vals


    #
    # template and data-related functions
    #

    def is_template_loaded(self):
        return self.csv_filename != ''


    @transaction.atomic
    def load_template(self, template_path, file_name=None):
        """
        Loads the data from the passed Path into my corresponding ForecastData. First validates the data against my
        Project's template.

        :param template_path: Path to a CDC CSV forecast file
        :param file_name: optional name to use for the file. if None (default), uses template_path. helpful b/c uploaded
            files have random template_path file names, so original ones must be extracted and passed separately
        """
        self.csv_filename = file_name or template_path.name
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
        expected_location_template_pairs = set(product(template_locations, found_targets))
        if location_template_pairs != expected_location_template_pairs:
            raise RuntimeError("Target(s) was not found in every location. csv_filename={}, "
                               "missing location, target: {}"
                               .format(self.csv_filename, location_template_pairs ^ expected_location_template_pairs))


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

    is_step_ahead = BooleanField(help_text="Flag that's True if this Target is a 'k-step-ahead' one that can be used "
                                           "in analysis tools to reference forward and back in a Project's TimeZeros "
                                           "(when sorted by timezero_date). If True then step_ahead_increment must be "
                                           "set. Default is False.",
                                 default=False)

    step_ahead_increment = IntegerField(help_text="Optional field that's required when Target.is_step_ahead "
                                                  "is True, is an integer specifing how many time steps "
                                                  "ahead the Target is. Can be negative, zero, or positive.",
                                        default=0)


    def __repr__(self):
        return str((self.pk, self.name, self.is_step_ahead, self.step_ahead_increment))


    def __str__(self):  # todo
        return basic_str(self)


#
# ---- TimeZero class ----
#

class TimeZero(models.Model):
    """
    A date that a target is relative to. Additionally, contains an optional data_version_date the specifies the database
    date at which models should work with for this timezero_date date. Akin to rolling back (versioning) the database
    to that date. Also contains optional season demarcation information in the form of a pair of fields, which are
    both required if a TimeZero marks a season start. The starting TimeZero includes that TimeZero (is inclusive).
     
    Assumes dates from any project can be converted to actual dates, e.g., from Dengue biweeks or CDC MMWR weeks
    ( https://ibis.health.state.nm.us/resource/MMWRWeekCalendar.html ).
    """
    project = models.ForeignKey(Project, related_name='timezeros', on_delete=models.CASCADE)

    timezero_date = models.DateField(help_text="A date that a target is relative to.")

    data_version_date = models.DateField(
        null=True, blank=True,
        help_text="The optional database date at which models should work with for the timezero_date.")  # nullable

    is_season_start = models.BooleanField(
        default=False,
        help_text="True if this TimeZero starts a season.")

    season_name = models.CharField(
        null=True, blank=True,
        max_length=50, help_text="The name of the season this TimeZero starts, if is_season_start.")  # nullable


    def __repr__(self):
        return str((self.pk, self.timezero_date, self.data_version_date, self.is_season_start, self.season_name))


    def __str__(self):  # todo
        return basic_str(self)


    # Note: a model’s clean() method is not invoked when you call your model’s save() method.
    def clean(self):
        # must have season_name if is_season_start
        if self.is_season_start and not self.season_name:
            raise ValidationError('passed is_season_start with no season_name')

        # can't have season_name if no is_season_start
        if not self.is_season_start and self.season_name:
            raise ValidationError('passed season_name but no is_season_start')


    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)
