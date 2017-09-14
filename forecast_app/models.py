import csv
import json
from ast import literal_eval

import datetime
import re
import requests
from django.db import connection
from django.db import models, transaction
from django.urls import reverse


#
# ---- utilities ----
#

def parse_value(value):
    """
    Parses a value numerically as smartly as possible, in order: float, int, None. o/w is an error
    """
    # https://stackoverflow.com/questions/34425583/how-to-check-if-string-is-int-or-float-in-python-2-7
    try:
        return literal_eval(value)
    except ValueError:
        return None


def basic_str(obj):
    return obj.__class__.__name__ + ': ' + obj.__repr__()


def increment_week(year, week, delta_weeks):
    """
    Adds delta_weeks to timezero_week in timezero_year modulo 52, wrapping around to next year as needed. Returns a
    2-tuple: (incremented_year, incremented_week)
    """
    if (delta_weeks < 1) or (delta_weeks > 52):
        raise RuntimeError("delta_weeks wasn't between 1 and 52: {}".format(delta_weeks))

    incremented_week = week + delta_weeks
    if incremented_week > 52:
        return year + 1, incremented_week - 52
    else:
        return year, incremented_week


#
# ---- filename component functions ----
#

def filename_components(filename):
    """
    :param filename: something like 'EW1-KoTstable-2017-01-17.csv'
    :return: either () (if filename invalid) or a 3-tuple (if valid) that indicates if filename matches the CDC
    standard format as defined in [1]. The tuple format is: (ew_week_number, team_name, submission_datetime) . Note that
    "ew_week_number" AKA the forecast's "time zero"
    
    [1] https://webcache.googleusercontent.com/search?q=cache:KQEkQw99egAJ:https://predict.phiresearchlab.org/api/v1/attachments/flusight/flu_challenge_2016-17_update.docx+&cd=1&hl=en&ct=clnk&gl=us
        From that document: 
    
        For submission, the filename should be modified to the following standard naming convention: a forecast
        submission using week 43 surveillance data submitted by John Doe University on November 7, 2016, should be named
        “EW43-JDU-2016-11-07.csv” where EW43 is the latest week of ILINet data used in the forecast, JDU is the name of
        the team making the submission (e.g. John Doe University), and 2016-11-07 is the date of submission.
        
    """
    re_split = re.split(r'^EW(\d*)-(\S*)-(\d{4})-(\d{2})-(\d{2})\.csv$', filename)
    if len(re_split) != 7:
        return ()

    re_split = re_split[1:-1]  # drop outer two ''
    if any(map(lambda part: len(part) == 0, re_split)):
        return ()

    return int(re_split[0]), re_split[1], datetime.date(int(re_split[2]), int(re_split[3]), int(re_split[4]))


#
# ---- functions to access the delphi API ----
#

def delphi_wili_for_epi_week(year, week, location_name):
    """
    Looks up the 'wili' value for the past args, using the delphi REST API. Returns as a float.

    :param year:
    :param week: EW week number between 1 and 52 inclusive
    :param location_name:
    :return: actual value for the passed args, looked up dynamically via xhttps://github.com/cmu-delphi/delphi-epidata
    """
    region = region_for_location_name(location_name)
    if not region:
        raise RuntimeError("location_name is not a valid Delphi location: {}".format(location_name))

    url = 'https://delphi.midas.cs.cmu.edu/epidata/api.php' \
          '?source=fluview' \
          '&regions={region}' \
          '&epiweeks={epi_year}{ew_week_number:02d}'. \
        format(region=region, epi_year=year, ew_week_number=week)
    response = requests.get(url)
    response.raise_for_status()  # does nothing if == requests.codes.ok
    delph_dict = json.loads(response.text)
    wili_str = delph_dict['epidata'][0]['wili']  # will raise KeyError if response json not structured as expected
    return parse_value(wili_str)


#
# ---- model-specific target and location name functions ----
#
# todo abstract these to elsewhere
#

def week_increment_for_target_name(target_name):
    """
    :return: returns an incremented week value based on the future specified by target_name
    """
    target_name_to_week_increment = {
        '1 wk ahead': 1,
        '2 wk ahead': 2,
        '3 wk ahead': 3,
        '4 wk ahead': 4,
    }
    return target_name_to_week_increment[target_name]


def region_for_location_name(location_name):
    """
    :param location_name: model-specific location
    :return: maps synonyms to official Delphi 'region' API parameter. returns None if not found.
        see https://github.com/cmu-delphi/delphi-epidata/blob/758b6ad25cb98127038c430ebb57801a05f4cd56/labels/regions.txt
    """
    region_to_synonyms = {
        'nat': ['US National'],
        'hhs1': ['HHS Region 1'],
        'hhs2': ['HHS Region 2'],
        'hhs3': ['HHS Region 3'],
        'hhs4': ['HHS Region 4'],
        'hhs5': ['HHS Region 5'],
        'hhs6': ['HHS Region 6'],
        'hhs7': ['HHS Region 7'],
        'hhs8': ['HHS Region 8'],
        'hhs9': ['HHS Region 9'],
        'hhs10': ['HHS Region 10'],
    }
    if location_name in region_to_synonyms:
        return location_name

    for region, synonyms in region_to_synonyms.values():
        if location_name in synonyms:
            return region

    return None


#
# ---- models ----
#

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


class ForecastModel(models.Model):
    """
    Represents a project's model entry by a competing team, including metadata, model-specific auxiliary data beyond
    core data, and a list of the actual forecasts.
    """
    project = models.ForeignKey(Project, on_delete=models.CASCADE, null=True)

    name = models.CharField(max_length=200)

    description = models.CharField(max_length=2000, help_text="A few paragraphs describing the model. should include "
                                                              "information on reproducing the model’s results")

    url = models.URLField(help_text="The model's development URL")

    auxiliary_data = models.URLField(null=True,
                                     help_text="optional model-specific Zip file containing data files (e.g., CSV "
                                               "files) beyond Project.core_data that were used by the this model")


    def __repr__(self):
        return str((self.pk, self.name))


    def __str__(self):  # todo
        return basic_str(self)


    def get_absolute_url(self):
        return reverse('forecastmodel-detail', args=[str(self.id)])


    @transaction.atomic
    def load_forecast_via_sql(self, csv_file_path, time_zero):  # alternative implementation using direct SQL INSERTs
        """
        :param csv_file_path: Path to a CDC CSV forecast file
        :param time_zero: the TimeZero this forecast applies to
        :return: loads the data from the passed Path into my corresponding CDCData, and returns a new Forecast for it
        """
        forecast = Forecast.objects.create(forecast_model=self, time_zero=time_zero, data_filename=csv_file_path.name)

        # insert the data using direct SQL. for now simply use separate INSERTs per row
        with open(str(csv_file_path)) as csv_path_fp, \
                connection.cursor() as cursor:
            csv_reader = csv.reader(csv_path_fp, delimiter=',')
            next(csv_reader)  # skip header
            # todo better way to get FK name? - Forecast._meta.model_name + '_id' . also, maybe use CDCData._meta.fields ?
            column_names = ', '.join(['location', 'target', 'row_type', 'unit', 'bin_start_incl', 'bin_end_notincl',
                                      'value', Forecast._meta.model_name + '_id'])
            for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
                location, target, row_type, unit, bin_start_incl, bin_end_notincl, value = row[:7]
                row_type = CDCData.POINT_ROW_TYPE if row_type == 'Point' else CDCData.BIN_ROW_TYPE
                sql = """
                    INSERT INTO {cdcdata_table_name} ({column_names})
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """.format(cdcdata_table_name=CDCData._meta.db_table,
                           column_names=column_names)
                # we use parse_value() to handle non-numeric cases like 'NA' and 'none'
                cursor.execute(sql, [location, target, row_type, unit, parse_value(bin_start_incl),
                                     parse_value(bin_end_notincl), parse_value(value), forecast.pk])

        # done
        return forecast


    @transaction.atomic
    # def load_forecast_using_managed(self, csv_file_path, time_zero):
    def load_forecast(self, csv_file_path, time_zero):  # alternative implementation using ORM
        """
        :param csv_file_path: Path to a CDC CSV forecast file
        :param time_zero: the TimeZero this forecast applies to
        :return: loads the data from the passed Path into my corresponding CDCData, and returns a new Forecast for it
        """
        forecast = Forecast.objects.create(forecast_model=self, time_zero=time_zero, data_filename=csv_file_path.name)

        # bulk insert the data using the ORM. might be slow!
        cdc_data_objs = []
        with open(str(csv_file_path)) as csv_path_fp:
            csv_reader = csv.reader(csv_path_fp, delimiter=',')
            next(csv_reader)  # skip header
            for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
                location, target, row_type, unit, bin_start_incl, bin_end_notincl, value = row[:7]
                cdc_data_objs.append(CDCData(
                    forecast=forecast, location=location, target=target,
                    row_type=CDCData.POINT_ROW_TYPE if row_type == 'Point' else CDCData.BIN_ROW_TYPE,
                    unit=unit,
                    bin_start_incl=parse_value(bin_start_incl),
                    bin_end_notincl=parse_value(bin_end_notincl),
                    value=value))
        CDCData.objects.bulk_create(cdc_data_objs)  # ignore returned list of new objects

        # done
        return forecast


    def time_zero_for_timezero_date_str(self, timezero_date_str):
        """
        :return: the first TimeZero in forecast_model's Project that has a timezero_date matching timezero_date
        """
        for time_zero in self.project.timezero_set.all():
            if time_zero.timezero_date == timezero_date_str:
                return time_zero

        return None


    def mean_absolute_error(self, season_start_year, location, target,
                            wili_for_epi_week_fcn=delphi_wili_for_epi_week):
        """
        :param:season_start_year: year of the season, e.g., 2016 for the season 2016-2017
        :param:true_value_for_epi_week_fcn: a function of three args (year, week, location_name) that returns the
            true/actual wili value for an epi week
        :return: mean absolute error (scalar) for my predictions for a location and target
        """
        cdc_file_name_to_abs_error = {}
        for forecast in self.forecast_set.all():
            # set timezero week and year, inferring the latter based on @Evan's @Nick's reply:
            # > We used week 30.  I don't think this is a standardized concept outside of our lab though."
            # > We use separate concepts for a "season" and a "year". So, e.g. the "2016/2017 season" starts with
            # > EW30-2016 and ends with EW29-2017.
            # todo abstract this to elsewhere
            timezero_week = filename_components(forecast.data_filename)[0]
            timezero_year = season_start_year if timezero_week >= 30 else season_start_year + 1
            future_year, future_week = increment_week(timezero_year, timezero_week,
                                                      week_increment_for_target_name(target))
            true_value = wili_for_epi_week_fcn(future_year, future_week, location)
            predicted_value = forecast.get_target_point_value(location, target)
            abs_error = abs(predicted_value - true_value)
            # print('xx', forecast, ':', timezero_week, timezero_year, '.', future_week, future_year, '.', true_value, predicted_value, abs_error)
            cdc_file_name_to_abs_error[forecast.data_filename] = abs_error

        return sum(cdc_file_name_to_abs_error.values()) / len(cdc_file_name_to_abs_error)


class Forecast(models.Model):
    """
    Represents a model's forecasted data. There is one Forecast for each of my ForecastModel's Project's TimeZeros.
    """
    forecast_model = models.ForeignKey(ForecastModel, on_delete=models.CASCADE, null=True)

    time_zero = models.ForeignKey(TimeZero, on_delete=models.CASCADE, null=True,
                                  help_text="TimeZero that this forecast is in relation to")

    data_filename = models.CharField(max_length=200,
                                     help_text="Original CSV file name of this forecast's data source")


    def __repr__(self):
        return str((self.pk, self.time_zero, self.data_filename))


    def __str__(self):  # todo
        return basic_str(self)


    def get_absolute_url(self):
        return reverse('forecast-detail', args=[str(self.id)])


    def get_data_rows(self):
        """
        Main accessor of my data. Abstracts where data is located.

        :return: a list of my rows, excluding CDCData PK and Forecast FK
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT *
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s;
        """.format(cdcdata_table_name=CDCData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk])
            rows = cursor.fetchall()
            return [row[1:-1] for row in rows]


    def get_data_preview(self):
        """
        :return: a preview of my data in the form of a table that's represented as a nested list of rows
        """
        return self.get_data_rows()[:10]


    def get_locations(self):
        """
        :return: a list of Location names corresponding to my CDCData
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT location
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s
            GROUP BY location;
        """.format(cdcdata_table_name=CDCData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk])
            rows = cursor.fetchall()
            return [row[0] for row in rows]


    def get_targets(self, location):
        """
        :return: list of target names for a location
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT target
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s AND location = %s
            GROUP BY target;
        """.format(cdcdata_table_name=CDCData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk, location])
            rows = cursor.fetchall()
            return [row[0] for row in rows]


    def _get_point_row(self, location, target):
        """
        :return: the first row of mine whose row_type = CDCData.POINT_ROW_TYPE . includes CDCData PK and Forecast FK
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT *
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s AND row_type = %s AND location = %s and target = %s;
        """.format(cdcdata_table_name=CDCData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk, CDCData.POINT_ROW_TYPE, location, target])
            rows = cursor.fetchall()
            return rows[0]


    def get_target_unit(self, location, target):
        """
        :return: name of the unit column. arbitrarily uses the point row's unit
        """
        point_row = self._get_point_row(location, target)
        return point_row[4]


    def get_target_point_value(self, location, target):
        """
        :return: point value for a location and target 
        """
        point_row = self._get_point_row(location, target)
        return parse_value(point_row[7])  # todo if [use numbers of correct type] above, change this to not cast


    def get_target_bins(self, location, target):
        """
        :return: the CDCData.BIN_ROW_TYPE rows of mine for a location and target
        """
        # todo better way to get FK name? - {forecast_model_name}_id
        sql = """
            SELECT bin_start_incl, bin_end_notincl, value
            FROM {cdcdata_table_name}
            WHERE {forecast_model_name}_id = %s AND row_type = %s AND location = %s and target = %s;
        """.format(cdcdata_table_name=CDCData._meta.db_table,
                   forecast_model_name=Forecast._meta.model_name)
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.pk, CDCData.BIN_ROW_TYPE, location, target])
            rows = cursor.fetchall()
            return [(parse_value(bin_start_incl), parse_value(bin_end_notincl), parse_value(value))
                    for bin_start_incl, bin_end_notincl, value in rows]


class CDCData(models.Model):
    """
    Contains the content of a CDC format CSV file as documented in about.html . Content is manually managed by
    ForecastModel.load_forecast. Django manages migration (CREATE TABLE) and cascading deletion.
    """
    forecast = models.ForeignKey(Forecast, on_delete=models.CASCADE, null=True)

    # the standard CDC format columns from the source forecast.data_filename:
    location = models.CharField(max_length=200)
    target = models.CharField(max_length=200)

    POINT_ROW_TYPE = 'p'
    BIN_ROW_TYPE = 'b'
    ROW_TYPE_CHOICES = ((POINT_ROW_TYPE, 'Point'),
                        (BIN_ROW_TYPE, 'Bin'))
    row_type = models.CharField(max_length=1, choices=ROW_TYPE_CHOICES)

    unit = models.CharField(max_length=200)

    # todo use numbers of correct type - see parse_value() -> change data_row().
    # see "my issue is that I have to pick a field type for the latter three, which can be *either* int or float"
    bin_start_incl = models.CharField(max_length=200, null=True)
    bin_end_notincl = models.CharField(max_length=200, null=True)
    value = models.CharField(max_length=200)


    def __repr__(self):
        return str((self.pk, self.forecast.pk, *self.data_row()))


    def __str__(self):  # todo
        return basic_str(self)


    def data_row(self):
        # todo if [use numbers of correct type] above, change this to not cast
        return [self.location, self.target, self.row_type, self.unit,
                parse_value(self.bin_start_incl), parse_value(self.bin_end_notincl), parse_value(self.value)]
