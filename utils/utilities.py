import datetime
import re
from ast import literal_eval

import pymmwr


#
# __str__()-related functions
#

def basic_str(obj):
    """
    Handy for writing quick and dirty __str__() implementations.
    """
    return obj.__class__.__name__ + ': ' + obj.__repr__()


#
# numberic functions
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


# from https://stats.stackexchange.com/questions/25894/changing-the-scale-of-a-variable-to-0-100/95174
def rescale(values, new_min=0, new_max=100):
    try:
        output = []
        old_min, old_max = min(values), max(values)
        for v in values:
            new_v = (new_max - new_min) / (old_max - old_min) * (v - old_min) + new_min
            output.append(new_v)
        return output
    except Exception as ex:
        raise ValueError("invalid argument. values={}, exception='{}'".format(values, ex))


#
# Reichlab season-related functions
#

# This number is the internal reichlab standard: "We used week 30. I don't think this is a standardized concept outside
# of our lab though. We use separate concepts for a "season" and a "year". So, e.g. the "2016/2017 season" starts with
# EW30-2016 and ends with EW29-2017."
SEASON_START_EW_NUMBER = 30


def is_date_in_season(date, season_start_year):
    """
    :param date: a Date object
    :param season_year: an int. ex: 2016 represents the "2016/2017 season"
    :return: True if date is in  season_start_year
    """
    ywd_mmwr_dict = pymmwr.date_to_mmwr_week(date)
    mmwr_year = ywd_mmwr_dict['year']
    mmwr_week = ywd_mmwr_dict['week']
    return ((mmwr_week >= SEASON_START_EW_NUMBER) and (mmwr_year == season_start_year)) or \
           ((mmwr_week < SEASON_START_EW_NUMBER) and (mmwr_year == (season_start_year + 1)))


def season_start_year_for_date(date):
    """
    example seasons:
    - 2015/2016: EW30-2015 through EW29-2016
    - 2016/2017: EW30-2016 through EW29-2017
    - 2017/2018: EW30-2017 through EW29-2018

    rule:
    - EW01 through EW29: the previous year
    - EW30 through EW52/EW53: the current year

    :param date:
    :return: the season start year that date is in, based on SEASON_START_EW_NUMBER
    """
    ywd_mmwr_dict = pymmwr.date_to_mmwr_week(date)
    mmwr_year = ywd_mmwr_dict['year']
    mmwr_week = ywd_mmwr_dict['week']
    return mmwr_year - 1 if mmwr_week < SEASON_START_EW_NUMBER else mmwr_year


#
# *.cdc.csv file functions
#
# The following functions implement this project's file naming standard, and defined in 'Forecast data file names' in
# documentation.html, e.g., '<time_zero>-<model_name>[-<data_version_date>].cdc.csv' . For example:
#
# - '20170419-gam_lag1_tops3-20170516.cdc.csv'
# - '20161023-KoTstable-20161109.cdc.csv'
# - '20170504-gam_lag1_tops3.cdc.csv'
#

CDC_CSV_HEADER = ['location', 'target', 'type', 'unit', 'bin_start_incl', 'bin_end_notincl', 'value']

CDC_CSV_FILENAME_EXTENSION = 'cdc.csv'

CDC_CSV_FILENAME_RE_PAT = re.compile(r"""
^
(\d{4})(\d{2})(\d{2})    # time_zero YYYYMMDD
-                        # dash
([a-zA-Z0-9_]+)          # model_name
(?:                      # non-repeating group so that '-20170516' doesn't get included
  -                      # optional dash and dvd
  (\d{4})(\d{2})(\d{2})  # data_version_date YYYYMMDD
  )?                     #
\.cdc.csv$
""", re.VERBOSE)


def cdc_csv_components_from_data_dir(cdc_csv_dir):
    """
    A utility that helps process a directory containing cdc cvs files.

    :return a list of 4-tuples for each *.cdc.csv file in cdc_csv_dir, with the last three in the form returned by
        get_components_from_cdc_csv_filename(): (cdc_csv_file, time_zero, model_name, data_version_date).
        cdc_csv_file is a Path
    """
    cdc_csv_components = []
    for cdc_csv_file in cdc_csv_dir.glob('*.' + CDC_CSV_FILENAME_EXTENSION):
        time_zero, model_name, data_version_date = cdc_csv_filename_components(cdc_csv_file.name)
        cdc_csv_components.append((cdc_csv_file, time_zero, model_name, data_version_date))
    return cdc_csv_components


def cdc_csv_filename_components(cdc_csv_filename):
    """
    :param cdc_csv_filename: a *.cdc.csv file name, e.g., '20170419-gam_lag1_tops3-20170516.cdc.csv'
    :return: a 3-tuple of components from cdc_csv_file: (time_zero, model_name, data_version_date), where dates are
        datetime.date objects. data_version_date is None if not found in the file name. returns None if the file name
        is invalid, i.e., does not conform to our standard.
    """
    match = CDC_CSV_FILENAME_RE_PAT.match(cdc_csv_filename)
    if not match:
        return None

    # groups has our two cases: with and without data_version_date, e.g.,
    # ('2017', '04', '19', 'gam_lag1_tops3', '2017', '05', '16')
    # ('2017', '04', '19', 'gam_lag1_tops3', None, None, None)
    groups = match.groups()
    time_zero = datetime.date(int(groups[0]), int(int(groups[1])), int(int(groups[2])))
    model_name = groups[3]
    data_version_date = datetime.date(int(groups[4]), int(int(groups[5])), int(int(groups[6]))) if groups[4] else None
    return time_zero, model_name, data_version_date
