import datetime
import json
import re

import requests

from utils.CDCFile import CDCFile, parse_value


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
# ---- 'statistical' functions ----
#


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


def mean_absolute_error_for_model_dir(model_csv_path, season_start_year, location_name, target_name,
                                      wili_for_epi_week_fcn=delphi_wili_for_epi_week):
    """
    :param:model_csv_path: directory of a model's forecasts in CDC csv format
    :param:season_start_year: year of the season, e.g., 2016 for the season 2016-2017
    :param:location_name: as in the 'Location' column in csv files
    :param:target_name: "" 'Target' ""
    :param:true_value_for_epi_week_fcn: a function of three args (year, week, location_name) that returns the
        true/actual wili value for an epi week
    :return: mean absolute error (scalar) for the model's predictions in the passed path, for location and target
    """
    cdc_file_name_to_abs_error = {}
    for cdc_file in cdc_files_for_dir(model_csv_path):
        # set timezero week and year, inferring the latter based on Nick's comment: see 'stable definition of the
        # first "week of a season"' -> 40 is magic
        timezero_week = filename_components(cdc_file.csv_path.name)[0]
        timezero_year = season_start_year if timezero_week > 40 else season_start_year + 1
        future_year, future_week = increment_week(timezero_year, timezero_week,
                                                  week_increment_for_target_name(target_name))
        true_value = wili_for_epi_week_fcn(future_year, future_week, location_name)
        predicted_value = cdc_file.get_location(location_name).get_target(target_name).point
        abs_error = abs(predicted_value - true_value)
        cdc_file_name_to_abs_error[cdc_file.csv_path.name] = abs_error
    return sum(cdc_file_name_to_abs_error.values()) / len(cdc_file_name_to_abs_error)


def cdc_files_for_dir(csv_dir_path):
    """
    :return: a list of CDCFiles for each csv file in csv_dir_path
    """
    cdc_files = []
    for csv_file_p in csv_dir_path.iterdir():
        if csv_file_p.suffix != '.csv':
            continue

        cdc_files.append(CDCFile(csv_file_p))
    return cdc_files


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
