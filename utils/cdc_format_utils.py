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
    standard format as defined in [1]. The tuple format is: (ew_week_number, team_name, submission_datetime) . 
    
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

def true_value_for_epi_week(season_start_year, ew_week_number, location_name):
    """
    Looks up the 'wili' value for the past args, using the delphi REST API. Returns as a float.

    :param season_start_year:
    :param ew_week_number:
    :param location_name:
    :return: actual value for the passed args, looked up dynamically via xhttps://github.com/cmu-delphi/delphi-epidata
    """
    region = region_for_location_name(location_name)
    if not region:
        raise RuntimeError("location_name is not a valid Delphi location: {}".format(location_name))

    url = 'https://delphi.midas.cs.cmu.edu/epidata/api.php' \
          '?source=fluview' \
          '&regions={region}' \
          '&epiweeks={epiweeks}{ew_week_number:02d}'. \
        format(region=region, epiweeks=season_start_year, ew_week_number=ew_week_number)
    response = requests.get(url)
    response.raise_for_status()  # does nothing if == requests.codes.ok
    delph_dict = json.loads(response.text)
    wili_str = delph_dict['epidata'][0]['wili']  # will raise KeyError if response json not structured as expected
    return parse_value(wili_str)


#
# ---- 'statistical' functions ----
#

def mean_absolute_error_for_model_dir(model_csv_path, season_start_year, location_name, target_name,
                                      true_value_for_epi_week_fcn=true_value_for_epi_week):
    """
    :return: mean absolute error (scalar) for the model's predictions in the passed path, for location and target
    """
    cdc_file_name_to_abs_error = {}
    for cdc_file in cdc_files_for_dir(model_csv_path):
        ew_week_number = filename_components(cdc_file.csv_path.name)[0]
        predicted_value = cdc_file.get_location(location_name).get_target(target_name).point
        true_value = true_value_for_epi_week_fcn(season_start_year, ew_week_number, location_name)
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


REGION_TO_SYNONYMS = {
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


def region_for_location_name(location_name):
    """
    :param location_name: model-specific location
    :return: maps synonyms to official Delphi 'region' API parameter. returns None if not found.
        see https://github.com/cmu-delphi/delphi-epidata/blob/758b6ad25cb98127038c430ebb57801a05f4cd56/labels/regions.txt
    """
    # todo abstract this out to a model-specific configuration
    if location_name in REGION_TO_SYNONYMS:
        return location_name

    for region, synonyms in REGION_TO_SYNONYMS.values():
        if location_name in synonyms:
            return region

    return None
