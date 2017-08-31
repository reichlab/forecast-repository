import csv
import datetime

import re


#
# ---- filename functions ----
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
# ---- content functions ----
#
# Recall that each data file essentially contains a hierarchy of data: Location > Target > Type. Thus we have accessors
# for each level for a file:
#   - file -> locations
#   - file + location -> targets
#   - file + location + target -> data (data_type, unit, bin_start_incl, bin_end_notincl, value)

# todo following should be implemented OOP, and probably also using pandas. possible API:
#   - get_locations(csv_path)  # list of Locations (parser/entry point)
#   - Location.name
#   - Location.targets   # list of Targets
#   - Target.name
#   - Target.data_type   # 'Point' or 'Bin'. todo other types?
#   - Target.unit        # e.g., 'week' or 'percent'. todo other units?
#   - Target.point       # a value. 'Point' data_type only. todo ever None?
#   - Target.bin         # list of 3-tuple rows: (bin_start_incl, bin_end_notincl, value). NB: first two might be None.
#                        # todo ever None? 'Bin' data_type only.

def get_locations(csv_path):
    """
    :return: Top-level entry point for parsing a CDC data file, returns a list of Locations for the passed file. raises
        if invalid filename or contents 
    """
    if not filename_components(csv_path.name):
        raise RuntimeError("invalid filename: {}".format(csv_path.name))

    locations = []
    with open(str(csv_path)) as csv_path_fp:
        csv_reader = csv.reader(csv_path_fp, delimiter=',')
        next(csv_reader)  # skip header
        # todo xx groupby location, etc.
        # for location, target, data_type, unit, bin_start_incl, bin_end_notincl, value in enumerate(csv_reader):
        #     xx
    return locations
