import csv
import datetime

import re
from itertools import groupby


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
# Following functions provide access to contents of files in CDC format. Recall the columns:
#
#   Location,Target,Type,Unit,Bin_start_incl,Bin_end_notincl,Value
#
# Also recall that each data file essentially contains a hierarchy of data: Location > Target > Type. Overall usage:
#
#   get_locations(csv_path)  # entry point that parses a file into a list of Locations
#
# Then iterate over Location.targets.
#
# todo implemented using pandas?


def get_locations(csv_path):
    """
    :return: Top-level entry point for parsing a CDC data file, returns a list of Locations for the passed file. raises
        if invalid filename or contents
    """
    if not filename_components(csv_path.name):
        raise RuntimeError("invalid filename: {}".format(csv_path.name))

    locations = []  # return value. filled next
    with open(str(csv_path)) as csv_path_fp:
        csv_reader = csv.reader(csv_path_fp, delimiter=',')
        next(csv_reader)  # skip header

        # group by location, creating a Location, then group by Target
        location_groupby = groupby(sorted(csv_reader), key=lambda _: _[0])
        for location_name, location_group in location_groupby:
            location = Location(location_name)
            locations.append(location)
            target_groupby = groupby(sorted(location_group, key=lambda _: _[1]), key=lambda _: _[1])
            for target_name, target_group in target_groupby:
                # [(Type,Unit,Bin_start_incl,Bin_end_notincl,Value), ...]:
                target_data = [row[2:] for row in list(target_group)]
                target = Target(target_name, target_data)
                location.targets.append(target)
    return locations


class Location:
    """
    Represents a location in a CDC file. Has a list of targets.
    """

    def __init__(self, name):
        self.name = name
        self.targets = []

    def __repr__(self):
        return str((self.name, self.targets))


class Target:
    """
    Represents a particular Location's target. Fields:

    - Target.name        # Target column value
    - Target.data_type   # Type column. either 'Point' or 'Bin'. todo other types?
    - Target.unit        # Unit column. either 'week' or 'percent'. todo other units?
    - Target.point       # Value column for the 'Point' data_type only. todo ever None?
    - Target.bins        # list of 3-tuples (rows) for the columns: (bin_start_incl, bin_end_notincl, value), sorted by
                         #   bin_start_incl. NB: first two might be None. for the 'Bin' data_type only. todo ever None?
    """

    def __init__(self, name, target_data):
        """
        :param name:
        :param target_data: the raw column data for this target: [(Type,Unit,Bin_start_incl,Bin_end_notincl,Value), ...]
        """
        self.name = name

        # set my type and unit arbitrarily to first row's values
        self.data_type = target_data[0][0]
        self.unit = target_data[0][1]

        # process target_data into either my point or my bins, based on data_type
        self.bins = []
        for data_type, unit, bin_start_incl, bin_end_notinclk, value in target_data:
            if data_type == 'Point':
                self.point = value
            else:
                self.bins.append((bin_start_incl, bin_end_notinclk, value))

        # sort bins by bin_start_incl? todo correct?
        self.bins.sort(key=lambda _: _[0])

    def __repr__(self):
        return str((self.name,))  # todo
