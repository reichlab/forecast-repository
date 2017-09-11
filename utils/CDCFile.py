import csv
from ast import literal_eval
from itertools import groupby


#
# Following functions provide access to contents of files in CDC format. Recall the columns:
#
#   Location,Target,Type,Unit,Bin_start_incl,Bin_end_notincl,Value
#
# Also recall that each data file essentially contains a hierarchy of data: Location > Target > Type.
# Overall usage: create a CDCFile and then access its Locations and their Targets
#

class CDCFile:
    """
    Represents a CDC format CSV file as documented in about.html .
    """

    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.locations = self._get_locations()

    # def __repr__(self):
    #     return str((self.csv_path,))  # todo

    def __str__(self):
        return '<' + hex(id(self)) + ' ' + str(self.csv_path.name) + '>'

    def _get_locations(self):
        """
        :return: parses my CDC data file and returns a list of Locations for the passed file. raises if invalid filename or
            contents
        """
        from utils.cdc_format_utils import filename_components  # circular import hack
        if not filename_components(self.csv_path.name):
            raise RuntimeError("invalid filename: {}".format(self.csv_path.name))

        locations = []  # return value. filled next
        with open(str(self.csv_path)) as csv_path_fp:
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

    def get_location(self, location_name):
        """
        :param location_name:
        :return: the first Location in me with name. returns None if not found
        """
        for location in self.locations:
            if location.name == location_name:
                return location
        return None


class Location:
    """
    Represents a location in a CDC file. Has a list of targets.
    """

    def __init__(self, name):
        self.name = name
        self.targets = []

    def __repr__(self):
        return str((self.name, self.targets))

    def get_target(self, target_name):
        """
        :param target_name:
        :return: the first Target in me with name. returns None if not found
        """
        for target in self.targets:
            if target.name == target_name:
                return target
        return None


class Target:
    """
    Represents a particular Location's target. Fields:
    - Target.name : Target column value
    - Target.unit : Unit column
    - Target.point: Value column for the 'Point' data_type only. todo ever None?
    - Target.bins : list of 3-tuples (rows) for the columns: (bin_start_incl, bin_end_notincl, value), sorted by
                    bin_start_incl. NB: one or both of the first two might be None. for the 'Bin' data_type only
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
        for data_row in target_data:
            if len(data_row) != 5:
                if (len(data_row) == 6) and not (data_row[-1]):  # sometimes rows have an ending ',' char
                    data_row = data_row[:5]
                else:
                    raise RuntimeError("target_data did not have exactly 5 non-empty columns: {}".format(data_row))

            data_type, unit, bin_start_incl, bin_end_notinclk, value = data_row
            if data_type == 'Point':  # either 'Point' or 'Bin'
                self.point = parse_value(value)
                if not self.point:
                    raise RuntimeError("point value was not a number: {!r}".format(value))
            else:
                parsed_vals = list(map(parse_value, [bin_start_incl, bin_end_notinclk, value]))
                if None in parsed_vals:
                    # print("skipping row with a non-numeric bin value: {}".format(data_row))  # todo logging
                    pass
                else:
                    self.bins.append(parsed_vals)

        # sort bins by bin_start_incl. NB: this might be in a different order than the original file, which might (?)
        # have implications for files like EW1-KoTstable-2017-01-17.csv that order implicitly by year, where weeks 40-
        # 52 is 2016, and weeks 1-20 are 2017
        self.bins.sort(key=lambda _: _[0])

    def __repr__(self):
        return str((self.name,))  # todo


def parse_value(value):
    """
    Parses a value numerically as smartly as possible, in order: float, int, None. o/w is an error
    """
    # https://stackoverflow.com/questions/34425583/how-to-check-if-string-is-int-or-float-in-python-2-7
    try:
        return literal_eval(value)
    except ValueError:
        return None
