import csv
import datetime
import json
import re
from itertools import groupby
from pathlib import Path

import pymmwr
from django.db import transaction

from forecast_app.models import PointPrediction, BinDistribution
from forecast_app.models.forecast import Forecast
from utils.forecast import load_predictions_from_json_io_dict, PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS
from utils.project import validate_and_create_locations, validate_and_create_targets
from utils.utilities import parse_value, YYYY_MM_DD_DATE_FORMAT


#
# load_cdc_csv_forecast_file() and friends
#

# This number is the internal reichlab standard: "We used week 30. I don't think this is a standardized concept outside
# of our lab though. We use separate concepts for a "season" and a "year". So, e.g. the "2016/2017 season" starts with
# EW30-2016 and ends with EW29-2017."
SEASON_START_EW_NUMBER = 30


@transaction.atomic
def load_cdc_csv_forecast_file(season_start_year, forecast_model, cdc_csv_file_path, time_zero):
    """
    Loads the passed cdc csv file into a new forecast_model Forecast for time_zero. NB: does not check if a Forecast
    already exists for time_zero and file_name. Is atomic so that an invalid forecast's data is not saved.

    :param season_start_year: as returned by season_start_year_from_ew_and_year()
    :param forecast_model: the ForecastModel to create the new Forecast in
    :param cdc_csv_file_path: string or Path to a CDC CSV forecast file. the CDC CSV file format is documented at
        https://predict.cdc.gov/api/v1/attachments/flusight/flu_challenge_2016-17_update.docx
    :param time_zero: the TimeZero this forecast applies to
    :return returns a new Forecast for it
    :raises RuntimeError if the data could not be loaded
    """
    if time_zero not in forecast_model.project.timezeros.all():
        raise RuntimeError(f"time_zero was not in project. time_zero={time_zero}, "
                           f"project timezeros={forecast_model.project.timezeros.all()}")

    cdc_csv_file_path = Path(cdc_csv_file_path)
    file_name = cdc_csv_file_path.name
    new_forecast = Forecast.objects.create(forecast_model=forecast_model, time_zero=time_zero, source=file_name)
    with open(cdc_csv_file_path) as cdc_csv_file_fp:
        json_io_dict = json_io_dict_from_cdc_csv_file(season_start_year, cdc_csv_file_fp)
        load_predictions_from_json_io_dict(new_forecast, json_io_dict)
    return new_forecast


def json_io_dict_from_cdc_csv_file(season_start_year, cdc_csv_file_fp):
    """
    Utility that extracts the three types of predictions found in CDC CSV files (PointPredictions, BinLwrDistributions,
    and BinCatDistributions), returning them as a "JSON IO dict" suitable for loading into the database (see
    load_predictions_from_json_io_dict()). Note that the returned dict's "meta" section is empty.

    :param season_start_year: as returned by season_start_year_from_ew_and_year()
    :param cdc_csv_file_fp: an open cdc csv file-like object. the CDC CSV file format is documented at
        https://predict.cdc.gov/api/v1/attachments/flusight/flu_challenge_2016-17_update.docx
    :return a "JSON IO dict" (aka 'json_io_dict' by callers) that contains the three types of predictions. see docs for
        details
    """
    return {'meta': {},
            'predictions': _prediction_dicts_for_csv_rows(season_start_year,
                                                          _cleaned_rows_from_cdc_csv_file(cdc_csv_file_fp))}


def _cleaned_rows_from_cdc_csv_file(cdc_csv_file_fp):
    """
    Loads the rows from cdc_csv_file_fp, cleans them, and then returns them as a list. Does some basic validation,
    but does not check locations and targets. This is b/c Locations and Targets might not yet exist (if they're
    dynamically created by this method's callers). Does *not* skip bin rows where the value is 0.

    :param cdc_csv_file_fp: the *.cdc.csv data file to load
    :return: a list of rows: location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value
    """
    csv_reader = csv.reader(cdc_csv_file_fp, delimiter=',')

    # validate header. must be 7 columns (or 8 with the last one being '') matching
    try:
        orig_header = next(csv_reader)
    except StopIteration:  # a kind of Exception, so much come first
        raise RuntimeError("empty file.")
    except Exception as exc:
        raise RuntimeError(f"error reading from cdc_csv_file_fp={cdc_csv_file_fp}. exc={exc}")

    header = orig_header
    if (len(header) == 8) and (header[7] == ''):
        header = header[:7]
    header = [h.lower() for h in [i.replace('"', '') for i in header]]
    if header != CDC_CSV_HEADER:
        raise RuntimeError(f"invalid header. header={header!r}, orig_header={orig_header!r}")

    # collect the rows. first we load them all into memory (processing and validating them as we go)
    rows = []
    for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
        if (len(row) == 8) and (row[7] == ''):
            row = row[:7]

        if len(row) != 7:
            raise RuntimeError(f"Invalid row (wasn't 7 columns): {row!r}")

        location_name, target_name, row_type, unit, bin_start_incl, bin_end_notincl, value = row  # unit ignored

        # validate row_type
        row_type = row_type.lower()
        if (row_type != CDC_POINT_ROW_TYPE.lower()) and (row_type != CDC_BIN_ROW_TYPE.lower()):
            raise RuntimeError(f"row_type was neither '{CDC_POINT_ROW_TYPE}' nor '{CDC_BIN_ROW_TYPE}': {row_type!r}")
        is_point_row = (row_type == CDC_POINT_ROW_TYPE.lower())

        # parse_value() handles non-numeric cases like 'NA' and 'none', which it turns into None. o/w it's a number
        bin_start_incl = parse_value(bin_start_incl)
        bin_end_notincl = parse_value(bin_end_notincl)
        value = parse_value(value)
        rows.append([location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value])

    return rows


def _prediction_dicts_for_csv_rows(season_start_year, rows):
    """
    json_io_dict_from_cdc_csv_file() helper that returns a list of prediction dicts for the 'predictions' section of the
    exported json. Each dict corresponds to either a PointPrediction or BinDistribution depending on each row in rows.
    Uses season_start_year to convert EWs to YYYYMMDD_DATE_FORMAT dates.

    Recall the seven cdc-project.json targets and their types:
    -------------------------+-------------------------------+-----------+-----------+---------------------
    Target name              | target_type                   | unit      | data_type | step_ahead_increment
    -------------------------+-------------------------------+-----------+-----------+---------------------
    "Season onset"           | Target.NOMINAL_TARGET_TYPE    | "week"    | date      | n/a
    "Season peak week"       | Target.DATE_TARGET_TYPE       | "week"    | text      | n/a
    "Season peak percentage" | Target.CONTINUOUS_TARGET_TYPE | "percent" | float     | n/a
    "1 wk ahead"             | Target.CONTINUOUS_TARGET_TYPE | "percent" | float     | 1
    "2 wk ahead"             | ""                            | ""        | ""        | 2
    "3 wk ahead"             | ""                            | ""        | ""        | 3
    "4 wk ahead"             | ""                            | ""        | ""        | 4
    -------------------------+-------------------------------+-----------+-----------+---------------------

    Note that the "Season onset" target is nominal and not date. This is due to how the CDC decided to represent the
    case when predicting no season onset, i.e., the threshold is not exceeded. This is done via a "none" bin where
    both Bin_start_incl and Bin_end_notincl are the strings "none" and not an EW week number. Thus, we have to store
    all bin starts as strings and not dates. At one point the lab was going to represent this case by splitting the
    "Season onset" target into two: "season_onset_binary" (a Target.BINARY that indicates whether there is an onset or
    not) and "season_onset_date" (a Target.DATE_TARGET_TYPE that is the onset date if "season_onset_binary" is true).
    But we dropped that idea and stayed with the original single nominal target.

    :param season_start_year: as returned by season_start_year_from_ew_and_year()
    :param rows: as returned by _cleaned_rows_from_cdc_csv_file():
        location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value
    :return: a list of PointPrediction or BinDistribution prediction dicts
    """
    prediction_dicts = []  # return value
    rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby()
    for (location_name, target_name, is_point_row), bin_start_end_val_grouper in \
            groupby(rows, key=lambda _: (_[0], _[1], _[2])):
        # NB: should only be one point row per location/target pair, but collect all (i.e., don't validate here):
        point_values = []
        bin_cats, bin_probs = [], []
        for _, _, _, bin_start_incl, bin_end_notincl, value in bin_start_end_val_grouper:  # all 3 are numbers or None
            try:
                if is_point_row:  # save value in point_values, possibly converted based on target
                    if target_name == 'Season onset':  # nominal target. value: None or an EW Monday date
                        if value is None:
                            value = 'none'
                        else:  # value is an EW week number
                            monday_date = monday_date_from_ew_and_season_start_year(value, season_start_year)
                            value = monday_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                    elif target_name == 'Season peak week':  # date target. value: an EW Monday date
                        monday_date = monday_date_from_ew_and_season_start_year(value, season_start_year)
                        value = monday_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                    point_values.append(value)
                # is_bin_row:
                elif target_name == 'Season onset':  # nominal target. start: None or an EW Monday date
                    if (bin_start_incl is None) and (bin_end_notincl is None):  # "none" bin (probability of no onset)
                        bin_cat = 'none'  # convert back from None to original 'none' input
                    elif (bin_start_incl is not None) and (bin_end_notincl is not None):  # regular (non-"none") bin
                        monday_date = monday_date_from_ew_and_season_start_year(bin_start_incl, season_start_year)
                        bin_cat = monday_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                    else:
                        raise RuntimeError(f"got 'Season onset' row but not both start and end were None. "
                                           f"bin_start_incl={bin_start_incl}, bin_end_notincl={bin_end_notincl}")
                    bin_cats.append(bin_cat)
                    bin_probs.append(value)
                elif target_name == 'Season peak week':  # date target. start: an EW Monday date
                    monday_date = monday_date_from_ew_and_season_start_year(bin_start_incl, season_start_year)
                    bin_cats.append(monday_date.strftime(YYYY_MM_DD_DATE_FORMAT))
                    bin_probs.append(value)
                elif target_name in ['Season peak percentage', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead']:
                    bin_cats.append(bin_start_incl)
                    bin_probs.append(value)
                else:
                    raise RuntimeError(f"invalid target_name: {target_name!r}")
            except ValueError as ve:
                row = [location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value]
                raise RuntimeError(f"could not coerce either bin_start_incl or value to float. bin_start_incl="
                                   f"{bin_start_incl}, value={value}, row={row}, error={ve}")

        # add the actual prediction dicts
        if point_values:
            if len(point_values) > 1:
                raise RuntimeError(f"len(point_values) > 1: {point_values}")

            point_value = point_values[0]
            prediction_dicts.append({"location": location_name,
                                     "target": target_name,
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction],
                                     'prediction': {
                                         'value': point_value}})
        if bin_cats:
            prediction_dicts.append({"location": location_name,
                                     "target": target_name,
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution],
                                     'prediction': {
                                         "cat": bin_cats,
                                         "prob": bin_probs}})
    return prediction_dicts


#
# *.cdc.csv file variables
#

CDC_POINT_NA_VALUE = 'NA'
CDC_POINT_ROW_TYPE = 'Point'
CDC_BIN_ROW_TYPE = 'Bin'
CDC_CSV_HEADER = ['location', 'target', 'type', 'unit', 'bin_start_incl', 'bin_end_notincl', 'value']


#
# ---- test utilities ----
#

def make_cdc_locations_and_targets(project):
    """
    Creates CDC standard Targets for project.
    """
    with open(Path('forecast_app/tests/projects/cdc-project.json')) as fp:
        project_dict = json.load(fp)
    validate_and_create_locations(project, project_dict)
    validate_and_create_targets(project, project_dict)


#
# ---- CDC EW utilities ----
#


#
# The following defines the CDC's file naming standard, e.g., 'EW<ew_week>-<season_start_year>-<model_name>.csv' . For example:
#
# 'EW01-2011-CU_EAKFC_SEIRS.csv'
# 'EW07-2018-ReichLab_kde.csv'
# 'EW53-2014-Delphi_BasisRegression.csv'
#

CDC_CSV_FILENAME_RE_PAT = re.compile(r"""
^
EW
(\d{2})            # ew_week
-                  # dash
(\d{4})            # season_start_year
-                  # dash
([a-zA-Z0-9_]+)    # model_name
\.csv$             # extension
""", re.VERBOSE)


def ew_and_year_from_cdc_file_name(filename):
    """
    Parses and returns the EW week and EW year from filename.

    :param filename: a CDC EW filename as documented in CDC_CSV_FILENAME_RE_PAT
    :return: 2-tuple: (ew_week, season_start_year). returns None if does not match the pattern
    """
    match = CDC_CSV_FILENAME_RE_PAT.match(filename)
    if not match:
        return None

    groups = match.groups()
    return int(groups[0]), int(int(groups[1]))


def season_start_year_from_ew_and_year(ew_week, ew_year):
    """
    :param ew_week: as returned by ew_and_year_from_cdc_file_name(). e.g., 1, 30, 52
    :param ew_year: "". e.g., 2019
    :return: a year naming the start of the season that the two args represent, based on SEASON_START_EW_NUMBER.
        for example, (29, 2010) -> 2009 . (30, 2010) -> 2010
    """
    datetime_for_mmwr_week = pymmwr.mmwr_week_to_date(ew_year, ew_week)  # a Sunday
    return datetime_for_mmwr_week.year - 1 if ew_week < SEASON_START_EW_NUMBER else datetime_for_mmwr_week.year


def monday_date_from_ew_and_season_start_year(ew_week, season_start_year):
    """
    :param ew_week: an epi week from within a cdc csv forecast file. e.g., 1, 30, 52
    :param season_start_year: as returned by season_start_year_from_ew_and_year(). e.g., 2010, which implies the season
        "2010/2011" (that is, EW30 through EW52 of 2010 continuing to EW01 through EW29 of 2011)
    :return: a datetime.date that is the Monday of the EW corresponding to the args
    """
    if ew_week < SEASON_START_EW_NUMBER:
        sunday_date = pymmwr.mmwr_week_to_date(season_start_year + 1, ew_week)
    else:
        sunday_date = pymmwr.mmwr_week_to_date(season_start_year, ew_week)
    return sunday_date + + datetime.timedelta(days=1)
