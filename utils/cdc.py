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


# these are project-specific: Impetus ('1_biweek_ahead', ...) and CDC ensemble (all other targets)
# BINLWR_TARGET_NAMES = ['Season peak percentage', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead',
#                        '1_biweek_ahead', '2_biweek_ahead', '3_biweek_ahead', '4_biweek_ahead', '5_biweek_ahead']
# BINCAT_TARGET_NAMES = ['Season onset', 'Season peak week']


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

        # use parse_value() to handle non-numeric cases like 'NA' and 'none'
        bin_start_incl = parse_value(bin_start_incl)
        bin_end_notincl = parse_value(bin_end_notincl)
        value = parse_value(value)
        rows.append([location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value])

    return rows


def _prediction_dicts_for_csv_rows(season_start_year, rows):
    """
    json_io_dict_from_cdc_csv_file() helper that returns a list of prediction dicts for the 'predictions' section of the
    exported json. Each dict corresponds to either a PointPrediction or BinDistribution depending on each row in rows.
    Translates 'Season onset' targets by splitting them into two Reichlab non-CDC ones: 'season_onset_binary' and
    'season_onset_date'. Also, uses season_start_year to convert EWs to YYYYMMDD_DATE_FORMAT dates.

    Recall the eight cdc-project.json targets and their types:
    -------------------------+-------------------------------+-----------+-----------+---------------------
    Target name              | target_type                   | unit      | data_type | step_ahead_increment
    -------------------------+-------------------------------+-----------+-----------+---------------------
    "season_onset_binary"    | Target.BINARY                 | n/a       | boolean   | n/a    } new targets split from
    "season_onset_date"      | Target.DATE_TARGET_TYPE       | "week"    | date      | n/a    } original 'Season onset'
    "Season peak week"       | Target.DATE_TARGET_TYPE       | "week"    | date      | n/a
    "Season peak percentage" | Target.CONTINUOUS_TARGET_TYPE | "percent" | float     | n/a
    "1 wk ahead"             | Target.CONTINUOUS_TARGET_TYPE | "percent" | float     | 1
    "2 wk ahead"             | ""                            | ""        | ""        | 2
    "3 wk ahead"             | ""                            | ""        | ""        | 3
    "4 wk ahead"             | ""                            | ""        | ""        | 4
    -------------------------+-------------------------------+-----------+-----------+---------------------

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
        point_targets_and_values = []  # point prediction 2-tuples: (point_target_name, point_value)
        # similarly, should only be one row per location/target pair, but "":
        season_onset_binary_probs = []  # 'Season onset binary' True bin probabilities
        season_onset_dates_and_probs = []  # 'Season onset date' 2-tuples: (monday_date, probability)
        season_peak_week_dates_and_probs = []  # 'Season peak week' 2-tuples: (monday_date, probability)
        bin_cats_and_probs = []  # 2-tuples for the remaining non-date targets: (cat, probability)
        for _, _, _, bin_start_incl, bin_end_notincl, value in bin_start_end_val_grouper:
            try:
                if is_point_row:
                    is_date_target = target_name in ['Season onset', 'Season peak week']
                    is_season_onset = target_name == 'Season onset'
                    if is_date_target:
                        monday_date = monday_date_from_ew_and_season_start_year(value, season_start_year)
                        value = monday_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                    point_targets_and_values.append(('Season onset date' if is_season_onset else target_name, value))
                elif (target_name == 'Season onset') and (bin_start_incl is None) and (bin_end_notincl is None):  # date
                    # convert into 'Season onset binary' BinDistribution prediction dict
                    season_onset_binary_probs.append(1 - value)  # 1 - p_none
                elif (target_name == 'Season onset') and ((bin_start_incl is None) or (bin_end_notincl is None)):
                    raise RuntimeError(f"got 'Season onset' row but not both start and end were None. "
                                       f"bin_start_incl={bin_start_incl}, bin_end_notincl={bin_end_notincl}")
                elif target_name == 'Season onset':  # date
                    # convert into 'Season onset date' BinDistribution prediction dict. NB: these values still need to
                    # be scaled: value/(1 - p_none) . however, we aren't guaranteed to have p_none until after we've
                    # processed the special 'p_none' row (see test above that appends to season_onset_binary_probs). so
                    # we defer scaling to below after all bin rows have been processed
                    monday_date = monday_date_from_ew_and_season_start_year(bin_start_incl, season_start_year)
                    season_onset_dates_and_probs.append((monday_date.strftime(YYYY_MM_DD_DATE_FORMAT), value))
                elif target_name == 'Season peak week':  # date
                    monday_date = monday_date_from_ew_and_season_start_year(bin_start_incl, season_start_year)
                    season_peak_week_dates_and_probs.append((monday_date.strftime(YYYY_MM_DD_DATE_FORMAT), value))
                elif target_name in ['Season peak percentage', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead']:
                    bin_cats_and_probs.append((bin_start_incl, value))
                else:
                    raise RuntimeError(f"invalid target_name: {target_name!r}")
            except ValueError as ve:
                row = [location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value]
                raise RuntimeError(f"could not coerce either bin_start_incl or value to float. bin_start_incl="
                                   f"{bin_start_incl}, value={value}, row={row}, error={ve}")

        # add the actual prediction dicts
        if point_targets_and_values:
            if len(point_targets_and_values) > 1:
                raise RuntimeError(f"len(point_targets_and_values) > 1: {point_targets_and_values}")

            point_target_name, point_value = point_targets_and_values[0]
            prediction_dicts.append({"location": location_name,
                                     "target": point_target_name,
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction],
                                     'prediction': {
                                         'value': point_value}})
        if season_onset_binary_probs:
            if len(season_onset_binary_probs) > 1:
                raise RuntimeError(f"len(season_onset_binary_probs) > 1: {season_onset_binary_probs}")

            prob = season_onset_binary_probs[0]
            prediction_dicts.append({"location": location_name,
                                     "target": 'Season onset binary',
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution],
                                     'prediction': {
                                         'cat': [True],
                                         'prob': [prob]}})
        if season_onset_dates_and_probs:
            # recall from above that we need to scale probs now that we have p_none
            one_minus_p_none = season_onset_binary_probs[0]
            prediction_dicts.append({"location": location_name,
                                     "target": 'Season onset date',
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution],
                                     'prediction': {
                                         'cat': [_[0] for _ in season_onset_dates_and_probs],  # dates
                                         'prob': [_[1] / one_minus_p_none for _ in season_onset_dates_and_probs]}})
        if season_peak_week_dates_and_probs:
            prediction_dicts.append({"location": location_name,
                                     "target": 'Season peak week',
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution],
                                     'prediction': {
                                         'cat': [_[0] for _ in season_peak_week_dates_and_probs],  # dates
                                         'prob': [_[1] for _ in season_peak_week_dates_and_probs]}})
        if bin_cats_and_probs:
            prediction_dicts.append({"location": location_name,
                                     "target": target_name,
                                     'class': PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[BinDistribution],
                                     'prediction': {
                                         'cat': [_[0] for _ in bin_cats_and_probs],
                                         'prob': [_[1] for _ in bin_cats_and_probs]}})
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
