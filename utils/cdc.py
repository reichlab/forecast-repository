import csv
import datetime
import re
from itertools import groupby
from pathlib import Path

import click
from django.db import transaction

from forecast_app.models.forecast import Forecast
from utils.forecast import load_predictions_from_json_io_dict
from utils.utilities import parse_value


#
# load_cdc_csv_forecast_file() and friends
#

# todo these are project-specific: CDC ensemble and Impetus´
BINLWR_TARGET_NAMES = ['Season peak percentage', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead',
                       '1_biweek_ahead', '2_biweek_ahead', '3_biweek_ahead', '4_biweek_ahead', '5_biweek_ahead']
BINCAT_TARGET_NAMES = ['Season onset', 'Season peak week']


def load_cdc_csv_forecasts_from_dir(forecast_model, data_dir, is_load_file=None):
    """
    Adds Forecast objects to forecast_model using the cdc csv files under data_dir. Assumes TimeZeros match those in my
    Project. Skips files that have already been loaded. Skips files that cause load_forecast() to raise a RuntimeError.

    :param forecast_model: a ForecastModel to load the data into
    :param data_dir: Path of the directory that contains cdc csv files
    :param is_load_file: a boolean function of one arg (cdc_csv_file) that returns True if that file should be
        loaded. cdc_csv_file is a Path
    :return list of loaded Forecasts
    """
    forecasts = []
    for cdc_csv_file, timezero_date, _, _ in cdc_csv_components_from_data_dir(data_dir):
        if is_load_file and not is_load_file(cdc_csv_file):
            click.echo("s (!is_load_file)\t{}\t".format(cdc_csv_file.name))
            continue

        timezero_date = forecast_model.project.time_zero_for_timezero_date(timezero_date)
        if not timezero_date:
            click.echo("x (no TimeZero found)\t{}\t".format(cdc_csv_file.name))
            continue

        found_forecast_for_time_zero = forecast_model.forecast_for_time_zero(timezero_date)
        if found_forecast_for_time_zero:
            click.echo("s (found forecast)\t{}\t".format(cdc_csv_file.name))
            continue

        try:
            forecast = load_cdc_csv_forecast_file(forecast_model, cdc_csv_file, timezero_date)
            forecasts.append(forecast)
            click.echo("o\t{}\t".format(cdc_csv_file.name))
        except RuntimeError as rte:
            click.echo("f\t{}\t{}".format(cdc_csv_file.name, rte))
    if not forecasts:
        click.echo("Warning: no valid forecast files found in directory: {}".format(data_dir))
    return forecasts


@transaction.atomic
def load_cdc_csv_forecast_file(forecast_model, cdc_csv_file_path, time_zero):
    """
    Loads the passed cdc csv file into a new forecast_model Forecast for time_zero. NB: does not check if a Forecast
    already exists for time_zero and file_name. Is atomic so that an invalid forecast's data is not saved.

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
        json_io_dict = json_io_dict_from_cdc_csv_file(cdc_csv_file_fp)
        load_predictions_from_json_io_dict(new_forecast, json_io_dict)
    return new_forecast


def json_io_dict_from_cdc_csv_file(cdc_csv_file_fp):
    """
    Utility that extracts the three types of predictions found in CDC CSV files (PointPredictions, BinLwrDistributions,
    and BinCatDistributions), returning them as a "JSON IO dict" suitable for loading into the database (see
    load_predictions_from_json_io_dict()). Note that the returned dict's "meta" section is empty.

    :param cdc_csv_file_fp: an open cdc csv file-like object. the CDC CSV file format is documented at
        https://predict.cdc.gov/api/v1/attachments/flusight/flu_challenge_2016-17_update.docx
    :return a "JSON IO dict" (aka 'json_io_dict' by callers) that contains the three types of predictions. see docs for
        details
    """
    return {'meta': {},
            'predictions': _prediction_dicts_for_csv_rows(_cleaned_rows_from_cdc_csv_file(cdc_csv_file_fp))}


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
        raise RuntimeError("error reading from cdc_csv_file_fp={}. exc={}".format(cdc_csv_file_fp, exc))

    header = orig_header
    if (len(header) == 8) and (header[7] == ''):
        header = header[:7]
    header = [h.lower() for h in [i.replace('"', '') for i in header]]
    if header != CDC_CSV_HEADER:
        raise RuntimeError("invalid header: {}".format(', '.join(orig_header)))

    # collect the rows. first we load them all into memory (processing and validating them as we go)
    rows = []
    for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
        if (len(row) == 8) and (row[7] == ''):
            row = row[:7]

        if len(row) != 7:
            raise RuntimeError("Invalid row (wasn't 7 columns): {!r}".format(row))

        location_name, target_name, row_type, unit, bin_start_incl, bin_end_notincl, value = row  # unit ignored

        # validate row_type
        row_type = row_type.lower()
        if (row_type != CDC_POINT_ROW_TYPE.lower()) and (row_type != CDC_BIN_ROW_TYPE.lower()):
            raise RuntimeError("row_type was neither '{}' nor '{}': "
                               .format(CDC_POINT_ROW_TYPE, CDC_BIN_ROW_TYPE))
        is_point_row = (row_type == CDC_POINT_ROW_TYPE.lower())

        # use parse_value() to handle non-numeric cases like 'NA' and 'none'
        bin_start_incl = parse_value(bin_start_incl)
        bin_end_notincl = parse_value(bin_end_notincl)
        value = parse_value(value)
        rows.append([location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value])

    return rows


def _prediction_dicts_for_csv_rows(rows):
    """
    json_io_dict_from_cdc_csv_file() helper that returns a list of prediction dicts for the 'predictions' section of the
    exported json. Each dict corresponds to either a PointPrediction, BinLwrDistribution, or BinCatDistribution
    depending on each row in rows. See predictions-example.json for an example.

    :param rows: as returned by _cleaned_rows_from_cdc_csv_file():
        location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value
    """
    prediction_dicts = []  # return value
    rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby()
    for (location_name, target_name, is_point_row), bin_start_end_val_grouper in \
            groupby(rows, key=lambda _: (_[0], _[1], _[2])):
        point_values = []  # NB: should only be one point row, but collect all (but don't validate here)
        bincat_cats, bincat_probs = [], []
        binlwr_lwrs, binlwr_probs = [], []
        for _, _, _, bin_start_incl, bin_end_notincl, value in bin_start_end_val_grouper:
            try:
                if is_point_row:
                    # NB: point comes in as a number (see parse_value() below), but should be a string
                    # for Targets whose point_value_type is Target.POINT_TEXT
                    point_value = str(value) if target_name in BINCAT_TARGET_NAMES else value
                    point_values.append(point_value)
                elif target_name in BINCAT_TARGET_NAMES:
                    bincat_cats.append(str(bin_start_incl))
                    bincat_probs.append(float(value))
                elif target_name in BINLWR_TARGET_NAMES:
                    binlwr_lwrs.append(float(bin_start_incl))
                    binlwr_probs.append(float(value))
                else:
                    raise RuntimeError(
                        f"unexpected bin target_name. target_name={target_name!r}, "
                        f"BINLWR_TARGET_NAMES={BINLWR_TARGET_NAMES}, "
                        f"BINCAT_TARGET_NAMES={BINCAT_TARGET_NAMES}")
            except ValueError as ve:
                row = [location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value]
                raise RuntimeError(f"could not coerce either bin_start_incl or value to float. bin_start_incl="
                                   f"{bin_start_incl}, value={value}, row={row}, error={ve}")

        # add the actual prediction dicts
        if bincat_cats:
            prediction_dicts.append({"location": location_name,
                                     "target": target_name,
                                     "class": "BinCat",
                                     "prediction": {
                                         "cat": bincat_cats,
                                         "prob": bincat_probs}})
        if binlwr_lwrs:
            prediction_dicts.append({"location": location_name,
                                     "target": target_name,
                                     "class": "BinLwr",
                                     "prediction": {
                                         "lwr": binlwr_lwrs,
                                         "prob": binlwr_probs}})
        if point_values:
            for point_value in point_values:
                prediction_dicts.append({"location": location_name,
                                         "target": target_name,
                                         'class': 'Point',
                                         'prediction': {
                                             'value': point_value}})
    return prediction_dicts


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

CDC_POINT_NA_VALUE = 'NA'
CDC_POINT_ROW_TYPE = 'Point'
CDC_BIN_ROW_TYPE = 'Bin'
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
    A utility that helps process a directory containing cdc csv files.

    :return a list of 4-tuples for each *.cdc.csv file in cdc_csv_dir, with the last three in the form returned by
        cdc_csv_filename_components(): (cdc_csv_file, timezero_date, model_name, data_version_date). cdc_csv_file is a
        Path. the list is sorted by timezero_date. Returns [] if no
    """
    cdc_csv_components = []
    for cdc_csv_file in cdc_csv_dir.glob('*.' + CDC_CSV_FILENAME_EXTENSION):
        filename_components = cdc_csv_filename_components(cdc_csv_file.name)
        if not filename_components:
            continue

        timezero_date, model_name, data_version_date = filename_components
        cdc_csv_components.append((cdc_csv_file, timezero_date, model_name, data_version_date))
    return sorted(cdc_csv_components, key=lambda _: _[1])


def cdc_csv_filename_components(cdc_csv_filename):
    """
    :param cdc_csv_filename: a *.cdc.csv file name, e.g., '20170419-gam_lag1_tops3-20170516.cdc.csv'
    :return: a 3-tuple of components from cdc_csv_file: (timezero_date, model_name, data_version_date), where dates are
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
    timezero_date = datetime.date(int(groups[0]), int(int(groups[1])), int(int(groups[2])))
    model_name = groups[3]
    data_version_date = datetime.date(int(groups[4]), int(int(groups[5])), int(int(groups[6]))) if groups[4] else None
    return timezero_date, model_name, data_version_date


def first_model_subdirectory(directory):
    """
    :param directory: a Path of a directory that contains one or more model subdirectories, i.e., directories with
        *.cdc.csv files
    :return: the first one of those. returns None if directory contains no model subdirectories.
    """
    for subdir in directory.iterdir():
        if not subdir.is_dir():
            continue

        cdc_csv_components = cdc_csv_components_from_data_dir(subdir)
        if cdc_csv_components:
            return subdir

    return None


def epi_week_filename_components_2016_2017_flu_contest(filename):
    """
    :param filename: something like 'EW1-KoTstable-2017-01-17.csv'
    :return: either None (if filename invalid) or a 3-tuple (if valid) that indicates if filename matches the CDC
        standard format as defined in [1]. The tuple format is: (ew_week_number, team_name, submission_datetime) .
        Note that "ew_week_number" is AKA the forecast's "time zero".

    [1] https://predict.cdc.gov/api/v1/attachments/flusight/flu_challenge_2016-17_update.docx
        From that document:

        For submission, the filename should be modified to the following standard naming convention: a forecast
        submission using week 43 surveillance data submitted by John Doe University on November 7, 2016, should be named
        “EW43-JDU-2016-11-07.csv” where EW43 is the latest week of ILINet data used in the forecast, JDU is the name of
        the team making the submission (e.g. John Doe University), and 2016-11-07 is the date of submission.

    """
    re_split = re.split(r'^EW(\d*)-(\S*)-(\d{4})-(\d{2})-(\d{2})\.csv$', filename)
    if len(re_split) != 7:
        return None

    re_split = re_split[1:-1]  # drop outer two ''
    if any(map(lambda part: len(part) == 0, re_split)):
        return None

    return int(re_split[0]), re_split[1], datetime.date(int(re_split[2]), int(re_split[3]), int(re_split[4]))


def epi_week_filename_components_ensemble(filename):
    """
    Similar to epi_week_filename_components_2016_2017_flu_contest(), but instead parses the format used by the
    https://github.com/FluSightNetwork/cdc-flusight-ensemble project. From README.md:

        Each forecast file must represent a single submission file, as would be submitted to the CDC challenge. Every
        filename should adopt the following standard naming convention: a forecast submission using week 43 surveillance
        data from 2016 submitted by John Doe University using a model called "modelA" should be named
        “EW43-2016-JDU_modelA.csv” where EW43-2016 is the latest week and year of ILINet data used in the forecast, and
        JDU is the abbreviated name of the team making the submission (e.g. John Doe University). Neither the team or
        model names are pre-defined, but they must be consistent for all submissions by the team and match the
        specifications in the metadata file. Neither should include special characters or match the name of another
        team.

    ex:
        'EW01-2011-CUBMA.csv'
        'EW01-2011-CU_EAKFC_SEIRS.csv'

    :return: either None (if filename invalid) or a 3-tuple (if valid) that indicates if filename matches the format
        described above. The tuple format is: (ew_week_number, ew_year, team_name) .
        Note that "ew_week_number" is AKA the forecast's "time zero".
    """
    re_split = re.split(r'^EW(\d{2})-(\d{4})-(\S*)\.csv$', filename)
    if len(re_split) != 5:
        return None

    re_split = re_split[1:-1]  # drop outer two ''
    if any(map(lambda part: len(part) == 0, re_split)):
        return None

    return int(re_split[0]), int(re_split[1]), re_split[2]
