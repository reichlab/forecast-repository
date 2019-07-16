import csv
import datetime
import re
import tempfile
from pathlib import Path

from django.db import transaction

from forecast_app.models.forecast import Forecast
from utils.utilities import CDC_CSV_HEADER, CDC_POINT_ROW_TYPE, CDC_BIN_ROW_TYPE, parse_value


BINLWR_TARGETS = ['Season peak percentage', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead']
BINCAT_TARGETS = ['Season onset', 'Season peak week']


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


@transaction.atomic
def load_cdc_csv_forecast_file(forecast_model, csv_file_path_or_fp, time_zero, file_name=None):
    """
    Loads the passed cdc csv file into a new forecast_model Forecast for time_zero. NB: does not check if a Forecast
    already exists for time_zero and file_name. Is atomic so that an invalid forecast's data is not saved.

    :param csv_file_path_or_fp: Path to a CDC CSV forecast file, OR an already-open file-like object
    :param time_zero: the TimeZero this forecast applies to
    :param file_name: optional name to use for the file. if None (default), uses csv_file_path_or_fp. helpful b/c uploaded
        files have random csv_file_path_or_fp file names, so original ones must be extracted and passed separately
    :return returns a new Forecast for it
    :raises RuntimeError if the data could not be loaded
    """
    if time_zero not in forecast_model.project.timezeros.all():
        raise RuntimeError(f"time_zero was not in project. time_zero={time_zero}, "
                           f"project timezeros={forecast_model.project.timezeros.all()}")

    file_name = file_name or csv_file_path_or_fp.name
    new_forecast = Forecast.objects.create(forecast_model=forecast_model, time_zero=time_zero, csv_filename=file_name)
    with tempfile.NamedTemporaryFile(mode='r+') as points_fp, \
            tempfile.NamedTemporaryFile(mode='r+') as binlwr_fp, \
            tempfile.NamedTemporaryFile(mode='r+') as bincat_fp:
        convert_cdc_csv_to_predictions_files(csv_file_path_or_fp, points_fp, binlwr_fp, bincat_fp)
        new_forecast.load_predictions(points_fp)
        new_forecast.load_predictions(binlwr_fp)
        new_forecast.load_predictions(bincat_fp)
    return new_forecast


def convert_cdc_csv_to_predictions_files(cdc_csv_file, out_points_fp, out_binlwr_fp, out_bincat_fp):
    """
    Utility that extracts the three types of predictions found in cdc csv files, saving them into the three passed
    separate csv file paths, one per prediction type: PointPredictions, BinLwrDistributions, BinCatDistribution. Seeks
    each file to its start.

    :param cdc_csv_file: a cdc csv file (string or Path). todo xx pointer to docs
    :param out_points_fp: file to save PointPredictions to
    :param out_binlwr_fp: "" BinLwrDistributions ""
    :param out_bincat_fp: "" BinCatDistribution ""
    """
    with open(Path(cdc_csv_file)) as cdc_csv_file_fp:
        points_file_csv_writer = csv.writer(out_points_fp, delimiter=',')
        binlwr_file_csv_writer = csv.writer(out_binlwr_fp, delimiter=',')
        bincat_file_csv_writer = csv.writer(out_bincat_fp, delimiter=',')

        points_file_csv_writer.writerow(Forecast.POINT_PREDICTION_HEADER)
        binlwr_file_csv_writer.writerow(Forecast.BINLWR_DISTRIBUTION_HEADER)
        bincat_file_csv_writer.writerow(Forecast.BINCAT_DISTRIBUTION_HEADER)

        location_names, target_names, rows = read_cdc_csv_file_rows(cdc_csv_file_fp)
        for row in rows:
            location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value = row
            if is_point_row:
                points_file_csv_writer.writerow([location_name, target_name, value])
            elif target_name in BINLWR_TARGETS:
                binlwr_file_csv_writer.writerow(
                    [location_name, target_name, bin_start_incl, value])  # todo xx lwr == bin_start_incl !?
            elif target_name in BINCAT_TARGETS:
                bincat_file_csv_writer.writerow(
                    [location_name, target_name, bin_start_incl, value])  # todo xx cat == bin_start_incl !?
            else:
                raise RuntimeError("unexpected bin target. target_name={!r}, BINLWR_TARGETS={}, BINCAT_TARGETS={}"
                                   .format(target_name, BINLWR_TARGETS, BINCAT_TARGETS))

        # done
        out_points_fp.seek(0)
        out_binlwr_fp.seek(0)
        out_bincat_fp.seek(0)


#
# read_cdc_csv_file_rows()
#

def read_cdc_csv_file_rows(cdc_csv_file_fp):
    """
    Loads the rows from cdc_csv_file_fp, cleans them, and then returns them as a list. Does some basic validation,
    but does not check locations and targets against the template. This is b/c Locations and Targets might not yet
    exist (if they're dynamically created by this method's callers). Skips bin rows where the value is 0.

    :param cdc_csv_file_fp: the *.cdc.csv data file to load
    :return: a 3-tuple: (location_names, target_names, rows) where the first two are sets and the last is a list of
        rows: location_name, target_name, row_type, bin_start_incl, bin_end_notincl, value
    """
    csv_reader = csv.reader(cdc_csv_file_fp, delimiter=',')

    # validate header. must be 7 columns (or 8 with the last one being '') matching
    try:
        orig_header = next(csv_reader)
    except StopIteration:  # a kind of Exception, so much come first
        raise RuntimeError("Empty file.")
    except Exception as exc:
        raise RuntimeError("Error reading from cdc_csv_file_fp={}. exc={}".format(cdc_csv_file_fp, exc))

    header = orig_header
    if (len(header) == 8) and (header[7] == ''):
        header = header[:7]
    header = [h.lower() for h in [i.replace('"', '') for i in header]]
    if header != CDC_CSV_HEADER:
        raise RuntimeError("Invalid header: {}".format(', '.join(orig_header)))

    # collect the rows. first we load them all into memory (processing and validating them as we go)
    locations = set()
    targets = set()
    rows = []
    for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
        if (len(row) == 8) and (row[7] == ''):
            row = row[:7]

        if len(row) != 7:
            raise RuntimeError("Invalid row (wasn't 7 columns): {!r}".format(row))

        location_name, target_name, row_type, unit, bin_start_incl, bin_end_notincl, value = row  # unit ignored

        # validate row_type
        row_type = row_type.lower()
        if (row_type != CDC_POINT_ROW_TYPE) and (row_type != CDC_BIN_ROW_TYPE):
            raise RuntimeError("row_type was neither '{}' nor '{}': "
                               .format(CDC_POINT_ROW_TYPE, CDC_BIN_ROW_TYPE))
        is_point_row = (row_type == CDC_POINT_ROW_TYPE)

        locations.add(location_name)
        targets.add(target_name)

        # use parse_value() to handle non-numeric cases like 'NA' and 'none'
        bin_start_incl = parse_value(bin_start_incl)
        bin_end_notincl = parse_value(bin_end_notincl)
        value = parse_value(value)

        # skip bin rows with a value of zero - a storage (and thus performance) optimization that does not affect
        # score calculation, etc. see [Consider not storing bin rows with zero values #84](https://github.com/reichlab/forecast-repository/issues/84)
        # Note however from that issue:
        #   Point 3 means Zoltar's export features (CSV and JSON formats) will not include those skipped rows. Thus,
        #   the exported CSV files will not be identical to the imported ones. This represents the first change in
        #   Zoltar in which data is lost.
        if (row_type == CDC_BIN_ROW_TYPE) and (value == 0):
            continue

        rows.append([location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value])

    return locations, targets, rows
