import datetime

import re


CDC_CONFIG_DICT = {
    "visualization-targets": ["1 wk ahead", "2 wk ahead", "3 wk ahead", "4 wk ahead"],
    "visualization-y-label": "Weighted ILI (%)"
}


def epi_week_filename_components_2016_2017_flu_contest(filename):
    """
    :param filename: something like 'EW1-KoTstable-2017-01-17.csv'
    :return: either None (if filename invalid) or a 3-tuple (if valid) that indicates if filename matches the CDC
        standard format as defined in [1]. The tuple format is: (ew_week_number, team_name, submission_datetime) .
        Note that "ew_week_number" is AKA the forecast's "time zero".

    [1] https://webcache.googleusercontent.com/search?q=cache:KQEkQw99egAJ:https://predict.phiresearchlab.org/api/v1/attachments/flusight/flu_challenge_2016-17_update.docx+&cd=1&hl=en&ct=clnk&gl=us
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
