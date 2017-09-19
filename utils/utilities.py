import datetime
import json
import re
from ast import literal_eval

import requests


def basic_str(obj):
    return obj.__class__.__name__ + ': ' + obj.__repr__()


def parse_value(value):
    """
    Parses a value numerically as smartly as possible, in order: float, int, None. o/w is an error
    """
    # https://stackoverflow.com/questions/34425583/how-to-check-if-string-is-int-or-float-in-python-2-7
    try:
        return literal_eval(value)
    except ValueError:
        return None


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

# server memory cache that maps a 2-tuple (epi_year, epi_week) to to the retrieved 'wili' value
DELPHI_EPIWEEKS_WILI_CACHE = {}

# todo xx make a Project method. must replace mock_wili_for_epi_week_fcn() with a patch
def delphi_wili_for_epi_week(forecast_model, year, week, location_name):
    """
    Looks up the 'wili' value for the past args, using the delphi REST API. Returns as a float. Caches the retrieved
    value for speed-ups.

    NB: caching means that the values in server memory won't be updated if they change on delphi.midas.cs.cmu.edu ,
    i.e., they could be come stale and need flushing.

    :param forecast_model: the ForecastModel instance making the call
    :param year:
    :param week: EW week number between 1 and 52 inclusive
    :param location_name:
    :return: actual value for the passed args, looked up dynamically via xhttps://github.com/cmu-delphi/delphi-epidata
    """
    region = forecast_model.project.region_for_location_name(location_name)
    if not region:
        raise RuntimeError("location_name is not a valid Delphi location: {}".format(location_name))

    if (year, week) in DELPHI_EPIWEEKS_WILI_CACHE:
        return DELPHI_EPIWEEKS_WILI_CACHE[(year, week)]

    url = 'https://delphi.midas.cs.cmu.edu/epidata/api.php' \
          '?source=fluview' \
          '&regions={region}' \
          '&epiweeks={epi_year}{ew_week_number:02d}'. \
        format(region=region, epi_year=year, ew_week_number=week)
    response = requests.get(url)
    response.raise_for_status()  # does nothing if == requests.codes.ok
    delphi_dict = json.loads(response.text)
    wili_val = delphi_dict['epidata'][0]['wili']  # will raise KeyError if response json not structured as expected
    DELPHI_EPIWEEKS_WILI_CACHE[(year, week)] = wili_val
    return wili_val


#
# ---- view-related functions ----
#

def mean_abs_error_rows_for_project(project):
    """
    Called by the project_visualizations() view function, returns a table in the form of a list of rows where each row
    corresponds to a model, and each column corresponds to a target, i.e., X=target vs. Y=Model. The format:
    
        [[model_name1, target1_mae, target2_mae, ...], ...]

    The first row is the header.

    Recall the Mean Absolute Error table from http://reichlab.io/flusight/ , such as for these settings:

        US National > 2016-2017 > 1 wk, 2 wk, 3 wk, 4 wk ->

        +----------+------+------+------+------+
        | Model    | 1 wk | 2 wk | 3 wk | 4 wk |
        +----------+------+------+------+------+
        | kcde     | 0.29 | 0.45 | 0.61 | 0.69 |
        | kde      | 0.58 | 0.59 | 0.6  | 0.6  |
        | sarima   | 0.23 | 0.35 | 0.49 | 0.56 |
        | ensemble | 0.3  | 0.4  | 0.53 | 0.54 |
        +----------+------+------+------+------+

        my app:
        +--------------+------+------+------+------+
        | Model        | 1 wk | 2 wk | 3 wk | 4 wk |
        +--------------+------+------+------+------+
        | KoT KCDE     | 0.29 | 0.45 | 0.61 | 0.69 |
        | KoT KDE      | 0.58 | 0.59 | 0.59 | 0.59 |
        | KoT SARIMA   | 0.23 | 0.35 | 0.49 | 0.56 |
        | KoT ensemble | 0.36 | 0.49 | 0.61 | 0.61 |
        +--------------+------+------+------+------+

    """
    # NB: assumes all of project's models have the same targets - something that should be validated by
    # ForecastModel.load_forecast() or similar

    # todo return indication of best model for each target -> bold in project_visualizations.html
    mae_targets = project.targets_for_mean_absolute_error()
    rows = [['Model', *mae_targets]]  # header
    for forecast_model in project.forecastmodel_set.all():
        row = [forecast_model.name]
        for target in mae_targets:
            # TODO xx pull season_start_year and location from somewhere!
            mean_absolute_error = forecast_model.mean_absolute_error(2016, 'US National', target,
                                                                     wili_for_epi_week_fcn=delphi_wili_for_epi_week)
            row.append("{:0.2f}".format(mean_absolute_error))
        rows.append(row)
    return rows
