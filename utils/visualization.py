import datetime
import itertools
import logging
from collections import defaultdict

from django.utils.text import get_valid_filename

from forecast_app.models import Target
from forecast_app.models.target import reference_date_type_for_id
from forecast_app.views import ProjectDetailView
from utils.project import group_targets
from utils.project_queries import query_forecasts_for_project, query_truth_for_project
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


logger = logging.getLogger(__name__)


#
# functions that support the https://github.com/reichlab/Covid-19-Hub-Vizualization integration prototype
#

def viz_targets(project):
    """
    :return: project's targets that are valid for visualization
    """
    return project.targets.filter(is_step_ahead=True, reference_date_type=Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT,
                                  type__in=(Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE),
                                  numeric_horizon__lte=4)


#
# viz_target_variables()
#

def viz_target_variables(project):
    """
    Notes: We base key names on the outcome_variable of an arbitrary target in each target group
    - 'value':     get_valid_filename(outcome_variable)
    - 'text':      outcome_variable
    - 'plot_text': outcome_variable

    :return a list of dicts, one per target variable, each of which contains an internal abbreviation mapped to its long
    name. example:
        [{"value": "death", "text": "Deaths", "plot_text": "Incident weekly deaths"},
         {"value": "hosp", "text": "Hospitalizations", "plot_text": "Incident daily hospitalizations"},
         {"value": "case", "text": "Cases", "plot_text": "Incident weekly cases"},
         ...]
    """
    target_variables = []  # return value
    for group_name, target_list in group_targets(viz_targets(project)).items():
        if not target_list:
            continue

        first_target = sorted(target_list, key=lambda target: target.name)[0]
        target_variables.append({'value': viz_key_for_target(first_target),
                                 'text': first_target.outcome_variable,
                                 'plot_text': first_target.outcome_variable})
    return target_variables


def viz_key_for_target(target):
    """
    helper that returns a string suitable for keys in `viz_target_variables()` and `viz_available_reference_dates()`
    """
    return get_valid_filename(target.outcome_variable.lower())


#
# viz_units()
#

def viz_units(project):
    """
    :return a list of locations, represented as a dictionary of ID/text pairs: example:
        [{"value":"US","text":"US"},
         {"value":"01","text":"Alabama"},
         ...]
    """
    units = []  # return value
    for unit in project.units.all():
        units.append({'value': unit.abbreviation, 'text': unit.name})
    return units


#
# viz_available_reference_dates()
#

def viz_available_reference_dates(project):
    """
    Notes:
    - the returned value's keys match the 'value' values of `viz_target_variables()`

    :return a dict that contains a list of dates for which ground truth and forecasts are available, for each target
        variable. Basically, these are the times in the past at which we can view historical forecasts. dates are in
        'YYYY-MM-DD' format. example:
        {"case": ["2020-08-01", "2020-08-08", ...],
         "death":["2020-04-11", "2020-04-18", ...],
         "hosp":["2020-12-05", "2020-12-12", ...],
         ...}
    """
    reference_dates = defaultdict(list)  # returned value

    # build unsorted reference_dates with datetime.dates
    for target, timezero, reference_date, target_end_date in _viz_ref_and_target_end_dates(project):
        reference_dates[viz_key_for_target(target)].append(reference_date)

    # sort by date and convert to yyyy-mm-dd, removing duplicates
    for avail_ref_date_key in reference_dates:
        new_ref_dates = list(set(reference_dates[avail_ref_date_key]))
        new_ref_dates.sort()
        new_ref_dates = [ref_date.strftime(YYYY_MM_DD_DATE_FORMAT) for ref_date in new_ref_dates]
        reference_dates[avail_ref_date_key] = new_ref_dates

    return reference_dates


def _viz_ref_and_target_end_dates(project):
    """
    `viz_available_reference_dates()` helper. could be done once for a Project and then cached.

    :return: list of 4-tuples that contain visualization date-related information for relevant targets. only returns
    info for timezeros that have forecasts. tuples:
        (target, timezero, reference_date, target_end_date)  # first two: objects, latter two: datetime.dates
    """
    ref_and_target_end_dates = []  # return value
    timezeros = [timezero for timezero, num_forecasts in ProjectDetailView.timezeros_num_forecasts(project)
                 if num_forecasts != 0]  # NB: oracle excluded
    for target, timezero in itertools.product(viz_targets(project), timezeros):
        rdt = reference_date_type_for_id(target.reference_date_type)
        reference_date, target_end_date = rdt.calc_fcn(target, timezero)
        ref_and_target_end_dates.append((target, timezero, reference_date, target_end_date))
    return ref_and_target_end_dates


#
# viz_model_names()
#

def viz_model_names(project):
    """
    :return a list of model abbreviations being displayed. example: ["COVIDhub-baseline", "COVIDhub-ensemble", ...]
    """
    # todo xx list special one(s) first
    return [model.abbreviation for model in project.models.all() if not model.is_oracle]


#
# viz_data() API method
#

def viz_data(project, is_forecast, target_key, unit_abbrev, reference_date):
    """
    Top-level viz API endpoint that returns either truth or forecast values for the passed args.

    :param project: a Project
    :param is_forecast: True for forecast data, False for truth data
    :param target_key: which Targets to use. see `viz_key_for_target()`
    :param unit_abbrev: a Unit.abbreviation
    :param reference_date: a string in 'YYYY-MM-DD' format as returned by `viz_available_reference_dates()`
    :return a dict containing the data. format depends on `is_forecast` - see `_viz_data_truth()` and
    `_viz_data_forecasts()` for details
    """
    reference_date = datetime.datetime.strptime(reference_date, YYYY_MM_DD_DATE_FORMAT).date()
    return _viz_data_forecasts(project, target_key, unit_abbrev, reference_date) if is_forecast \
        else _viz_data_truth(project, target_key, unit_abbrev, reference_date)


#
# _viz_data_truth()
#

def _viz_data_truth(project, target_key, unit_abbrev, reference_date):
    """
    args are as passed to viz_data()

    :return a dict with x/y pairs represented as columns, where x=date and y=truth_value. return None if truth not
        found. example:

    {"date": ["2020-03-07", "2020-03-14", ...],
        "y": [0, 15, ...]}
    """
    target_key_to_targets = _target_key_to_targets(project)

    # compute target_end_dates for Target x TimeZero, stored as dicts for fast lookup of CSV rows
    target_tz_to_target_end_date = {}  # (target_name, timezero_date) -> target_end_date: (str, str) -> str
    for target, timezero in itertools.product(viz_targets(project), project.timezeros.all().order_by('timezero_date')):
        rdt = reference_date_type_for_id(target.reference_date_type)
        _, target_end_date = rdt.calc_fcn(target, timezero)
        target_timezero_tuple = (target.name, timezero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT))
        target_tz_to_target_end_date[target_timezero_tuple] = target_end_date.strftime(YYYY_MM_DD_DATE_FORMAT)

    one_step_ahead_targets = [target for target in target_key_to_targets[target_key] if target.numeric_horizon == 1]
    if len(one_step_ahead_targets) != 1:
        logger.error(f"could not find exactly one one-step-ahead target. "
                     f"one_step_ahead_targets={one_step_ahead_targets}")
        return

    one_step_ahead_target = one_step_ahead_targets[0]
    date_y_pairs = set()  # 2-tuples as returned by _viz_truth_for_target_unit_ref_date()
    dates, ys = _viz_truth_for_target_unit_ref_date(project, target_tz_to_target_end_date, one_step_ahead_target,
                                                    unit_abbrev, reference_date)  # datetime.date, *
    if not dates:  # if dates = [] then ys = [] too
        logger.warning(f"  x {target_key!r}, {unit_abbrev!r}, {reference_date!r}: {one_step_ahead_target.name!r}")
        return None  # no truth data

    date_y_pairs.update(zip(dates, ys))
    logger.info(f"  v {target_key!r}, {unit_abbrev!r}, {reference_date!r}: {one_step_ahead_target.name!r}: "
                f"{len(dates), len(ys)}")
    if not date_y_pairs:
        return None

    # save truth data as JSON, sorting first
    json_dates, json_ys = zip(*sorted(date_y_pairs, key=lambda _: _[0]))
    return {'date': json_dates, 'y': json_ys}


def _target_key_to_targets(project):
    """
    :return: dict mapping target_key (see `viz_key_for_target()`) -> list of that key's Targets
    """
    target_key_to_targets = defaultdict(list)
    targets = viz_targets(project)
    for target in sorted(targets, key=lambda _: _.name):
        target_key_to_targets[viz_key_for_target(target)].append(target)
    return target_key_to_targets


def _viz_truth_for_target_unit_ref_date(project, target_tz_to_target_end_date, one_step_ahead_target, unit_abbrev,
                                        ref_date):
    # todo xx re-alignment of dates hack. accounts for truth reporting delays & upload dates. specific to covid project
    ref_date_adjusted = ref_date + datetime.timedelta(days=2)
    as_of = f"{ref_date_adjusted.strftime(YYYY_MM_DD_DATE_FORMAT)} 12:00 EST"  # todo timezone?
    query = {'targets': [one_step_ahead_target.name], 'units': [unit_abbrev], 'as_of': as_of}
    dates, ys = [], []  # data columns. filled next. former is datetime.date
    for idx, (timezero, unit, target, value) in enumerate(query_truth_for_project(project, query)):
        if idx == 0:
            continue  # skip header

        target_end_date = target_tz_to_target_end_date[(target, timezero)]  # YYYY_MM_DD_DATE_FORMAT
        dates.append(target_end_date)
        ys.append(value)
    return dates, ys


#
# _viz_data_forecasts()
#

def _viz_data_forecasts(project, target_key, unit_abbrev, reference_date):
    """
    args are as passed to viz_data()

    :return a dict with one component for each model, each of which is in turn a dict with entries for target end date
    of the forecast and the quantiles required to use to display point predictions and 50% or 95% prediction intervals.
    return None if forecasts not found. example:

    {"UChicagoCHATTOPADHYAY-UnIT": {
      "target_end_date": ["2021-09-11", "2021-09-18"],
      "q0.025":[1150165.71, 1176055.78],
      "q0.25":[1151044.42, 1178626.67],
      "q0.5":[1151438.21, 1179605.9],
      "q0.75":[1152121.55, 1180758.16],
      "q0.975":[1152907.55, 1182505.14]
     },
     "USC-SI_kJalpha": {
       "target_end_date":["2021-09-11", "2021-09-18"],
       "q0.025":[941239.7761, 775112.557],
       "q0.25":[1010616.1863, 896160.705],
       "q0.5":[1149400.162, 1137280.4614],
       "q0.75":[1313447.0159, 1461013.716],
       "q0.975":[1456851.692, 1771312.0932]
     },
     ...}
    """
    target_key_to_targets = _target_key_to_targets(project)

    # compute target_end_dates for Target x TimeZero, stored as dicts for fast lookup of CSV rows
    ref_date_to_target_tzs = defaultdict(list)  # ref_date -> (target_name, timezero_date): datetime.date -> (str, str)
    target_tz_to_target_end_date = {}  # (target_name, timezero_date) -> target_end_date: (str, str) -> str
    for target, timezero in itertools.product(target_key_to_targets[target_key],
                                              project.timezeros.all().order_by('timezero_date')):
        rdt = reference_date_type_for_id(target.reference_date_type)
        calc_reference_date, target_end_date = rdt.calc_fcn(target, timezero)
        target_timezero_tuple = (target.name, timezero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT))
        ref_date_to_target_tzs[calc_reference_date].append(target_timezero_tuple)
        target_tz_to_target_end_date[target_timezero_tuple] = target_end_date.strftime(YYYY_MM_DD_DATE_FORMAT)

    if reference_date not in ref_date_to_target_tzs:
        logger.error(f"ref_date not found in ref_date_to_target_tzs: {reference_date}")
        return None

    # query forecasts
    timezeros = sorted(list(set([timezero for target, timezero in ref_date_to_target_tzs[reference_date]])))
    query = {'models': viz_model_names(project),
             'units': [unit_abbrev],
             'targets': [target.name for target in target_key_to_targets[target_key]],
             'timezeros': timezeros,
             'types': ['quantile']}  # NB: no point, just quantile
    rows = list(query_forecasts_for_project(project, query))  # list for generator
    rows.pop(0)  # header

    if not rows:
        logger.warning(f"query returned no rows")
        return None

    # build and save viz_dict via a nested groupby() to fill viz_dict. recall query csv output columns:
    #  model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
    rows.sort(key=lambda _: (_[0], _[1], _[4]))  # sort for groupby(): model, timezero, target
    viz_dict = defaultdict(lambda: defaultdict(list))  # dict for JSON output. filled next
    for model, tz_target_grouper in itertools.groupby(rows, key=lambda _: _[0]):
        for (timezero, target), quantile_grouper in itertools.groupby(tz_target_grouper, key=lambda _: (_[1], _[4])):
            target_end_date = target_tz_to_target_end_date[(target, timezero)]  # YYYY_MM_DD_DATE_FORMAT

            if target_end_date in viz_dict[model]['target_end_date']:  # todo xx correct? think!
                logger.error(f"target_end_date already in viz_dict: {target_end_date!r}")
                continue

            viz_dict[model]['target_end_date'].append(target_end_date)
            for _, _, _, _, _, _, value, _, _, _, quantile, _, _, _, _ in quantile_grouper:
                quantile_key = f"q{quantile}"  # e.g., 'q0.025'
                if quantile_key not in ["q0.025", "q0.25", "q0.5", "q0.75", "q0.975"]:
                    continue  # viz only wants five quantiles. todo xx generalize?

                viz_dict[model][quantile_key].append(value)

    return viz_dict
