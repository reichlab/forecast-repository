import datetime
import itertools
import logging
from collections import defaultdict

from django.core.cache import cache
from django.db import models
from django.utils.text import get_valid_filename

from forecast_app.models import Target
from forecast_app.models.target import reference_date_type_for_id
from forecast_app.views import ProjectDetailView
from utils.project import group_targets, _group_name_for_target
from utils.project_queries import query_forecasts_for_project, query_truth_for_project, FORECAST_CSV_HEADER
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


logger = logging.getLogger(__name__)


#
# functions that support the https://github.com/reichlab/Covid-19-Hub-Vizualization integration prototype
# - note that we use utils.project.group_targets() and friends to manage visualization-related target grouping
#

#
# viz_targets()
#

def viz_targets(project):
    """
    :return: project's targets that are valid for visualization
    """
    return project.targets \
        .filter(is_step_ahead=True, type__in=(Target.CONTINUOUS_TARGET_TYPE, Target.DISCRETE_TARGET_TYPE)) \
        .filter(models.Q(reference_date_type=Target.DAY_RDT) |
                models.Q(reference_date_type=Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT) |
                models.Q(reference_date_type=Target.MMWR_WEEK_LAST_TIMEZERO_SATURDAY_RDT))  # implemented ones


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
    for _, target_list in group_targets(viz_targets(project)).items():
        if not target_list:
            continue

        first_target = sorted(target_list, key=lambda target: target.name)[0]
        target_variables.append({'value': viz_key_for_target(first_target),
                                 'text': _group_name_for_target(first_target),
                                 'plot_text': _group_name_for_target(first_target)})
    return sorted(target_variables, key=lambda _: _['value'])


def viz_key_for_target(target):
    """
    helper that returns a string suitable for keys in `viz_target_variables()` and `viz_available_reference_dates()`
    """
    return get_valid_filename(_group_name_for_target(target).lower())


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
    for target, reference_date in _viz_target_ref_dates(project):
        reference_dates[viz_key_for_target(target)].append(reference_date)

    # sort by date and convert to yyyy-mm-dd, removing duplicates
    for avail_ref_date_key in reference_dates:
        new_ref_dates = list(set(reference_dates[avail_ref_date_key]))
        new_ref_dates.sort()
        new_ref_dates = [ref_date.strftime(YYYY_MM_DD_DATE_FORMAT) for ref_date in new_ref_dates]
        reference_dates[avail_ref_date_key] = new_ref_dates

    return reference_dates


def _viz_target_ref_dates(project):
    """
    `viz_available_reference_dates()` helper. could be done once for a Project and then cached.

    :return: list of 2-tuples that contain visualization date-related information for relevant targets. only returns
    info for timezeros that have forecasts. tuples: (target, reference_date) - types: (Target, datetime.date)
    """
    targets = viz_targets(project)
    timezeros = [timezero for timezero, num_forecasts in ProjectDetailView.timezeros_num_forecasts(project)
                 if num_forecasts != 0]  # NB: oracle excluded

    target_ref_dates = []  # return value
    for target, timezero in itertools.product(targets, timezeros):  # NB: slow
        rdt = reference_date_type_for_id(target.reference_date_type)
        reference_date, _ = rdt.calc_fcn(target.numeric_horizon, timezero.timezero_date)  # _ = target_end_date
        target_ref_dates.append((target, reference_date))

    return target_ref_dates


#
# viz_model_names()
#

def viz_model_names(project):
    """
    :return a list of model abbreviations being displayed. example: ["COVIDhub-baseline", "COVIDhub-ensemble", ...]
        Does not return them an any particular order.
    """
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
    try:
        reference_date = datetime.datetime.strptime(reference_date, YYYY_MM_DD_DATE_FORMAT).date()
        return _viz_data_forecasts(project, target_key, unit_abbrev, reference_date) if is_forecast \
            else _viz_data_truth(project, target_key, unit_abbrev, reference_date)
    except ValueError as ve:
        logger.error(f"could not parse reference_date={reference_date}. exc={ve!r}")
        return {}


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
    if target_key not in target_key_to_targets:
        logger.error(f"target_key not found in target_key_to_targets: {target_key!r}. "
                     f"keys={list(target_key_to_targets.keys())}")
        return {}

    # compute target_end_dates for Target x TimeZero, stored as dicts for fast lookup of CSV rows
    one_step_ahead_targets = [target for target in target_key_to_targets[target_key] if target.numeric_horizon == 1]
    if len(one_step_ahead_targets) != 1:
        logger.error(f"could not find exactly one one-step-ahead target. target_key={target_key}, "
                     f"one_step_ahead_targets={one_step_ahead_targets}")
        return {}

    one_step_ahead_target = one_step_ahead_targets[0]
    date_y_pairs = set()  # 2-tuples as returned by _viz_truth_for_target_unit_ref_date()
    dates, ys = _viz_truth_for_target_unit_ref_date(project, one_step_ahead_target, unit_abbrev, reference_date)
    if not dates:  # if dates = [] then ys = [] too
        logger.warning(f"_viz_data_truth(): no dates: {target_key!r}, {unit_abbrev!r}, {reference_date!r}: "
                       f"{one_step_ahead_target.name!r}")
        return {}  # no truth data

    date_y_pairs.update(zip(dates, ys))
    if not date_y_pairs:
        return {}

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


def _viz_truth_for_target_unit_ref_date(project, one_step_ahead_target, unit_abbrev, ref_date):
    # todo xx re-alignment of dates hack. accounts for truth reporting delays & upload dates. specific to covid project
    ref_date_adjusted = ref_date + datetime.timedelta(days=2)
    as_of = f"{ref_date_adjusted.strftime(YYYY_MM_DD_DATE_FORMAT)} 12:00 EST"  # todo timezone?
    query = {'targets': [one_step_ahead_target.name], 'units': [unit_abbrev], 'as_of': as_of}
    dates, ys = [], []  # data columns. filled next. former is datetime.date
    # `list` makes this much faster than without!:
    for idx, (timezero, unit, target, value) in enumerate(list(query_truth_for_project(project, query))):
        if idx == 0:
            continue  # skip header

        timezero_date = datetime.datetime.strptime(timezero, YYYY_MM_DD_DATE_FORMAT).date()
        rdt = reference_date_type_for_id(one_step_ahead_target.reference_date_type)
        _, target_end_date = rdt.calc_fcn(one_step_ahead_target.numeric_horizon, timezero_date)  # _ = reference_date
        dates.append(target_end_date.strftime(YYYY_MM_DD_DATE_FORMAT))
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
    if target_key not in target_key_to_targets:
        logger.error(f"target_key not found in target_key_to_targets: {target_key!r}. "
                     f"keys={list(target_key_to_targets.keys())}")
        return {}

    # compute target_end_dates for Target x TimeZero, stored as dicts for fast lookup of CSV rows
    targets = target_key_to_targets[target_key]
    timezeros = project.timezeros.all().order_by('timezero_date')
    ref_date_to_target_tzs = _ref_date_to_target_tzs(targets, timezeros)
    if reference_date not in ref_date_to_target_tzs:
        logger.error(f"ref_date not found in ref_date_to_target_tzs: {reference_date!r}")
        return {}

    # query forecasts
    timezeros = sorted(list(set([timezero for target, timezero in ref_date_to_target_tzs[reference_date]])))
    query = {'models': viz_model_names(project),
             'units': [unit_abbrev],
             'targets': [target.name for target in targets],
             'timezeros': timezeros,
             'types': ['quantile']}  # NB: no point, just quantile
    rows = list(query_forecasts_for_project(project, query))  # `list` makes this much faster than without!
    rows.pop(0)  # header

    if not rows:
        logger.warning(f"query returned no rows")
        return {}

    # build and save viz_dict via a nested groupby() to fill viz_dict. recall query csv output columns:
    #  model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, 2, 3
    target_name_to_obj = {target.name: target for target in targets}
    rows.sort(key=lambda _: (_[0], _[1], _[4]))  # sort for groupby(): model, timezero, target
    viz_dict = defaultdict(lambda: defaultdict(list))  # dict for JSON output. filled next
    for model, tz_target_grouper in itertools.groupby(rows, key=lambda _: _[0]):
        for (timezero, target), quantile_grouper in itertools.groupby(tz_target_grouper, key=lambda _: (_[1], _[4])):
            timezero_date = datetime.datetime.strptime(timezero, YYYY_MM_DD_DATE_FORMAT).date()
            target_obj = target_name_to_obj[target]
            rdt = reference_date_type_for_id(target_obj.reference_date_type)
            _, target_end_date = rdt.calc_fcn(target_obj.numeric_horizon, timezero_date)  # _ = reference_date
            target_end_date = target_end_date.strftime(YYYY_MM_DD_DATE_FORMAT)

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


def _ref_date_to_target_tzs(targets, timezeros):
    ref_date_to_target_tzs = defaultdict(list)  # ref_date -> (target_name, timezero_date): datetime.date -> (str, str)
    for target, timezero in itertools.product(targets, timezeros):  # NB: slow
        rdt = reference_date_type_for_id(target.reference_date_type)
        calc_reference_date, target_end_date = rdt.calc_fcn(target.numeric_horizon, timezero.timezero_date)
        target_timezero_tuple = (target.name, timezero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT))
        ref_date_to_target_tzs[calc_reference_date].append(target_timezero_tuple)
    return ref_date_to_target_tzs


#
# viz options-related functions
#

def validate_project_viz_options(project, viz_options, is_validate_objects=True):
    """
    Validates viz_options, which is a dict suitable for saving in the `Project.viz_options` field. An example
    viz_options:

        {"included_target_vars": ["incident_deaths"],
         "initial_unit": "48",
         "intervals": [0, 50, 95],
         "initial_checked_models": ["COVIDhub-baseline", "COVIDhub-ensemble"],
         "models_at_top": ["COVIDhub-ensemble", "COVIDhub-baseline"],
         "disclaimer": "Most forecasts have failed to reliably predict rapid changes ...",
         "x_axis_range_offset": [52, 6]}

    :param project: a Project. ignored if is_validate_objects is False
    :param viz_options: a dict as documented at https://docs.zoltardata.com/xx <- todo xx . briefly: it has six keys:
        - "disclaimer": arbitrary string that's shown at the top of the viz
        - "initial_checked_models": a list of strs naming model abbreviations to initially check in the viz.
            see viz_model_names()
        - "included_target_vars": a list of valid target groups for `project`. see viz_target_variables()' `value` key.
            The first will be used as the initially-selected variable.
        - "initial_unit": a valid Unit abbreviation for `project`. see viz_units()' `value` key
        - "intervals": a list of one or more ints between 0 and 100 inclusive. these represent percentages
        - "models_at_top": a list of strs naming model abbreviations to sort at the top of the viz model list. see
            viz_model_names()
        - "x_axis_range_offset": controls the predtimechart's initial xaxis range. is either None or a list of two
            positive (>0) ints: [weeks_before_final_reference_date, weeks_after_final_reference_date]
    :param is_validate_objects: boolean indicating whether object-related fields should be validated (Targets, Units,
        and Models)
    :return: a list of error messages if viz_options is invalid, or [] o/w
    """
    if not isinstance(viz_options, dict):
        return [f"viz_options is not a dict. viz_options={viz_options}, type={type(viz_options)}"]

    expected_keys = {'included_target_vars', 'initial_unit', 'intervals', 'initial_checked_models', 'models_at_top',
                     'disclaimer', 'x_axis_range_offset'}
    actual_keys = set(viz_options.keys())
    if actual_keys != expected_keys:
        return [f"viz_options keys are invalid. expected_keys={expected_keys}, actual_keys={actual_keys}, "
                f"difference={actual_keys ^ expected_keys}"]

    # validate field types
    errors = []
    field_name_to_type = {'included_target_vars': list, 'initial_unit': str, 'intervals': list,
                          'initial_checked_models': list, 'models_at_top': list, 'disclaimer': str}
    for field_name, field_type in field_name_to_type.items():
        if not isinstance(viz_options[field_name], field_type):
            errors.append(f"top level field type was not {field_type}. field_name={field_name!r}, "
                          f"value={viz_options[field_name]!r}, type={type(viz_options[field_name])}")

    # validate 'x_axis_range_offset' field: either None or a list
    x_axis_range_offset = viz_options['x_axis_range_offset']
    if (x_axis_range_offset is not None) and (not isinstance(x_axis_range_offset, list)):
        errors.append(f"'top level field type was not not None or list. value={x_axis_range_offset!r}, "
                      f"type={type(x_axis_range_offset)!r}")

    if errors:
        return errors

    # validate individual field values
    # 'included_target_vars'
    if is_validate_objects:
        target_var_vals = {target_var['value'] for target_var in viz_target_variables(project)}
        included_target_vars = set(viz_options['included_target_vars'])
        if (not included_target_vars) or (not included_target_vars <= target_var_vals):  # empty or not subset
            errors.append(f"included_target_vars is invalid (not a subset of target_var_vals). "
                          f"included_target_vars={included_target_vars !r}, "
                          f"target_var_vals={target_var_vals!r}")

    # 'initial_unit'
    if is_validate_objects:
        unit_vals = {unit['value'] for unit in viz_units(project)}
        if viz_options['initial_unit'] not in unit_vals:
            errors.append(f"initial_unit is invalid. viz_opt_unit_vals={viz_options['initial_unit']!r}, "
                          f"unit_vals={unit_vals!r}")

    # 'intervals'
    intervals = viz_options['intervals']
    if not isinstance(intervals, list) \
            or not intervals \
            or not all(map(lambda _: isinstance(_, int) and 0 <= _ <= 100, intervals)):
        errors.append(f"intervals is invalid. must be between 0 and 100 inclusive: {intervals !r}")

    # 'initial_checked_models'
    if is_validate_objects:
        model_names = set(viz_model_names(project))
        viz_opt_checked_models = set(viz_options['initial_checked_models'])
        if is_validate_objects and (not viz_opt_checked_models
                                    or not all(map(lambda _: isinstance(_, str), viz_opt_checked_models))
                                    or not viz_opt_checked_models <= model_names):
            errors.append(f"initial_checked_models is invalid. viz_opt_checked_models={viz_opt_checked_models!r}, "
                          f"model_names={model_names!r}")

    # 'models_at_top'
    viz_opt_models_at_top = set(viz_options['models_at_top'])
    if is_validate_objects and (not viz_opt_models_at_top
                                or not all(map(lambda _: isinstance(_, str), viz_opt_models_at_top))
                                or not viz_opt_models_at_top <= model_names):
        errors.append(f"models_at_top is invalid. viz_opt_models_at_top={viz_opt_models_at_top!r}, "
                      f"model_names={model_names!r}")

    # 'disclaimer'
    if not isinstance(viz_options['disclaimer'], str):
        errors.append(f"disclaimer is invalid (not a str): {type(viz_options['disclaimer'])}")

    # 'x_axis_range_offset'
    if (x_axis_range_offset is not None) and ((len(x_axis_range_offset) != 2)
                                              or (not isinstance(x_axis_range_offset[0], int))
                                              or (not isinstance(x_axis_range_offset[1], int))
                                              or (x_axis_range_offset[0] < 1)
                                              or (x_axis_range_offset[1] < 1)):
        errors.append(f"x_axis_range_offset is invalid (not a list of two ints > 0): {x_axis_range_offset}")

    # done
    return errors


def viz_initial_xaxis_range_from_range_offset(x_axis_range_offset, reference_date):
    """
    :param x_axis_range_offset: as documented in `validate_project_viz_options()` above: either None or a list of two
        positive (>0) ints: [weeks_before_final_reference_date, weeks_after_final_reference_date]. we assume it has been
        validated via `validate_project_viz_options()`
    :param reference_date: date str in 'YYYY-MM-DD' format that `x_axis_range_offset` is relative to
    :return: a predtimechart `initial_xaxis_range` value for `x_axis_range_offset` and `reference_date` as documented at
        https://github.com/reichlab/predtimechart/ : either null or an array of two dates in 'YYYY-MM-DD' format that
        specify the initial xaxis range to use
    """
    if not x_axis_range_offset or not reference_date:
        return None

    try:
        weeks_before_ref_date, weeks_after_ref_date = x_axis_range_offset
        reference_date = datetime.datetime.strptime(reference_date, YYYY_MM_DD_DATE_FORMAT).date()
        ref_date_0 = reference_date - datetime.timedelta(weeks=weeks_before_ref_date)
        ref_date_1 = reference_date + datetime.timedelta(weeks=weeks_after_ref_date)
        return [ref_date_0.strftime(YYYY_MM_DD_DATE_FORMAT), ref_date_1.strftime(YYYY_MM_DD_DATE_FORMAT)]
    except ValueError as ve:
        raise RuntimeError(f"could not parse reference_date={reference_date}. exc={ve!r}")


#
# viz_human_ensemble_model()
#

def viz_human_ensemble_model(project, component_models, target_key, reference_date, user_model_name):
    """
    Top-level human judgement ensemble model viz API endpoint that returns a CSV forecast for the passed args.

    :param project: a Project
    :param component_models: list of model names to build the ensemble from. see `viz_model_names()
    :param target_key: which Targets to use. see `viz_key_for_target()`
    :param reference_date: a string in 'YYYY-MM-DD' format as returned by `viz_available_reference_dates()`
    :param user_model_name: a string naming the user model. must not be None or '', and must pass unchanged through
        django.utils.text.get_valid_filename() (i.e., no spaces, commas, tabs, etc.)
    :return: a list of CSV rows (lists) including the header - see `query_forecasts_for_project()`
    """
    logger.debug(f"viz_human_ensemble_model(): {project}, {component_models}, {target_key!r}, {reference_date!r}, "
                 f"{user_model_name!r}")

    # validate args and convert inputs to zoltar data structures
    reference_date = datetime.datetime.strptime(reference_date, YYYY_MM_DD_DATE_FORMAT).date()
    valid_model_names = viz_model_names(project)
    for model in component_models:
        if model not in valid_model_names:
            raise RuntimeError(f"invalid model name: {model!r}. valid model names: {valid_model_names}")

    target_key_to_targets = _target_key_to_targets(project)
    if target_key not in target_key_to_targets:
        raise RuntimeError(f"target_key not found in target_key_to_targets: {target_key!r}. "
                           f"keys={list(target_key_to_targets.keys())}")

    targets = [target for target in target_key_to_targets[target_key]
               if target.numeric_horizon <= 4]  # todo xx hard-coded
    ref_date_to_target_tzs = _ref_date_to_target_tzs(targets, project.timezeros.all().order_by('timezero_date'))
    if reference_date not in ref_date_to_target_tzs:
        raise RuntimeError(f"ref_date not found in ref_date_to_target_tzs: {reference_date!r}. "
                           f"ref_date_to_target_tzs={ref_date_to_target_tzs}")

    if (not user_model_name) or (user_model_name != get_valid_filename(user_model_name)):
        raise RuntimeError(f"invalid user_model_name: {user_model_name!r}")

    # query forecasts. NB: we only query for quantile forecasts, not point or any other types
    query = {'models': component_models,
             'targets': [target.name for target in targets],
             'timezeros': (
                 sorted(list(set([timezero for target, timezero in ref_date_to_target_tzs[reference_date]])))),
             'types': ['quantile']}

    # recall query output columns:
    # - model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, param2, param3.
    # in our case, we don't care about the non-quantile columns (cat, sample, family, param1, param2, param3). nor do we
    # need class b/c it's always 'quantile'
    rows = list(query_forecasts_for_project(project, query))  # `list` makes this much faster than without!
    rows.pop(0)  # header

    # now that we have query rows, build three data structures to help processing. first two are based on using this
    # 3-tuple as a key: (unit, target, quantile). note that we do *not* include timezero in the key tuple because recall
    # that different forecasts could have different timezeros corresponding to the same reference_date. this means that
    # this solution must handle multiple timezeros for the same model/reference_date combination (which it does via
    # `existing_key` below). also note that this approach is memory-intensive

    # unique keys across all models:
    tz_unit_targ_quants = set()  # (unit, target, quantile)

    # maps: {model_name: {(unit, target, quantile): (value, timezero)}} :
    model_to_tzutq_to_val_tz = defaultdict(lambda: defaultdict(tuple))

    # used for output. we assume season is the same for all timezeros:
    tz_to_season = {}

    for model, timezero, season, unit, target, _, value, _, _, _, quantile, _, _, _, _ in rows:
        key = (unit, target, quantile)
        season = '' if season is None else season
        tz_unit_targ_quants.add(key)

        # add the key, but only if the current timezero is > existing, if any
        if model in model_to_tzutq_to_val_tz and key in model_to_tzutq_to_val_tz[model]:
            existing_key = model_to_tzutq_to_val_tz[model][key]  # (value, timezero)
            if datetime.datetime.strptime(timezero, YYYY_MM_DD_DATE_FORMAT).date() > \
                    datetime.datetime.strptime(existing_key[1], YYYY_MM_DD_DATE_FORMAT).date():
                model_to_tzutq_to_val_tz[model][key] = (value, timezero)
        else:  # no existing key
            model_to_tzutq_to_val_tz[model][key] = (value, timezero)

        if timezero not in tz_to_season:
            tz_to_season[timezero] = season

    # output one user forecast row for each key that's present in all models. we use the intermediate `user_rows` var
    # to collect the varying row information, and then use it to generate the final full row with model, season, class,
    # and all the empty non-quantile columns
    user_rows = []  # (timezero, unit, target, quantile, value)
    for key in sorted(list(tz_unit_targ_quants), key=lambda _: str(_)):
        value_tzs = [model_to_tzutq_to_val_tz[model][key]  # [(value, timezero), ...]
                     for model in component_models if key in model_to_tzutq_to_val_tz[model]]
        if len(value_tzs) != len(component_models):
            continue  # skip this row b/c at least one model doesn't have this key

        unit, target, quantile = key
        values = [value for value, timezero in value_tzs]
        value = sum(values) / len(values)  # mean

        # NB: timezeros could be different even though they correspond to the same reference_date. we use the latest
        timezero = sorted([timezero for value, timezero in value_tzs],  # timezeros
                          key=lambda _: datetime.datetime.strptime(_, YYYY_MM_DD_DATE_FORMAT).date())[-1]
        user_rows.append((timezero, unit, target, quantile, value))

    # collect final rows
    user_rows_final = [FORECAST_CSV_HEADER]
    for timezero, unit, target, quantile, value in user_rows:
        # model, timezero, season, unit, target, class, value, cat, prob, sample, quantile, family, param1, param2, param3
        user_rows_final.append([user_model_name, timezero, tz_to_season[timezero], unit, target, 'quantile',
                                value, '', '', '', quantile, '', '', '', ''])

    # done
    return user_rows_final


#
# viz caching utility functions
#

def _viz_cache_keys(project):
    """
    Note: Only works for this 'BACKEND': 'django.core.cache.backends.redis.RedisCache'

    :param project: a Project
    :return: ALL (Redis-based) cache keys for `project`, including available_reference_dates and viz data.
    """
    # example keys:
    #   ":1:viz:avail_ref_dates:44"
    #   ":1:viz:data:44:0|week_ahead_incident_deaths|US|2022-11-12"
    #   ":1:viz:data:44:1|week_ahead_incident_deaths|US|2022-11-12"

    # We use `make_key()` to help get the final Django key prefix - https://docs.djangoproject.com/en/4.1/topics/cache/#cache-key-transformation .
    # This factors in for us the settings KEY_FUNCTION, VERSION, and KEY_PREFIX.
    dj_key_prefix = cache.make_key('')  # the default is ':1:'
    all_keys_query = f"{dj_key_prefix}*{project.pk}*"
    keys = cache._cache.get_client().keys(all_keys_query)  # binary, e.g., b':1:viz:avail_ref_dates:44'

    # convert binary keys to strings and then strip off the Django prefix - decode is OK b/c Django cache uses str keys
    return [key.decode()[len(dj_key_prefix):] for key in keys]


def viz_cache_delete_all(project):
    """
    Deletes ALL cached viz data related to `project`.

    :param project: a Project
    """
    keys = _viz_cache_keys(project)
    if keys:
        cache.delete_many(keys)


#
# viz caching functions: viz_available_reference_dates()
#

VIZ_CACHE_TIMEOUT_AVAIL_REF_DATES = 14_400  # 4 hours (4 hours * 60 min/hr * 60 sec/min)  # todo xx save in env var?


def _viz_cache_key_avail_ref_dates(project):
    """
    NB: Django's cache framework adds prefixes to keys. For example, for project 44 this function returns
    "viz:avail_ref_dates:44" which Django translates to ":1:viz:avail_ref_dates:44" in Redis. See:
    https://docs.djangoproject.com/en/4.1/topics/cache/#cache-key-transformation
      https://docs.djangoproject.com/en/4.1/ref/settings/#std-setting-CACHES-KEY_PREFIX
      https://docs.djangoproject.com/en/4.1/ref/settings/#std-setting-CACHES-VERSION

    :param project: a Project
    :return: `viz_cache_avail_ref_dates()` cache key to use for args
    """
    return f"viz:avail_ref_dates:{project.pk}"  # note our convention of using colons for key "namespace"


def viz_cache_avail_ref_dates(project):
    """
    Implements caching of `viz_available_reference_dates()` using Django's cache framework.

    :param project: the Project to call `viz_available_reference_dates()` on
    :return: `viz_available_reference_dates()` result, either from the cache (if present) or freshly-computed
    """
    viz_cache_key = _viz_cache_key_avail_ref_dates(project)
    available_as_ofs = cache.get(viz_cache_key)
    if available_as_ofs is None:
        available_as_ofs = dict(viz_available_reference_dates(project))  # defaultdict -> dict
        cache.set(viz_cache_key, available_as_ofs, VIZ_CACHE_TIMEOUT_AVAIL_REF_DATES)
        return available_as_ofs
    else:
        return available_as_ofs


def viz_cache_avail_ref_dates_delete(project):
    """
    Deletes the `viz_available_reference_dates()` cache entry for `project`.

    :param project: a Project
    :return True if the key was successfully deleted, False otherwise
    """
    return cache.delete(_viz_cache_key_avail_ref_dates(project))


#
# viz caching functions: viz_data()
#

VIZ_CACHE_TIMEOUT_DATA = 14_400  # 4 hours (4 hours * 60 min/hr * 60 sec/min)  # todo xx save in env var?


def _viz_cache_key_data(project, is_forecast, target_key, unit_abbrev, reference_date):
    """
    :param project: a Project
    :return: `viz_data()` cache key to use for args
    """
    # note our convention of using colons for key "namespace":
    return f"viz:data:{project.pk}:{'1' if is_forecast else '0'}|{target_key}|{unit_abbrev}|{reference_date}"


def viz_cache_data(project, is_forecast, target_key, unit_abbrev, reference_date, force=False):
    """
    Implements caching of `viz_data()` using Django's cache framework.

    :param project: the Project to call `viz_data()` on
    :param is_forecast: as passed to `viz_data()`
    :param target_key: ""
    :param unit_abbrev: ""
    :param reference_date: ""
    :param force: True cause `set()` to be called regardless of whether the passed combination is already cached. False
        skips calling `set()` if cache exists
    :return: `viz_data()` result, either from the cache (if present) or freshly-computed
    """
    viz_cache_key = _viz_cache_key_data(project, is_forecast, target_key, unit_abbrev, reference_date)
    data = cache.get(viz_cache_key)
    if force or (data is None):
        data = viz_data(project, is_forecast, target_key, unit_abbrev, reference_date)
        cache.set(viz_cache_key, dict(data), VIZ_CACHE_TIMEOUT_DATA)  # defaultdict -> dict. o/w can't pickle
        return data
    else:
        return data
