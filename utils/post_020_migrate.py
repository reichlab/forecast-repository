# /Users/cornell/IdeaProjects/forecast-repository/utils/post_020_migrate.py

import logging

import click
import django
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models.target import reference_date_type_for_id
from forecast_app.models import Project, Target


logger = logging.getLogger(__name__)

#
# PROJ_NAME_TO_REGEX_LIST
#

# maps project_name -> list of 5-tuples:
#   [target_name_regex, is_step_ahead, num_targets, reference_date_type, outcome_variable]  # example_target_name
PROJ_NAME_TO_REGEX_LIST = {
    'Election Forecasts': [
        ["popvote_win_dem", False, 1, None, "popular vote win Democrats"],
        ["ec_win_dem", False, 1, None, "Electoral College win Democrats"],
        ["voteshare_dem_twoparty", False, 1, None, "share of two-party (D/R) popular vote for Democrats"],
        ["senate_seats_won_dem", False, 1, None, "number of Senate seats won by Democrats"],
        ["senate_win_dem", False, 1, None, "Senate win (control) for Democrats"],
        ["ev_won_dem", False, 1, None, "number of electoral votes won by Democrats"],
    ],
    'CDC Real-time Forecasts': [
        ["Season onset", False, 1, None, "season onset"],
        ["Season peak percentage", False, 1, None, "season peak percentage"],
        ["Season peak week", False, 1, None, "season peak week"],
        ["wk ahead", True, 4, Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT, "ILI percent"],  # "1 wk ahead"
    ],
    'CDC Retrospective Forecasts': [
        ["Season onset", False, 1, None, "season onset"],
        ["Season peak percentage", False, 1, None, "season peak percentage"],
        ["Season peak week", False, 1, None, "season peak week"],
        ["wk ahead", True, 4, Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT, "ILI percent"],  # "1 wk ahead"
    ],
    'COVID-19 Forecasts': [
        ["day ahead cum death", True, 131, Target.DAY_RDT, "cumulative deaths"],  # "0 day ahead cum death"
        ["day ahead inc death", True, 131, Target.DAY_RDT, "incident deaths"],  # "0 day ahead inc death"
        ["day ahead inc hosp", True, 131, Target.DAY_RDT, "incident hospitalizations"],  # "0 day ahead inc hosp"
        ["wk ahead cum death", True, 20, Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT, "cumulative deaths"],
        # "1 wk ahead cum death"
        ["wk ahead inc case", True, 8, Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT, "incident cases"],
        # "1 wk ahead inc case"
        ["wk ahead inc death", True, 20, Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT, "incident deaths"],
        # "1 wk ahead inc death"
    ],
    'Docs Example Project': [
        ["above baseline", False, 1, None, "above baseline"],
        ["cases next week", True, 1, Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT, "cases"],  # "cases next week"
        ["pct next week", True, 1, Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT, "percentage positive tests"],
        # "pct next week"
        ["Season peak week", False, 1, None, "season peak week"],
        ["season severity", False, 1, None, "season severity"],
    ],
    'ECDC European COVID-19 Forecast Hub': [
        ["wk ahead inc case", True, 20, Target.MMWR_WEEK_LAST_TIMEZERO_TUESDAY_RDT, "incident cases"],
        # "1 wk ahead inc case"
        ["wk ahead inc death", True, 20, Target.MMWR_WEEK_LAST_TIMEZERO_TUESDAY_RDT, "incident deaths"],
        # "1 wk ahead inc death"
        ["wk ahead inc hosp", True, 20, Target.MMWR_WEEK_LAST_TIMEZERO_TUESDAY_RDT, "incident hospitalizations"],
        # "1 wk ahead inc hosp"
    ],
    'Impetus Province Forecasts': [
        ["biweek_ahead", True, 5, Target.BIWEEK_RDT, "cases"],  # "1_biweek_ahead"
    ],
    'CDC Influenza Hospitalization Forecasts 2021/2022': [
        ["wk ahead inc flu hosp", True, 4, Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT, "incident hospitalizations"],
        # "1 wk ahead inc flu hosp"
    ],
}


#
# app
#

@click.command()
def post_020_migrate_app():
    for proj_name, regex_tuples in PROJ_NAME_TO_REGEX_LIST.items():
        project = get_object_or_404(Project, name=proj_name)
        print(f"{project}")
        for target in project.targets.all().order_by('name'):
            ref_date_type_outcome_var = _ref_date_type_outcome_var_for_target(target, regex_tuples)
            if not ref_date_type_outcome_var:
                raise RuntimeError(f"no regex_for_target. target={target}, regex_tuples={regex_tuples}")

            new_ref_date_type_id, new_outcome_var = ref_date_type_outcome_var
            print(f"  {target}, {ref_date_type_outcome_var}")
            target.outcome_variable = new_outcome_var
            if target.is_step_ahead:
                print(f"    v")
                new_rdt = reference_date_type_for_id(new_ref_date_type_id)
                old_rdt = reference_date_type_for_id(target.reference_date_type)
                old_outcome_var = target.outcome_variable
                print(f"  {target.name!r}: {old_outcome_var!r}, {old_rdt.name!r} <- "
                      f"{new_outcome_var!r}, {new_rdt.name!r}")
                target.reference_date_type = new_ref_date_type_id
            else:
                print(f"    x")
                target.reference_date_type = None
            target.save()


def _ref_date_type_outcome_var_for_target(target, regex_tuples):
    """
    :param target: a Target
    :param regex_tuples: list of 5-tuples as found in PROJ_NAME_TO_REGEX_LIST
    :return: a 2-tuple for the matching target.name: (reference_date_type, outcome_variable). return None if no matches
    """
    for target_name_regex, is_step_ahead, num_targets, reference_date_type, outcome_variable in regex_tuples:
        if target_name_regex in target.name:
            return reference_date_type, outcome_variable

    return None


if __name__ == '__main__':
    post_020_migrate_app()
