import itertools
import json
import logging
from pathlib import Path

import click
import dateutil
import django
from django.db import transaction


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.project_truth import load_truth_data
from utils.print_project_info import print_project_info
from utils.forecast import cache_forecast_metadata, load_predictions_from_json_io_dict
from utils.project import delete_project_iteratively, create_project_from_json
from forecast_app.models import Project, ForecastModel, Forecast
from utils.utilities import YYYY_MM_DD_DATE_FORMAT, get_or_create_super_po_mo_users


logger = logging.getLogger(__name__)


#
# app
#

@click.command()
def make_covid_viz_test_project_app():
    _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
    _make_covid_viz_test_project(po_user)
    # project, models, forecasts = _make_covid_viz_test_project(po_user)
    # print(project, models, len(forecasts))
    # print_project_info(project, 4)


#
# _make_covid_viz_test_project()
#

@transaction.atomic
def _make_covid_viz_test_project(user):
    """
    Creates a project based on docs-project.json with forecasts from docs-predictions.json.

    :return: 3-tuple: (project, models, forecasts)
    """
    # project name is same as that in the project config json file:
    found_project = Project.objects.filter(name='COVID-19 Forecasts Viz Test').first()
    if found_project:
        logger.warning("* deleting previous project: {}".format(found_project))
        delete_project_iteratively(found_project)

    test_viz_proj_path = Path(__file__).parents[1] / 'forecast_app/tests/projects/covid-viz-test-project/'
    project = create_project_from_json(test_viz_proj_path / 'covid-viz-test-project-config.json', user)  # atomic

    # create models
    ensemble_model = ForecastModel.objects.create(project=project, name="ensemble", abbreviation="COVIDhub-ensemble")
    baseline_model = ForecastModel.objects.create(project=project, name="baseline", abbreviation="COVIDhub-baseline")
    models = [ensemble_model, baseline_model]

    # load forecast files. e.g., '2022-01-03-COVIDhub-ensemble.csv.json' or '2022-01-03-COVIDhub-baseline.csv.json'
    logger.info("loading forecast files")
    forecasts = []  # filled below
    # tz_datetimes = [datetime.date(2022, 1, 3), datetime.date(2022, 1, 10), datetime.date(2022, 1, 17), datetime.date(2022, 1, 24), datetime.date(2022, 1, 31)]
    tz_datetimes = [dateutil.parser.parse(date) for date in
                    ["2022-01-03", "2022-01-10", "2022-01-17", "2022-01-24", "2022-01-31"]]
    for tz_datetime, model in itertools.product(tz_datetimes, models):
        time_zero = project.timezeros.filter(timezero_date=tz_datetime).first()
        forecast_filename = f"{tz_datetime.strftime(YYYY_MM_DD_DATE_FORMAT)}-COVIDhub-{model.name}.json"
        forecast = Forecast.objects.create(forecast_model=model, source=forecast_filename, time_zero=time_zero,
                                           notes=forecast_filename)
        forecasts.append(forecast)
        with open(test_viz_proj_path / 'forecasts-json-small' / forecast_filename) as ensemble_fp:
            json_io_dict_in = json.load(ensemble_fp)
            logger.info(f"loading {forecast_filename}")
            load_predictions_from_json_io_dict(forecast, json_io_dict_in, is_validate_cats=False)  # atomic
            cache_forecast_metadata(forecast)  # atomic

    # load truth, setting issued_at based on commit date. note that we must process truth from oldest to newest so we
    # don't get the error "editing a version's issued_at cannot reposition it before any existing forecasts"
    logger.info("loading truth files")
    truth_commit_hash_date = [('1d85ba3e9', 'Sun Jan 30 17:28:25 2022 +0000'),
                              ('b7987099e', 'Sun Jan 23 16:58:11 2022 +0000'),
                              ('bb98fe32c', 'Sun Jan 16 17:16:26 2022 +0000'),
                              ('fc252a233', 'Sun Jan 9 16:57:07 2022 +0000'),
                              ('c8dbd265b', 'Sun Jan 2 17:01:26 2022 +0000'),
                              ('9808d47d0', 'Sun Dec 26 17:12:08 2021 +0000'),
                              ('0a507f66c', 'Sun Dec 19 16:53:13 2021 +0000')]  # ordered newer to older!
    for commit_hash, commit_date in reversed(truth_commit_hash_date):
        try:
            csv_file = test_viz_proj_path / 'truths-small' / f'{commit_hash}-zoltar-truth.csv'
            num_rows, forecasts = load_truth_data(project, csv_file, file_name='zoltar-truth.csv')
            for forecast in forecasts:
                forecast.issued_at = dateutil.parser.parse(commit_date)
                forecast.save()
        except RuntimeError as rte:
            logger.error(f"skipping: {rte!r}")

    # done
    logger.info(f"done. project={project}, models={models}, # forecasts={len(forecasts)}")
    return project, models, forecasts


#
# main
#

if __name__ == '__main__':
    make_covid_viz_test_project_app()
