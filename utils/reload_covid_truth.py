import logging
import os

import click
import dateutil
import django
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.project_truth import oracle_model_for_project, load_truth_data
from forecast_app.models import Project


logger = logging.getLogger(__name__)

#
# app
#
HUB_DIR = '../../covid19-forecast-hub'
TRUTH_FILE = f"{HUB_DIR}/data-truth/zoltar-truth.csv"


@click.command()
def reload_covid_truth_app():
    """
    Reloads all COVID-19 zoltar project truth versions using git revisions of zoltar-truth.csv. Required due to the bug
    [loading truth validates Unit.name instead of Unit.abbreviation #333](https://github.com/reichlab/forecast-repository/issues/333).
    We think the missing truth is all batches after 2021-10-18 to now (2022-03-29). Recall from `truth_batches`() that
    batches are identified by the oracle model's `(source, issued_at)` key. Instead of deleting and reloading those
    batches, this app instead reloads them all going back to the first revision available.

    Note: I determined which timezeros were missing by querying for Texas - a convenient state to use:
        SELECT *
        FROM forecast_app_predictionelement AS pe
                 JOIN forecast_app_forecast AS f ON pe.forecast_id = f.id
                 JOIN forecast_app_timezero AS tz ON f.time_zero_id = tz.id
        WHERE pe.unit_id = 379           -- TX
          AND pe.target_id = 1901        -- 1 wk ahead inc death
          AND f.forecast_model_id = 464  -- oracle
          AND tz.timezero_date >= '2021-10-19';

    Experimenting with different timezero_date and unit_id values in the b079 database resulted in these # rows:
        tz date      TX   US
              none:  500  4606
        2021-01-01:  248  3568
        2021-05-01:  146  2881
        2021-10-01:   16  1366
        2021-10-15:    3  1189
        2021-10-18:    1  1163  # row: issued_at: 2021-10-24 16:51:34.738985 , value: {"value": 1267.0}
        2021-10-19:    0  1150
        2021-10-22:    0  1105
        2021-11-01:    0  1000
        2021-11-30:    0   676
        2021-12-31:    0   348
    """
    project = get_object_or_404(Project, name='COVID-19 Forecasts')
    logger.info(f"entered. project={project}")

    # NB: delete *all* forecasts, i.e., every source and issued_at. slow iteration approach so we can order to avoid:
    # "RuntimeError: you cannot delete a forecast that has any newer versions"
    forecasts_qs = oracle_model_for_project(project).forecasts
    logger.info(f"deleting all truth forecasts: count={forecasts_qs.count()}")
    for forecast in forecasts_qs.order_by('-issued_at').iterator():
        logger.info(f"- {forecast.pk}")
        forecast.delete()

    # iterate over all zoltar-truth.csv revisions, loading each as a batch and setting each batch's forecast's issued_at
    # to the revision date. this is basically what the https://github.com/reichlab/covid19-forecast-hub automation does
    # each week. run in chronological order to simulate reality, e.g., so that duplicates are handled, etc.
    logger.info(os.popen(f'ls -al {TRUTH_FILE}').readline().strip())
    stream = os.popen(f'git -C "{HUB_DIR}" log --pretty="%h|%cd" -- data-truth/zoltar-truth.csv')
    for line in reversed(stream.readlines()):
        # ex: 'd57a04562|Thu May 7 01:40:35 2020 -0400' ... '72bbe2073|Sun Mar 27 17:17:48 2022 +0000'
        commit_hash, committer_date = line.strip().split('|')
        logger.info(f"checking out: {commit_hash, committer_date}")
        os.system(f'git -C "{HUB_DIR}" checkout {commit_hash} -- data-truth/zoltar-truth.csv')
        logger.info(os.popen(f'ls -al {TRUTH_FILE}').readline().strip())
        logger.info(f"loading truth")
        try:
            num_rows, forecasts = load_truth_data(project, TRUTH_FILE, file_name='zoltar-truth.csv')
        except RuntimeError as rte:
            logger.error(f"error loading truth: {rte!r}")

        logger.info(f"setting forecasts' issued_at: {num_rows}, {len(forecasts)}, {committer_date!r}")
        for forecast in forecasts:
            forecast.issued_at = dateutil.parser.parse(committer_date)
            forecast.save()

    # done
    logger.info("done. resetting repo")
    os.system(f'git -C "{HUB_DIR}" reset --hard')
    logger.info(os.popen(f'ls -al {TRUTH_FILE}').readline().strip())


#
# main
#

if __name__ == '__main__':
    reload_covid_truth_app()
