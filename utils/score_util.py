import csv
import logging
from collections import defaultdict

import click
import django
import django_rq
import requests
from django.db import connection
from django.shortcuts import get_object_or_404

# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models.score import _update_model_scores_worker
from forecast_app.models import Score, ScoreValue, Project, ForecastModel, Forecast

logger = logging.getLogger(__name__)


# https://stackoverflow.com/questions/44051647/get-params-sent-to-a-subcommand-of-a-click-group
class MyGroup(click.Group):
    def invoke(self, ctx):
        ctx.obj = tuple(ctx.args)
        super().invoke(ctx)


@click.group(cls=MyGroup)
@click.pass_context
def cli(ctx):
    args = ctx.obj
    logger.info('cli: {} {}'.format(ctx.invoked_subcommand, ' '.join(args)))


@cli.command(name="print")
def print_scores():
    """
    A subcommand that prints all projects' scores. Runs in the calling thread and therefore blocks.
    """
    Score.ensure_all_scores_exist()

    logger.info("\n* Scores:")
    for score in Score.objects.all():
        logger.info(f"- {score} | {ScoreValue.objects.filter(score=score).count()}")

    logger.info("\n* Score Forecasts:")
    for score in Score.objects.all().order_by('name'):
        for project in Project.objects.all():
            for forecast_model in project.models.all().order_by('project__name', 'name'):
                score_last_update = score.last_update_for_forecast_model(forecast_model)  # None o/w
                score_values_qs = ScoreValue.objects.filter(score=score, forecast__forecast_model=forecast_model)
                last_update_str = '{:%Y-%m-%d %H:%M:%S}'.format(score_last_update.updated_at) if score_last_update \
                    else '[no updated_at]'
                # e.g.,  + (score=5) 'pit' | 3135 | 2019-11-14 16:18:53 . (proj=46, model=127) 'SARIMA model with seasonal differencing'
                logger.info(f"  + (score={score.pk}) '{score.abbreviation}' | {score_values_qs.count()} | "
                            f"{last_update_str} . (proj={forecast_model.project.pk}, model={forecast_model.pk}) "
                            f"'{forecast_model.name}'")


@cli.command()
@click.option('--score-abbrev')
def clear(score_abbrev):
    """
    A subcommand that clears score values and last update dates, controlled by the args. Runs in the calling thread, and
    therefore blocks.

    :param score_abbrev: if a valid Score abbreviation then only that score is cleared. o/w all scores are cleared
    """
    Score.ensure_all_scores_exist()

    logger.info(f"clear(): score_abbrev={score_abbrev!r}")
    scores = [get_object_or_404(Score, abbreviation=score_abbrev)] if score_abbrev else Score.objects.all()
    for score in scores:
        logger.info("clearing {}".format(score))
        score.clear()
    logger.info("clear done")


@cli.command()
@click.option('--score-abbrev')
@click.option('--project-pk')
@click.option('--model-abbrev')
@click.option('--no-enqueue', is_flag=True, default=False)
def update(score_abbrev, project_pk, model_abbrev, no_enqueue):
    """
    A subcommand that enqueues or (executes immediately) updating model scores, controlled by the args. NB: Does NOT
    exclude those that do not need updating according to how ForecastModel.forecasts_changed_at compares to
    ScoreLastUpdate.updated_at .

    :param score_abbrev: if a valid Score abbreviation then only that score is updated. o/w all scores are updated
    :param project_pk: if a valid Project pk then only that project's models are updated. o/w defers to `model_abbrev` arg
    :param model_abbrev: if a valid ForecastModel abbreviation then only that model is updated. o/w all models are updated
    :param no_enqueue: controls whether the update will be immediate in the calling thread (blocks), or enqueued for RQ
    """
    from forecast_repo.settings.base import UPDATE_MODEL_SCORES_QUEUE_NAME  # avoid circular imports

    Score.ensure_all_scores_exist()
    logger.info(f"update(): score_abbrev={score_abbrev!r}, project_pk={project_pk}, model_abbrev={model_abbrev!r}, "
                f"no_enqueue={no_enqueue}")

    scores = [get_object_or_404(Score, abbreviation=score_abbrev)] if score_abbrev else Score.objects.all()

    # set models
    project = get_object_or_404(Project, pk=project_pk) if project_pk else None
    model = get_object_or_404(ForecastModel, project__id=project_pk, abbreviation=model_abbrev) \
        if model_abbrev and project_pk else None
    if project:
        models = project.models.all()
    elif model:
        models = [model]
    else:
        models = ForecastModel.objects.all()

    for score in scores:
        logger.info(f"* {score}")
        for forecast_model in models:
            if no_enqueue:
                logger.info(f"** (no enqueue) calculating score={score}, forecast_model={forecast_model}")
                _update_model_scores_worker(score.pk, forecast_model.pk)
            else:
                logger.info(f"** enqueuing score={score}, forecast_model={forecast_model}")
                queue = django_rq.get_queue(UPDATE_MODEL_SCORES_QUEUE_NAME)
                queue.enqueue(_update_model_scores_worker, score.pk, forecast_model.pk)
    logger.info("update done")


@cli.command()
@click.option('--dry-run', is_flag=True, default=False)
def update_all_changed(dry_run):
    """
    A subcommand that enqueues all Score/ForecastModel pairs, excluding models that have not changed since the last
    score update

    :param dry_run: True means just print a report of Score/ForecastModel pairs that would be updated
    """
    logger.info(f"searching for changed Score/ForecastModel pairs. dry_run={dry_run}")
    enqueued_score_model_pks = Score.enqueue_update_scores_for_all_models(is_only_changed=True, dry_run=dry_run)
    logger.info(f"enqueuing done. dry_run={dry_run}. {len(enqueued_score_model_pks)} Score/ForecastModel pairs. "
                f"enqueued_score_model_pks={enqueued_score_model_pks}")


@cli.command()
@click.option('--project-pk')
def report(project_pk):
    """
    A subcommand that creates a csv file whose rows are models and columns are scores, with ScoreValue counts in cells.
    The file is useful for debugging failed model-score combinations. Output is a printed https://www.file.io/
    (Ephemeral file sharing) download URL for the file.

    :param project_pk: a valid Project pk
    """
    logger.info(f"report(): project_pk={project_pk}")
    project = get_object_or_404(Project, pk=project_pk)
    logger.info(f'report(): project={project}')

    score_abbrevs = list(Score.objects.all()
                         .order_by('id')
                         .values_list('abbreviation', flat=True))
    model_abbrevs = list(ForecastModel.objects
                         .filter(project=project)
                         .order_by('abbreviation')
                         .values_list('abbreviation', flat=True))

    # fill model_score_counts
    logger.debug('getting counts')
    model_score_counts = _model_score_counts(project)  # (model_abbrev, score_abbrev, num_score_values)
    model_to_score_to_count = defaultdict(dict)  # [model_abbrev][score_abbrev] -> num_score_values
    for model in model_abbrevs:  # 1/2 fill in all, defaulting to 0
        for score in score_abbrevs:
            model_to_score_to_count[model][score] = 0
    for model, score, count in model_score_counts:  # 2/2 fill in actual
        model_to_score_to_count[model][score] = count

    # print score and model PK reference
    logger.info(f'* score IDs:')
    for score in Score.objects.all().order_by('abbreviation'):
        logger.info(f'- {score.abbreviation}\t{score.id}')

    logger.info(f'* model IDs:')
    for forecast_model in project.models.all().order_by('abbreviation'):
        logger.info(f'- {forecast_model.abbreviation}\t{forecast_model.id}')

    # create csv
    logger.debug('saving csv')
    csv_filename = '/tmp/temp.csv'
    with open(csv_filename, 'w') as fp:
        csv_writer = csv.writer(fp, delimiter=',')
        header = ['model'] + score_abbrevs
        csv_writer.writerow(header)
        for model in model_abbrevs:
            row = [model]
            for score in score_abbrevs:
                row.append(model_to_score_to_count[model][score])
            csv_writer.writerow(row)

    # done! upload to file.io
    logger.debug('uploading to file.io')
    r = requests.post('https://file.io', files={'file': open(csv_filename, 'rb')})
    # r.text: '{"success":true,"key":"iMt03r7jHCjE","link":"https://file.io/iMt03r7jHCjE","expiry":"14 days"}'
    logger.info(f'done! r.text={r.text}')


def _model_score_counts(project):
    """
    `report()` helper

    :param project: a Project
    :return: list of 3-tuples for all ForecastModel-Score combinations in `project`:
        (model_abbrev, score_abbrev, num_score_values)
    """
    sql = f"""
        SELECT fm.abbreviation AS model, s.abbreviation AS score, count(*)
        FROM forecast_app_scorevalue AS sv
                 JOIN {Score._meta.db_table} s ON sv.score_id = s.id
                 JOIN {Forecast._meta.db_table} f on sv.forecast_id = f.id
                 JOIN {ForecastModel._meta.db_table} fm on f.forecast_model_id = fm.id
        WHERE fm.project_id = %s
        GROUP BY fm.abbreviation, s.abbreviation;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (project.id,))
        return cursor.fetchall()  # todo xx return batched_rows(cursor)


if __name__ == '__main__':
    cli()
