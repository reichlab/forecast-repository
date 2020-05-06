import logging

import click
import django
import django_rq
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models.score import _update_model_scores
from forecast_app.models import Score, ScoreValue, Project, ForecastModel


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
@click.option('--score-pk')
def clear(score_pk):
    """
    A subcommand that clears score values and last update dates, controlled by the args. Runs in the calling thread, and
    therefore blocks.

    :param score_pk: if a valid Score pk then only that score is cleared. o/w all scores are cleared
    """
    Score.ensure_all_scores_exist()

    scores = [get_object_or_404(Score, pk=score_pk)] if score_pk else Score.objects.all()
    for score in scores:
        logger.info("clearing {}".format(score))
        score.clear()
    logger.info("clear done")


@cli.command()
@click.option('--score-pk')
@click.option('--model-pk')
@click.option('--no-enqueue', is_flag=True, default=False)
def update(score_pk, model_pk, no_enqueue):
    """
    A subcommand that enqueues or (executes immediately) updating model scores, controlled by the args. NB: Does NOT
    exclude those that do not need updating according to how ForecastModel.forecasts_changed_at compares to
    ScoreLastUpdate.updated_at .

    :param score_pk: if a valid Score pk then only that score is updated. o/w all scores are updated
    :param model_pk: if a valid ForecastModel pk then only that model is updated. o/w all models are updated
    :param no_enqueue: controls whether the update will be immediate in the calling thread (blocks), or enqueued for RQ
    """
    from forecast_repo.settings.base import UPDATE_MODEL_SCORES_QUEUE_NAME


    Score.ensure_all_scores_exist()

    scores = [get_object_or_404(Score, pk=score_pk)] if score_pk else Score.objects.all()
    models = [get_object_or_404(ForecastModel, pk=model_pk)] if model_pk else ForecastModel.objects.all()
    for score in scores:
        logger.info(f"* {score}")
        for forecast_model in models:
            if no_enqueue:
                logger.info(f"** (no enqueue) calculating score={score}, forecast_model={forecast_model}")
                _update_model_scores(score.pk, forecast_model.pk)
            else:
                logger.info(f"** enqueuing score={score}, forecast_model={forecast_model}")
                queue = django_rq.get_queue(UPDATE_MODEL_SCORES_QUEUE_NAME)
                queue.enqueue(_update_model_scores, score.pk, forecast_model.pk)
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


if __name__ == '__main__':
    cli()
