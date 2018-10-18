import click
import django
import django_rq
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models.score import _update_model_scores, ScoreLastUpdate

from forecast_app.models import Score, ScoreValue, Project, ForecastModel


# https://stackoverflow.com/questions/44051647/get-params-sent-to-a-subcommand-of-a-click-group
class MyGroup(click.Group):
    def invoke(self, ctx):
        ctx.obj = tuple(ctx.args)
        super().invoke(ctx)


@click.group(cls=MyGroup)
@click.pass_context
def cli(ctx):
    args = ctx.obj
    click.echo('cli: {} {}'.format(ctx.invoked_subcommand, ' '.join(args)))


@cli.command()
def print():
    """
    A subcommand that prints all projects' scores. Runs in the calling thread and therefore blocks.
    """
    Score.ensure_all_scores_exist()

    click.echo("\n* Scores:")
    for score in Score.objects.all():
        click.echo("- {} | {}".format(score, ScoreValue.objects.filter(score=score).count()))

    click.echo("\n* Score Forecasts:")
    for score in Score.objects.all().order_by('name'):
        for project in Project.objects.order_by('name'):
            for forecast_model in project.models.all().order_by('project__id', 'name'):
                score_last_update = score.last_update_for_forecast_model(forecast_model)  # None o/w
                score_values_qs = ScoreValue.objects.filter(score=score, forecast__forecast_model__project=project)
                last_update_str = '{:%Y-%m-%d %H:%M:%S}'.format(score_last_update.updated_at) if score_last_update \
                    else '[no updated_at]'
                click.echo("  + (score={}) '{}' | {} | {} . (proj={}, model={}) '{}'"
                           .format(score.pk, score.abbreviation, score_values_qs.count(),
                                   last_update_str, forecast_model.project.pk, forecast_model.pk, forecast_model.name))


@cli.command()
@click.option('--score-pk')
def clear(score_pk):
    """
    A subcommand that clears score values and last update dates, controlled by the args. Runs in the calling thread, and
    therefore blocks.

    :param score_pk: if a valid Score pk then only that score is cleared. o/w all scores are
    """
    scores = [get_object_or_404(Score, pk=score_pk)] if score_pk else Score.objects.all()
    for score in scores:
        click.echo("clearing {}".format(score))
        ScoreValue.objects.filter(score=score).delete()
        ScoreLastUpdate.objects.filter(score=score).delete()
    click.echo("clear done")


@cli.command()
@click.option('--score-pk')
@click.option('--model-pk')
def update(score_pk, model_pk):
    """
    A subcommand that enqueues updating model scores, controlled by the args. Runs in the calling thread, and therefore
    blocks.

    :param score_pk: if a valid Score pk then only that score is updated. o/w all scores are
    :param model_pk: if a valid ForecastModel pk then only that model is updated. o/w all models are
    """
    scores = [get_object_or_404(Score, pk=score_pk)] if score_pk else Score.objects.all()
    models = [get_object_or_404(ForecastModel, pk=model_pk)] if model_pk else ForecastModel.objects.all()
    for score in scores:
        for forecast_model in models:
            click.echo("enqueuing score={}, forecast_model={}".format(score, forecast_model))
            django_rq.enqueue(_update_model_scores, score.pk, forecast_model.pk)
    click.echo("enqueuing done")


if __name__ == '__main__':
    cli()
