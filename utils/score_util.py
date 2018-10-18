import click
import django
import django_rq
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models.score import _update_model_scores

from forecast_app.scores.definitions import SCORE_ABBREV_TO_NAME_AND_DESCR

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
    A subcommand that prints all projects' scores in the calling thread, and therefore blocks.
    """
    # by Score
    click.echo("\n* scores:")
    for score in Score.objects.all():
        click.echo("- {} | {}".format(score, ScoreValue.objects.filter(score=score).count()))

    # by Project
    click.echo("\n* project scores. {}".format(SCORE_ABBREV_TO_NAME_AND_DESCR))
    for project in Project.objects.all():
        click.echo("- {}".format(project.name))
        for forecast_model in project.models.all():
            for score in Score.objects.all():
                # abbreviation, name, description = Score.SCORE_TYPE_TO_INFO[score.score_type]
                score_last_update = score.last_update_for_forecast_model(forecast_model)  # None o/w
                score_values_qs = ScoreValue.objects.filter(score=score, forecast__forecast_model__project=project)
                click.echo("  + pk={} | '{}' | '{}' | num={} | {}"
                           .format(score.pk, score.abbreviation, score.name, score_values_qs.count(),
                                   score_last_update.updated_at if score_last_update else 'no update'))


@cli.command()
def clear():
    """
    A subcommand that resets all projects' scores in the calling thread, and therefore blocks.
    """
    click.echo("clearing all projects' scores")
    for project in Project.objects.all():
        for score in Score.objects.all():
            click.echo("- clearing {} > {}".format(project, score))
            ScoreValue.objects.filter(score=score, forecast__forecast_model__project=project).delete()
    click.echo("clear done")


@cli.command()
def delete():
    """
    A subcommand that deletes all scores in the calling thread, and therefore blocks.
    """
    click.echo("deleting all scores")
    for score in Score.objects.all():
        click.echo("- deleting {}".format(score))
        score.delete()
    click.echo("delete done")


@cli.command()
@click.option('--model-pk')
def update(model_pk):
    """
    A subcommand that enqueues updating all projects' scores, or scores for a single model if model_pk is valid.
    """
    if model_pk:
        forecast_model = get_object_or_404(ForecastModel, pk=model_pk)
        click.echo("enqueuing scores. forecast_model={}".format(forecast_model))
        for score in Score.objects.all():
            django_rq.enqueue(_update_model_scores, score.pk, forecast_model.pk)
    else:
        click.echo("enqueuing scores for all projects")
        Score.enqueue_update_scores_for_all_projects()
    click.echo("enqueuing done")


if __name__ == '__main__':
    cli()
