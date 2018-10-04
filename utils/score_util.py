import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set


django.setup()
from utils.scores import enqueue_score_updates_all_projs

from forecast_app.models import Score, ScoreValue, Project, ScoreLastUpdate


@click.group()
def cli():
    pass


@cli.command()
def print():
    """
    A subcommand that prints all projects' scores in the calling thread, and therefore blocks.
    """
    # todo show last_update

    # by Score
    click.echo("\n* scores:")
    for score in Score.objects.all():
        click.echo("- {} | {}".format(score, ScoreValue.objects.filter(score=score).count()))

    # by Project. NB nested loops are the dumb/slow way. todo xx use LEFT OUTER JOIN, GROUP BY, etc.
    click.echo("\n* project scores:")
    for project in Project.objects.all():
        click.echo("- {}".format(project.name))
        for score in Score.objects.all():
            score_last_update = score.last_update_for_project(project)  # None o/w
            score_values_qs = ScoreValue.objects.filter(score=score, forecast__forecast_model__project=project)
            click.echo("  + {} | {} | {}".format(score.name, score_values_qs.count(),
                                                 score_last_update.last_update if score_last_update else 'no update'))


@cli.command()
def clear():
    """
    A subcommand that resets all projects' scores in the calling thread, and therefore blocks.
    """
    click.echo("clearing all projects' scores")
    # NB nested loops are the dumb/slow way. todo xx use LEFT OUTER JOIN, GROUP BY, etc.
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
def update():
    """
    A subcommand that enqueues updating all projects' 'Absolute Error' score.
    """
    click.echo("enqueuing all projects' scores")
    enqueue_score_updates_all_projs()
    click.echo("enqueuing done")


if __name__ == '__main__':
    cli()
