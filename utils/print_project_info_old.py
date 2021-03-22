import click
import django
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.migration_0014_utils import _num_rows_old_data
from utils.project_truth import first_truth_data_forecast, oracle_model_for_project
from django.contrib.auth.models import User
from forecast_app.models import Project, Forecast


@click.command()
@click.argument('verbosity', type=click.Choice(['1', '2', '3', '4']), default='1')
@click.option('--project-pk')
def main(verbosity, project_pk):
    """
    :param verbosity: increasing from 1 (minimal verbosity) to 3 (maximal)
    :param project_pk: if a valid Project pk then only that project's models are updated. o/w defers to `model_pk` arg
    """
    project = get_object_or_404(Project, pk=project_pk) if project_pk else None
    projects = [project] if project else Project.objects.order_by('name')
    click.echo(f"Users: {User.objects.all()}")

    if len(projects) != 0:
        click.echo(f"Found {len(projects)} projects: {projects}")
        for project in projects:
            print_project_info_old(project, int(verbosity))
    else:
        click.echo("<No Projects>")


def print_project_info_old(project, verbosity):
    # verbosity == 1
    first_truth_forecast = first_truth_data_forecast(project)
    click.echo(f"\n\n* {project}. truth: # predictions={truth_data_qs_old(project).count()}, "
               f"source={repr(first_truth_forecast.source) if first_truth_forecast else '<no truth>'}, "
               f"created_at={first_truth_forecast.created_at if first_truth_forecast else '<no truth>'}. "
               f"owner={project.owner}, model_owners={project.model_owners.all()}, (num_models, num_forecasts, "
               f"num_rows): {get_summary_counts_old(project)}")
    if verbosity == 1:
        return

    # verbosity == 2
    click.echo(f"\n** Targets ({project.targets.count()})")
    for target in project.targets.all():
        click.echo(f"- {target}")

    click.echo(f"\n** Units ({project.units.count()})")
    for unit in project.units.all().order_by('name'):
        click.echo(f"- {unit}")

    click.echo(f"\n** TimeZeros ({project.timezeros.count()})")
    for timezero in project.timezeros.all():
        click.echo(f"- {timezero}")

    if verbosity == 2:
        return

    # verbosity == 3
    click.echo(f"\n** ForecastModels ({project.models.count()})")
    for forecast_model in project.models.all():
        if verbosity == 3:
            click.echo(f"- {forecast_model}")
        else:
            click.echo(f"*** {forecast_model} ({forecast_model.forecasts.count()} forecasts)")
        if verbosity == 4:
            for forecast in forecast_model.forecasts.order_by('time_zero', 'issue_date'):
                click.echo(f"- {forecast}: {_num_rows_old_data(forecast)} rows")


def truth_data_qs_old(project):
    """
    :return: A QuerySet of project's truth data - PointPrediction instances.
    """
    from forecast_app.models import PointPrediction  # avoid circular imports


    oracle_model = oracle_model_for_project(project)
    if not oracle_model:
        return PointPrediction.objects.none()

    return PointPrediction.objects.filter(forecast__forecast_model=oracle_model)


def get_summary_counts_old(project):
    """
    :return: a 3-tuple summarizing total counts in me: (num_models, num_forecasts, num_rows). The latter is
        estimated.
    """
    return project.models.filter(project=project, is_oracle=False).count(), \
           Forecast.objects.filter(forecast_model__project=project, forecast_model__is_oracle=False).count(), \
           get_num_forecast_rows_all_models_estimated_old(project)


def get_num_forecast_rows_all_models_estimated_old(project):
    """
    :return: like num_pred_ele_rows_all_models(), but returns an estimate that is much faster to calculate. the
        estimate is based on getting the number of rows for an arbitrary Forecast and then multiplying by the number
        of forecasts times the number of models in me. it will be exact for projects whose models all have the same
        number of rows
    """
    first_model = project.models.first()
    first_forecast = first_model.forecasts.first() if first_model else None
    first_forecast_num_rows = _num_rows_old_data(first_forecast) if first_forecast else None
    return (project.models.count() * first_model.forecasts.count() * first_forecast_num_rows) \
        if first_forecast_num_rows else 0


if __name__ == '__main__':
    main()
