import click
import django
import django_rq
from django.shortcuts import get_object_or_404


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.forecast import _cache_forecast_metadata_worker, cache_forecast_metadata, clear_forecast_metadata, \
    forecast_metadata

from forecast_app.models import Project


# https://stackoverflow.com/questions/44051647/get-params-sent-to-a-subcommand-of-a-click-group
class MyGroup(click.Group):
    def invoke(self, ctx):
        ctx.obj = tuple(ctx.args)
        super().invoke(ctx)


@click.group(cls=MyGroup)
@click.pass_context
def cli(ctx):
    args = ctx.obj
    print('cli: {} {}'.format(ctx.invoked_subcommand, ' '.join(args)))


@cli.command(name="print")
@click.option('--project-pk')
def print_forecast_metadata_all_projects(project_pk):
    """
    A subcommand that prints info about one or all projects' forecast metadata. Runs in the calling thread and therefore
    blocks.

    :param project_pk: if a valid Project pk then only that project's metadata is cleared. o/w clears all
    """
    projects = [get_object_or_404(Project, pk=project_pk)] if project_pk else Project.objects.all()
    print("printing metadata")
    for project in projects:
        print(f"* {project}")
        for forecast_model in project.models.all().order_by('abbreviation'):
            print(f"- {forecast_model}")
            for forecast in forecast_model.forecasts.all().order_by('time_zero__timezero_date'):
                forecast_meta_prediction, forecast_meta_unit_qs, forecast_meta_target_qs = forecast_metadata(forecast)
                if all([forecast_meta_prediction, forecast_meta_unit_qs.count(), forecast_meta_target_qs.count()]):
                    print(f"  = {forecast.pk}|{forecast.source}: pnbsq: {forecast_meta_prediction.point_count}|"
                          f"{forecast_meta_prediction.named_count}|{forecast_meta_prediction.bin_count}|"
                          f"{forecast_meta_prediction.sample_count}|{forecast_meta_prediction.quantile_count}, "
                          f"{len(forecast_meta_unit_qs)} units, {len(forecast_meta_target_qs)} targets")
                elif any([forecast_meta_prediction, forecast_meta_unit_qs.count(), forecast_meta_target_qs.count()]):
                    print(f"  = {forecast.pk}|{forecast.source}: not all! {forecast_meta_prediction}, "
                          f"{forecast_meta_unit_qs.count()}, {forecast_meta_target_qs.count()}")
    print("print done")


@cli.command()
@click.option('--project-pk')
def clear(project_pk):
    """
    A subcommand that clears all one or all projects' forecast metadata. Runs in the calling thread, and therefore
    blocks.

    :param project_pk: if a valid Project pk then only that project's metadata is cleared. o/w clears all
    """
    projects = [get_object_or_404(Project, pk=project_pk)] if project_pk else Project.objects.all()
    print("clearing metadata")
    for project in projects:
        print(f"* {project}")
        for forecast_model in project.models.all():
            print(f"- {forecast_model}")
            for forecast in forecast_model.forecasts.all():
                print(f"  = {forecast}")
                clear_forecast_metadata(forecast)
    print("clear done")


@cli.command()
@click.option('--project-pk')
@click.option('--no-enqueue', is_flag=True, default=False)
def update(project_pk, no_enqueue):
    """
    A subcommand that updates all one or all projects' forecast metadata.

    :param project_pk: if a valid Project pk then only that project's metadata is updated. o/w updates all
    :param no_enqueue: controls whether the update will be immediate in the calling thread (blocks), or enqueued for RQ
    """
    from forecast_repo.settings.base import CACHE_FORECAST_METADATA_QUEUE_NAME  # avoid circular imports


    queue = django_rq.get_queue(CACHE_FORECAST_METADATA_QUEUE_NAME)
    projects = [get_object_or_404(Project, pk=project_pk)] if project_pk else Project.objects.all()
    print("updating metadata")
    for project in projects:
        print(f"* {project}")
        for forecast_model in project.models.all():
            if forecast_model.is_oracle:
                # by convention we do not compute metadata for oracle forecasts. o/w they show up in project summary
                # counts
                continue

            print(f"- {forecast_model}")
            for forecast in forecast_model.forecasts.all():
                if no_enqueue:
                    print(f"  = caching metadata (no enqueue): {forecast}")
                    cache_forecast_metadata(forecast)  # todo xx BUG: do not do if forecast_model.is_oracle!
                else:
                    print(f"  = enqueuing caching metadata: {forecast}")
                    queue.enqueue(_cache_forecast_metadata_worker, forecast.pk)
    print("update done")


if __name__ == '__main__':
    cli()
