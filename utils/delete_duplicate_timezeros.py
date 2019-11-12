from collections import defaultdict

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models import Project, Forecast


@click.command()
@click.argument('project_name', type=click.STRING, required=True)
@click.option('--delete', '-d', is_flag=True, help="Go ahead and delete duplicates.")
def delete_duplicate_timezeros_app(project_name, delete):
    """
    A utility to correct a bug that allows users to create duplicate TimeZeros. Deletes duplicate TimeZeros in the named
    project. Compares both timezero_date and data_version_date (which may be None). Ignores is_season_start and
    season_name. Only deletes those that have no forecasts.
    """
    project = Project.objects.filter(name=project_name).first()
    if not project:
        click.echo(f"no project found: '{project_name}'", err=True)
        return

    # do one pass to collect just the duplicate timezero_date and data_version_date pairs.
    tz_dvd_date_to_count = defaultdict(int)  # key: 2-tuple: (timezero_date, data_version_date)
    for timezero in project.timezeros.all():
        tz_dvd_date_to_count[(timezero.timezero_date, timezero.data_version_date)] += 1

    # collect all duplicate TimeZeros with no forecasts
    timezeros_to_delete = []
    for timezero_date, data_version_date in sorted(tz_dvd_date_to_count.keys()):
        count = tz_dvd_date_to_count[(timezero_date, data_version_date)]
        if count <= 1:
            continue

        click.echo(f"- duplicate pair: ({timezero_date}, {data_version_date}): {count}")
        matching_timezeros = project.timezeros \
            .filter(timezero_date=timezero_date, data_version_date=data_version_date) \
            .order_by("timezero_date", "data_version_date")
        click.echo(f"  => matching_timezeros: {matching_timezeros}")

        # remove those with any forecasts
        matching_timezeros = [matching_timezero for matching_timezero in matching_timezeros
                              if Forecast.objects.filter(time_zero=matching_timezero).count() == 0]
        click.echo(f"  => matching_timezeros no forecasts: {matching_timezeros}")

        # if multiple duplicates with no forecasts, remove all but one
        matching_timezeros = matching_timezeros[1:] if len(matching_timezeros) > 1 else matching_timezeros
        click.echo(f"  => matching_timezeros all but one: {matching_timezeros}")

        timezeros_to_delete.extend(matching_timezeros)

    # finally, delete the TimeZeros
    tzs_to_delete_w_counts = [(timezero_to_delete, Forecast.objects.filter(time_zero=timezero_to_delete).count())
                              for timezero_to_delete in timezeros_to_delete]
    click.echo(f"timezeros_to_delete={tzs_to_delete_w_counts}")
    for timezero_to_delete in timezeros_to_delete:
        if delete:
            click.echo(f"- YES deleting timezero_to_delete={timezero_to_delete}")
            timezero_to_delete.delete()
        else:
            click.echo(f"- NOT deleting timezero_to_delete={timezero_to_delete}")


if __name__ == '__main__':
    delete_duplicate_timezeros_app()
