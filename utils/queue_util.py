import click
import django

import django_rq


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_repo.settings.base import HIGH_QUEUE_NAME, DEFAULT_QUEUE_NAME, LOW_QUEUE_NAME


@click.group()
def cli():
    pass


@cli.command(name="print")
def print_queue():
    """
    A subcommand that prints all RQ queues's statuses.
    """
    click.echo("queues:")
    for queue_name in [HIGH_QUEUE_NAME, DEFAULT_QUEUE_NAME, LOW_QUEUE_NAME]:
        queue = django_rq.get_queue(queue_name)
        click.echo(f"- {queue}: {len(queue)} jobs")


@cli.command()
def clear():
    """
    A subcommand that clears all RQ queues.
    """
    click.echo("clearing all queues")
    for queue_name in [HIGH_QUEUE_NAME, DEFAULT_QUEUE_NAME, LOW_QUEUE_NAME]:
        # clear non-failed jobs
        queue = django_rq.get_queue(queue_name)
        click.echo(f"- emptying queue: {queue}")
        queue.empty()  # deletes all jobs in the queue

        # clear failed jobs. per expiring failed jobs - https://github.com/rq/rq/issues/964
        for job_id in queue.failed_job_registry.get_job_ids():
            job = queue.fetch_job(job_id)
            # "I think this check [job.is_failed]is not needed as FailedJobRegistry may have only the failed jobs.
            # Just to confirm, I added this condition.":
            if job and job.is_failed:
                click.echo(f"- deleting failed job: {job}")
                job.delete()
    click.echo("clear done")


if __name__ == '__main__':
    cli()
