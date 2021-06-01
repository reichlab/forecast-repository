# Generated by Django 3.1.7 on 2021-04-29 14:19
import datetime

from django.db import migrations, models


#
# This file does both schema and data migrations for renaming the `Forecast.issue_date` field to `Forecast.issued_at`.
# I edited the Django-generated file to get this. We default the conversion from date to datetime arbitrarily to 12
# noon.
#

def forwards_func(apps, schema_editor):
    # via https://docs.djangoproject.com/en/2.2/ref/migration-operations/#runpython : We get the model from the
    # versioned app registry; if we directly import it, it'll be the wrong version:
    Forecast = apps.get_model("forecast_app", "Forecast")

    # this is the slow one-at-a-time approach. I tried using https://docs.djangoproject.com/en/2.2/ref/models/expressions/#f-expressions
    # but was too complicated
    for forecast in Forecast.objects.all().iterator():
        forecast.issued_at = datetime.datetime.combine(forecast.issue_date, datetime.time(hour=12),
                                                       tzinfo=datetime.timezone.utc)
        forecast.save()


class Migration(migrations.Migration):
    dependencies = [
        ('forecast_app', '0016_job_json_fields'),
    ]

    operations = [
        migrations.RemoveConstraint(
            model_name='forecast',
            name='unique_version',
        ),
        migrations.AddField(
            model_name='forecast',
            name='issued_at',
            field=models.DateTimeField(db_index=True, default=None, null=True),
            preserve_default=False,
        ),
        migrations.RunPython(forwards_func, reverse_code=migrations.RunPython.noop),
        migrations.AlterField(
            model_name='forecast',
            name='issued_at',
            field=models.DateTimeField(db_index=True),
        ),
        migrations.RemoveField(
            model_name='forecast',
            name='issue_date',
        ),
        migrations.AddConstraint(
            model_name='forecast',
            constraint=models.UniqueConstraint(fields=('forecast_model', 'time_zero', 'issued_at'),
                                               name='unique_version'),
        ),
    ]
