# Generated by Django 3.1.12 on 2021-10-06 18:27

from django.db import migrations, models


#
# This file does both schema and data migrations for the issue [expand Unit to include human-readable field(s) #228]
# where we 1) changed the meaning of `Unit.name` to be the Unit's long name, and 2) added the `Unit.abbreviation` field,
# which now has the previous meaning of `Unit.name`, that of the "official" name used in queries, etc. I edited the
# Django-generated file to get this. We default the value of `Unit.abbreviation` to `Unit.name` so that existing queries
# continue working.
#

def forwards_func(apps, schema_editor):
    # via https://docs.djangoproject.com/en/2.2/ref/migration-operations/#runpython : We get the model from the
    # versioned app registry; if we directly import it, it'll be the wrong version:
    Unit = apps.get_model("forecast_app", "Unit")

    # this is the slow one-at-a-time approach. I tried using https://docs.djangoproject.com/en/2.2/ref/models/expressions/#f-expressions
    # but was too complicated
    for unit in Unit.objects.all().iterator():
        unit.abbreviation = unit.name
        unit.save()


class Migration(migrations.Migration):
    dependencies = [
        ('forecast_app', '0017_forecast_issued_at'),
    ]

    operations = [
        migrations.AddField(
            model_name='unit',
            name='abbreviation',
            field=models.TextField(default='default_abbrev',
                                   help_text="Short name of the unit. This field is the 'official' one used by queries, etc."),
            preserve_default=False,
        ),
        migrations.RunPython(forwards_func, reverse_code=migrations.RunPython.noop),
        migrations.AlterField(
            model_name='unit',
            name='name',
            field=models.TextField(help_text='Long name of the unit. Used for displays.'),
        ),
        migrations.AddConstraint(
            model_name='unit',
            constraint=models.UniqueConstraint(fields=('project', 'abbreviation'), name='unique_unit_abbreviation'),
        ),
    ]