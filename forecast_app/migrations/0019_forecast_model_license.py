# Generated by Django 3.1.12 on 2021-11-17 20:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('forecast_app', '0018_unit_abbreviation'),
    ]

    operations = [
        migrations.AlterField(
            model_name='forecastmodel',
            name='license',
            field=models.TextField(choices=[('afl-3.0', 'Academic Free License v3.0'), ('apache-2.0', 'Apache license 2.0'), ('artistic-2.0', 'Artistic license 2.0'), ('bsl-1.0', 'Boost Software License 1.0'), ('bsd-2-clause', 'BSD 2-clause "Simplified" license'), ('bsd-3-clause', 'BSD 3-clause "New" or "Revised" license'), ('bsd-3-clause-clear', 'BSD 3-clause Clear license'), ('cc', 'Creative Commons license family'), ('cc0-1.0', 'Creative Commons Zero v1.0 Universal'), ('cc-by-nc-4.0', 'Creative Commons Attribution Non-Commercial 4.0'), ('cc-by-4.0', 'Creative Commons Attribution 4.0'), ('cc-by-nc-nd-4.0', 'Creative Commons Attribution Non-Commercial No-Derivatives 4.0'), ('cc-by-sa-4.0', 'Creative Commons Attribution Share Alike 4.0'), ('wtfpl', 'Do What The F*ck You Want To Public License'), ('ecl-2.0', 'Educational Community License v2.0'), ('epl-1.0', 'Eclipse Public License 1.0'), ('eupl-1.1', 'European Union Public License 1.1'), ('agpl-3.0', 'GNU Affero General Public License v3.0'), ('gpl', 'GNU General Public License family'), ('gpl-2.0', 'GNU General Public License v2.0'), ('gpl-3.0', 'GNU General Public License v3.0'), ('lgpl', 'GNU Lesser General Public License family'), ('lgpl-2.1', 'GNU Lesser General Public License v2.1'), ('lgpl-3.0', 'GNU Lesser General Public License v3.0'), ('isc', 'ISC'), ('lppl-1.3c', 'LaTeX Project Public License v1.3c'), ('ms-pl', 'Microsoft Public License'), ('mit', 'MIT'), ('mpl-2.0', 'Mozilla Public License 2.0'), ('osl-3.0', 'Open Software License 3.0'), ('postgresql', 'PostgreSQL License'), ('ofl-1.1', 'SIL Open Font License 1.1'), ('ncsa', 'University of Illinois/NCSA Open Source License'), ('unlicense', 'The Unlicense'), ('zlib', 'zLib License'), ('other', 'Other License')], default='other'),
        ),
    ]
