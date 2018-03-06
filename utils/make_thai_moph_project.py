import timeit
from pathlib import Path

import click
import django

from utils.make_2016_2017_flu_contest_project import get_or_create_super_po_mo_users
from utils.utilities import cdc_csv_components_from_data_dir


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set


django.setup()

from forecast_app.models.project import Target, TimeZero
from forecast_app.models import Project, ForecastModel


@click.command()
@click.argument('data_dir', type=click.Path(file_okay=False, exists=True))
def make_thai_moph_project_app(data_dir):
    """
    Deletes and creates a database with one project, one group, and two classes of users. Then loads models from the
    Impetus project. Note: The input files to this program are the output from a spamd export script located the
    dengue-data repo ( https://github.com/reichlab/dengue-data/blob/master/misc/cdc-csv-export.R ) and are committed to
    https://epimodeling.springloops.io/project/156725/svn/source/browse/-/trunk%2Farchives%2Fdengue-reports%2Fdata-summaries
    They currently must be processed (currently by hand) via these rough steps:

        1. download template
        2. correct template header from 'bin_end_not_incl' to 'bin_end_notincl'
        3. delete files where first date (data_version_date) was before 0525
        4. for files with duplicate second dates (timzeros), keep the one with the most recent first date (data_version_date)

    """
    start_time = timeit.default_timer()
    click.echo("* started creating Thai MOPH project")

    project_name = 'Impetus Province Forecasts'
    found_project = Project.objects.filter(name=project_name).first()
    if found_project:
        click.echo("* deleting previous project")
        found_project.delete()

    po_user, _, mo_user, _ = get_or_create_super_po_mo_users(create_super=False)

    click.echo("* creating project")
    data_dir = Path(data_dir)
    template_path = data_dir / 'thai-moph-forecasting-template.csv'
    project = make_thai_moph_project(project_name, template_path, data_dir)
    project.owner = po_user
    project.model_owners.add(mo_user)
    project.save()

    click.echo("* creating model. data_dir={}".format(data_dir))
    make_model(project, mo_user, data_dir)

    # done
    click.echo("* Done. time: {}".format(timeit.default_timer() - start_time))


def make_thai_moph_project(project_name, template_path, data_dir):
    project = Project.objects.create(
        name=project_name,
        is_public=False,
        description="Impetus Project forecasts for real-time dengue hemorrhagic fever (DHF) in Thailand. Beginning in "
                    "May 2017, this project contains forecasts for biweekly DHF incidence at the province level in "
                    "Thailand. Specifically, each timezero date is associated with a biweek in which data were "
                    "delivered from the Thai Ministry of Public Health to servers in the US. We use standard biweek "
                    "definitions described in the supplemental materials of Reich et al. (2016). Each timezero also "
                    "has a data-version-date that represents the day the forecast model was run. This can be the same "
                    "as the timezero, but cannot be earlier.\n\nFiles follow the naming conventions of "
                    "`[timezero]-[modelname]-[data-version-date].cdc.csv`, where dates are in YYYYMMDD format. For "
                    "example, `20170917-gam_lag1_tops3-20170919.cdc.csv`.\n\nFor each timezero, a forecast contains "
                    "predictive distributions for case counts at [-1, 0, 1, 2, 3] biweek ahead, relative to the "
                    "timezero. Predictive distributions must be defined according to this binned-interval structure:"
                    "{[0,1), [1, 10), [10, 20), [20, 30), ..., [1990, 2000), [2000, Inf)}.",
        home_url='http://www.iddynamics.jhsph.edu/projects/impetus',
        logo_url='http://www.iddynamics.jhsph.edu/sites/default/files/styles/project-logo/public/content/project/logos/ImpetusLogo.png',
        core_data='https://github.com/reichlab/dengue-data')

    click.echo("  loading template")
    project.load_template(template_path)

    # create Targets
    click.echo("  creating targets")
    for target_name, description in {
        '-1_biweek_ahead': 'forecasted case counts for the biweek prior to the timezero biweek (minus-one-step-ahead '
                           'forecast)',
        '0_biweek_ahead': 'forecasted case counts for the timezero biweek (zero-step-ahead forecast)',
        '1_biweek_ahead': 'forecasted case counts for 1 biweek subsequent to the timezero biweek (1-step ahead '
                          'forecast)',
        '2_biweek_ahead': 'forecasted case counts for 2 biweeks subsequent to the timezero biweek (2-step ahead '
                          'forecast)',
        '3_biweek_ahead': 'forecasted case counts for 3 biweeks subsequent to the timezero biweek (3-step ahead '
                          'forecast)',
    }.items():
        Target.objects.create(project=project, name=target_name, description=description)

    # create TimeZeros from file names in data_dir. format (e.g., '20170419-gam_lag1_tops3-20170516.cdc.csv'):
    click.echo("  creating timezeros")
    for _, time_zero, _, data_version_date in cdc_csv_components_from_data_dir(data_dir):
        TimeZero.objects.create(project=project,
                                timezero_date=str(time_zero),
                                data_version_date=str(data_version_date) if data_version_date else None)

    # done
    return project


def make_model(project, model_owner, data_dir):
    """
    Creates the gam_lag1_tops3 ForecastModel and its Forecast.
    """
    description = "A spatio-temporal forecasting model for province-level dengue hemorrhagic fever incidence in " \
                  "Thailand. The model is fit using the generalized additive model framework, with the number of " \
                  "cases in the previous biweek in the top three correlated provinces informing the current " \
                  "forecast. Forecasts at multiple horizons into the future are made by recursively applying the model."
    forecast_model = ForecastModel.objects.create(
        owner=model_owner,
        project=project,
        name='gam_lag1_tops3',
        description=description,
        home_url='http://journals.plos.org/plosntds/article?id=10.1371/journal.pntd.0004761',
        aux_data_url=None)
    forecast_model.load_forecasts_from_dir(data_dir)

    # done
    return project


if __name__ == '__main__':
    make_thai_moph_project_app()
