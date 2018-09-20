import timeit
from pathlib import Path

import click
import django

from utils.make_cdc_flu_contests_project import get_or_create_super_po_mo_users
from utils.utilities import cdc_csv_components_from_data_dir


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_app.models.project import Target, TimeZero
from forecast_app.models import Project, ForecastModel


#
# ---- application----
#

THAI_PROJECT_NAME = 'Impetus Province Forecasts'
THAI_CONFIG_DICT = {
    "visualization-y-label": "DHF cases"
}


@click.command()
@click.argument('data_dir', type=click.Path(file_okay=False, exists=True))
@click.option('--make_project', is_flag=True, default=False)
@click.option('--load_data', is_flag=True, default=False)
def make_thai_moph_project_app(data_dir, make_project, load_data):
    """
    Deletes and creates a database with one project, one group, and two classes of users. Hard-coded for 2017-2018
    season. Then loads models from the Impetus project. Note: The input files to this program are the output from a
    spamd export script located the dengue-data repo ( https://github.com/reichlab/dengue-data/blob/master/misc/cdc-csv-export.R )
    and are committed to https://epimodeling.springloops.io/project/156725/svn/source/browse/-/trunk%2Farchives%2Fdengue-reports%2Fdata-summaries
    They currently must be processed (currently by hand) via these rough steps:

        1. download template
        2. correct template header from 'bin_end_not_incl' to 'bin_end_notincl'
        3. delete files where first date (data_version_date) was before 0525
        4. for files with duplicate second dates (timzeros), keep the one with the most recent first date (data_version_date)

    """
    start_time = timeit.default_timer()
    data_dir = Path(data_dir)
    click.echo("* make_thai_moph_project_app(): data_dir={}, make_project={}, load_data={}"
               .format(data_dir, make_project, load_data))

    project = Project.objects.filter(name=THAI_PROJECT_NAME).first()
    if make_project:
        if project:
            click.echo("* Deleting existing project: {}".format(project))
            project.delete()

        # create the Project (and Users if necessary), including loading the template and creating Targets
        po_user, _, mo_user, _ = get_or_create_super_po_mo_users(create_super=False)
        template_path = data_dir / 'thai-moph-forecasting-template.csv'
        project = make_thai_moph_project(THAI_PROJECT_NAME, template_path)
        project.owner = po_user
        project.model_owners.add(mo_user)
        project.save()
        click.echo("* Created project: {}".format(project))

        # make the model
        forecast_model = make_model(project, mo_user, data_dir)
        click.echo("* created model: {}".format(forecast_model))
    elif not project:  # not make_project, but couldn't find existing
        raise RuntimeError("Could not find existing project named '{}'".format(THAI_PROJECT_NAME))

    # create TimeZeros. NB: we skip existing TimeZeros in case we are loading new forecasts. for is_season_start and
    # season_name we use year transitions: the first 2017 we encounter -> start of that year, etc.
    seen_years = []  # indicates a year has been processed. used to determine season starts
    for cdc_csv_file, timezero_date, _, data_version_date in cdc_csv_components_from_data_dir(data_dir):
        timezero_year = timezero_date.year
        is_season_start = timezero_year not in seen_years
        if is_season_start:
            seen_years.append(timezero_year)

        found_time_zero = project.time_zero_for_timezero_date(timezero_date)
        if found_time_zero:
            click.echo("s (TimeZero exists)\t{}\t".format(cdc_csv_file.name))  # 's' from load_forecasts_from_dir()
            continue

        TimeZero.objects.create(project=project,
                                timezero_date=str(timezero_date),
                                data_version_date=str(data_version_date) if data_version_date else None,
                                is_season_start=(True if is_season_start else False),
                                season_name=(str(timezero_year) if is_season_start else None))
    click.echo("- created TimeZeros: {}".format(project.timezeros.all()))

    if make_project:
        # load the truth
        click.echo("- loading truth values")
        project.load_truth_data(Path('utils/dengue-truth-table-script/truths.csv'))

    # load data if necessary
    if load_data:
        click.echo("* Loading forecasts")
        forecast_model = project.models.first()
        forecasts = forecast_model.load_forecasts_from_dir(data_dir)
        click.echo("- Loading forecasts: loaded {} forecast(s)".format(len(forecasts)))

    # done
    click.echo("* Done. time: {}".format(timeit.default_timer() - start_time))


def make_thai_moph_project(project_name, template_path):
    project = Project.objects.create(
        name=project_name,
        is_public=False,
        time_interval_type=Project.BIWEEK_TIME_INTERVAL_TYPE,
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
        core_data='https://github.com/reichlab/dengue-data',
        config_dict=THAI_CONFIG_DICT)

    click.echo("  creating targets")
    create_thai_targets(project)

    click.echo("  loading template")
    project.load_template(template_path)

    # done
    return project


def create_thai_targets(project):
    """
    Creates Thai Targets for project. Returns a list of them.
    """
    targets = []
    for target_name, description, is_step_ahead, step_ahead_increment in (
            ('1_biweek_ahead',
             'forecasted case counts for 1 biweek subsequent to the timezero biweek (1-step ahead forecast)',
             True, 1),
            ('2_biweek_ahead',
             'forecasted case counts for 2 biweeks subsequent to the timezero biweek (2-step ahead forecast)',
             True, 2),
            ('3_biweek_ahead',
             'forecasted case counts for 3 biweeks subsequent to the timezero biweek (3-step ahead forecast)',
             True, 3),
            ('4_biweek_ahead',
             'forecasted case counts for 4 biweeks subsequent to the timezero biweek (4-step ahead forecast)',
             True, 4),
            ('5_biweek_ahead',
             'forecasted case counts for 5 biweeks subsequent to the timezero biweek (3-step ahead forecast)',
             True, 5),
    ):
        targets.append(Target.objects.create(project=project, name=target_name, description=description,
                                             is_step_ahead=is_step_ahead, step_ahead_increment=step_ahead_increment))
    return targets


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

    # done
    return forecast_model


if __name__ == '__main__':
    make_thai_moph_project_app()
