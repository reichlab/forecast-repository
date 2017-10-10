import sys
from pathlib import Path

# set up django. must be done before loading models. requires: os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
import django
import os


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
django.setup()

from utils.mmwr_utils import end_date_2016_2017_for_mmwr_week
from forecast_app.models import Project, Target, TimeZero, ForecastModel, Forecast
from forecast_app.models.data import ProjectTemplateData, ForecastData


#
# ---- print and delete (!) all user objects ----
#

# print('* current database')
# for model_class in [DataFile, Project, Target, TimeZero, ForecastModel, Forecast]:
#     print('-', model_class)
#     for instance in model_class.objects.all():
#         print('  =', str(instance))

print('* deleting database...')
for model_class in [Project, Target, TimeZero, ForecastModel, Forecast, ProjectTemplateData, ForecastData]:
    model_class.objects.all().delete()

#
# ---- create the CDC Flu challenge (2016-2017) project and targets ----
#

print('* creating CDC Flu challenge project and models...')

config_dict = {
    'target_to_week_increment': {
        '1 wk ahead': 1,
        '2 wk ahead': 2,
        '3 wk ahead': 3,
        '4 wk ahead': 4,
    },
    'location_to_delphi_region': {
        'US National': 'nat',
        'HHS Region 1': 'hhs1',
        'HHS Region 2': 'hhs2',
        'HHS Region 3': 'hhs3',
        'HHS Region 4': 'hhs4',
        'HHS Region 5': 'hhs5',
        'HHS Region 6': 'hhs6',
        'HHS Region 7': 'hhs7',
        'HHS Region 8': 'hhs8',
        'HHS Region 9': 'hhs9',
        'HHS Region 10': 'hhs10',
    },
}

p = Project.objects.create(
    name='CDC Flu challenge (2016-2017)',
    description="Code, results, submissions, and method description for the 2016-2017 CDC flu contest submissions "
                "based on ensembles.",
    url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
    core_data='https://github.com/reichlab/2016-2017-flu-contest-ensembles/tree/master/inst/submissions',
    config_dict=config_dict)

WEEK_AHEAD_DESCR = "One- to four-week ahead forecasts will be defined as the weighted ILINet percentage for the target week."
for target_name, descr in (
        ('Season onset',
         "The onset of the season is defined as the MMWR surveillance week "
         "(http://wwwn.cdc.gov/nndss/script/downloads.aspx) when the percentage of visits for influenza-like illness (ILI) "
         "reported through ILINet reaches or exceeds the baseline value for three consecutive weeks (updated 2016-2017 "
         "ILINet baseline values for the US and each HHS region will be available at "
         "http://www.cdc.gov/flu/weekly/overview.htm the week of October 10, 2016). Forecasted 'onset' week values should "
         "be for the first week of that three week period."),
        ('Season peak week',
         "The peak week will be defined as the MMWR surveillance week that the weighted ILINet percentage is the highest "
         "for the 2016-2017 influenza season."),
        ('Season peak percentage',
         "The intensity will be defined as the highest numeric value that the weighted ILINet percentage reaches during " \
         "the 2016-2017 influenza season."),
        ('1 wk ahead', WEEK_AHEAD_DESCR),
        ('2 wk ahead', WEEK_AHEAD_DESCR),
        ('3 wk ahead', WEEK_AHEAD_DESCR),
        ('4 wk ahead', WEEK_AHEAD_DESCR)):
    Target.objects.create(project=p, name=target_name, description=descr)

# create the project's TimeZeros. b/c this is a CDC project, timezero_dates are all MMWR Week ENDING Dates as listed in
# MMWR_WEEK_TO_YEAR_TUPLE. note that the project has no data_version_dates
for mmwr_week in list(range(43, 53)) + list(range(1, 19)):  # [43, ..., 52, 1, ..., 18] for 2016-2017
    TimeZero.objects.create(project=p,
                            timezero_date=str(end_date_2016_2017_for_mmwr_week(mmwr_week)),
                            data_version_date=None)

#
# ---- create the four Kernel of Truth (KoT) ForecastModels and their Forecasts ----
#

# Set kot_data_dir. We assume the KOT_DATA_DIR is set to the cloned location of
# https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble e.g.,
KOT_DATA_DIR = Path(os.getenv('KOT_DATA_DIR', '~/IdeaProjects/split_kot_models_from_submissions')).expanduser()


def add_forecasts_to_model(forecast_model, kot_model_dir_name):
    """
    Adds Forecast objects to forecast_model based on kot_model_dir_name. Recall data file naming scheme:
        'EW<mmwr_week>-<team_name>-<sub_date_yyy_mm_dd>.csv'
    """
    print('add_forecasts_to_model', forecast_model, kot_model_dir_name)
    kot_model_dir = KOT_DATA_DIR / kot_model_dir_name
    if not Path(kot_model_dir).exists():
        raise RuntimeError("KOT_DATA_DIR does not exist: {}".format(KOT_DATA_DIR))

    for csv_file in [csv_file for csv_file in kot_model_dir.glob('*.csv')]:  # 'EW1-KoTstable-2017-01-17.csv'
        mmwr_week = csv_file.name.split('-')[0].split('EW')[1]  # re.split(r'^EW(\d*).*$', csv_file.name)[1]
        timezero_date = end_date_2016_2017_for_mmwr_week(int(mmwr_week))
        time_zero = forecast_model.time_zero_for_timezero_date_str(timezero_date)
        if not time_zero:
            raise RuntimeError("no time_zero found for timezero_date={}. csv_file={}, mmwr_week={}".format(
                timezero_date, csv_file, mmwr_week))

        print('  ', csv_file)
        forecast_model.load_forecast(csv_file, time_zero)


#
# KoT ensemble
#

forecast_model = ForecastModel.objects.create(
    project=p,
    name='KoT ensemble',
    description="Team Kernel of Truth's ensemble model.",
    url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
    auxiliary_data='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble')

add_forecasts_to_model(forecast_model, 'ensemble')

#
# KoT Kernel Density Estimation (KDE)
#

forecast_model = ForecastModel.objects.create(
    project=p,
    name='KoT KDE',
    description="Team Kernel of Truth's 'fixed' model using Kernel Density Estimation.",
    url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
    auxiliary_data='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/kde')

add_forecasts_to_model(forecast_model, 'kde')

#
# KoT Kernel Conditional Density Estimation (KCDE)
#

forecast_model = ForecastModel.objects.create(
    project=p,
    name='KoT KCDE',
    description="Team Kernel of Truth's model combining Kernel Conditional Density Estimation (KCDE) and copulas.",
    url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
    auxiliary_data='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/kcde')

add_forecasts_to_model(forecast_model, 'kcde')

#
# KoT SARIMA
#

forecast_model = ForecastModel.objects.create(
    project=p,
    name='KoT SARIMA',
    description="Team Kernel of Truth's SARIMA model.",
    url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
    auxiliary_data='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/sarima')

add_forecasts_to_model(forecast_model, 'sarima')

#
# ---- create the Predict the District Challenge project and targets ----
#

print('* creating Predict the District Challenge project and models...')

p = Project.objects.create(
    name='Predict the District Challenge',
    description="A Reich Lab challenge of predicting dengue fever in Thailand at the district level.",
    url='http://reichlab.io/guidelines.html',
    core_data='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/kcde',
    config_dict=config_dict)  # same config_dict as above

TEN_BIWEEK_DESCR = "The number of reported cases in each of the following 10 biweeks. If data is observed through " \
                   "time t then forecasts for times t+1, …, t+10 will be handed in. If time t falls within 10 " \
                   "biweeks of the end of the calendar year, still, the forecast should include 10 biweeks into the " \
                   "future."
# todo use exact target names to match data files' 'Target' columns:
TEN_BIWEEK_TARGETS = [('{} wk ahead'.format(biweek_num + 1), TEN_BIWEEK_DESCR) for biweek_num in range(10)]
for target_name, descr in TEN_BIWEEK_TARGETS + [
    ('Year Total',
     "The total number of reported cases for that calendar year. Unlike the biweekly incidence targets, this target "
     "is a single scalar value; the target does not change throughout the year."),
    ('Peak Incidence per Biweek',
     "Peak number of reported cases in a single biweek for that calendar year. Unlike the biweekly incidence targets, "
     "this target is a single scalar value; the target does not change throughout the year."),
    ('Biweek with Peak Incidence',
     "The biweek in which the peak incidence will occur. This is the only target whose set of units are not based on "
     "case incidence. The predictive distribution here is a discrete distribution that can take values between 1 and "
     "26."),
]:
    Target.objects.create(project=p, name=target_name, description=descr)

#
# create the project's TimeZeros. recall from http://reichlab.io/guidelines.html :
# - forecasts for each target must be submitted for each biweek in 2008 through 2013 (6 years)
# - 6 training set years * 26 biweeks/year = 156 separate files
# - file naming convention: "BW13-2016-TeamKCDE.csv"
#   = uses biweek 13 surveillance data from 2016 (BW13-2016 is the latest biweek and year of data used in the forecast)
#   = TeamKCDE is the abbreviated name of the team making the submission
#

for training_year in range(2008, 2013 + 1):
    for biweek in range(26):
        TimeZero.objects.create(project=p,
                                timezero_date=str(start_date_for_biweek(biweek, training_year)),
                                data_version_date=None)

#
# ---- create the ForecastModels and their Forecasts ----
#

for model_name, team_name in [('spatial model/HHH4', 'Harley'),
                              ('KCDE variant?', 'Casey'),
                              ('annual GAM?', 'Steve'),
                              ('TSIR w/serotype?', 'Xi'),
                              ('Prophet', 'facebook, Kristina'),
                              ('SARIMA?, Google?', 'MS student'),
                              ('Hierarchical, state space', '?')]:
    forecast_model = ForecastModel.objects.create(project=p, name=model_name, description="Team {}'s model.".format(
        team_name))

#
# ---- done ----
#

print('* done!')
