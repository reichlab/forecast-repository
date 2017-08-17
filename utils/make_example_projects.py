import datetime
# set up django. must be done before loading models. requires: os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
from pathlib import Path

import django
import os


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
django.setup()

from forecast_app.models import DataFile, Project, Target, TimeZero, ForecastModel, Forecast


#
# ---- print and delete (!) all user objects ----
#

print('* current database')
for model_class in [DataFile, Project, Target, TimeZero, ForecastModel, Forecast]:
    print('-', model_class)
    for instance in model_class.objects.all():
        print('  =', str(instance))

print('* deleting database...')
for model_class in [DataFile, Project, Target, TimeZero, ForecastModel, Forecast]:
    model_class.objects.all().delete()

#
# --- MMWR utils ----
#

# https://ibis.health.state.nm.us/resource/MMWRWeekCalendar.html
# MMWR Week -> ENDING Dates for MMWR Weeks (Weeks start on Sunday and ends on Saturday with this date)
MMWR_WEEK_TO_2016_17_TUPLE = {  # {week_number: (2016 date, 2017 date), ...}
    1: ('1/9/2016', '1/7/2017'),
    2: ('1/16/2016', '1/14/2017'),
    3: ('1/23/2016', '1/21/2017'),
    4: ('1/30/2016', '1/28/2017'),
    5: ('2/6/2016', '2/4/2017'),
    6: ('2/13/2016', '2/11/2017'),
    7: ('2/20/2016', '2/18/2017'),
    8: ('2/27/2016', '2/25/2017'),
    9: ('3/5/2016', '3/4/2017'),
    10: ('3/12/2016', '3/11/2017'),
    11: ('3/19/2016', '3/18/2017'),
    12: ('3/26/2016', '3/25/2017'),
    13: ('4/2/2016', '4/1/2017'),
    14: ('4/9/2016', '4/8/2017'),
    15: ('4/16/2016', '4/15/2017'),
    16: ('4/23/2016', '4/22/2017'),
    17: ('4/30/2016', '4/29/2017'),
    18: ('5/7/2016', '5/6/2017'),
    19: ('5/14/2016', '5/13/2017'),
    20: ('5/21/2016', '5/20/2017'),
    21: ('5/28/2016', '5/27/2017'),
    22: ('6/4/2016', '6/3/2017'),
    23: ('6/11/2016', '6/10/2017'),
    24: ('6/18/2016', '6/17/2017'),
    25: ('6/25/2016', '6/24/2017'),
    26: ('7/2/2016', '7/1/2017'),
    27: ('7/9/2016', '7/8/2017'),
    28: ('7/16/2016', '7/15/2017'),
    29: ('7/23/2016', '7/22/2017'),
    30: ('7/30/2016', '7/29/2017'),
    31: ('8/6/2016', '8/5/2017'),
    32: ('8/13/2016', '8/12/2017'),
    33: ('8/20/2016', '8/19/2017'),
    34: ('8/27/2016', '8/26/2017'),
    35: ('9/3/2016', '9/2/2017'),
    36: ('9/10/2016', '9/9/2017'),
    37: ('9/17/2016', '9/16/2017'),
    38: ('9/24/2016', '9/23/2017'),
    39: ('10/1/2016', '9/30/2017'),
    40: ('10/8/2016', '10/7/2017'),
    41: ('10/15/2016', '10/14/2017'),
    42: ('10/22/2016', '10/21/2017'),
    43: ('10/29/2016', '10/28/2017'),
    44: ('11/5/2016', '11/4/2017'),
    45: ('11/12/2016', '11/11/2017'),
    46: ('11/19/2016', '11/18/2017'),
    47: ('11/26/2016', '11/25/2017'),
    48: ('12/3/2016', '12/2/2017'),
    49: ('12/10/2016', '12/9/2017'),
    50: ('12/17/2016', '12/16/2017'),
    51: ('12/24/2016', '12/23/2017'),
    52: ('12/31/2016', '12/30/2017'),
    53: ('', ''),
}


def mmwr_week_to_end_date_2016_2017(mmwr_week):  # ex: 43
    # assumes 40-52 = 2016, 2017 o/w
    week_num_2016_17_tuple = MMWR_WEEK_TO_2016_17_TUPLE[mmwr_week]
    m_d_y = week_num_2016_17_tuple[1 if mmwr_week < 40 else 0]  # column 0: 2016, 1: 2017
    month = int(m_d_y.split('/')[0])
    day = int(m_d_y.split('/')[1])
    year = int(m_d_y.split('/')[2])
    return datetime.date(year, month, day)


#
# ---- create the CDC Flu challenge (2016-2017) project and targets ----
#

print('* creating project and models...')

p = Project.objects.create(
    name='CDC Flu challenge (2016-2017)',
    description='Code, results, submissions, and method description for the 2016-2017 CDC flu contest submissions '
                'based on ensembles.',
    url='https://github.com/reichlab/2016-2017-flu-contest-ensembles')

for target_name in ['Season onset', 'Season peak week', 'Season peak percentage', '1 wk ahead', '2 wk ahead',
                    '3 wk ahead', '4 wk ahead']:
    Target.objects.create(project=p, name=target_name, description='{} description TBD'.format(target_name))

# create the project's TimeZeros. b/c this is a CDC project, timezero_dates are all MMWR Week ENDING Dates as listed in
# MMWR_WEEK_TO_2016_17_TUPLE. xx. note that the project has no version_dates
for mmwr_week in list(range(43, 53)) + list(range(1, 19)):  # [43, ..., 52, 1, ..., 18] for 2016-2017
    TimeZero.objects.create(project=p,
                            timezero_date=str(mmwr_week_to_end_date_2016_2017(mmwr_week)),
                            version_date=None)

#
# ---- create the four Kernel of Truth (KoT) ForecastModels and their Forecasts ----
#


#
# KoT ensemble
#

df = DataFile.objects.create(
    location='https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble',
    file_type='d')

fm = ForecastModel.objects.create(
    project=p,
    name='KoT ensemble',
    description='Team Kernel of Truth is submitting predictions from an ensemble model.',
    url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
    auxiliary_data=df)

# make the Forecasts. recall data file naming scheme: 'EW<mmwr_week>-<team_name>-<sub_date_yyy_mm_dd>.csv'
# we assume the KOT_DATA_DIR is set to the cloned location of
# https://github.com/matthewcornell/split_kot_models_from_submissions/tree/master/ensemble e.g.,
#   $ export KOT_DATA_DIR=/Users/cornell/IdeaProjects/split_kot_models_from_submissions
kot_data_dir = Path('/Users/cornell/IdeaProjects/split_kot_models_from_submissions')
kot_model_dir_name = 'ensemble'
kot_model_dir = kot_data_dir / kot_model_dir_name

for csv_file in [csv_file for csv_file in kot_model_dir.glob('*.csv')]:  # 'EW1-KoTstable-2017-01-17.csv'
    mmwr_week = csv_file.name.split('-')[0].split('EW')[1]  # re.split(r'^EW(\d*).*$', csv_file.name)[1]
    timezero_date = mmwr_week_to_end_date_2016_2017(int(mmwr_week))
    time_zero = fm.time_zero_for_timezero_date_str(timezero_date)
    csv_df = DataFile.objects.create(location=csv_file, file_type='c')
    Forecast.objects.create(forecast_model=fm, time_zero=time_zero, data=csv_df)

#
# KoT Kernel Density Estimation (KDE)
#

# same as above, but:
kot_model_dir_name = 'kde'
fm=xx

#
# KoT Kernel Conditional Density Estimation (KCDE)
#

xx  # todo

#
# KoT SARIMA
#

xx  # todo

#
# ---- done ----
#

print('* done!')
