import csv
import datetime

from pathlib import Path


# https://ibis.health.state.nm.us/resource/MMWRWeekCalendar.html
# MMWR Week -> ENDING Dates for MMWR Weeks (Weeks start on Sunday and ends on Saturday with this date)
# 16 columns. header: MMWR_Week	2006_end_date	2007_end_date	2008_end_date	2009_end_date	2010_end_date	2011_end_date	2012_end_date	2013_end_date	2014_end_date	2015_end_date	2016_end_date	2017_end_date	2018_end_date	2019_end_date	2020_end_date
MMWR_DATA_FILE = Path('utils/mmwr-calendar-dates-2006-2020.csv').expanduser()


def make_mmwr_week_to_year_tuple():
    """
    :return: a dict that maps MMWR week_number -> a 15-tuple with the columns above, excluding the first (MMWR_Week)
    """
    mmwr_week_to_year_tuple = {}
    with open(str(MMWR_DATA_FILE)) as input_csv_file_fp:
        input_csv_reader = csv.reader(input_csv_file_fp, dialect=csv.excel_tab)
        next(input_csv_reader, None)  # skip header
        for row in input_csv_reader:
            mmwr_week_to_year_tuple[int(row[0])] = row[1:]
    return mmwr_week_to_year_tuple


MMWR_WEEK_TO_YEAR_TUPLE = make_mmwr_week_to_year_tuple()


def end_date_2016_2017_for_mmwr_week(mmwr_week):  # ex: 43. assumes 40-52 = 2016, 2017 o/w
    week_num_2006_20_tuple = MMWR_WEEK_TO_YEAR_TUPLE[mmwr_week]
    m_d_y = week_num_2006_20_tuple[11 if mmwr_week < 40 else 10]  # column 10: 2016, 11: 2017
    year = int(m_d_y.split('-')[0])
    month = int(m_d_y.split('-')[1])
    day = int(m_d_y.split('-')[2])
    return datetime.date(year, month, day)
