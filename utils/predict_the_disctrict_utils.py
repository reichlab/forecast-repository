import datetime

#
#  from `Appendix: Definition of biweeks` in http://reichlab.io/guidelines.html :
#
# "The following table is a map of Julian calendar days to biweeks used in data aggregation. Columns show the date a
# biweek starts and the duration for non-leap (``reg’’) and leap years."
#
# biweek, reg_yr_datestart ('mm dd'), reg_yr_dur, leap_yr_datestart ('mm dd'), leap_yr_dur
# NB: biweeks start at 1:
#
BIWEEK_TABLE_TEXT = """ 1	01 01	14	01 01	14
2	01 15	14	01 15	14
3	01 29	14	01a 29	14
4	02 12	14	02 12	14
5	02 26	14	02 26	15
6	03 12	14	03 12	14
7	03 26	14	03 26	14
8	04 09	14	04 09	14
9	04 23	14	04 23	14
10	05 07	14	05 07	14
11	05 21	14	05 21	14
12	06 04	14	06 04	14
13	06 18	14	06 18	14
14	07 02	14	07 02	14
15	07 16	14	07 16	14
16	07 30	14	07 30	14
17	08 13	14	08 13	14
18	08 27	14	08 27	14
19	09 10	14	09 10	14
20	09 24	14	09 24	14
21	10 08	14	10 08	14
22	10 22	14	10 22	14
23	11 05	14	11 05	14
24	11 19	14	11 19	14
25	12 03	14	12 03	14
26	12 17	15	12 17	15 """


def make_biweek_to_date_start_tuple():
    """
    :return: a zero-indexed dict that maps biweek_number -> a 4-tuple with the columns above, excluding the first
        (biweek)
    """
    biweek_to_date_start_tuple = {}
    for row in BIWEEK_TABLE_TEXT.split('\n'):
        tab_split = row.split('\t')
        biweek_to_date_start_tuple[int(tab_split[0]) - 1] = tab_split[1:]
    return biweek_to_date_start_tuple


BIWEEK_TO_DATE_START_TUPLE = make_biweek_to_date_start_tuple()


def start_date_for_biweek(biweek, year):
    """
    :param biweek b/w 0 and 25
    :return: datetime.date for biweek in year. note that we ignore the leap_yr columns b/c we don't deal with duration
        information
   """
    date_start_tuple = BIWEEK_TO_DATE_START_TUPLE[
        biweek]  # reg_yr_datestart, reg_yr_dur, leap_yr_datestart, leap_yr_dur
    reg_yr_datestart_pair = date_start_tuple[0]
    month = reg_yr_datestart_pair.split(' ')[0]
    day = reg_yr_datestart_pair.split(' ')[1]
    return datetime.date(year, int(month), int(day))
