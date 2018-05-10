import json

import requests


# Django server memory cache that maps a 2-tuple (mmwr_year, mmwr_week) to the retrieved 'wili' value from Delphi. keys
# are ints. managed by delphi_wili_for_mmwr_year_week()
DELPHI_MMWR_YEAR_AND_WEEK_TO_WILI = {}


def delphi_wili_for_mmwr_year_week(project, mmwr_year, mmwr_week, location):
    """
    Looks up the fluview 'wili' value for the past args, using the delphi REST API. FluView Parameters (source=fluview)
    from https://github.com/cmu-delphi/delphi-epidata#the-api :
    - Required:
      = epiweeks: a list of epiweeks - Epiweeks use the U.S. definition. That is, the first epiweek each mmwr_year is
        the week, starting on a Sunday, containing January 4 - http://www.cmmcp.org/epiweek.htm . HOWEVER, we use this
        more specific (and comprehensive - it correctly has EW 53 in some cases):
        https://ibis.health.state.nm.us/resource/MMWRWeekCalendar.html
      = regions: a list of region labels - https://github.com/cmu-delphi/delphi-epidata/blob/master/labels/regions.txt
        -> nat hhs1 hhs2 hhs3 hhs4 hhs5 hhs6 hhs7 hhs8 hhs9 hhs10 cen1 cen2 cen3 cen4 cen5 cen6 cen7 cen8 cen9
    - Optional:
      = issues: a list of epiweeks
      = lag: a number of weeks
      = auth: the password for private imputed data

    :param: project: the Project whose config_dict['location_to_delphi_region'] is to be used to find the region that's
        being looked up
    :param: mmwr_year: MMWR year
    :param: mmwr_week: MMWR week number between 1 and 53 inclusive
    :param: location: project location name. used to look up the delphi region via Project.region_for_location_name()
    :return: true/actual wili value for the passed mmwr_year and week, using the delphi REST API. Returns as a float -
        see: https://github.com/cmu-delphi/delphi-epidata#fluview . Returns None if the true value is not found.
        Caches the retrieved value for speed-ups. NB: caching means that the values in server memory won't be updated if
        they change on delphi.midas.cs.cmu.edu , i.e., they could become stale and need flushing.
    """
    region = project.get_region_for_location_name(location)
    if not region:
        raise RuntimeError("could not find region for Delphi location: {}".format(location))

    if (mmwr_year, mmwr_week) in DELPHI_MMWR_YEAR_AND_WEEK_TO_WILI:
        return DELPHI_MMWR_YEAR_AND_WEEK_TO_WILI[(mmwr_year, mmwr_week)]
    else:  # cache entire mmwr_year (requires only one lookup using a Delphi range)
        url = 'https://delphi.midas.cs.cmu.edu/epidata/api.php' \
              '?source=fluview' \
              '&regions={region}' \
              '&epiweeks={epi_year}01-{epi_year}53'. \
            format(region=region, epi_year=mmwr_year)
        response = requests.get(url)
        response.raise_for_status()  # does nothing if == requests.codes.ok
        delphi_dict = json.loads(response.text)
        for epidata_dict in delphi_dict['epidata']:
            epiweek_val = str(epidata_dict['epiweek'])
            epi_year = int(epiweek_val[:4])
            epi_week = int(epiweek_val[4:])
            wili_val = epidata_dict['wili']
            DELPHI_MMWR_YEAR_AND_WEEK_TO_WILI[(epi_year, epi_week)] = wili_val
    if (mmwr_year, mmwr_week) not in DELPHI_MMWR_YEAR_AND_WEEK_TO_WILI:  # truth for this year and week not available
        DELPHI_MMWR_YEAR_AND_WEEK_TO_WILI[(mmwr_year, mmwr_week)] = None
    return DELPHI_MMWR_YEAR_AND_WEEK_TO_WILI[(mmwr_year, mmwr_week)]
