import datetime
from pathlib import Path

import django
import os
from django.test import TestCase

from utils.cdc_format_utils import filename_components, get_locations


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
django.setup()


class DBLoaderTestCase(TestCase):
    """
    """


    def setUp(self):
        self.ew1_csv_path = Path('~/IdeaProjects/forecast-repository/forecast_app/tests/'
                                 'EW1-KoTstable-2017-01-17.csv').expanduser()


    def test_filename_components(self):
        filename_component_tuples = (('EW1-KoTstable-2017-01-17.csv', (1, 'KoTstable', datetime.date(2017, 1, 17))),
                                     ('-KoTstable-2017-01-17.csv', ()),
                                     ('EW1--2017-01-17.csv', ()),
                                     ('EW1-KoTstable--01-17.csv', ()),
                                     ('EW1-KoTstable--01-17.txt', ()))
        for filename, component in filename_component_tuples:
            self.assertEqual(component, filename_components(filename))


    def test_locations(self):
        exp_loc_names = {'HHS Region 1', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4', 'HHS Region 5', 'HHS Region 6',
                         'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'HHS Region 10', 'US National'}
        self.assertEqual(exp_loc_names, {loc.name for loc in get_locations(self.ew1_csv_path)})


    def test_xx(self):
        self.fail()  # todo xx
