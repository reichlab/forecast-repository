import datetime
import os
from pathlib import Path

import django
from django.test import TestCase

from utils.CDCFile import CDCFile
from utils.cdc_format_utils import filename_components

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "forecast_repo.settings")
django.setup()


class DBLoaderTestCase(TestCase):
    """
    """

    def setUp(self):
        # NB: this file has a trailing ',' after ea row, which is treated as an additional row:
        self.stable_csv_path = Path('EW1-KoTstable-2017-01-17.csv')

    def test_filename_components(self):
        filename_component_tuples = (('EW1-KoTstable-2017-01-17.csv', (1, 'KoTstable', datetime.date(2017, 1, 17))),
                                     ('-KoTstable-2017-01-17.csv', ()),
                                     ('EW1--2017-01-17.csv', ()),
                                     ('EW1-KoTstable--01-17.csv', ()),
                                     ('EW1-KoTstable--01-17.txt', ()))
        for filename, component in filename_component_tuples:
            self.assertEqual(component, filename_components(filename))

    def test_cdc_file_object(self):
        cdc_file = CDCFile(self.stable_csv_path)
        self.assertEqual(cdc_file.csv_path, cdc_file.csv_path)
        self.assertEqual(11, len(cdc_file.locations))

        exp_loc_names = {'HHS Region 1', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4', 'HHS Region 5', 'HHS Region 6',
                         'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'HHS Region 10', 'US National'}
        self.assertEqual(exp_loc_names, {loc.name for loc in cdc_file.locations})

        # spot-check a Location
        us_natl_location = cdc_file.get_location('US National')
        self.assertEqual('US National', us_natl_location.name)
        self.assertEqual(7, len(us_natl_location.targets))

        exp_target_names = {'1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead', 'Season onset',
                            'Season peak percentage', 'Season peak week'}
        self.assertEqual(exp_target_names, {target.name for target in us_natl_location.targets})

        # spot-check a Target
        target = us_natl_location.get_target('Season onset')
        self.assertEqual('Season onset', target.name)
        self.assertEqual('week', target.unit)
        self.assertEqual(50.0012056690978, target.point)
        self.assertEqual(33, len(target.bins))

        # spot-check bin boundaries (recall these get sorted by constructor)
        start_end_val_tuples = [[1, 2, 9.7624532252505e-05],
                                [20, 21, 1.22490002826229e-07],
                                [40, 41, 1.95984004521967e-05],
                                [52, 53, 0.000147110493394302]]
        for start_end_val_tuple in start_end_val_tuples:
            self.assertIn(start_end_val_tuple, target.bins)

        # spot check data types:
        # - US National > Season onset: int int float: tested in previous assert (start_end_val_tuples)
        # - US National > Season peak percentage: float float float (except Bin_start_incl=0, 1, ...):
        target = us_natl_location.get_target('Season peak percentage')
        self.assertEqual([0, 0.1, 4.01898428400709e-07], target.bins[0])
        self.assertEqual([0.1, 0.2, 4.01898428400709e-07], target.bins[1])

    def test_other_file(self):
        # NB: this file's values are delimited with double quotes. also, it's bins and vals are ints
        sarima_csv_path = Path('EW1-KoTsarima-2017-01-17.csv')
        cdc_file = CDCFile(sarima_csv_path)
        self.assertEqual(11, len(cdc_file.locations))

        exp_loc_names = {'HHS Region 1', 'HHS Region 2', 'HHS Region 3', 'HHS Region 4', 'HHS Region 5', 'HHS Region 6',
                         'HHS Region 7', 'HHS Region 8', 'HHS Region 9', 'HHS Region 10', 'US National'}
        self.assertEqual(exp_loc_names, {loc.name for loc in cdc_file.locations})
