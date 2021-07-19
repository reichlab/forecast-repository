import csv
import logging
import tempfile
import unittest
from pathlib import Path

from django.db import connection
from django.test import TestCase

from utils.bulk_data_dump import bulk_data_dump
from utils.make_minimal_projects import _make_docs_project
from utils.utilities import get_or_create_super_po_mo_users


logging.getLogger().setLevel(logging.ERROR)


@unittest.skipIf(connection.vendor != 'postgresql', "bulk data dump does not support sqlite3")
class BulkQueryTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        _, _, cls.po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        cls.project, cls.time_zero, cls.forecast_model, cls.forecast = _make_docs_project(cls.po_user)


    def test_dump_entire_db(self):
        """
        REF: _make_docs_project() content:

        project (from docs-project.json):
        - three units
        - five targets
        - three timezeros
        - two models: 'oracle', 'docs_mod'

        'oracle' model:
        - three forecasts (three timezeros in docs-ground-truth.csv)
            = 2011-10-02: 5 prediction elements (all points)
                location1 | pct next week    | point
                location1 | cases next week  | point
                location1 | season severity  | point
                location1 | above baseline   | point
                location1 | Season peak week | point
            = 2011-10-09: 5 ""
                location2 | pct next week    | point
                location2 | cases next week  | point
                location2 | season severity  | point
                location2 | above baseline   | point
                location2 | Season peak week | point
            = 2011-10-16: 4 ""
                location1 | pct next week    | point
                location1 | cases next week  | point
                location1 | above baseline   | point
                location1 | Season peak week | point

        'docs_mod' model:
        - one forecast (from docs-predictions.json):
            = 29 prediction elements:
                location1 | pct next week | point
                location1 | pct next week | named
                location2 | pct next week | named
                location2 | pct next week | bin
                location2 | pct next week | quantile
                location3 | pct next week | point
                location3 | pct next week | sample

                location1 | cases next week | named
                location2 | cases next week | point
                location2 | cases next week | sample
                location3 | cases next week | point
                location3 | cases next week | bin
                location3 | cases next week | quantile

                location1 | season severity | point
                location1 | season severity | bin
                location2 | season severity | point
                location2 | season severity | sample

                location1 | above baseline | point
                location2 | above baseline | bin
                location2 | above baseline | sample
                location3 | above baseline | sample

                location1 | Season peak week | point
                location1 | Season peak week | bin
                location1 | Season peak week | sample
                location2 | Season peak week | point
                location2 | Season peak week | bin
                location2 | Season peak week | quantile
                location3 | Season peak week | point
                location3 | Season peak week | sample
        """
        query = {}
        exp_csv_file_to_num_rows = {
            'forecast.csv': 4,
            'predictiondata.csv': 29 + 5 + 5 + 4,
            'predictionelement.csv': 29 + 5 + 5 + 4,
            'project.csv': 1,
            'forecastmodel.csv': 2,
            'unit.csv': 3,
            'target.csv': 5,
            'timezero.csv': 3,
        }
        self._test_dump_db(exp_csv_file_to_num_rows, query)


    def test_dump_partial_all_options(self):
        # test dumping partial database, constraining all options. this combination results in one matching prediction
        # element from one forecast
        query = {'models': ['docs_mod'],
                 'units': ['location1'],
                 'targets': ['above baseline'],
                 'timezeros': ['2011-10-02']}
        exp_csv_file_to_num_rows = {
            'forecast.csv': 1,
            'predictiondata.csv': 1,
            'predictionelement.csv': 1,
            'project.csv': 1,
            'forecastmodel.csv': 1,
            'unit.csv': 1,
            'target.csv': 1,
            'timezero.csv': 1,
        }
        self._test_dump_db(exp_csv_file_to_num_rows, query)


    def test_dump_partial_unit_option(self):
        # test dumping partial database, constraining all options. this combination results in 18 matching prediction
        # elements from 3 forecasts
        query = {'units': ['location1']}
        exp_csv_file_to_num_rows = {
            'forecast.csv': 3,
            'predictiondata.csv': 18,
            'predictionelement.csv': 18,
            'project.csv': 1,
            'forecastmodel.csv': 2,
            'unit.csv': 1,
            'target.csv': 5,
            'timezero.csv': 3,
        }
        self._test_dump_db(exp_csv_file_to_num_rows, query)


    def _test_dump_db(self, exp_csv_file_to_num_rows, query):
        exp_file_names = ['forecast.csv', 'forecastmodel.csv', 'predictiondata.csv', 'predictionelement.csv',
                          'project.csv', 'target.csv', 'timezero.csv', 'unit.csv']
        with tempfile.TemporaryDirectory() as temp_csv_dir:
            bulk_data_dump(self.project, query, temp_csv_dir)
            temp_csv_dir = Path(temp_csv_dir)
            csv_files = list(temp_csv_dir.glob('*.csv'))

            # test all the files are present
            self.assertEqual(exp_file_names, sorted([p.name for p in csv_files]))

            # test each file's row count, but not actual rows - we trust they're OK
            for csv_file in csv_files:
                with open(csv_file) as csv_file_fp:
                    csv_reader = csv.reader(csv_file_fp)
                    csv_rows = list(csv_reader)
                    self.assertEqual(exp_csv_file_to_num_rows[csv_file.name], len(csv_rows))
