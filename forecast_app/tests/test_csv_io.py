import csv
import json
from unittest import TestCase

from utils.csv_io import json_io_dict_from_csv_rows
from utils.project_queries import CSV_HEADER


class CsvIOTestCase(TestCase):
    """
    Tests conversion between "JSON IO dict" and zoltar CSV files.
    """


    def test_json_io_dict_from_csv_rows(self):
        # no header
        with self.assertRaises(RuntimeError) as context:
            json_io_dict_from_csv_rows([])
        self.assertIn('first row was not the proper header', str(context.exception))

        # bad header
        with self.assertRaises(RuntimeError) as context:
            json_io_dict_from_csv_rows([['bad header']])
        self.assertIn('first row was not the proper header', str(context.exception))

        # > one row for named and point prediction types
        with self.assertRaises(RuntimeError) as context:
            json_io_dict_from_csv_rows([CSV_HEADER,
                                        ['loc1', 'pct next week', 'point', 2.1, '', '', '', '', '', '', '', ''],
                                        ['loc1', 'pct next week', 'point', 2.1, '', '', '', '', '', '', '', '']])
        self.assertIn('not exactly one row for point class', str(context.exception))

        with self.assertRaises(RuntimeError) as context:
            json_io_dict_from_csv_rows([CSV_HEADER,
                                        ['loc1', 'cases next week', 'named', '', '', '', '', '', 'pois', 1.1, '', ''],
                                        ['loc1', 'cases next week', 'named', '', '', '', '', '', 'pois', 1.1, '', '']])
        self.assertIn('not exactly one row for named class', str(context.exception))

        # wrong non-empty column combinations
        row_req_idxs = [  # indexes are copied from code - all need offsetting by 3 for unit, target, and pred_class
            (['loc2', 'pct next week', 'bin', '', 1.1, 0.3, '', '', '', '', '', ''], [1, 2]),
            (['loc1', 'cases next week', 'named', '', '', '', '', '', 'pois', 1.1, '', ''], [5, 6]),
            (['loc1', 'pct next week', 'point', 2.1, '', '', '', '', '', '', '', ''], [0]),
            (['loc3', 'pct next week', 'sample', '', '', '', 2.3, '', '', '', '', ''], [3]),
            (['loc2', 'pct next week', 'quantile', 1.0, '', '', '', 0.025, '', '', '', ''], [0, 4])]
        for row, required_idxs in row_req_idxs:
            for required_idx in required_idxs:
                required_idx += 3  # offset for validate_empties() call
                bad_row = [str(_) for _ in row]  # csv comes in as strs
                bad_row[required_idx] = ''
                with self.assertRaises(RuntimeError) as context:
                    json_io_dict_from_csv_rows([CSV_HEADER, bad_row])
                self.assertIn('row missing required value', str(context.exception))

        # no data rows
        self.assertEqual({'meta': {}, 'predictions': []}, json_io_dict_from_csv_rows([CSV_HEADER]))

        # blue sky - no retractions
        with open('forecast_app/tests/predictions/docs-predictions.csv') as csv_fp, \
                open('forecast_app/tests/predictions/docs-predictions.json') as json_fp:
            exp_dict = json.load(json_fp)
            act_dict = json_io_dict_from_csv_rows(list(csv.reader(csv_fp)))
            self.assertEqual(sorted(exp_dict['predictions'], key=lambda _: (_['unit'], _['target'], _['class'])),
                             sorted(act_dict['predictions'], key=lambda _: (_['unit'], _['target'], _['class'])))

        # blue sky - retractions
        with open('forecast_app/tests/predictions/docs-predictions-all-retracted.csv') as csv_fp, \
                open('forecast_app/tests/predictions/docs-predictions-all-retracted.json') as json_fp:
            exp_dict = json.load(json_fp)
            act_dict = json_io_dict_from_csv_rows(list(csv.reader(csv_fp)))
            self.assertEqual(sorted(exp_dict['predictions'], key=lambda _: (_['unit'], _['target'], _['class'])),
                             sorted(act_dict['predictions'], key=lambda _: (_['unit'], _['target'], _['class'])))
