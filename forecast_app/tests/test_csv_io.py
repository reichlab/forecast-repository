import csv
import json
from unittest import TestCase

from utils.csv_io import csv_rows_from_json_io_dict, json_io_dict_from_csv_rows
from utils.project_queries import CSV_HEADER


class CsvIOTestCase(TestCase):
    """
    Tests conversion between "JSON IO dict" and zoltar CSV files.
    """


    def test_csv_rows_from_json_io_dict(self):
        # invalid prediction class. ok: forecast-repository.utils.forecast.PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS
        with self.assertRaises(RuntimeError) as context:
            json_io_dict = {'meta': {'targets': []},
                            'predictions': [{'class': 'InvalidClass'}]}
            csv_rows_from_json_io_dict(json_io_dict)
        self.assertIn('invalid prediction_dict class', str(context.exception))

        # blue sky - no retractions. note that we hard-code the rows here instead of loading from an expected csv file
        # b/c the latter reads all values as strings, which means we'd have to cast types based on target. it became too
        # painful :-)
        exp_rows = [
            CSV_HEADER,
            ['loc1', 'pct next week', 'point', 2.1, '', '', '', '', '', '', '', ''],
            ['loc1', 'pct next week', 'mean', 2.11, '', '', '', '', '', '', '', ''],
            ['loc1', 'pct next week', 'median', 2.12, '', '', '', '', '', '', '', ''],
            ['loc1', 'pct next week', 'mode', 2.13, '', '', '', '', '', '', '', ''],
            ['loc1', 'pct next week', 'named', '', '', '', '', '', 'norm', 1.1, 2.2, ''],
            ['loc2', 'pct next week', 'point', 2.0, '', '', '', '', '', '', '', ''],
            ['loc2', 'pct next week', 'bin', '', 1.1, 0.3, '', '', '', '', '', ''],
            ['loc2', 'pct next week', 'bin', '', 2.2, 0.2, '', '', '', '', '', ''],
            ['loc2', 'pct next week', 'bin', '', 3.3, 0.5, '', '', '', '', '', ''],
            ['loc2', 'pct next week', 'quantile', 1.0, '', '', '', 0.025, '', '', '', ''],
            ['loc2', 'pct next week', 'quantile', 2.2, '', '', '', 0.25, '', '', '', ''],
            ['loc2', 'pct next week', 'quantile', 2.2, '', '', '', 0.5, '', '', '', ''],
            ['loc2', 'pct next week', 'quantile', 5.0, '', '', '', 0.75, '', '', '', ''],
            ['loc2', 'pct next week', 'quantile', 50.0, '', '', '', 0.975, '', '', '', ''],
            ['loc3', 'pct next week', 'point', 3.567, '', '', '', '', '', '', '', ''],
            ['loc3', 'pct next week', 'sample', '', '', '', 2.3, '', '', '', '', ''],
            ['loc3', 'pct next week', 'sample', '', '', '', 6.5, '', '', '', '', ''],
            ['loc3', 'pct next week', 'sample', '', '', '', 0.0, '', '', '', '', ''],
            ['loc3', 'pct next week', 'sample', '', '', '', 10.0234, '', '', '', '', ''],
            ['loc3', 'pct next week', 'sample', '', '', '', 0.0001, '', '', '', '', ''],
            ['loc1', 'cases next week', 'named', '', '', '', '', '', 'pois', 1.1, '', ''],
            ['loc2', 'cases next week', 'point', 5, '', '', '', '', '', '', '', ''],
            ['loc2', 'cases next week', 'sample', '', '', '', 0, '', '', '', '', ''],
            ['loc2', 'cases next week', 'sample', '', '', '', 2, '', '', '', '', ''],
            ['loc2', 'cases next week', 'sample', '', '', '', 5, '', '', '', '', ''],
            ['loc3', 'cases next week', 'point', 10, '', '', '', '', '', '', '', ''],
            ['loc3', 'cases next week', 'bin', '', 0, 0.0, '', '', '', '', '', ''],
            ['loc3', 'cases next week', 'bin', '', 2, 0.1, '', '', '', '', '', ''],
            ['loc3', 'cases next week', 'bin', '', 50, 0.9, '', '', '', '', '', ''],
            ['loc3', 'cases next week', 'quantile', 0, '', '', '', 0.25, '', '', '', ''],
            ['loc3', 'cases next week', 'quantile', 50, '', '', '', 0.75, '', '', '', ''],
            ['loc1', 'season severity', 'point', 'mild', '', '', '', '', '', '', '', ''],
            ['loc1', 'season severity', 'bin', '', 'mild', 0.0, '', '', '', '', '', ''],
            ['loc1', 'season severity', 'bin', '', 'moderate', 0.1, '', '', '', '', '', ''],
            ['loc1', 'season severity', 'bin', '', 'severe', 0.9, '', '', '', '', '', ''],
            ['loc2', 'season severity', 'point', 'moderate', '', '', '', '', '', '', '', ''],
            ['loc2', 'season severity', 'sample', '', '', '', 'moderate', '', '', '', '', ''],
            ['loc2', 'season severity', 'sample', '', '', '', 'severe', '', '', '', '', ''],
            ['loc2', 'season severity', 'sample', '', '', '', 'high', '', '', '', '', ''],
            ['loc2', 'season severity', 'sample', '', '', '', 'moderate', '', '', '', '', ''],
            ['loc2', 'season severity', 'sample', '', '', '', 'mild', '', '', '', '', ''],
            ['loc1', 'above baseline', 'point', True, '', '', '', '', '', '', '', ''],
            ['loc2', 'above baseline', 'bin', '', True, 0.9, '', '', '', '', '', ''],
            ['loc2', 'above baseline', 'bin', '', False, 0.1, '', '', '', '', '', ''],
            ['loc2', 'above baseline', 'sample', '', '', '', True, '', '', '', '', ''],
            ['loc2', 'above baseline', 'sample', '', '', '', False, '', '', '', '', ''],
            ['loc2', 'above baseline', 'sample', '', '', '', True, '', '', '', '', ''],
            ['loc3', 'above baseline', 'sample', '', '', '', False, '', '', '', '', ''],
            ['loc3', 'above baseline', 'sample', '', '', '', True, '', '', '', '', ''],
            ['loc3', 'above baseline', 'sample', '', '', '', True, '', '', '', '', ''],
            ['loc1', 'Season peak week', 'point', '2019-12-22', '', '', '', '', '', '', '', ''],
            ['loc1', 'Season peak week', 'bin', '', '2019-12-15', 0.01, '', '', '', '', '', ''],
            ['loc1', 'Season peak week', 'bin', '', '2019-12-22', 0.1, '', '', '', '', '', ''],
            ['loc1', 'Season peak week', 'bin', '', '2019-12-29', 0.89, '', '', '', '', '', ''],
            ['loc1', 'Season peak week', 'sample', '', '', '', '2020-01-05', '', '', '', '', ''],
            ['loc1', 'Season peak week', 'sample', '', '', '', '2019-12-15', '', '', '', '', ''],
            ['loc2', 'Season peak week', 'point', '2020-01-05', '', '', '', '', '', '', '', ''],
            ['loc2', 'Season peak week', 'bin', '', '2019-12-15', 0.01, '', '', '', '', '', ''],
            ['loc2', 'Season peak week', 'bin', '', '2019-12-22', 0.05, '', '', '', '', '', ''],
            ['loc2', 'Season peak week', 'bin', '', '2019-12-29', 0.05, '', '', '', '', '', ''],
            ['loc2', 'Season peak week', 'bin', '', '2020-01-05', 0.89, '', '', '', '', '', ''],
            ['loc2', 'Season peak week', 'quantile', '2019-12-22', '', '', '', 0.5, '', '', '', ''],
            ['loc2', 'Season peak week', 'quantile', '2019-12-29', '', '', '', 0.75, '', '', '', ''],
            ['loc2', 'Season peak week', 'quantile', '2020-01-05', '', '', '', 0.975, '', '', '', ''],
            ['loc3', 'Season peak week', 'point', '2019-12-29', '', '', '', '', '', '', '', ''],
            ['loc3', 'Season peak week', 'sample', '', '', '', '2020-01-06', '', '', '', '', ''],
            ['loc3', 'Season peak week', 'sample', '', '', '', '2019-12-16', '', '', '', '', '']]
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            act_rows = csv_rows_from_json_io_dict(json_io_dict)
        self.assertEqual(exp_rows, act_rows)

        # blue sky - retractions
        with open('forecast_app/tests/predictions/docs-predictions-all-retracted.json') as json_fp, \
                open('forecast_app/tests/predictions/docs-predictions-all-retracted.csv') as csv_fp:
            json_io_dict = json.load(json_fp)
            exp_rows = list(csv.reader(csv_fp))
        self.assertEqual(exp_rows, csv_rows_from_json_io_dict(json_io_dict))


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
