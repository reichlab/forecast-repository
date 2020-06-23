import logging

from django.test import TestCase

from utils.forecast import data_rows_from_forecast
from utils.make_minimal_projects import _make_docs_project
from utils.utilities import get_or_create_super_po_mo_users


logging.getLogger().setLevel(logging.ERROR)


class ForecastUtilTestCase(TestCase):
    """
    """


    # def test_load_predictions_from_json_io_dict(self):
    #     # NB: `load_predictions_from_json_io_dict` is essentially tested in many other places
    #     pass


    def test_data_rows_from_forecast(self):
        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        unit_loc1 = project.units.filter(name='location1').first()
        unit_loc2 = project.units.filter(name='location2').first()
        unit_loc3 = project.units.filter(name='location3').first()
        target_pct_next_week = project.targets.filter(name='pct next week').first()
        target_cases_next_week = project.targets.filter(name='cases next week').first()
        target_season_severity = project.targets.filter(name='season severity').first()
        target_above_baseline = project.targets.filter(name='above baseline').first()
        target_season_peak_week = project.targets.filter(name='Season peak week').first()

        # rows: 5-tuple: (data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample)
        loc_targ_to_exp_rows = {
            (unit_loc1, target_pct_next_week): ([],
                                                [('location1', 'pct next week', 'norm', 1.1, 2.2, None)],  # named
                                                [('location1', 'pct next week', 2.1)],  # point
                                                [], []),
            (unit_loc1, target_cases_next_week): ([],
                                                  [('location1', 'cases next week', 'pois', 1.1, None, None)],  # named
                                                  [], [], []),
            (unit_loc1, target_season_severity): ([('location1', 'season severity', 0.1, 'moderate'),  # bin
                                                   ('location1', 'season severity', 0.9, 'severe')],
                                                  [],
                                                  [('location1', 'season severity', 'mild')],  # point
                                                  [], []),
            (unit_loc1, target_above_baseline): ([], [],
                                                 [('location1', 'above baseline', True)],  # point
                                                 [], []),
            (unit_loc1, target_season_peak_week): ([('location1', 'Season peak week', 0.01, '2019-12-15'),  # bin
                                                    ('location1', 'Season peak week', 0.1, '2019-12-22'),
                                                    ('location1', 'Season peak week', 0.89, '2019-12-29')],
                                                   [],
                                                   [('location1', 'Season peak week', '2019-12-22')],  # point
                                                   [],
                                                   [('location1', 'Season peak week', '2020-01-05'),  # sample
                                                    ('location1', 'Season peak week', '2019-12-15')]),

            (unit_loc2, target_pct_next_week): ([('location2', 'pct next week', 0.3, 1.1),  # bin
                                                 ('location2', 'pct next week', 0.2, 2.2),
                                                 ('location2', 'pct next week', 0.5, 3.3)],
                                                [],
                                                [('location2', 'pct next week', 2.0)],  # point
                                                [('location2', 'pct next week', 0.025, 1.0),  # quantile
                                                 ('location2', 'pct next week', 0.25, 2.2),
                                                 ('location2', 'pct next week', 0.5, 2.2),
                                                 ('location2', 'pct next week', 0.75, 5.0),
                                                 ('location2', 'pct next week', 0.975, 50.0)],
                                                []),
            (unit_loc2, target_cases_next_week): ([], [],
                                                  [('location2', 'cases next week', 5)],  # point
                                                  [],
                                                  [('location2', 'cases next week', 0),  # sample
                                                   ('location2', 'cases next week', 2),
                                                   ('location2', 'cases next week', 5)]),
            (unit_loc2, target_season_severity): ([], [],
                                                  [('location2', 'season severity', 'moderate')],  # point
                                                  [],
                                                  [('location2', 'season severity', 'moderate'),  # sample
                                                   ('location2', 'season severity', 'severe'),
                                                   ('location2', 'season severity', 'high'),
                                                   ('location2', 'season severity', 'moderate'),
                                                   ('location2', 'season severity', 'mild')]),
            (unit_loc2, target_above_baseline): ([('location2', 'above baseline', 0.9, True),
                                                  ('location2', 'above baseline', 0.1, False)],  # bin
                                                 [], [], [],
                                                 [('location2', 'above baseline', True),  # sample
                                                  ('location2', 'above baseline', False),
                                                  ('location2', 'above baseline', True)]),
            (unit_loc2, target_season_peak_week): ([('location2', 'Season peak week', 0.01, '2019-12-15'),  # bin
                                                    ('location2', 'Season peak week', 0.05, '2019-12-22'),
                                                    ('location2', 'Season peak week', 0.05, '2019-12-29'),
                                                    ('location2', 'Season peak week', 0.89, '2020-01-05')],
                                                   [],
                                                   [('location2', 'Season peak week', '2020-01-05')],  # point
                                                   [('location2', 'Season peak week', 0.5, '2019-12-22'),  # quantile
                                                    ('location2', 'Season peak week', 0.75, '2019-12-29'),
                                                    ('location2', 'Season peak week', 0.975, '2020-01-05')],
                                                   []),

            (unit_loc3, target_pct_next_week): ([], [],
                                                [('location3', 'pct next week', 3.567)],  # point
                                                [],
                                                [('location3', 'pct next week', 2.3),  # sample
                                                 ('location3', 'pct next week', 6.5),
                                                 ('location3', 'pct next week', 0.0),
                                                 ('location3', 'pct next week', 10.0234),
                                                 ('location3', 'pct next week', 0.0001)]),
            (unit_loc3, target_cases_next_week): ([('location3', 'cases next week', 0.1, 2),  # bin
                                                   ('location3', 'cases next week', 0.9, 50)],
                                                  [],
                                                  [('location3', 'cases next week', 10)],  # point
                                                  [('location3', 'cases next week', 0.25, 0),  # quantile
                                                   ('location3', 'cases next week', 0.75, 50)],
                                                  []),
            (unit_loc3, target_season_severity): ([], [], [], [], []),
            (unit_loc3, target_above_baseline): ([], [], [], [],
                                                 [('location3', 'above baseline', False),  # sample
                                                  ('location3', 'above baseline', True),
                                                  ('location3', 'above baseline', True)]),
            (unit_loc3, target_season_peak_week): ([], [],
                                                   [('location3', 'Season peak week', '2019-12-29')],  # point
                                                   [],
                                                   [('location3', 'Season peak week', '2020-01-06'),  # sample
                                                    ('location3', 'Season peak week', '2019-12-16')]),
        }
        for (unit, target), exp_rows in loc_targ_to_exp_rows.items():
            act_rows = data_rows_from_forecast(forecast, unit, target)
            self.assertEqual(exp_rows, act_rows)
