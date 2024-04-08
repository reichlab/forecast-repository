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
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        unit_loc1 = project.units.get(abbreviation='loc1')
        unit_loc2 = project.units.get(abbreviation='loc2')
        unit_loc3 = project.units.get(abbreviation='loc3')
        target_pct_next_week = project.targets.get(name='pct next week')
        target_cases_next_week = project.targets.get(name='cases next week')
        target_season_severity = project.targets.get(name='season severity')
        target_above_baseline = project.targets.get(name='above baseline')
        target_season_peak_week = project.targets.get(name='Season peak week')

        # rows: 8-tuple: (data_rows_bin, data_rows_named, data_rows_point, data_rows_quantile, data_rows_sample,
        #                 data_rows_mean, data_rows_median, data_rows_mode)
        loc_targ_to_exp_rows = {
            (unit_loc1, target_pct_next_week): ([],
                                                [('loc1', 'pct next week', 'norm', 1.1, 2.2, None)],  # named
                                                [('loc1', 'pct next week', 2.1)],  # point
                                                [], [],
                                                [('loc1', 'pct next week', 2.11)],  # mean
                                                [('loc1', 'pct next week', 2.12)],  # median
                                                [('loc1', 'pct next week', 2.13)]),  # mode
            (unit_loc1, target_cases_next_week): ([],
                                                  [('loc1', 'cases next week', 'pois', 1.1, None, None)],  # named
                                                  [], [], [], [], [], []),
            (unit_loc1, target_season_severity): ([('loc1', 'season severity', 'mild', 0.0),  # bin
                                                   ('loc1', 'season severity', 'moderate', 0.1),
                                                   ('loc1', 'season severity', 'severe', 0.9)],
                                                  [],
                                                  [('loc1', 'season severity', 'mild')],  # point
                                                  [], [], [], [], []),
            (unit_loc1, target_above_baseline): ([], [],
                                                 [('loc1', 'above baseline', True)],  # point
                                                 [], [], [], [], []),
            (unit_loc1, target_season_peak_week): ([('loc1', 'Season peak week', '2019-12-15', 0.01),  # bin
                                                    ('loc1', 'Season peak week', '2019-12-22', 0.1),
                                                    ('loc1', 'Season peak week', '2019-12-29', 0.89)],
                                                   [],
                                                   [('loc1', 'Season peak week', '2019-12-22')],  # point
                                                   [],
                                                   [('loc1', 'Season peak week', '2020-01-05'),  # sample
                                                    ('loc1', 'Season peak week', '2019-12-15')], [], [], [],),

            (unit_loc2, target_pct_next_week): ([('loc2', 'pct next week', 1.1, 0.3),  # bin
                                                 ('loc2', 'pct next week', 2.2, 0.2),
                                                 ('loc2', 'pct next week', 3.3, 0.5)],
                                                [],
                                                [('loc2', 'pct next week', 2.0)],  # point
                                                [('loc2', 'pct next week', 0.025, 1.0),  # quantile
                                                 ('loc2', 'pct next week', 0.25, 2.2),
                                                 ('loc2', 'pct next week', 0.5, 2.2),
                                                 ('loc2', 'pct next week', 0.75, 5.0),
                                                 ('loc2', 'pct next week', 0.975, 50.0)],
                                                [], [], [], [],),
            (unit_loc2, target_cases_next_week): ([], [],
                                                  [('loc2', 'cases next week', 5)],  # point
                                                  [],
                                                  [('loc2', 'cases next week', 0),  # sample
                                                   ('loc2', 'cases next week', 2),
                                                   ('loc2', 'cases next week', 5)], [], [], [],),
            (unit_loc2, target_season_severity): ([], [],
                                                  [('loc2', 'season severity', 'moderate')],  # point
                                                  [],
                                                  [('loc2', 'season severity', 'moderate'),  # sample
                                                   ('loc2', 'season severity', 'severe'),
                                                   ('loc2', 'season severity', 'high'),
                                                   ('loc2', 'season severity', 'moderate'),
                                                   ('loc2', 'season severity', 'mild')], [], [], [],),
            (unit_loc2, target_above_baseline): ([('loc2', 'above baseline', True, 0.9),
                                                  ('loc2', 'above baseline', False, 0.1)],  # bin
                                                 [], [], [],
                                                 [('loc2', 'above baseline', True),  # sample
                                                  ('loc2', 'above baseline', False),
                                                  ('loc2', 'above baseline', True)], [], [], [],),
            (unit_loc2, target_season_peak_week): ([('loc2', 'Season peak week', '2019-12-15', 0.01),  # bin
                                                    ('loc2', 'Season peak week', '2019-12-22', 0.05),
                                                    ('loc2', 'Season peak week', '2019-12-29', 0.05),
                                                    ('loc2', 'Season peak week', '2020-01-05', 0.89)],
                                                   [],
                                                   [('loc2', 'Season peak week', '2020-01-05')],  # point
                                                   [('loc2', 'Season peak week', 0.5, '2019-12-22'),  # quantile
                                                    ('loc2', 'Season peak week', 0.75, '2019-12-29'),
                                                    ('loc2', 'Season peak week', 0.975, '2020-01-05')],
                                                   [], [], [], [],),

            (unit_loc3, target_pct_next_week): ([], [],
                                                [('loc3', 'pct next week', 3.567)],  # point
                                                [],
                                                [('loc3', 'pct next week', 2.3),  # sample
                                                 ('loc3', 'pct next week', 6.5),
                                                 ('loc3', 'pct next week', 0.0),
                                                 ('loc3', 'pct next week', 10.0234),
                                                 ('loc3', 'pct next week', 0.0001)], [], [], [],),
            (unit_loc3, target_cases_next_week): ([('loc3', 'cases next week', 0, 0.0),  # bin
                                                   ('loc3', 'cases next week', 2, 0.1),
                                                   ('loc3', 'cases next week', 50, 0.9)],
                                                  [],
                                                  [('loc3', 'cases next week', 10)],  # point
                                                  [('loc3', 'cases next week', 0.25, 0),  # quantile
                                                   ('loc3', 'cases next week', 0.75, 50)],
                                                  [], [], [], [],),
            (unit_loc3, target_season_severity): ([], [], [], [], [], [], [], [],),
            (unit_loc3, target_above_baseline): ([], [], [], [],
                                                 [('loc3', 'above baseline', False),  # sample
                                                  ('loc3', 'above baseline', True),
                                                  ('loc3', 'above baseline', True)], [], [], [],),
            (unit_loc3, target_season_peak_week): ([], [],
                                                   [('loc3', 'Season peak week', '2019-12-29')],  # point
                                                   [],
                                                   [('loc3', 'Season peak week', '2020-01-06'),  # sample
                                                    ('loc3', 'Season peak week', '2019-12-16')], [], [], [],),
        }
        for (unit, target), exp_rows in loc_targ_to_exp_rows.items():
            act_rows = data_rows_from_forecast(forecast, unit, target)
            self.assertEqual(exp_rows, act_rows)
