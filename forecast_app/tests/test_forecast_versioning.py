import datetime
import json
import unittest

import django
from django.test import TestCase

from forecast_app.models import Forecast, Score, TruthData, ScoreValue, TimeZero
from forecast_app.scores.calc_interval import _calculate_interval_score_values
from utils.forecast import load_predictions_from_json_io_dict, cache_forecast_metadata
from utils.make_minimal_projects import _make_docs_project
from utils.project import models_summary_table_rows_for_project, latest_forecast_ids_for_project
from utils.utilities import get_or_create_super_po_mo_users


class ForecastVersionsTestCase(TestCase):
    """
    Forecast queries:
    - query_forecasts_for_project(): handles optional `as_of`

    Project detail:
    - utils.project.models_summary_table_rows_for_project()

    Scores:
    - either don't puke (!) or use latest version if not too much engineering

    Web ui:
    - TBC

    API:
    - serializers: include `issue_date`
    - model detail: include all versions
    - forecast queries: optional `issue_date`
    - TBC
    """


    def setUp(self):  # runs before every test. done here instead of setUpTestData(cls) b/c below tests modify the db
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        self.project, self.time_zero, self.forecast_model, self.forecast = _make_docs_project(po_user)
        self.tz1 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 2)).first()
        self.tz2 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
        self.tz3 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 16)).first()


    def test_multiple_forecasts_per_timezero(self):
        # test forecast.issue_date
        self.assertEqual(self.forecast.created_at.date(), self.forecast.issue_date)

        # test starting version counts
        self.assertEqual(1, len(Forecast.objects.filter(time_zero=self.time_zero)))
        self.assertEqual(0, len(Forecast.objects.filter(time_zero=self.tz2)))

        # add a second forecast to tz1, test count. first change issue_date so that we don't have an integrity error
        # (i.e., that it looks like the same version)
        self.forecast.issue_date -= datetime.timedelta(days=1)  # make it an older version
        self.forecast.save()

        forecast2 = Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)
        self.assertEqual(2, len(Forecast.objects.filter(time_zero=self.time_zero)))
        self.assertEqual(0, len(Forecast.objects.filter(time_zero=self.tz2)))

        # delete one of the forecasts, test count
        forecast2.delete()
        self.assertEqual(1, len(Forecast.objects.filter(time_zero=self.time_zero)))
        self.assertEqual(0, len(Forecast.objects.filter(time_zero=self.tz2)))

        # test that there is only one "version" of every forecast: exactly 0 or 1 forecasts are allowed with the same
        # (timezero_id, issue_date) 2-tuple per model. do so by trying to add a second forecast for that combination.
        # note that this could technically fail if the date changes between the two Forecast.objects.create() calls
        # because Forecast.issue_date is auto_now_add. note that we do not test the exact message in context b/c it
        # varies depending on the database/vendor:
        # - sqlite3: "UNIQUE constraint failed"
        # - postgres: "duplicate key value violates unique constraint"
        Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)
        with self.assertRaises(django.db.utils.IntegrityError) as context:
            Forecast.objects.create(forecast_model=self.forecast_model, time_zero=self.time_zero)


    def test_models_summary_table_rows_for_project_case_1(self):
        # case 1/3: three timezeros, two forecasts, no versions:
        # tz1 -- f1 - tz1.date (issue_date)
        # tz2 -- f2 - tz2.date
        # tz3 -- x  - x
        f1 = self.forecast
        f1.issue_date = self.tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=self.forecast_model, source='f2', time_zero=self.tz2)
        f2.issue_date = self.tz2.timezero_date
        f2.save()

        # NB: we have to work around a Django bug where DateField and DateTimeField come out of the database as either
        # datetime.date/datetime.datetime objects (postgres) or strings (sqlite3). also have to convert datetime to
        # comparable format using utctimetuple()
        exp_row = (self.forecast_model, 2,
                   str(f1.time_zero.timezero_date),  # oldest_forecast_tz_date
                   str(f2.time_zero.timezero_date),  # newest_forecast_tz_date
                   f2.id, f2.created_at.utctimetuple())  # id and created_at of ""
        act_rows = models_summary_table_rows_for_project(self.project)
        self.assertEqual(1, len(act_rows))

        act_row = (act_rows[0][0], act_rows[0][1],
                   str(act_rows[0][2]),  # oldest_forecast_tz_date
                   str(act_rows[0][3]),  # newest_forecast_tz_date
                   act_rows[0][4], act_rows[0][5].utctimetuple())  # id and created_at of ""
        self.assertEqual(exp_row, act_row)

        # test `latest_forecast_ids_for_project()` b/c it's convenient here
        exp_fm_tz_ids_to_f_id = {(self.forecast_model.id, self.tz1.id): f1.id,
                                 (self.forecast_model.id, self.tz2.id): f2.id}
        act_fm_tz_ids_to_f_id = latest_forecast_ids_for_project(self.project, False)
        self.assertEqual(exp_fm_tz_ids_to_f_id, act_fm_tz_ids_to_f_id)


    def test_models_summary_table_rows_for_project_case_2(self):
        # case 2/3: three timezeros, three forecasts, oldest tz has two versions:
        # tz1 -- f1 - tz1.date (issue_date)  v.1/2
        #     \- f2 - tz1.date + 1           v.2/2
        # tz2 -- f3 - tz2.date
        # tz3 -- x
        f1 = self.forecast
        f1.issue_date = self.tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=self.forecast_model, source='f2', time_zero=self.tz1)
        f2.issue_date = self.tz1.timezero_date + datetime.timedelta(days=1)
        f2.save()

        f3 = Forecast.objects.create(forecast_model=self.forecast_model, source='f3', time_zero=self.tz2)
        f3.issue_date = self.tz2.timezero_date
        f3.save()

        exp_row = (self.forecast_model, 3,
                   str(f1.time_zero.timezero_date),  # oldest_forecast_tz_date
                   str(f3.time_zero.timezero_date),  # newest_forecast_tz_date
                   f3.id, f3.created_at.utctimetuple())  # id and created_at of ""
        act_rows = models_summary_table_rows_for_project(self.project)
        self.assertEqual(1, len(act_rows))

        act_row = (act_rows[0][0], act_rows[0][1],
                   str(act_rows[0][2]),  # oldest_forecast_tz_date
                   str(act_rows[0][3]),  # newest_forecast_tz_date
                   act_rows[0][4], act_rows[0][5].utctimetuple())  # id and created_at of ""
        self.assertEqual(exp_row, act_row)

        # test `latest_forecast_ids_for_project()` b/c it's convenient here
        exp_fm_tz_ids_to_f_id = {(self.forecast_model.id, self.tz1.id): f2.id,
                                 (self.forecast_model.id, self.tz2.id): f3.id}
        act_fm_tz_ids_to_f_id = latest_forecast_ids_for_project(self.project, False)
        self.assertEqual(exp_fm_tz_ids_to_f_id, act_fm_tz_ids_to_f_id)


    def test_models_summary_table_rows_for_project_case_3(self):
        # case 3/3: three timezeros, four forecasts, newest tz has two versions:
        # tz1 -- f1 - tz1.date (issue_date)
        # tz2 -- f2 - tz2.date
        # tz3 -- f3 - tz3.date       v.1/2
        #     \- f4 - tz3.date + 1   v.2/2
        f1 = self.forecast
        f1.issue_date = self.tz1.timezero_date
        f1.save()

        f2 = Forecast.objects.create(forecast_model=self.forecast_model, source='f2', time_zero=self.tz2)
        f2.issue_date = self.tz2.timezero_date
        f2.save()

        f3 = Forecast.objects.create(forecast_model=self.forecast_model, source='f3', time_zero=self.tz3)
        f3.issue_date = self.tz3.timezero_date
        f3.save()

        f4 = Forecast.objects.create(forecast_model=self.forecast_model, source='f4', time_zero=self.tz3)
        f4.issue_date = self.tz3.timezero_date + datetime.timedelta(days=1)
        f4.save()

        exp_row = (self.forecast_model, 4,
                   str(f1.time_zero.timezero_date),  # oldest_forecast_tz_date
                   str(f4.time_zero.timezero_date),  # newest_forecast_tz_date
                   f4.id, f4.created_at.utctimetuple())  # id and created_at of ""
        act_rows = models_summary_table_rows_for_project(self.project)
        self.assertEqual(1, len(act_rows))

        act_row = (act_rows[0][0], act_rows[0][1],
                   str(act_rows[0][2]),  # oldest_forecast_tz_date
                   str(act_rows[0][3]),  # newest_forecast_tz_date
                   act_rows[0][4], act_rows[0][5].utctimetuple())  # id and created_at of ""
        self.assertEqual(exp_row, act_row)

        # test `latest_forecast_ids_for_project()` b/c it's convenient here
        act_fm_tz_ids_to_f_id = latest_forecast_ids_for_project(self.project, False)
        exp_fm_tz_ids_to_f_id = {(self.forecast_model.id, self.tz1.id): f1.id,
                                 (self.forecast_model.id, self.tz2.id): f2.id,
                                 (self.forecast_model.id, self.tz3.id): f4.id}
        self.assertEqual(exp_fm_tz_ids_to_f_id, act_fm_tz_ids_to_f_id)


    # copy of test_calc_interval_20_docs_project() that adds a version
    @unittest.skip("todo remove this failing test when we remove scoring from zoltar proper")
    def test_calc_interval_20_docs_project_additional_version(self):
        Score.ensure_all_scores_exist()
        interval_20_score = Score.objects.filter(abbreviation='interval_20').first()
        self.assertIsNotNone(interval_20_score)

        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)

        unit_loc2 = project.units.filter(name='location2').first()
        targ_pct_next_wk = project.targets.filter(name='pct next week').first()  # continuous
        unit_loc3 = project.units.filter(name='location3').first()
        targ_cases_next_wk = project.targets.filter(name='cases next week').first()  # discrete

        # add two truths that result in two ScoreValues
        project.delete_truth_data()
        TruthData.objects.create(time_zero=time_zero, unit=unit_loc2, target=targ_pct_next_wk, value_f=2.2)  # 2/7)
        TruthData.objects.create(time_zero=time_zero, unit=unit_loc3, target=targ_cases_next_wk, value_i=50)  # 6/7
        ScoreValue.objects \
            .filter(score=interval_20_score, forecast__forecast_model=forecast_model) \
            .delete()  # usually done by update_score_for_model()
        _calculate_interval_score_values(interval_20_score, forecast_model, 0.5)
        self.assertEqual(2, interval_20_score.values.count())
        self.assertEqual([2.8, 50], sorted(interval_20_score.values.all().values_list('value', flat=True)))

        # add a second forecast for a newer timezero
        time_zero2 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2011, 10, 3))
        forecast2 = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                            time_zero=time_zero2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, False)
        TruthData.objects.create(time_zero=time_zero2, unit=unit_loc2, target=targ_pct_next_wk, value_f=2.2)  # 2/7)
        TruthData.objects.create(time_zero=time_zero2, unit=unit_loc3, target=targ_cases_next_wk, value_i=50)  # 6/7
        ScoreValue.objects \
            .filter(score=interval_20_score, forecast__forecast_model=forecast_model) \
            .delete()  # usually done by update_score_for_model()
        _calculate_interval_score_values(interval_20_score, forecast_model, 0.5)
        self.assertEqual(4, interval_20_score.values.count())

        # finally, add a new version to timezero
        forecast.issue_date = forecast.time_zero.timezero_date
        forecast.save()

        forecast2.issue_date = forecast2.time_zero.timezero_date
        forecast2.save()

        forecast2 = Forecast.objects.create(forecast_model=forecast_model, source='f2', time_zero=time_zero)
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast2, json_io_dict_in, False)  # atomic
            cache_forecast_metadata(forecast2)  # atomic

        # s/b no change from previous
        ScoreValue.objects \
            .filter(score=interval_20_score, forecast__forecast_model=forecast_model) \
            .delete()  # usually done by update_score_for_model()

        # RuntimeError: >2 lower_upper_interval_values: [2.2, 2.2, 5.0, 5.0]. timezero_id=4, unit_id=5, target_id=6
        _calculate_interval_score_values(interval_20_score, forecast_model, 0.5)

        self.assertEqual(4, interval_20_score.values.count())
