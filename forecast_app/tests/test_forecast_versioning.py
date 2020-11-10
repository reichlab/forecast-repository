import datetime

import django
from django.test import TestCase

from forecast_app.models import Forecast
from utils.make_minimal_projects import _make_docs_project
from utils.project import models_summary_table_rows_for_project
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
