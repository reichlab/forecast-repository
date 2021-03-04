import datetime
import json

from django.test import TestCase

from forecast_app.models import Forecast
from utils.forecast import load_predictions_from_json_io_dict, json_io_dict_from_forecast
from utils.make_minimal_projects import _make_docs_project
from utils.migration_0014_utils import _copy_new_data_to_old_tables, _delete_new_data, copy_old_data_to_new_tables, \
    delete_old_data, _num_rows_new_data, _num_rows_old_data, _pred_dicts_from_forecast_old, _grouped_version_rows
from utils.project_truth import oracle_model_for_project
from utils.utilities import get_or_create_super_po_mo_users


class Migration0014TestCase(TestCase):
    """
    """


    def setUp(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        self.project, self.time_zero, self.forecast_model, self.forecast = _make_docs_project(po_user)
        tz2 = self.project.timezeros.filter(timezero_date=datetime.date(2011, 10, 9)).first()
        self.forecast2 = Forecast.objects.create(forecast_model=self.forecast_model,
                                                 source='docs-predictions-non-dup.json',
                                                 time_zero=tz2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions-non-dup.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(self.forecast2, json_io_dict_in, is_validate_cats=False)


    def test_copy(self):
        # starts with new data, but no old data. point_count, named_count, bin_count, sample_count, quantile_count:
        # - docs-predictions.json: 11 + 2 + 18 + 23 + 10 = 64
        # - docs-predictions-non-dup.json: 64 - 4 = 60
        self.assertEqual(0, _num_rows_old_data(self.forecast))
        self.assertEqual(0, _num_rows_old_data(self.forecast2))
        self.assertEqual(64, _num_rows_new_data(self.forecast))
        self.assertEqual(60, _num_rows_new_data(self.forecast2))
        self.assertEqual(29, self.forecast.pred_eles.count())
        self.assertEqual(29, self.forecast2.pred_eles.count())

        # copy new data -> old and ensure that there is now old data
        _copy_new_data_to_old_tables(self.project)
        self.assertEqual(64, _num_rows_old_data(self.forecast))
        self.assertEqual(60, _num_rows_old_data(self.forecast2))
        self.assertEqual(64, _num_rows_new_data(self.forecast))
        self.assertEqual(60, _num_rows_new_data(self.forecast2))
        self.assertEqual(29, self.forecast.pred_eles.count())
        self.assertEqual(29, self.forecast2.pred_eles.count())

        # delete the new data to create a real-life migration scenario: we have old data to migrate, and no new data
        _delete_new_data(self.project)
        self.assertEqual(64, _num_rows_old_data(self.forecast))
        self.assertEqual(60, _num_rows_old_data(self.forecast2))
        self.assertEqual(0, _num_rows_new_data(self.forecast))
        self.assertEqual(0, _num_rows_new_data(self.forecast2))
        self.assertEqual(0, self.forecast.pred_eles.count())
        self.assertEqual(0, self.forecast2.pred_eles.count())

        # test the actual migration utility
        copy_old_data_to_new_tables(self.forecast)
        copy_old_data_to_new_tables(self.forecast2)
        self.assertEqual(64, _num_rows_old_data(self.forecast))
        self.assertEqual(60, _num_rows_old_data(self.forecast2))
        self.assertEqual(64, _num_rows_new_data(self.forecast))
        self.assertEqual(60, _num_rows_new_data(self.forecast2))
        self.assertEqual(29, self.forecast.pred_eles.count())
        self.assertEqual(29, self.forecast2.pred_eles.count())

        # back to square one
        delete_old_data(self.forecast)
        delete_old_data(self.forecast2)
        self.assertEqual(0, _num_rows_old_data(self.forecast))
        self.assertEqual(0, _num_rows_old_data(self.forecast2))
        self.assertEqual(64, _num_rows_new_data(self.forecast))
        self.assertEqual(60, _num_rows_new_data(self.forecast2))
        self.assertEqual(29, self.forecast.pred_eles.count())
        self.assertEqual(29, self.forecast2.pred_eles.count())


    def test__pred_dicts_from_forecast_old(self):
        def sort_key(pred_dict):
            return pred_dict['unit'], pred_dict['target'], pred_dict['class']


        _copy_new_data_to_old_tables(self.project)  # copy itself is tested above
        for forecast in Forecast.objects.filter(forecast_model__project=self.project):
            json_io_dict_new = json_io_dict_from_forecast(forecast, None)
            json_io_dict_old = {'meta': {},
                                'predictions': sorted(_pred_dicts_from_forecast_old(forecast), key=sort_key)}
            json_io_dict_new['predictions'].sort(key=sort_key)
            self.assertEqual(json_io_dict_new, json_io_dict_old)


    def test__grouped_version_rows(self):
        self.forecast.issue_date = self.time_zero.timezero_date  # v1
        self.forecast.save()
        f1 = self.forecast
        f2 = self.forecast2
        f3 = Forecast.objects.create(forecast_model=self.forecast_model, source='f3', time_zero=self.time_zero)  # v2

        # is_versions_only = True
        exp_rows = [  # fm_id, tz_id, issue_date, f_id, f_source, f_created_at, rank  # NB: no: f_source, f_created_at
            (f1.forecast_model.pk, f1.time_zero.pk, f1.issue_date, f1.pk, 1),
            (f3.forecast_model.pk, f3.time_zero.pk, f3.issue_date, f3.pk, 2)]
        act_rows = sorted([(row[0], row[1], row[2], row[3], row[6])
                           for row in _grouped_version_rows(self.project, True)])
        self.assertEqual(sorted(exp_rows), act_rows)

        # is_versions_only = False
        exp_rows += [
            (f2.forecast_model.pk, f2.time_zero.pk, f2.issue_date, f2.pk, 1),
        ]
        exp_rows += [(f.forecast_model.pk, f.time_zero.pk, f.issue_date, f.pk, 1)
                     for f in oracle_model_for_project(self.project).forecasts.all()]
        act_rows = sorted([(row[0], row[1], row[2], row[3], row[6])
                           for row in _grouped_version_rows(self.project, False)])
        self.assertEqual(sorted(exp_rows), sorted(act_rows))
