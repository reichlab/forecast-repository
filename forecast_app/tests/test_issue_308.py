import datetime
from pathlib import Path
from unittest.mock import patch

from django.test import TestCase

from forecast_app.models import ForecastModel, Unit, Target, Forecast
from utils.forecast import load_predictions_from_json_io_dict, json_io_dict_from_forecast
from utils.issue_308_app import add_deleted_file_retractions, DELETE_NOTE
from utils.project import create_project_from_json
from utils.utilities import get_or_create_super_po_mo_users


class Issue308TestCase(TestCase):
    """
    """


    def test_issue_308(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project = create_project_from_json(Path('forecast_app/tests/projects/docs-project.json'), po_user)
        forecast_model = ForecastModel.objects.create(project=project, name='case model', abbreviation='case_model')

        tz1 = project.timezeros.filter(timezero_date=datetime.date(2011, 10, 2)).first()
        u1 = Unit.objects.filter(name='location1').first()
        t1 = Target.objects.filter(name='cases next week').first()
        t2 = Target.objects.filter(name='pct next week').first()
        t3 = Target.objects.filter(name='season severity').first()

        f1 = Forecast.objects.create(forecast_model=forecast_model, source='f1', time_zero=tz1)
        f1.issue_date = tz1.timezero_date
        f1.save()

        predictions = [
            {'unit': u1.name, 'target': t1.name, 'class': 'point', 'prediction': {'value': 1}},
            {'unit': u1.name, 'target': t2.name, 'class': 'point', 'prediction': {'value': 2.0}},
            {'unit': u1.name, 'target': t3.name, 'class': 'point', 'prediction': {'value': 'high'}},
        ]
        load_predictions_from_json_io_dict(f1, {'meta': {}, 'predictions': predictions})

        # test case: target group not found
        with self.assertRaisesRegex(RuntimeError, 'target group not found'):
            add_deleted_file_retractions(project.pk, [], ['bad target group'])

        # test case: bad commit date
        with self.assertRaisesRegex(RuntimeError, 'invalid commit_date'):
            add_deleted_file_retractions(project.pk, [('f1', '01/08/xx-16:34:58', False)],
                                         ['cases next week', 'pct next week'])

        # test case: skip if forecast not found
        act_new_forecasts = add_deleted_file_retractions(project.pk, [('f0', '01/08/21-16:34:58', False)],
                                                         ['cases next week', 'pct next week'])
        self.assertEqual([], act_new_forecasts)

        # test case: one file, all retractions, commit_date > tz1
        act_new_forecasts = add_deleted_file_retractions(project.pk, [('f1', '10/03/11-01:43:28', False)],
                                                         ['cases next week', 'pct next week'])
        self.assertEqual(1, len(act_new_forecasts))

        new_forecast = act_new_forecasts[0]
        self.assertEqual(datetime.date(2011, 10, 2), new_forecast.time_zero.timezero_date)
        self.assertEqual(datetime.date(2011, 10, 3), new_forecast.issue_date)
        self.assertEqual(f1.source, new_forecast.source)
        self.assertEqual(DELETE_NOTE, new_forecast.notes)

        exp_pred_dicts = [
            {'unit': u1.name, 'target': t1.name, 'class': 'point', 'prediction': None},
            {'unit': u1.name, 'target': t2.name, 'class': 'point', 'prediction': None},
            {'unit': u1.name, 'target': t3.name, 'class': 'point', 'prediction': {'value': 'high'}}]
        act_pred_dicts = json_io_dict_from_forecast(new_forecast, None, is_include_retract=True)['predictions']
        self.assertEqual(exp_pred_dicts, act_pred_dicts)

        # test case: duplicated file with different commit_dates, all > tz1
        act_new_forecasts = add_deleted_file_retractions(project.pk, [('f1', '10/03/11-01:43:28', False),
                                                                      ('f1', '10/04/11-01:43:28', False)],
                                                         ['cases next week', 'pct next week'])
        self.assertEqual(1, len(act_new_forecasts))

        new_forecast = act_new_forecasts[0]
        self.assertEqual(datetime.date(2011, 10, 4), new_forecast.issue_date)

        # test case: one file with commit date < tz1
        act_new_forecasts = add_deleted_file_retractions(project.pk, [('f1', '10/01/11-01:43:28', False)],
                                                         ['cases next week', 'pct next week'])
        self.assertEqual(1, len(act_new_forecasts))
        new_forecast = act_new_forecasts[0]
        self.assertEqual(datetime.date(2011, 10, 3), new_forecast.issue_date)

        # test case: is_force_retract: one file, all retractions, commit_date > tz1
        act_new_forecasts = add_deleted_file_retractions(project.pk, [('f1', '10/03/11-01:43:28', True)],
                                                         ['cases next week', 'pct next week'])
        self.assertEqual(1, len(act_new_forecasts))

        new_forecast = act_new_forecasts[0]
        exp_pred_dicts = [{'unit': 'location1', 'target': 'cases next week', 'class': 'point', 'prediction': None},
                          {'unit': 'location1', 'target': 'pct next week', 'class': 'point', 'prediction': None},
                          {'unit': 'location1', 'target': 'season severity', 'class': 'point', 'prediction': None}]
        act_pred_dicts = json_io_dict_from_forecast(new_forecast, None, is_include_retract=True)['predictions']
        self.assertEqual(exp_pred_dicts, act_pred_dicts)

        # test case: cache_forecast_metadata() called for each new forecast
        with patch('utils.forecast.cache_forecast_metadata') as cache_metadata_mock:
            add_deleted_file_retractions(project.pk, [('f1', '10/03/11-01:43:28', True)],
                                         ['cases next week', 'pct next week'])
            cache_metadata_mock.assert_called_once()

        # test case: previous forecasts with DELETE_NOTE are deleted
        delete_note_forecasts_qs = Forecast.objects.filter(notes=DELETE_NOTE)
        self.assertEqual(1, delete_note_forecasts_qs.count())

        act_new_forecasts = add_deleted_file_retractions(project.pk, [],
                                                         ['cases next week', 'pct next week'])
        self.assertEqual(0, len(act_new_forecasts))
        self.assertEqual(0, delete_note_forecasts_qs.count())
