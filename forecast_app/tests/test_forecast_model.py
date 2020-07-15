import logging

from django.core.exceptions import ValidationError
from django.test import TestCase

from forecast_app.models import ForecastModel
from utils.make_minimal_projects import _make_docs_project
from utils.utilities import get_or_create_super_po_mo_users


logging.getLogger().setLevel(logging.ERROR)


class ForecastModelTestCase(TestCase):
    """
    """


    def test_null_or_empty_name_or_abbreviation(self):
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        for empty_name in [None, '']:
            with self.assertRaises(ValidationError) as context:
                ForecastModel.objects.create(project=project, name=empty_name, abbreviation='abbrev')
            self.assertIn('both name and abbreviation are required', str(context.exception))

        for empty_abbreviation in [None, '']:
            with self.assertRaises(ValidationError) as context:
                ForecastModel.objects.create(project=project, name=forecast_model.name + '2',
                                             abbreviation=empty_abbreviation)
            self.assertIn('both name and abbreviation are required', str(context.exception))


    def test_duplicate_abbreviation(self):
        # duplicate names are OK, but not abbreviations
        _, _, po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)

        # case: new name, duplicate abbreviation
        with self.assertRaises(ValidationError) as context:
            ForecastModel.objects.create(project=project, name=forecast_model.name + '2',
                                         abbreviation=forecast_model.abbreviation)
        self.assertIn('abbreviation must be unique', str(context.exception))

        # case: duplicate name, new abbreviation
        try:
            forecast_model2 = ForecastModel.objects.create(project=project, name=forecast_model.name,
                                                           abbreviation=forecast_model.abbreviation + '2')
            forecast_model2.delete()
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        # case: new name, duplicate abbreviation, but saving existing forecast_model
        try:
            forecast_model.name = forecast_model.name + '2'  # new name, duplicate abbreviation
            forecast_model.save()

            forecast_model.abbreviation = forecast_model.abbreviation + '2'  # duplicate name, new abbreviation
            forecast_model.save()
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")
