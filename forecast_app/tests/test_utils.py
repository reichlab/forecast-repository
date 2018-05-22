import datetime
import json
from pathlib import Path

import pymmwr
from django.template import Template, Context
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import epi_week_filename_components_2016_2017_flu_contest, epi_week_filename_components_ensemble, \
    CDC_CONFIG_DICT
from utils.make_cdc_flusight_ensemble_project import season_start_year_for_date
from utils.mean_absolute_error import _model_id_to_point_values_dict
from utils.utilities import cdc_csv_filename_components


EPI_YR_WK_TO_ACTUAL_WILI = {
    (2016, 51): 2.74084,
    (2016, 52): 3.36496,
    (2017, 1): 3.0963,
    (2017, 2): 3.08492,
    (2017, 3): 3.51496,
    (2017, 4): 3.8035,
    (2017, 5): 4.45059,
    (2017, 6): 5.07947,
}


class UtilitiesTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        cls.project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))

        cls.forecast_model = ForecastModel.objects.create(project=cls.project)

        # EW1-KoTstable-2017-01-17.csv -> EW1 in 2017:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 1)))
        cls.forecast1 = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'), time_zero)

        # EW2-KoTstable-2017-01-23.csv -> EW2 in 2017:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2017, 2)))
        cls.forecast2 = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW2-KoTstable-2017-01-23.csv'), time_zero)

        # EW51-KoTstable-2017-01-03.csv -> EW51 in 2016:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 51)))
        cls.forecast3 = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW51-KoTstable-2017-01-03.csv'), time_zero)

        # EW52-KoTstable-2017-01-09.csv -> EW52 in 2016:
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=(pymmwr.mmwr_week_to_date(2016, 52)))
        cls.forecast4 = cls.forecast_model.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW52-KoTstable-2017-01-09.csv'), time_zero)


    def test_epi_week_filename_components_2016_2017_flu_contest(self):
        filename_components_tuples = (('EW1-KoTstable-2017-01-17.csv', (1, 'KoTstable', datetime.date(2017, 1, 17))),
                                      ('-KoTstable-2017-01-17.csv', None),
                                      ('EW1--2017-01-17.csv', None),
                                      ('EW1-KoTstable-2017-01-17.txt', None))
        for filename, components in filename_components_tuples:
            self.assertEqual(components, epi_week_filename_components_2016_2017_flu_contest(filename))


    def test_epi_week_filename_components_ensemble(self):
        filename_components_tuples = (('EW01-2011-CU_EAKFC_SEIRS.csv', (1, 2011, 'CU_EAKFC_SEIRS')),
                                      ('EW01-2011-CUBMA.csv', (1, 2011, 'CUBMA')),
                                      ('-2011-CUBMA.csv', None),
                                      ('EW01--2011-CUBMA.csv', None),
                                      ('EW01-CUBMA.csv', None),
                                      ('EW01-2011.csv', None),
                                      ('EW01-2011-CUBMA.txt', None))
        for filename, components in filename_components_tuples:
            self.assertEqual(components, epi_week_filename_components_ensemble(filename))


    def test_name_components_from_cdc_csv_filename(self):
        filename_to_exp_component_tuples = {
            '20170419-gam_lag1_tops3-20170516.cdc.csv':
                (datetime.date(2017, 4, 19), 'gam_lag1_tops3', datetime.date(2017, 5, 16)),
            '20161023-KoTstable-20161109.cdc.csv':
                (datetime.date(2016, 10, 23), 'KoTstable', datetime.date(2016, 11, 9)),
            '20161023-KoTstable.cdc.csv':
                (datetime.date(2016, 10, 23), 'KoTstable', None),

            '': None,
            '20170419-gam_lag1_tops3-20170516.csv': None,
            'gam_lag1_tops3-20170516.csv': None,
            '-gam_lag1_tops3-20170516.cdc.csv': None,
            '20170419--.cdc.csv': None,
            '20170419-.cdc.csv': None,
            '20170419.cdc.csv': None,
            '20170419-gam_lag1_tops3-.cdc.csv': None,
            '20170419-gam-lag1-tops3-20170516.cdc.csv': None,
            '20170419-gam/lag1*tops3-20170516.cdc.csv': None,
        }
        for cdc_csv_filename, exp_components in filename_to_exp_component_tuples.items():
            self.assertEqual(exp_components, cdc_csv_filename_components(cdc_csv_filename))


    def test_season_start_year_for_date(self):
        date_exp_season_start_year = [
            (pymmwr.mmwr_week_to_date(2016, 29), 2015),
            (pymmwr.mmwr_week_to_date(2016, 30), 2016),
            (pymmwr.mmwr_week_to_date(2016, 52), 2016),
            (pymmwr.mmwr_week_to_date(2017, 1), 2016),
            (pymmwr.mmwr_week_to_date(2017, 29), 2016),
            (pymmwr.mmwr_week_to_date(2017, 30), 2017),
        ]
        for date, exp_season_start_year in date_exp_season_start_year:
            self.assertEqual(exp_season_start_year, season_start_year_for_date(date))


    def test__model_id_to_point_values_dict(self):
        project1 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        project1.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))

        time_zero_11 = TimeZero.objects.create(project=project1, timezero_date='2017-01-01',
                                               is_season_start=True, season_name='season p1')
        TimeZero.objects.create(project=project1, timezero_date='2017-01-02')  # 2nd TZ ensures start AND end dates
        forecast_model_11 = ForecastModel.objects.create(project=project1)
        forecast_11 = forecast_model_11.load_forecast(
            Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'),
            time_zero_11)

        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template-small.csv'))
        # 20161023-KoTstable-20161109.cdc.csv {'year': 2016, 'week': 43, 'day': 1}:
        time_zero_21 = TimeZero.objects.create(project=project2,
                                               timezero_date=datetime.date(2016, 10, 23), data_version_date=None,
                                               is_season_start=True, season_name='season p2')
        # 20161030-KoTstable-20161114.cdc.csv {'year': 2016, 'week': 44, 'day': 1}:
        TimeZero.objects.create(project=project2,
                                timezero_date=datetime.date(2016, 10, 30),
                                data_version_date=None)
        forecast_model_21 = ForecastModel.objects.create(project=project2)
        forecast_21 = forecast_model_21.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv'),
                                                      time_zero_21)

        targets = ['1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead']
        with open('forecast_app/tests/exp-models-to-point-values.json', 'r') as fp:
            exp_json_template_str = fp.read()
            exp_json_template = Template(exp_json_template_str)
            exp_json_str = exp_json_template.render(Context({'forecast_model1_id': forecast_model_11.id,
                                                             'forecast1_id': forecast_11.id,
                                                             'forecast_model2_id': forecast_model_21.id,
                                                             'forecast2_id': forecast_21.id}))

            # note: we must wire up exp_dict_loaded to replace keys with actual int IDs, not just strings
            exp_dict_loaded = json.loads(exp_json_str)
            exp_dict_p1 = {forecast_model_11.pk:
                               {forecast_11.pk: exp_dict_loaded[str(forecast_model_11.id)][str(forecast_11.id)]}}
            act_point_values_dict = _model_id_to_point_values_dict(project1, 'season p1', targets)
            self.assertEqual(exp_dict_p1, act_point_values_dict)

            exp_dict_p2 = {forecast_model_21.pk:
                               {forecast_21.pk: exp_dict_loaded[str(forecast_model_21.id)][str(forecast_21.id)]}}
            act_point_values_dict = _model_id_to_point_values_dict(project2, 'season p2', targets)
            self.assertEqual(exp_dict_p2, act_point_values_dict)
