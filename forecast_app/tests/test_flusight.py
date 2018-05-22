import datetime
import json
from pathlib import Path

from django.template import Template, Context
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import CDC_CONFIG_DICT
from utils.flusight import flusight_data_dicts_for_models


class FlusightTestCase(TestCase):
    """
    """


    def test_d3_foresight(self):
        project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        project.load_template(Path('forecast_app/tests/2016-2017_submission_template-small.csv'))
        time_zero = TimeZero.objects.create(project=project,
                                            timezero_date=datetime.date(2016, 10, 23),
                                            # 20161023-KoTstable-20161109.cdc.csv {'year': 2016, 'week': 43, 'day': 1}
                                            data_version_date=None)
        TimeZero.objects.create(project=project,
                                timezero_date=datetime.date(2016, 10, 30),
                                # 20161030-KoTstable-20161114.cdc.csv {'year': 2016, 'week': 44, 'day': 1}
                                data_version_date=None)
        forecast_model1 = ForecastModel.objects.create(project=project)
        forecast_model1.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv'), time_zero)

        # test different projects
        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        forecast_model2 = ForecastModel.objects.create(project=project2)
        with self.assertRaises(RuntimeError) as context:
            flusight_data_dicts_for_models([forecast_model2, forecast_model1], None)
        self.assertIn('Not all models are in the same Project', str(context.exception))

        # we treat the json file as a Django's template b/c mode lIDs are hard-coded, but can vary depending on the
        # RDBMS
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17-small-exp-flusight.json', 'r') as fp:
            exp_json_template_str = fp.read()
            exp_json_template = Template(exp_json_template_str)
            exp_json_str = exp_json_template.render(Context({'forecast_model_id': forecast_model1.id}))
            exp_flusight_data_dict = json.loads(exp_json_str)
            act_flusight_data_dict = flusight_data_dicts_for_models([forecast_model1], None)
            self.assertEqual(exp_flusight_data_dict, act_flusight_data_dict)


    def test_d3_foresight_out_of_season(self):
        project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        project.load_template(Path('forecast_app/tests/2016-2017_submission_template-small.csv'))
        # pymmwr.mmwr_week_to_date(2016, 29) -> datetime.date(2016, 7, 17):
        time_zero = TimeZero.objects.create(project=project,
                                            timezero_date=datetime.date(2016, 7, 17),  # 29 < SEASON_START_EW_NUMBER
                                            data_version_date=None,
                                            is_season_start=True, season_name='2016')
        # 20161030-KoTstable-20161114.cdc.csv {'year': 2016, 'week': 44, 'day': 1} -> datetime.date(2016, 10, 30):
        TimeZero.objects.create(project=project,
                                timezero_date=datetime.date(2016, 10, 30),
                                data_version_date=None,
                                is_season_start=True, season_name='2017')  # season has no forecast data
        forecast_model = ForecastModel.objects.create(project=project)
        forecast_model.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv'), time_zero)
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17-small-exp-flusight-no-points.json', 'r') as fp:
            exp_json_template_str = fp.read()
            exp_json_template = Template(exp_json_template_str)
            exp_json_str = exp_json_template.render(Context({'forecast_model_id': forecast_model.id}))
            exp_flusight_data_dict = json.loads(exp_json_str)
            act_flusight_data_dict = flusight_data_dicts_for_models([forecast_model], '2017')
            self.assertEqual(exp_flusight_data_dict, act_flusight_data_dict)


    # straight from test_load_forecasts_from_dir():
    def test_d3_foresight_larger(self):
        project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        project.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))
        TimeZero.objects.create(project=project,
                                timezero_date=datetime.date(2016, 10, 23),
                                # 20161023-KoTstable-20161109.cdc.csv {'year': 2016, 'week': 43, 'day': 1}
                                data_version_date=None)
        TimeZero.objects.create(project=project,
                                timezero_date=datetime.date(2016, 10, 30),
                                # 20161030-KoTstable-20161114.cdc.csv {'year': 2016, 'week': 44, 'day': 1}
                                data_version_date=None)
        TimeZero.objects.create(project=project,
                                timezero_date=datetime.date(2016, 11, 6),
                                # 20161106-KoTstable-20161121.cdc.csv {'year': 2016, 'week': 45, 'day': 1}
                                data_version_date=None)
        forecast_model1 = ForecastModel.objects.create(name='forecast_model1', project=project)
        forecast_model2 = ForecastModel.objects.create(name='forecast_model2', project=project)
        forecst_dir = Path('forecast_app/tests/load_forecasts')
        forecast_model1.load_forecasts_from_dir(forecst_dir)
        forecast_model2.load_forecasts_from_dir(forecst_dir / 'third-file')
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17-small-exp-flusight-data.json', 'r') as fp:
            exp_json_template_str = fp.read()
            exp_json_template = Template(exp_json_template_str)
            exp_json_str = exp_json_template.render(Context({'forecast_model1_id': forecast_model1.id,
                                                             'forecast_model2_id': forecast_model2.id}))
            exp_flusight_data_dict = json.loads(exp_json_str)
            act_flusight_data_dict = flusight_data_dicts_for_models([forecast_model1, forecast_model2], None)
            self.assertEqual(exp_flusight_data_dict, act_flusight_data_dict)
