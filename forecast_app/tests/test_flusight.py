import datetime
import json
from pathlib import Path

from django.template import Template, Context
from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from utils.cdc import load_cdc_csv_forecast_file, make_cdc_units_and_targets
from utils.flusight import flusight_unit_to_data_dict
from utils.make_thai_moph_project import load_cdc_csv_forecasts_from_dir


class FlusightTestCase(TestCase):
    """
    """


    def test_d3_foresight(self):
        project = Project.objects.create()
        make_cdc_units_and_targets(project)
        time_zero = TimeZero.objects.create(project=project,
                                            timezero_date=datetime.date(2016, 10, 23),
                                            # 20161023-KoTstable-20161109.cdc.csv {'year': 2016, 'week': 43, 'day': 1}
                                            data_version_date=datetime.date(2016, 10, 22))  # -> outputs dataVersionTime
        TimeZero.objects.create(project=project,
                                timezero_date=datetime.date(2016, 10, 30),
                                # 20161030-KoTstable-20161114.cdc.csv {'year': 2016, 'week': 44, 'day': 1}
                                data_version_date=datetime.date(2016, 10, 29))
        forecast_model1 = ForecastModel.objects.create(project=project)
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        load_cdc_csv_forecast_file(2016, forecast_model1, csv_file_path, time_zero)

        # we treat the json file as a Django's template b/c mode lIDs are hard-coded, but can vary depending on the
        # RDBMS
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17-small-exp-flusight.json', 'r') as fp:
            exp_json_template_str = fp.read()
            exp_json_template = Template(exp_json_template_str)
            exp_json_str = exp_json_template.render(Context({'forecast_model_id': forecast_model1.id}))
            exp_flusight_data_dict = json.loads(exp_json_str)
            act_flusight_data_dict = flusight_unit_to_data_dict(project, None)
            self.assertEqual(exp_flusight_data_dict, act_flusight_data_dict)


    def test_d3_foresight_out_of_season(self):
        project = Project.objects.create()
        make_cdc_units_and_targets(project)
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
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        load_cdc_csv_forecast_file(2016, forecast_model, csv_file_path, time_zero)
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17-small-exp-flusight-no-points.json', 'r') as fp:
            exp_json_template_str = fp.read()
            exp_json_template = Template(exp_json_template_str)
            exp_json_str = exp_json_template.render(Context({'forecast_model_id': forecast_model.id}))
            exp_flusight_data_dict = json.loads(exp_json_str)
            act_flusight_data_dict = flusight_unit_to_data_dict(project, '2017')
            self.assertEqual(exp_flusight_data_dict, act_flusight_data_dict)


    # straight from test_load_forecasts_from_dir():
    def test_d3_foresight_larger(self):
        project = Project.objects.create()
        make_cdc_units_and_targets(project)
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
        forecast_dir = Path('forecast_app/tests/load_forecasts')
        load_cdc_csv_forecasts_from_dir(forecast_model1, forecast_dir, 2016)
        load_cdc_csv_forecasts_from_dir(forecast_model2, forecast_dir / 'third-file', 2016)
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17-small-exp-flusight-data.json', 'r') as fp:
            exp_json_template_str = fp.read()
            exp_json_template = Template(exp_json_template_str)
            exp_json_str = exp_json_template.render(Context({'forecast_model1_id': forecast_model1.id,
                                                             'forecast_model2_id': forecast_model2.id}))
            exp_flusight_data_dict = json.loads(exp_json_str)
            act_flusight_data_dict = flusight_unit_to_data_dict(project, None)
            self.assertEqual(exp_flusight_data_dict, act_flusight_data_dict)
