import csv
import datetime
import io
from pathlib import Path

from django.test import TestCase

from forecast_app.models import Project, TimeZero
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.score import Score, _write_csv_score_data_for_project
from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, CDC_CONFIG_DICT
from utils.scores import calculate_absolute_error_score_values, ABSOLUTE_ERROR_SCORE_NAME


class ScoresTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        make_cdc_locations_and_targets(cls.project)

        cls.project.load_template(Path('forecast_app/tests/2016-2017_submission_template-small.csv'))

        # load truth only for the TimeZero in truths-2016-2017-reichlab.csv we're testing against
        time_zero = TimeZero.objects.create(project=cls.project, timezero_date=datetime.date(2017, 1, 1))
        cls.project.load_truth_data(Path('utils/ensemble-truth-table-script/truths-2016-2017-reichlab.csv'))

        cls.forecast_model = ForecastModel.objects.create(project=cls.project, name='test model')
        cls.forecast_model.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv'), time_zero)


    def test_abs_error_score(self):
        # test creates a correctly-named Score
        calculate_absolute_error_score_values(self.project)
        abs_err_score = Score.objects.filter(name=ABSOLUTE_ERROR_SCORE_NAME).first()
        self.assertIsNotNone(abs_err_score)

        # test score values
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17_exp-abs-errors.csv', 'r') as fp:
            csv_reader = csv.reader(fp, delimiter=',')  # Location, Target, predicted_value, truth_value, abs_err
            next(csv_reader)  # skip header
            exp_rows = sorted([(i[0], i[1], i[-1]) for i in csv_reader])  # Location, Target, abs_err

            # convert actual rows from IDs into strings for readability
            act_rows = sorted(abs_err_score.values.values_list('location__name', 'target__name', 'value'))
            for exp_row, act_row in zip(exp_rows, act_rows):
                self.assertEqual(exp_row[0], act_row[0])  # location name
                self.assertEqual(exp_row[1], act_row[1])  # target name
                self.assertAlmostEqual(float(exp_row[2]), act_row[2])  # value


    def test_download_scores(self):
        calculate_absolute_error_score_values(self.project)
        string_io = io.StringIO()
        csv_writer = csv.writer(string_io, delimiter=',')
        _write_csv_score_data_for_project(csv_writer, self.project)
        string_io.seek(0)
        # read actual rows using csv reader for easier comparison to expected
        act_csv_reader = csv.reader(string_io, delimiter=',')  # Location, Target, predicted_value, truth_value, abs_err
        act_rows = list(act_csv_reader)
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17_exp-download.csv', 'r') as fp:
            exp_csv_reader = csv.reader(fp, delimiter=',')  # model,timezero,season,location,target,Absolute_Error
            exp_rows = list(exp_csv_reader)
            for idx, (exp_row, act_row) in enumerate(zip(exp_rows, act_rows)):
                self.assertEqual(exp_row[0], act_row[0])  # model
                self.assertEqual(exp_row[1], act_row[1])  # timezero
                self.assertEqual(exp_row[2], act_row[2])  # season
                self.assertEqual(exp_row[3], act_row[3])  # location
                self.assertEqual(exp_row[4], act_row[4])  # target
                if idx == 0:  # header
                    self.assertEqual(exp_row[5], act_row[5])  # Absolute_Error
                else:  # float
                    self.assertAlmostEqual(float(exp_row[5]), float(act_row[5]))  # Absolute_Error
