import csv
import datetime
import io
import math
from collections import defaultdict
from pathlib import Path

from django.test import TestCase

from forecast_app.api_views import _write_csv_score_data_for_project
from forecast_app.models import Project, TimeZero, Location, Target
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.score import Score
from forecast_app.scores.definitions import SCORE_ABBREV_TO_NAME_AND_DESCR, _timezero_loc_target_pks_to_truth_values
from utils.make_cdc_flu_contests_project import make_cdc_locations_and_targets, CDC_CONFIG_DICT


class ScoresTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        make_cdc_locations_and_targets(cls.project)

        cls.project.load_template(Path('forecast_app/tests/2016-2017_submission_template-small.csv'))

        # load truth only for the TimeZero in truths-2016-2017-reichlab.csv we're testing against
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date=datetime.date(2017, 1, 1),
                                                is_season_start=True, season_name='season1')
        cls.project.load_truth_data(Path('utils/ensemble-truth-table-script/truths-2016-2017-reichlab.csv'))

        cls.forecast_model = ForecastModel.objects.create(project=cls.project, name='test model')
        cls.forecast_model.load_forecast(Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv'), cls.time_zero)


    def test_score_creation(self):
        # test creation of the current Scores/types
        Score.ensure_all_scores_exist()
        self.assertEqual(4, Score.objects.count())
        self.assertEqual(set(SCORE_ABBREV_TO_NAME_AND_DESCR.keys()),
                         set([score.abbreviation for score in Score.objects.all()]))


    def test_absolute_error_score(self):
        # sanity-test 'abs_error'
        Score.ensure_all_scores_exist()
        score = Score.objects.filter(abbreviation='abs_error').first()
        update_scores_for_all_projects()

        # test creation of a ScoreLastUpdate entry. we don't test score_last_update.updated_at
        score_last_update = score.last_update_for_forecast_model(self.forecast_model)
        self.assertIsNotNone(score_last_update)

        # test score values
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17_exp-abs-errors.csv', 'r') as fp:
            csv_reader = csv.reader(fp, delimiter=',')  # Location, Target, predicted_value, truth_value, abs_err
            next(csv_reader)  # skip header
            exp_rows = sorted([(i[0], i[1], i[-1]) for i in csv_reader])  # Location, Target, abs_err

            # convert actual rows from IDs into strings for readability
            act_rows = sorted(score.values.values_list('location__name', 'target__name', 'value'))
            for exp_row, act_row in zip(exp_rows, act_rows):
                self.assertEqual(exp_row[0], act_row[0])  # location name
                self.assertEqual(exp_row[1], act_row[1])  # target name
                self.assertAlmostEqual(float(exp_row[2]), act_row[2])  # value


    def test_log_score_single_bin_score(self):
        Score.ensure_all_scores_exist()
        score = Score.objects.filter(abbreviation='log_single_bin').first()
        self.assertIsNotNone(score)

        # creation of a ScoreLastUpdate entry is tested above

        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        make_cdc_locations_and_targets(project2)
        project2.load_template(Path('forecast_app/tests/scores/2016-2017_submission_template-small.csv'))

        time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2016, 10, 30))
        project2.load_truth_data(Path('forecast_app/tests/scores/truths-2016-2017-reichlab-small.csv'))

        forecast_model2 = ForecastModel.objects.create(project=project2, name='test model')
        forecast_model2.load_forecast(Path('forecast_app/tests/scores/20161030-KoTstable-20161114-small.cdc.csv'),
                                      time_zero2)

        # expected truth from truths-2016-2017-reichlab-small.csv: 20161030, US National, 1 wk ahead -> 1.55838
        # -> corresponding row in 20161030-KoTstable-20161114-small.cdc.csv:
        #    ['US National', '1 wk ahead', 'Bin', 'percent', 1.5, 1.6, 0.20253796115633]  # 1.5 <= 1.55838 < 1.6
        exp_score = math.log(0.20253796115633)

        # calculate the score and test results
        score.update_score_for_model(forecast_model2)
        self.assertEqual(1, score.values.count())  # only one location + target in the forecast -> only one bin

        score_value = score.values.first()
        self.assertEqual('US National', score_value.location.name)
        self.assertEqual('1 wk ahead', score_value.target.name)
        self.assertEqual(exp_score, score_value.value)

        # todo when truth falls exactly on Bin_start_incl and Bin_end_notincl
        self.fail()

        # todo "clip Math.log(0) to -999 instead of its real value (-Infinity)"
        # https://github.com/reichlab/flusight/wiki/Scoring#2-log-score-single-bin
        self.fail()


    def test_download_scores(self):
        Score.ensure_all_scores_exist()
        update_scores_for_all_projects()
        string_io = io.StringIO()
        csv_writer = csv.writer(string_io, delimiter=',')
        _write_csv_score_data_for_project(csv_writer, self.project)
        string_io.seek(0)
        # read actual rows using csv reader for easier comparison to expected
        act_csv_reader = csv.reader(string_io, delimiter=',')  # Location, Target, predicted_value, truth_value, abs_err
        act_rows = list(act_csv_reader)
        with open('forecast_app/tests/EW1-KoTsarima-2017-01-17_exp-download.csv', 'r') as fp:
            # each row: model,timezero,season,location,target,error,abs_error,const
            exp_csv_reader = csv.reader(fp, delimiter=',')
            exp_rows = list(exp_csv_reader)
            for idx, (exp_row, act_row) in enumerate(zip(exp_rows, act_rows)):
                if idx == 0:  # header
                    exp_header = ['model', 'timezero', 'season', 'location', 'target', 'error', 'abs_error', 'const',
                                  'log_single_bin']
                    self.assertEqual(exp_header, act_row)
                    continue

                self.assertEqual(exp_row[0], act_row[0])  # model
                self.assertEqual(exp_row[1], act_row[1])  # timezero. format: YYYYMMDD_DATE_FORMAT
                self.assertEqual(exp_row[2], act_row[2])  # season
                self.assertEqual(exp_row[3], act_row[3])  # location
                self.assertEqual(exp_row[4], act_row[4])  # target


                # test values: error, abs_error, const. any could be ''
                def test_float_or_empty(exp_val, act_val):
                    self.assertEqual(exp_val, act_val) if not exp_val \
                        else self.assertAlmostEqual(float(exp_val), float(act_val))


                test_float_or_empty(exp_row[5], act_row[5])
                test_float_or_empty(exp_row[6], act_row[6])
                test_float_or_empty(exp_row[7], act_row[7])


    def test_timezero_loc_target_pks_to_truth_values(self):
        tz_pk = self.time_zero.pk
        loc1_pk = Location.objects.filter(name='HHS Region 1').first().pk
        loc2_pk = Location.objects.filter(name='HHS Region 2').first().pk
        loc3_pk = Location.objects.filter(name='HHS Region 3').first().pk
        loc4_pk = Location.objects.filter(name='HHS Region 4').first().pk
        loc5_pk = Location.objects.filter(name='HHS Region 5').first().pk
        loc6_pk = Location.objects.filter(name='HHS Region 6').first().pk
        loc7_pk = Location.objects.filter(name='HHS Region 7').first().pk
        loc8_pk = Location.objects.filter(name='HHS Region 8').first().pk
        loc9_pk = Location.objects.filter(name='HHS Region 9').first().pk
        loc10_pk = Location.objects.filter(name='HHS Region 10').first().pk
        loc11_pk = Location.objects.filter(name='US National').first().pk
        target1_pk = Target.objects.filter(name='Season onset').first().pk
        target2_pk = Target.objects.filter(name='Season peak week').first().pk
        target3_pk = Target.objects.filter(name='Season peak percentage').first().pk
        target4_pk = Target.objects.filter(name='1 wk ahead').first().pk
        target5_pk = Target.objects.filter(name='2 wk ahead').first().pk
        target6_pk = Target.objects.filter(name='3 wk ahead').first().pk
        target7_pk = Target.objects.filter(name='4 wk ahead').first().pk
        exp_dict = {  # {timezero_pk: {location_pk: {target_id: truth_value}}}
            tz_pk: {
                loc1_pk: {target1_pk: [20161225.0], target2_pk: [20170205.0], target3_pk: [3.19221],
                          target4_pk: [1.52411], target5_pk: [1.73987], target6_pk: [2.06524], target7_pk: [2.51375]},
                loc2_pk: {target1_pk: [20161120.0], target2_pk: [20170205.0], target3_pk: [6.93759],
                          target4_pk: [5.07086], target5_pk: [5.68166], target6_pk: [6.01053], target7_pk: [6.49829]},
                loc3_pk: {target1_pk: [20161218.0], target2_pk: [20170212.0], target3_pk: [5.20003],
                          target4_pk: [2.81366], target5_pk: [3.09968], target6_pk: [3.45232], target7_pk: [3.73339]},
                loc4_pk: {target1_pk: [20161113.0], target2_pk: [20170212.0], target3_pk: [5.5107],
                          target4_pk: [2.89395], target5_pk: [3.68564], target6_pk: [3.69188], target7_pk: [4.53169]},
                loc5_pk: {target1_pk: [20161225.0], target2_pk: [20170212.0], target3_pk: [4.31787],
                          target4_pk: [2.11757], target5_pk: [2.4432], target6_pk: [2.76295], target7_pk: [3.182]},
                loc6_pk: {target1_pk: [20170108.0], target2_pk: [20170205.0], target3_pk: [9.87589],
                          target4_pk: [4.80185], target5_pk: [5.26955], target6_pk: [6.10427], target7_pk: [8.13221]},
                loc7_pk: {target1_pk: [20161225.0], target2_pk: [20170205.0], target3_pk: [6.35948],
                          target4_pk: [2.75581], target5_pk: [3.46528], target6_pk: [4.56991], target7_pk: [5.52653]},
                loc8_pk: {target1_pk: [20161218.0], target2_pk: [20170212.0], target3_pk: [2.72703],
                          target4_pk: [1.90851], target5_pk: [2.2668], target6_pk: [2.07104], target7_pk: [2.27632]},
                loc9_pk: {target1_pk: [20161218.0], target2_pk: [20161225.0], target3_pk: [3.30484],
                          target4_pk: [2.83778], target5_pk: [2.68071], target6_pk: [2.9577], target7_pk: [3.03987]},
                loc10_pk: {target1_pk: [20161211.0], target2_pk: [20161225.0], target3_pk: [3.67061],
                           target4_pk: [2.15197], target5_pk: [3.25108], target6_pk: [2.51434], target7_pk: [2.28634]},
                loc11_pk: {target1_pk: [20161211.0], target2_pk: [20170205.0], target3_pk: [5.06094],
                           target4_pk: [3.07623], target5_pk: [3.50708], target6_pk: [3.79872], target7_pk: [4.43601]},
            }
        }

        # convert exp_dict innermost dicts to defaultdicts, which is what _timezero_loc_target_pks_to_truth_values()
        # returns
        for pk0, dict0 in exp_dict.items():
            for pk1, dict1 in dict0.items():
                exp_dict[pk0][pk1] = defaultdict(list, dict1)

        act_dict = _timezero_loc_target_pks_to_truth_values(self.forecast_model)
        self.assertEqual(exp_dict, act_dict)


def update_scores_for_all_projects():
    """
    Update all scores for all projects. Limited usefulness b/c runs in the calling thread and therefore blocks.
    """
    for score in Score.objects.all():
        for project in Project.objects.all():
            for forecast_model in project.models.all():
                score.update_score_for_model(forecast_model)
