import csv
import datetime
import io
import math
from collections import defaultdict
from pathlib import Path

from django.test import TestCase

from forecast_app.api_views import _write_csv_score_data_for_project
from forecast_app.models import Project, TimeZero, Location, Target
from forecast_app.models.data import CDCData
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.score import Score, ScoreValue
from forecast_app.scores.definitions import SCORE_ABBREV_TO_NAME_AND_DESCR, _timezero_loc_target_pks_to_truth_values, \
    LOG_SINGLE_BIN_NEGATIVE_INFINITY
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
        _update_scores_for_all_projects()

        # test creation of a ScoreLastUpdate entry. we don't test score_last_update.updated_at
        score_last_update = score.last_update_for_forecast_model(self.forecast_model)
        self.assertIsNotNone(score_last_update)

        # test score values
        with open('forecast_app/tests/scores/EW1-KoTsarima-2017-01-17_exp-abs-errors.csv', 'r') as fp:
            # Location,Target,predicted_value,truth_value,abs_err,log_single_bin,log_multi_bin
            csv_reader = csv.reader(fp, delimiter=',')
            next(csv_reader)  # skip header
            exp_rows = sorted([(i[0], i[1], i[4]) for i in csv_reader])  # Location, Target, abs_err

            # convert actual rows from IDs into strings for readability
            act_rows = sorted(score.values.values_list('location__name', 'target__name', 'value'))
            for exp_row, act_row in zip(exp_rows, act_rows):
                self.assertEqual(exp_row[0], act_row[0])  # location name
                self.assertEqual(exp_row[1], act_row[1])  # target name
                self.assertAlmostEqual(float(exp_row[2]), act_row[2])  # value


    def test_log_single_bin_score(self):
        Score.ensure_all_scores_exist()
        score = Score.objects.filter(abbreviation='log_single_bin').first()
        self.assertIsNotNone(score)

        # creation of a ScoreLastUpdate entry is tested above

        project2, forecast_model2, forecast2 = _make_log_score_project()

        # truth from truths-2016-2017-reichlab-small.csv: 20161030, US National, 1 wk ahead -> 1.55838
        # -> corresponding bin in 20161030-KoTstable-20161114-small.cdc.csv:
        #    US National,1 wk ahead,Bin,percent,1.5,1.6,0.20253796115633  # where 1.5 <= 1.55838 < 1.6
        # expected score is therefore: math.log(0.20253796115633) = -1.596827947504047

        # calculate the score and test results
        print('xx')
        score.update_score_for_model(forecast_model2)
        print('xx2')
        self.assertEqual(1, score.values.count())  # only one location + target in the forecast -> only one bin

        score_value = score.values.first()
        self.assertEqual('US National', score_value.location.name)
        self.assertEqual('1 wk ahead', score_value.target.name)
        self.assertAlmostEqual(math.log(0.20253796115633), score_value.value)

        # test when truth falls exactly on Bin_end_notincl
        truth_data = project2.truth_data_qs().filter(target__name='1 wk ahead').first()
        # this takes us to the next bin:
        #   US National,1 wk ahead,Bin,percent,1.6,1.7,0.0770752152650201
        #   -> math.log(0.0770752152650201) = -2.562973512284597
        truth_data.value = 1.6
        truth_data.save()
        score.update_score_for_model(forecast_model2)
        score_value = score.values.first()
        self.assertAlmostEqual(math.log(0.0770752152650201), score_value.value)

        # test when truth falls exactly on Bin_start_incl
        truth_data = project2.truth_data_qs().filter(target__name='1 wk ahead').first()
        truth_data.value = 1.5  # 1.5 -> same bin: US National,1 wk ahead,Bin,percent,1.5,1.6,0.20253796115633
        truth_data.save()

        score.update_score_for_model(forecast_model2)
        score_value = score.values.first()
        self.assertAlmostEqual(math.log(0.20253796115633), score_value.value)

        # test "clip Math.log(0) to -999 instead of its real value (-Infinity)". do so by changing this bin to have a
        # value of zero: US National,1 wk ahead,Bin,percent,1.6,1.7,0.0770752152650201
        forecast_data = forecast2.cdcdata_set \
            .filter(location__name='US National', target__name='1 wk ahead', row_type=CDCData.BIN_ROW_TYPE,
                    bin_start_incl=1.6) \
            .first()
        forecast_data.value = 0
        forecast_data.save()

        truth_data = project2.truth_data_qs().filter(target__name='1 wk ahead').first()  # only one test row matches
        truth_data.value = 1.65  # 1.65 -> bin: US National,1 wk ahead,Bin,percent,1.6,1.7,0  # NB: value is now 0
        truth_data.save()

        score.update_score_for_model(forecast_model2)
        score_value = score.values.first()
        self.assertAlmostEqual(LOG_SINGLE_BIN_NEGATIVE_INFINITY, score_value.value)


    def test_log_multi_bin_score(self):
        # see log-score-multi-bin-hand-calc.xlsx for expected values for the cases
        Score.ensure_all_scores_exist()
        score = Score.objects.filter(abbreviation='log_multi_bin').first()
        self.assertIsNotNone(score)

        project2, forecast_model2, forecast2 = _make_log_score_project()

        # case 1: calculate the score and test results using actual truth:
        #   20161030,US National,1 wk ahead,1.55838  ->  bin: US National,1 wk ahead,Bin,percent,1.5,1.6,0.20253796115633
        score.update_score_for_model(forecast_model2)
        self.assertEqual(1, score.values.count())  # only one location + target in the forecast -> only one bin

        score_value = score.values.first()
        self.assertEqual('US National', score_value.location.name)
        self.assertEqual('1 wk ahead', score_value.target.name)
        # 5 predictions above + prediction for truth + 5 below:
        bin_value_sum = 0.007070248 + 0.046217761 + 0.135104139 + 0.196651291 + 0.239931096 + \
                        0.202537961 + \
                        0.077075215 + 0.019657804 + 0.028873246 + 0.003120724 + 0.019698882
        self.assertAlmostEqual(math.log(bin_value_sum), score_value.value)

        # case 2a:
        truth_data = project2.truth_data_qs().filter(target__name='1 wk ahead').first()  # only one test row matches
        truth_data.value = 0  # -> bin: US National,1 wk ahead,Bin,percent,0,0.1,1.39332920335022e-07
        truth_data.save()

        score.update_score_for_model(forecast_model2)
        one_pt_3e07 = 1.39332920335022e-07
        bin_value_sum = 0 + \
                        one_pt_3e07 + \
                        one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + 4.17998761005067e-07 + 1.81132796435529e-06
        self.assertAlmostEqual(math.log(bin_value_sum), score.values.first().value)

        # case 2b:
        truth_data = project2.truth_data_qs().filter(target__name='1 wk ahead').first()  # only one test row matches
        truth_data.value = 0.1  # -> US National,1 wk ahead,Bin,percent,0.1,0.2,1.39332920335022e-07
        truth_data.save()

        score.update_score_for_model(forecast_model2)
        bin_value_sum = one_pt_3e07 + \
                        one_pt_3e07 + \
                        one_pt_3e07 + one_pt_3e07 + 4.17998761005067e-07 + 1.81132796435529e-06 + 7.52397769809119e-06
        self.assertAlmostEqual(math.log(bin_value_sum), score.values.first().value)

        # case 3a:
        truth_data = project2.truth_data_qs().filter(target__name='1 wk ahead').first()  # only one test row matches
        truth_data.value = 13  # -> US National,1 wk ahead,Bin,percent,13,100,1.39332920335022e-07
        truth_data.save()

        score.update_score_for_model(forecast_model2)
        bin_value_sum = one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + \
                        one_pt_3e07 + \
                        0
        self.assertAlmostEqual(math.log(bin_value_sum), score.values.first().value)

        # case 3b:
        truth_data = project2.truth_data_qs().filter(target__name='1 wk ahead').first()  # only one test row matches
        truth_data.value = 12.95  # -> US National,1 wk ahead,Bin,percent,12.9,13,1.39332920335022e-07
        truth_data.save()

        score.update_score_for_model(forecast_model2)
        bin_value_sum = one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + \
                        one_pt_3e07 + \
                        one_pt_3e07
        self.assertAlmostEqual(math.log(bin_value_sum), score.values.first().value)


    def test_log_multi_bin_score_none_cases(self):
        Score.ensure_all_scores_exist()
        score = Score.objects.filter(abbreviation='log_multi_bin').first()
        project2, forecast_model2, forecast2 = _make_log_score_project()

        # case: truth = None, but no bin start/end that's None -> no matching bin -> no ScoreValue created
        truth_data = project2.truth_data_qs().filter(target__name='1 wk ahead').first()  # only one test row matches
        truth_data.value = None  # -> no matching bin
        truth_data.save()

        score.update_score_for_model(forecast_model2)
        score_value = score.values.first()  # None
        self.assertIsNone(score_value)

        # case: truth = None, with a bin start that's None. we'll change the first bin row:
        #   US National,1 wk ahead,Bin,percent,None,0.1,1.39332920335022e-07  # set start = None
        # NB: in this case, the score should degenerate to the num_bins_one_side=0 'Log score (single bin)' calculation
        forecast_data = forecast2.cdcdata_set \
            .filter(location__name='US National', target__name='1 wk ahead', row_type=CDCData.BIN_ROW_TYPE,
                    bin_start_incl=0) \
            .first()
        forecast_data.bin_start_incl = None
        forecast_data.save()

        score.update_score_for_model(forecast_model2)
        one_pt_3e07 = 1.39332920335022e-07
        self.assertAlmostEqual(math.log(one_pt_3e07), score.values.first().value)

        # case: truth = None, with a bin end that's None. we'll change the first bin row:
        #   US National,1 wk ahead,Bin,percent,0,None,1.39332920335022e-07  # reset start to 0, set end = None
        forecast_data = forecast2.cdcdata_set \
            .filter(location__name='US National', target__name='1 wk ahead', row_type=CDCData.BIN_ROW_TYPE,
                    bin_start_incl=None) \
            .first()
        forecast_data.bin_start_incl = 0  # reset
        forecast_data.bin_end_notincl = None
        forecast_data.save()

        score.update_score_for_model(forecast_model2)
        self.assertAlmostEqual(math.log(one_pt_3e07), score.values.first().value)


    def test_log_multi_bin_score_large_case(self):
        # a larger case with more than one loc+target
        Score.ensure_all_scores_exist()
        score = Score.objects.filter(abbreviation='log_multi_bin').first()

        project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
        time_zero2 = TimeZero.objects.create(project=project2, timezero_date='2017-01-01')
        make_cdc_locations_and_targets(project2)
        project2.load_template(Path('forecast_app/tests/2016-2017_submission_template.csv'))

        forecast_model2 = ForecastModel.objects.create(project=project2)
        forecast_model2.load_forecast(Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv'),
                                      time_zero2)

        project2.load_truth_data(Path('forecast_app/tests/truth_data/truths-ok.csv'))

        # test the scores - only ones with truth are created. see log-score-multi-bin-hand-calc.xlsx for how expected
        # values were verified
        score.update_score_for_model(forecast_model2)

        # check two targets from different distributions
        # '1 wk ahead': truth = 0.73102
        two_pt_83e07 = 2.83662889964352e-07
        bin_value_sum = two_pt_83e07 + two_pt_83e07 + two_pt_83e07 + two_pt_83e07 + two_pt_83e07 + \
                        two_pt_83e07 + \
                        8.50988669893058e-07 + 1.13465155985741e-06 + 1.98564022975047e-06 + 1.10628527086097e-05 + 1.24811671584315e-05
        score_value = ScoreValue.objects.filter(score=score, forecast__forecast_model__project=project2,
                                                target__name='1 wk ahead').first()
        self.assertAlmostEqual(math.log(bin_value_sum), score_value.value)

        # '2 wk ahead': truth = 0.688338
        one_pt_4e06 = 1.45380624364055e-06  # two_pt_83e07
        bin_value_sum = one_pt_4e06 + one_pt_4e06 + one_pt_4e06 + one_pt_4e06 + one_pt_4e06 + \
                        one_pt_4e06 + \
                        2.90761248728109e-06 + 4.36141873092164e-06 + 1.16304499491244e-05 + 1.45380624364055e-05 + 0.00031501033203663
        score_value = ScoreValue.objects.filter(score=score, forecast__forecast_model__project=project2,
                                                target__name='2 wk ahead').first()
        self.assertAlmostEqual(math.log(bin_value_sum), score_value.value)


    def test_download_scores(self):
        Score.ensure_all_scores_exist()
        _update_scores_for_all_projects()
        string_io = io.StringIO()
        csv_writer = csv.writer(string_io, delimiter=',')
        _write_csv_score_data_for_project(csv_writer, self.project)
        string_io.seek(0)

        # read actual rows using csv reader for easier comparison to expected
        act_csv_reader = csv.reader(string_io, delimiter=',')
        act_rows = list(act_csv_reader)
        with open('forecast_app/tests/scores/EW1-KoTsarima-2017-01-17_exp-download.csv', 'r') as fp:
            # model,timezero,season,location,target,error,abs_error,log_single_bin,log_multi_bin
            exp_csv_reader = csv.reader(fp, delimiter=',')
            exp_rows = list(exp_csv_reader)
            for idx, (exp_row, act_row) in enumerate(zip(exp_rows, act_rows)):
                if idx == 0:  # header
                    self.assertEqual(exp_row, act_row)
                    continue

                # test non-numeric columns
                self.assertEqual(exp_row[0], act_row[0])  # model
                self.assertEqual(exp_row[1], act_row[1])  # timezero. format: YYYYMMDD_DATE_FORMAT
                self.assertEqual(exp_row[2], act_row[2])  # season
                self.assertEqual(exp_row[3], act_row[3])  # location
                self.assertEqual(exp_row[4], act_row[4])  # target


                # test (numeric) values: error, abs_error, log_single_bin, log_multi_bin. NB: any could be ''
                def test_float_or_empty(exp_val, act_val):
                    self.assertEqual(exp_val, act_val) if (not exp_val) or (not act_val) \
                        else self.assertAlmostEqual(float(exp_val), float(act_val))


                test_float_or_empty(exp_row[5], act_row[5])  # 'error'
                test_float_or_empty(exp_row[6], act_row[6])  # 'abs_error'
                test_float_or_empty(exp_row[7], act_row[7])  # 'log_single_bin'  # always '' for setUpTestData()
                test_float_or_empty(exp_row[8], act_row[8])  # 'log_multi_bin'   # ""


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


def _update_scores_for_all_projects():
    """
    Update all scores for all projects. Limited usefulness b/c runs in the calling thread and therefore blocks.
    """
    for score in Score.objects.all():
        for project in Project.objects.all():
            for forecast_model in project.models.all():
                score.update_score_for_model(forecast_model)


def _make_log_score_project():
    project2 = Project.objects.create(config_dict=CDC_CONFIG_DICT)
    make_cdc_locations_and_targets(project2)
    project2.load_template(Path('forecast_app/tests/scores/2016-2017_submission_template-small.csv'))

    time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2016, 10, 30))
    project2.load_truth_data(Path('forecast_app/tests/scores/truths-2016-2017-reichlab-small.csv'))

    forecast_model2 = ForecastModel.objects.create(project=project2, name='test model')
    forecast2 = forecast_model2.load_forecast(
        Path('forecast_app/tests/scores/20161030-KoTstable-20161114-small.cdc.csv'), time_zero2)

    return project2, forecast_model2, forecast2
