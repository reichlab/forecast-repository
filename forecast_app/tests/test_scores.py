import csv
import datetime
import io
import json
import logging
import math
from pathlib import Path

from django.test import TestCase

from forecast_app.api_views import _write_csv_score_data_for_project
from forecast_app.models import Project, TimeZero, Unit, Target, TargetLwr, Forecast, TruthData
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.score import Score, ScoreValue
from forecast_app.scores.bin_utils import _tz_loc_targ_pk_to_true_lwr, _targ_pk_to_lwrs, \
    _tz_loc_targ_pk_lwr_to_pred_val
from forecast_app.scores.calc_error import _timezero_loc_target_pks_to_truth_values
from forecast_app.scores.calc_interval import _calculate_interval_score_values
from forecast_app.scores.calc_log import LOG_SINGLE_BIN_NEGATIVE_INFINITY
from forecast_app.scores.definitions import SCORE_ABBREV_TO_NAME_AND_DESCR
from utils.cdc import load_cdc_csv_forecast_file, make_cdc_units_and_targets
from utils.forecast import load_predictions_from_json_io_dict
from utils.make_minimal_projects import _make_docs_project
from utils.make_thai_moph_project import create_thai_units_and_targets
from utils.project import load_truth_data, create_project_from_json
from utils.utilities import get_or_create_super_po_mo_users


logging.getLogger().setLevel(logging.ERROR)


class ScoresTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        cls.project = Project.objects.create()
        make_cdc_units_and_targets(cls.project)

        # load truth only for the TimeZero in truths-2016-2017-reichlab.csv we're testing against
        cls.time_zero = TimeZero.objects.create(project=cls.project, timezero_date=datetime.date(2017, 1, 1),
                                                is_season_start=True, season_name='season1')
        load_truth_data(cls.project, Path('utils/ensemble-truth-table-script/truths-2016-2017-reichlab.csv'))

        # use default abbreviation (""):
        cls.forecast_model = ForecastModel.objects.create(project=cls.project, name='test model')
        csv_file_path = Path('forecast_app/tests/EW1-KoTsarima-2017-01-17-small.csv')  # EW01 2017
        load_cdc_csv_forecast_file(2016, cls.forecast_model, csv_file_path, cls.time_zero)


    def test_score_creation(self):
        Score.ensure_all_scores_exist()

        # test creation of the current Scores/types
        self.assertEqual(len(SCORE_ABBREV_TO_NAME_AND_DESCR), Score.objects.count())
        self.assertEqual(set(SCORE_ABBREV_TO_NAME_AND_DESCR),
                         set([score.abbreviation for score in Score.objects.all()]))


    #
    # test 'abs_error'
    #

    def test_absolute_error_score(self):
        Score.ensure_all_scores_exist()
        abs_error_score = Score.objects.filter(abbreviation='abs_error').first()
        self.assertIsNotNone(abs_error_score)

        # test creation of a ScoreLastUpdate entry. we don't test score_last_update.updated_at
        abs_error_score.update_score_for_model(self.forecast_model)
        score_last_update = abs_error_score.last_update_for_forecast_model(self.forecast_model)
        self.assertIsNotNone(score_last_update)

        # test score values
        with open('forecast_app/tests/scores/EW1-KoTsarima-2017-01-17_exp-abs-errors.csv', 'r') as fp:
            # Unit,Target,predicted_value,truth_value,abs_err,log_single_bin,log_multi_bin
            csv_reader = csv.reader(fp, delimiter=',')
            next(csv_reader)  # skip header
            exp_rows = sorted([(i[0], i[1], i[4]) for i in csv_reader])  # Unit, Target, abs_err

            # convert actual rows from IDs into strings for readability
            act_rows = sorted(abs_error_score.values.values_list('unit__name', 'target__name', 'value'))
            for exp_row, act_row in zip(exp_rows, act_rows):
                self.assertEqual(exp_row[0], act_row[0])  # unit name
                self.assertEqual(exp_row[1], act_row[1])  # target name
                self.assertAlmostEqual(float(exp_row[2]), act_row[2])  # value


    #
    # test 'log_single_bin' and 'log_multi_bin'
    #

    def test_log_single_bin_score(self):
        Score.ensure_all_scores_exist()
        log_single_bin_score = Score.objects.filter(abbreviation='log_single_bin').first()
        self.assertIsNotNone(log_single_bin_score)

        # creation of a ScoreLastUpdate entry is tested above

        project2, forecast_model2, forecast2, _ = _make_cdc_log_score_project()

        # truth from truths-2016-2017-reichlab-small.csv: 20161030, US National, 1 wk ahead -> 1.55838
        # -> corresponding bin in 20161030-KoTstable-20161114-small.cdc.csv:
        #    US National,1 wk ahead,Bin,percent,1.5,1.6,0.20253796115633  # where 1.5 <= 1.55838 < 1.6
        # expected score is therefore: math.log(0.20253796115633) = -1.596827947504047

        # calculate the score and test results
        log_single_bin_score.update_score_for_model(forecast_model2)
        # only one unit + target in the forecast -> only one bin:
        self.assertEqual(1, log_single_bin_score.values.count())

        score_value = log_single_bin_score.values.first()
        self.assertEqual('US National', score_value.unit.name)
        self.assertEqual('1 wk ahead', score_value.target.name)
        self.assertAlmostEqual(math.log(0.20253796115633), score_value.value)

        # test when truth falls exactly on Bin_end_notincl
        truth_data = project2.truth_data_qs().filter(unit__name='US National', target__name='1 wk ahead').first()
        # this takes us to the next bin:
        #   US National,1 wk ahead,Bin,percent,1.6,1.7,0.0770752152650201
        #   -> math.log(0.0770752152650201) = -2.562973512284597
        truth_data.value_f = 1.6  # value_f for continuous targets
        truth_data.save()

        log_single_bin_score.update_score_for_model(forecast_model2)
        score_value = log_single_bin_score.values.first()
        self.assertAlmostEqual(math.log(0.0770752152650201), score_value.value)

        # test when truth falls exactly on Bin_start_incl
        truth_data = project2.truth_data_qs().filter(unit__name='US National', target__name='1 wk ahead').first()
        truth_data.value_f = 1.5  # 1.5 -> same bin: US National,1 wk ahead,Bin,percent,1.5,1.6,0.20253796115633
        truth_data.save()

        log_single_bin_score.update_score_for_model(forecast_model2)
        score_value = log_single_bin_score.values.first()
        self.assertAlmostEqual(math.log(0.20253796115633), score_value.value)

        # test "clip Math.log(0) to -999 instead of its real value (-Infinity)". do so by changing this bin to have a
        # value of zero: US National,1 wk ahead,Bin,percent,1.6,1.7,0.0770752152650201
        bin_dist = forecast2.bin_distribution_qs() \
            .filter(unit__name='US National', target__name='1 wk ahead', cat_f=1.6) \
            .first()
        bin_dist.cat_f = 0.0
        bin_dist.save()

        truth_data = project2.truth_data_qs() \
            .filter(unit__name='US National', target__name='1 wk ahead') \
            .first()
        truth_data.value_f = 1.65  # 1.65 -> bin: US National,1 wk ahead,Bin,percent,1.6,1.7,0  # NB: value is now 0
        truth_data.save()

        log_single_bin_score.update_score_for_model(forecast_model2)
        score_value = log_single_bin_score.values.first()
        self.assertAlmostEqual(LOG_SINGLE_BIN_NEGATIVE_INFINITY, score_value.value)


    def test_log_multi_bin_score_missing_rows(self):
        # exposes the bug: [Bin-oriented scores do not account for missing zero-value bin rows](https://github.com/reichlab/forecast-repository/issues/123)
        Score.ensure_all_scores_exist()
        log_multi_bin_score = Score.objects.filter(abbreviation='log_multi_bin').first()

        _, forecast_model2, forecast2, _ = _make_cdc_log_score_project()

        # delete the last forecast bin row that's within the window of 5, which should change the calculation. the row:
        #   US National	1 wk ahead	Bin	percent	2	2.1	0.0196988816334531
        # the row after it is:
        #   US National	1 wk ahead	Bin	percent	2.1	2.2	0.000162775167244309
        forecast2.bin_distribution_qs() \
            .filter(unit__name='US National', target__name='1 wk ahead', cat_f=2.0) \
            .first() \
            .delete()

        # update and get the one ScoreValue (only one unit + target in the forecast)
        log_multi_bin_score.update_score_for_model(forecast_model2)
        score_value = log_multi_bin_score.values.first()

        # 5 predictions above + prediction for truth + 5 below.
        # 0 is correct - from the template - but the bug is that it uses 0.000162775167244309 instead
        bin_value_sum = 0.007070248 + 0.046217761 + 0.135104139 + 0.196651291 + 0.239931096 + \
                        0.202537961 + \
                        0.077075215 + 0.019657804 + 0.028873246 + 0.003120724 + 0
        # AssertionError: -0.04474688998028026 != -0.024355842680506955 within 7 places
        self.assertAlmostEqual(math.log(bin_value_sum), score_value.value)


    def test_drive_refactor_to_template_oriented_solution(self):
        # drive data structures needed for new score algorithm
        Score.ensure_all_scores_exist()

        project2, forecast_model2, _, time_zero2 = _make_cdc_log_score_project()
        loc_us_nat = project2.units.filter(name='US National').first()
        target_name_to_pk = {target.name: target.pk for target in project2.targets.all()}

        # get _tz_loc_targ_pk_to_true_lwr() - needed here, but tested more thoroughly below
        tz_loc_targ_pk_to_true_lwr = _tz_loc_targ_pk_to_true_lwr(project2)
        self.assertEqual(tz_loc_targ_pk_to_true_lwr,
                         {time_zero2.pk: {loc_us_nat.pk: {target_name_to_pk['1 wk ahead']: 1.5,
                                                          target_name_to_pk['2 wk ahead']: 1.6,
                                                          target_name_to_pk['3 wk ahead']: 1.9,
                                                          target_name_to_pk['4 wk ahead']: 1.8,
                                                          target_name_to_pk['Season peak percentage']: 5.0}}})

        # test _targ_pk_to_lwrs()
        targ_pk_to_lwrs = _targ_pk_to_lwrs(project2)
        act_lwrs = targ_pk_to_lwrs[target_name_to_pk['1 wk ahead']]
        self.assertEqual(131, len(act_lwrs))
        self.assertEqual(0, act_lwrs[0])
        self.assertEqual(1.5, act_lwrs[15])
        self.assertEqual(13, act_lwrs[-1])

        # test _tz_loc_targ_pk_lwr_to_pred_val()
        tzltpk_lwr_to_pred_val = _tz_loc_targ_pk_lwr_to_pred_val(forecast_model2)
        act_lwr_to_pred_val = tzltpk_lwr_to_pred_val[time_zero2.pk][loc_us_nat.pk][target_name_to_pk['1 wk ahead']]
        self.assertEqual(131, len(act_lwr_to_pred_val))  # same - no missing zero-value bins in this forecast
        self.assertEqual(1.39332920335022e-07, act_lwr_to_pred_val[0])
        self.assertEqual(0.20253796115633, act_lwr_to_pred_val[1.5])
        self.assertEqual(1.39332920335022e-07, act_lwr_to_pred_val[13])

        # get the true bin 'key' bin_start_incl from tz_loc_targ_pk_to_true_lwr
        true_lwr = tz_loc_targ_pk_to_true_lwr[time_zero2.pk][loc_us_nat.pk][target_name_to_pk['1 wk ahead']]  # 1.5
        targ_pk_to_lwrs = act_lwrs
        forec_st_to_pred_val = act_lwr_to_pred_val

        # implement _calculate_pit_score_values(). get all the bin rows up to truth
        true_bin_idx = targ_pk_to_lwrs.index(true_lwr)
        template_bin_keys_pre_truth = targ_pk_to_lwrs[:true_bin_idx]  # excluding true bin
        pred_vals_pre_truth = [forec_st_to_pred_val[key] for key in template_bin_keys_pre_truth]
        pred_vals_pre_truth_sum = sum(pred_vals_pre_truth)
        pred_val_true_bin = forec_st_to_pred_val[true_lwr]
        pit_score_value = ((pred_vals_pre_truth_sum * 2) + pred_val_true_bin) / 2
        self.assertAlmostEqual(0.7406917921528041, pit_score_value)

        # implement _calc_log_bin_score_values(). get 5 bin rows on each side of truth, handling start and end
        # boundaries
        num_bins_one_side = 5
        true_bin_idx = targ_pk_to_lwrs.index(true_lwr)
        start_idx = max(0, true_bin_idx - num_bins_one_side)  # max() in case goes before first bin
        end_idx = true_bin_idx + num_bins_one_side + 1  # don't care if it's after the last bin - slice ignores
        template_bin_keys_both_windows = targ_pk_to_lwrs[start_idx:end_idx]  # todo xx incl!?
        pred_vals_both_windows = [forec_st_to_pred_val[key] for key in template_bin_keys_both_windows]
        pred_vals_both_windows_sum = sum(pred_vals_both_windows)
        # todo xx LOG_SINGLE_BIN_NEGATIVE_INFINITY, true_value is None, ...:
        log_multi_bin_score_value = math.log(pred_vals_both_windows_sum)
        self.assertAlmostEqual(-0.024355842680506955, log_multi_bin_score_value)


    def test_log_multi_bin_score(self):
        # see log-score-multi-bin-hand-calc.xlsx for expected values for the cases
        Score.ensure_all_scores_exist()
        log_multi_bin_score = Score.objects.filter(abbreviation='log_multi_bin').first()
        self.assertIsNotNone(log_multi_bin_score)

        project2, forecast_model2, _, _ = _make_cdc_log_score_project()

        # case 1: calculate the score and test results using actual truth:
        #   20161030,US National,1 wk ahead,1.55838  ->  bin: US National,1 wk ahead,Bin,percent,1.5,1.6,0.20253796115633
        log_multi_bin_score.update_score_for_model(forecast_model2)
        # only one unit + target in the forecast -> only one bin:
        self.assertEqual(1, log_multi_bin_score.values.count())

        score_value = log_multi_bin_score.values.first()
        self.assertEqual('US National', score_value.unit.name)
        self.assertEqual('1 wk ahead', score_value.target.name)
        # 5 predictions above + prediction for truth + 5 below:
        bin_value_sum = 0.007070248 + 0.046217761 + 0.135104139 + 0.196651291 + 0.239931096 + \
                        0.202537961 + \
                        0.077075215 + 0.019657804 + 0.028873246 + 0.003120724 + 0.019698882
        self.assertAlmostEqual(math.log(bin_value_sum), score_value.value)

        # case 2a:
        truth_data = project2.truth_data_qs().filter(unit__name='US National', target__name='1 wk ahead').first()
        # value_f for continuous targets. -> bin: US National,1 wk ahead,Bin,percent,0,0.1,1.39332920335022e-07 :
        truth_data.value_f = 0
        truth_data.save()

        log_multi_bin_score.update_score_for_model(forecast_model2)
        one_pt_3e07 = 1.39332920335022e-07
        bin_value_sum = 0 + \
                        one_pt_3e07 + \
                        one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + 4.17998761005067e-07 + 1.81132796435529e-06
        self.assertAlmostEqual(math.log(bin_value_sum), log_multi_bin_score.values.first().value)

        # case 2b:
        truth_data = project2.truth_data_qs().filter(unit__name='US National', target__name='1 wk ahead').first()
        truth_data.value_f = 0.1  # -> US National,1 wk ahead,Bin,percent,0.1,0.2,1.39332920335022e-07
        truth_data.save()

        log_multi_bin_score.update_score_for_model(forecast_model2)
        bin_value_sum = one_pt_3e07 + \
                        one_pt_3e07 + \
                        one_pt_3e07 + one_pt_3e07 + 4.17998761005067e-07 + 1.81132796435529e-06 + 7.52397769809119e-06
        self.assertAlmostEqual(math.log(bin_value_sum), log_multi_bin_score.values.first().value)

        # case 3a:
        truth_data = project2.truth_data_qs().filter(unit__name='US National', target__name='1 wk ahead').first()
        truth_data.value_f = 13  # -> US National,1 wk ahead,Bin,percent,13,100,1.39332920335022e-07
        truth_data.save()

        log_multi_bin_score.update_score_for_model(forecast_model2)
        bin_value_sum = one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + \
                        one_pt_3e07 + \
                        0
        self.assertAlmostEqual(math.log(bin_value_sum), log_multi_bin_score.values.first().value)

        # case 3b:
        truth_data = project2.truth_data_qs().filter(unit__name='US National', target__name='1 wk ahead').first()
        truth_data.value_f = 12.95  # -> US National,1 wk ahead,Bin,percent,12.9,13,1.39332920335022e-07
        truth_data.save()

        log_multi_bin_score.update_score_for_model(forecast_model2)
        bin_value_sum = one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + one_pt_3e07 + \
                        one_pt_3e07 + \
                        one_pt_3e07
        self.assertAlmostEqual(math.log(bin_value_sum), log_multi_bin_score.values.first().value)


    def test_log_multi_bin_score_truth_none_cases(self):
        Score.ensure_all_scores_exist()

        project2, forecast_model2, forecast2, _ = _make_cdc_log_score_project()

        # case: truth = None, but no forecast bin start/end that's None -> no matching bin -> use zero for predicted
        # value (rather than not generating a ScoreValue at all). this test also tests the
        # LOG_SINGLE_BIN_NEGATIVE_INFINITY case
        truth_data = project2.truth_data_qs().filter(unit__name='US National', target__name='1 wk ahead').first()
        truth_data.value_f = None  # -> no matching bin
        truth_data.save()

        target_1wk = project2.targets.filter(name='1 wk ahead').first()
        target_lwr = target_1wk.lwrs.filter(lwr=0).first()
        target_lwr.lwr = None
        target_lwr.upper = None
        target_lwr.save()

        log_multi_bin_score = Score.objects.filter(abbreviation='log_multi_bin').first()
        log_multi_bin_score.update_score_for_model(forecast_model2)
        score_value = log_multi_bin_score.values.first()
        self.assertIsNotNone(score_value)
        self.assertAlmostEqual(LOG_SINGLE_BIN_NEGATIVE_INFINITY, score_value.value)

        # case: truth = None, with a matching forecast bin start/end that's None. we'll change the first bin row:
        #   US National,1 wk ahead,Bin,percent,None,0.1,1.39332920335022e-07  # set start = None
        # NB: in this case, the score should degenerate to the num_bins_one_side=0 'Log score (single bin)' calculation
        bin_dist = forecast2.bin_distribution_qs() \
            .filter(unit__name='US National', target__name='1 wk ahead', cat_f=0) \
            .first()
        bin_dist.cat_f = None
        bin_dist.save()

        log_multi_bin_score.update_score_for_model(forecast_model2)

        one_pt_3e07 = 1.39332920335022e-07
        score_value = log_multi_bin_score.values.first()
        self.assertIsNotNone(score_value)
        self.assertAlmostEqual(math.log(one_pt_3e07), score_value.value)


    def test_log_multi_bin_score_large_case(self):
        Score.ensure_all_scores_exist()

        # a larger case with more than one loc+target
        project2 = Project.objects.create()
        time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 1, 1))
        make_cdc_units_and_targets(project2)

        forecast_model2 = ForecastModel.objects.create(project=project2)
        csv_file_path = Path('forecast_app/tests/model_error/ensemble/EW1-KoTstable-2017-01-17.csv')
        load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, time_zero2)

        load_truth_data(project2, Path('forecast_app/tests/truth_data/truths-ok.csv'), is_convert_na_none=True)

        # test the scores - only ones with truth are created. see log-score-multi-bin-hand-calc.xlsx for how expected
        # values were verified
        log_multi_bin_score = Score.objects.filter(abbreviation='log_multi_bin').first()
        log_multi_bin_score.update_score_for_model(forecast_model2)

        # check two targets from different distributions
        # '1 wk ahead': truth = 0.73102
        two_pt_83e07 = 2.83662889964352e-07
        bin_value_sum = two_pt_83e07 + two_pt_83e07 + two_pt_83e07 + two_pt_83e07 + two_pt_83e07 + \
                        two_pt_83e07 + \
                        8.50988669893058e-07 + 1.13465155985741e-06 + 1.98564022975047e-06 + 1.10628527086097e-05 + 1.24811671584315e-05
        score_value = ScoreValue.objects.filter(score=log_multi_bin_score, forecast__forecast_model__project=project2,
                                                target__name='1 wk ahead').first()
        self.assertAlmostEqual(math.log(bin_value_sum), score_value.value)

        # '2 wk ahead': truth = 0.688338
        one_pt_4e06 = 1.45380624364055e-06  # two_pt_83e07
        bin_value_sum = one_pt_4e06 + one_pt_4e06 + one_pt_4e06 + one_pt_4e06 + one_pt_4e06 + \
                        one_pt_4e06 + \
                        2.90761248728109e-06 + 4.36141873092164e-06 + 1.16304499491244e-05 + 1.45380624364055e-05 + 0.00031501033203663
        score_value = ScoreValue.objects.filter(score=log_multi_bin_score, forecast__forecast_model__project=project2,
                                                target__name='2 wk ahead').first()
        self.assertAlmostEqual(math.log(bin_value_sum), score_value.value)


    #
    # test 'pit'
    #

    def test__tz_loc_targ_pk_to_true_lwr(self):
        Score.ensure_all_scores_exist()

        # test thai project
        project2, forecast_model2, forecast2, time_zero2 = _make_thai_log_score_project()

        loc_TH01 = Unit.objects.filter(project=project2, name='TH01').first()
        loc_TH02 = Unit.objects.filter(project=project2, name='TH02').first()

        targ_1bwk = Target.objects.filter(project=project2, name='1_biweek_ahead').first()
        targ_2bwk = Target.objects.filter(project=project2, name='2_biweek_ahead').first()
        targ_3bwk = Target.objects.filter(project=project2, name='3_biweek_ahead').first()
        targ_4bwk = Target.objects.filter(project=project2, name='4_biweek_ahead').first()
        targ_5bwk = Target.objects.filter(project=project2, name='5_biweek_ahead').first()

        exp_tz_loc_targ_pk_to_true_lwr = {
            time_zero2.pk: {
                loc_TH01.pk: {
                    targ_1bwk.pk: 1.0, targ_2bwk.pk: 0.0, targ_3bwk.pk: 10.0, targ_4bwk.pk: 1.0, targ_5bwk.pk: 10.0,
                },
                loc_TH02.pk: {
                    targ_1bwk.pk: 1.0, targ_2bwk.pk: 10.0, targ_3bwk.pk: 50.0, targ_4bwk.pk: 40.0, targ_5bwk.pk: 80.0,
                },
            }
        }
        self.assertEqual(exp_tz_loc_targ_pk_to_true_lwr, _tz_loc_targ_pk_to_true_lwr(project2))

        # test when truth value is None. requires TargetLwr lwr and upper be None as well, or won't match
        # _tz_loc_targ_pk_to_true_lwr() query
        truth_data = project2.truth_data_qs() \
            .filter(unit__name='TH01', target__name='1_biweek_ahead') \
            .first()  # TruthData: (78, 2, 12, 8, '.', 2, None, None, None, None)
        truth_data.value_i = None  # was 2. value_i is for discrete targets
        truth_data.save()

        target_lwr = TargetLwr.objects \
            .filter(target__name='1_biweek_ahead', lwr=0) \
            .first()  # TargetLwr: (656, 8, 0.0, 1.0)
        target_lwr.lwr = None
        target_lwr.upper = None
        target_lwr.save()

        exp_tz_loc_targ_pk_to_true_lwr[time_zero2.pk][loc_TH01.pk][targ_1bwk.pk] = None
        self.assertEqual(exp_tz_loc_targ_pk_to_true_lwr, _tz_loc_targ_pk_to_true_lwr(project2))

        # test CDC project
        project2, _, _, time_zero2 = _make_cdc_log_score_project()

        loc_us = Unit.objects.filter(project=project2, name='US National').first()

        exp_tz_loc_targ_pk_to_true_lwr = {
            time_zero2.pk: {
                loc_us.pk: {
                    Target.objects.filter(project=project2, name='1 wk ahead').first().pk: 1.5,
                    Target.objects.filter(project=project2, name='2 wk ahead').first().pk: 1.6,
                    Target.objects.filter(project=project2, name='3 wk ahead').first().pk: 1.9,
                    Target.objects.filter(project=project2, name='4 wk ahead').first().pk: 1.8,
                    Target.objects.filter(project=project2, name='Season peak percentage').first().pk: 5.0,
                },
            }
        }
        self.assertEqual(exp_tz_loc_targ_pk_to_true_lwr, _tz_loc_targ_pk_to_true_lwr(project2))


    def test_pit_score(self):
        Score.ensure_all_scores_exist()
        pit_score = Score.objects.filter(abbreviation='pit').first()
        self.assertIsNotNone(pit_score)

        _, forecast_model2, _, _ = _make_thai_log_score_project()
        pit_score.update_score_for_model(forecast_model2)
        exp_loc_targ_val = [('TH01', '1_biweek_ahead', 0.7879999999999999),
                            ('TH01', '2_biweek_ahead', 0.166),
                            ('TH01', '3_biweek_ahead', 0.999),
                            ('TH01', '4_biweek_ahead', 0.5545),
                            ('TH01', '5_biweek_ahead', 0.9774999999999999),
                            ('TH02', '1_biweek_ahead', 0.5195),
                            ('TH02', '2_biweek_ahead', 0.847),
                            # this predictive distribution has truth that doesn't match bins in database b/c bins with
                            # zero values are omitted. instead, the truth must be indexed into the template, which has
                            # all bins:
                            ('TH02', '3_biweek_ahead', 1),
                            ('TH02', '4_biweek_ahead', 0.9405),
                            ('TH02', '5_biweek_ahead', 0.9955)]
        score_values_qs = ScoreValue.objects.filter(score=pit_score, forecast__forecast_model=forecast_model2)
        act_loc_targ_vals = list(score_values_qs
                                 .order_by('score_id', 'forecast_id', 'unit_id', 'target_id')
                                 .values_list('unit__name', 'target__name', 'value'))
        self.assertEqual(10, score_values_qs.count())
        for exp_loc_targ_val, act_loc_targ_val in zip(exp_loc_targ_val, act_loc_targ_vals):
            self.assertEqual(exp_loc_targ_val[0], act_loc_targ_val[0])  # unit name
            self.assertEqual(exp_loc_targ_val[1], act_loc_targ_val[1])  # target name
            self.assertAlmostEqual(float(exp_loc_targ_val[2]), act_loc_targ_val[2])  # value


    def test_pit_score_none_cases(self):
        Score.ensure_all_scores_exist()

        # NB: using thai, not CDC (which test_log_multi_bin_score_truth_none_cases() uses):
        project2, forecast_model2, forecast2, _ = _make_thai_log_score_project()

        # case: truth = None, but no bin start/end that's None -> no matching bin -> no ScoreValue created.
        # we'll change this row:
        #   20170423	TH01	1_biweek_ahead	2  # 2 -> None
        truth_data = project2.truth_data_qs() \
            .filter(unit__name='TH01', target__name='1_biweek_ahead') \
            .first()
        truth_data.value_i = None  # -> no matching bin. value_i is for discrete targets
        truth_data.save()

        pit_score = Score.objects.filter(abbreviation='pit').first()
        pit_score.update_score_for_model(forecast_model2)
        score_value = pit_score.values \
            .filter(unit__name='TH01', target__name='1_biweek_ahead') \
            .first()
        self.assertIsNone(score_value)

        # case: truth = None, with a bin start that's None -> matching bin -> should only use the predicted true value.
        # we'll change this bin row:
        #   TH01	1_biweek_ahead	Bin	cases	0	1	0.576  # 0	1 -> None	None
        # requires TargetLwr start and ends be None as well, or won't match _tz_loc_targ_pk_to_true_lwr() query.
        # recall that '1_biweek_ahead' is a discrete (int) target
        bin_dist = forecast2.bin_distribution_qs() \
            .filter(unit__name='TH01', target__name='1_biweek_ahead', cat_i=0) \
            .first()
        bin_dist.cat_i = None
        bin_dist.save()

        target_lwr = project2.targets.filter(name='1_biweek_ahead').first().lwrs \
            .filter(lwr=0) \
            .first()
        target_lwr.lwr = None
        target_lwr.upper = None
        target_lwr.save()

        pit_score.update_score_for_model(forecast_model2)
        score_value = pit_score.values \
            .filter(unit__name='TH01', target__name='1_biweek_ahead') \
            .first()
        self.assertIsNotNone(score_value)
        self.assertEqual(0.288, score_value.value)


    #
    # test 'calc_interval_02'
    #

    def test_calc_interval_02_alpha_no_matching_quantiles(self):
        # test that no scores are calculated when there are no quantiles corresponding to alpha's lower and upper.
        # we use alpha=0.22 -> l=0.11, u=0.89 , which match no quantiles in 2020-04-26-CU-80contact-small.csv.json
        # (... 0.1, 0.15 ... 0.8, 0.85, 0.9 ...)
        Score.ensure_all_scores_exist()
        interval_02_score = Score.objects.filter(abbreviation='interval_02').first()
        self.assertIsNotNone(interval_02_score)

        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, forecast_model = _make_covid19_project(po_user)
        forecast = _load_forecast_(forecast_model, '2020-04-26',
                                   'forecast_app/tests/scores/2020-04-26-CU-80contact-small.csv.json')
        _calculate_interval_score_values(interval_02_score, forecast_model, 0.22)
        self.assertEqual(0, interval_02_score.values.count())


    def test_calc_interval_02_not_exactly_two_quantile_values(self):
        # test to expose bug: "not exactly two quantile values (no match for both lower and upper)"
        Score.ensure_all_scores_exist()
        interval_02_score = Score.objects.filter(abbreviation='interval_02').first()
        self.assertIsNotNone(interval_02_score)

        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, forecast_model = _make_covid19_project(po_user)
        load_truth_data(project, Path('forecast_app/tests/scores/zoltar-truth-2020-04-24.csv'))
        _load_forecast_(forecast_model, '2020-04-24', 'forecast_app/tests/scores/2020-04-24-JHU_IDD-CovidSP.csv.json')
        try:
            # there is only one matching quantile with truth: 2020-04-24, 'US', '1 wk ahead cum death'
            interval_02_score.update_score_for_model(forecast_model)
            self.assertEqual(1, interval_02_score.values.count())
            score_value = interval_02_score.values.first()
            unit_us = project.units.filter(name='US').first()
            targ_1_wk_ahead_cum_death = project.targets.filter(name='1 wk ahead cum death').first()
            self.assertEqual(unit_us, score_value.unit)
            self.assertEqual(targ_1_wk_ahead_cum_death, score_value.target)
            self.assertAlmostEqual(138337.40000000002, score_value.value)
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")


    def test_calc_interval_02_docs_project(self):
        # test score values for the seven cases in docs-predictions-quantile-exported-hand-calc.xlsx . note that all use
        # alpha=0.5
        Score.ensure_all_scores_exist()
        interval_02_score = Score.objects.filter(abbreviation='interval_02').first()
        self.assertIsNotNone(interval_02_score)

        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, time_zero, forecast_model, forecast = _make_docs_project(po_user)
        unit_loc2 = project.units.filter(name='location2').first()
        targ_pct_next_wk = project.targets.filter(name='pct next week').first()  # continuous
        unit_loc3 = project.units.filter(name='location3').first()
        targ_cases_next_wk = project.targets.filter(name='cases next week').first()  # discrete
        for unit, target, truth, exp_score in [
            (unit_loc2, targ_pct_next_wk, 1, 7.6),  # case 1/7) truth < l
            (unit_loc2, targ_pct_next_wk, 2.2, 2.8),  # case 2/7) truth == l
            # (unit_loc2, targ_pct_next_wk, 2.2, 2.8),  # case 3/7) 1 < truth < u. but different quantile, same value -> same score
            (unit_loc2, targ_pct_next_wk, 5, 2.8),  # case 4/7) truth == u
            (unit_loc2, targ_pct_next_wk, 50, 182.8),  # case 5/7) truth == l
            (unit_loc3, targ_cases_next_wk, 0, 50),  # case 6/7) truth == u
            (unit_loc3, targ_cases_next_wk, 50, 50),  # case 7/7) truth == u
        ]:
            project.delete_truth_data()
            # NB: use correct value column for target type
            TruthData.objects.create(time_zero=time_zero, unit=unit, target=target,
                                     value_i=truth if target == targ_cases_next_wk else None,
                                     value_f=truth if target == targ_pct_next_wk else None)
            ScoreValue.objects \
                .filter(score=interval_02_score, forecast__forecast_model=forecast_model) \
                .delete()  # usually done by update_score_for_model()
            _calculate_interval_score_values(interval_02_score, forecast_model, 0.5)
            self.assertEqual(1, interval_02_score.values.count())

            score_value = interval_02_score.values.first()
            self.assertEqual(unit, score_value.unit)
            self.assertEqual(target, score_value.target)
            self.assertAlmostEqual(exp_score, score_value.value)

        # add two truths that result in two ScoreValues
        project.delete_truth_data()
        TruthData.objects.create(time_zero=time_zero, unit=unit_loc2, target=targ_pct_next_wk, value_f=2.2)  # 2/7)
        TruthData.objects.create(time_zero=time_zero, unit=unit_loc3, target=targ_cases_next_wk, value_i=50)  # 6/7
        ScoreValue.objects \
            .filter(score=interval_02_score, forecast__forecast_model=forecast_model) \
            .delete()  # usually done by update_score_for_model()
        _calculate_interval_score_values(interval_02_score, forecast_model, 0.5)
        self.assertEqual(2, interval_02_score.values.count())
        self.assertEqual([2.8, 50], sorted(interval_02_score.values.all().values_list('value', flat=True)))

        # add a second forecast for a new timezero
        time_zero2 = TimeZero.objects.create(project=project, timezero_date=datetime.date(2011, 10, 3))
        forecast = Forecast.objects.create(forecast_model=forecast_model, source='docs-predictions.json',
                                           time_zero=time_zero2, notes="a small prediction file")
        with open('forecast_app/tests/predictions/docs-predictions.json') as fp:
            json_io_dict_in = json.load(fp)
            load_predictions_from_json_io_dict(forecast, json_io_dict_in, False)
        TruthData.objects.create(time_zero=time_zero2, unit=unit_loc2, target=targ_pct_next_wk, value_f=2.2)  # 2/7)
        TruthData.objects.create(time_zero=time_zero2, unit=unit_loc3, target=targ_cases_next_wk, value_i=50)  # 6/7
        ScoreValue.objects \
            .filter(score=interval_02_score, forecast__forecast_model=forecast_model) \
            .delete()  # usually done by update_score_for_model()
        _calculate_interval_score_values(interval_02_score, forecast_model, 0.5)
        self.assertEqual(4, interval_02_score.values.count())


    def test_calc_interval_02(self):
        # test score values for the five cases in 2020-04-26-CU-80contact-small-hand-calc.xlsx . note that all use
        # alpha=0.2
        Score.ensure_all_scores_exist()
        interval_02_score = Score.objects.filter(abbreviation='interval_02').first()
        self.assertIsNotNone(interval_02_score)

        _, _, po_user, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        project, forecast_model = _make_covid19_project(po_user)
        forecast = _load_forecast_(forecast_model, '2020-04-26',
                                   'forecast_app/tests/scores/2020-04-26-CU-80contact-small.csv.json')
        unit = project.units.filter(name='US').first()
        target = project.targets.filter(name='1 day ahead cum death').first()
        for truth, exp_score in [
            (51565, 2615),  # case 1/5) truth < l
            (51730, 965),  # case 2/5) truth == l
            (52072, 965),  # case 3/5a) 1 < truth < u. a) truth == an actual value
            (52073, 965),  # case 3/5b) 1 < truth < u. b) truth != an actual value
            (52695, 965),  # case 4/5) truth == u
            (52919, 3205),  # case 5/5) truth > u
        ]:
            project.delete_truth_data()
            TruthData.objects.create(time_zero=forecast.time_zero, unit=unit, target=target, value_i=truth)
            interval_02_score.update_score_for_model(forecast_model)
            self.assertEqual(1, interval_02_score.values.count())

            score_value = interval_02_score.values.first()
            self.assertEqual(unit, score_value.unit)
            self.assertEqual(target, score_value.target)
            self.assertEqual(exp_score, score_value.value)


    #
    # other tests
    #

    def test_download_scores_empty_abbrev(self):
        # the abbreviation for self.forecast_model is the default (""). in this case the model name is used for the
        # 'model' column value
        self.download_scores_internal_test(self.forecast_model.name)


    def test_download_scores_non_empty_abbrev(self):
        self.forecast_model.abbreviation = 'model_abbrev'
        self.forecast_model.save()
        # since there is a non-empty abbreviation, it should be used instead of the model name
        self.download_scores_internal_test(self.forecast_model.abbreviation)


    def download_scores_internal_test(self, exp_model_column_value):
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
            # model,timezero,season,unit,target,error,abs_error,log_single_bin,log_multi_bin,interval_02
            exp_csv_reader = csv.reader(fp, delimiter=',')
            exp_rows = list(exp_csv_reader)
            for idx, (exp_row, act_row) in enumerate(zip(exp_rows, act_rows)):
                if idx == 0:  # header
                    self.assertEqual(exp_row, act_row)
                    continue

                # test non-numeric columns
                self.assertEqual(exp_model_column_value, act_row[0])  # model
                self.assertEqual(exp_row[1], act_row[1])  # timezero. format: YYYY_MM_DD_DATE_FORMAT
                self.assertEqual(exp_row[2], act_row[2])  # season
                self.assertEqual(exp_row[3], act_row[3])  # unit
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
        loc1_pk = Unit.objects.filter(name='HHS Region 1').first().pk
        loc2_pk = Unit.objects.filter(name='HHS Region 2').first().pk
        loc3_pk = Unit.objects.filter(name='HHS Region 3').first().pk
        loc4_pk = Unit.objects.filter(name='HHS Region 4').first().pk
        loc5_pk = Unit.objects.filter(name='HHS Region 5').first().pk
        loc6_pk = Unit.objects.filter(name='HHS Region 6').first().pk
        loc7_pk = Unit.objects.filter(name='HHS Region 7').first().pk
        loc8_pk = Unit.objects.filter(name='HHS Region 8').first().pk
        loc9_pk = Unit.objects.filter(name='HHS Region 9').first().pk
        loc10_pk = Unit.objects.filter(name='HHS Region 10').first().pk
        loc11_pk = Unit.objects.filter(name='US National').first().pk
        target1_pk = Target.objects.filter(name='Season onset').first().pk
        target2_pk = Target.objects.filter(name='Season peak week').first().pk
        target3_pk = Target.objects.filter(name='Season peak percentage').first().pk
        target4_pk = Target.objects.filter(name='1 wk ahead').first().pk
        target5_pk = Target.objects.filter(name='2 wk ahead').first().pk
        target6_pk = Target.objects.filter(name='3 wk ahead').first().pk
        target7_pk = Target.objects.filter(name='4 wk ahead').first().pk
        exp_dict = {  # {timezero_pk: {unit_pk: {target_id: truth_value}}}
            tz_pk: {
                loc1_pk: {target1_pk: ['2016-12-25'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [3.19221],
                          target4_pk: [1.52411], target5_pk: [1.73987], target6_pk: [2.06524], target7_pk: [2.51375]},
                loc2_pk: {target1_pk: ['2016-11-20'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [6.93759],
                          target4_pk: [5.07086], target5_pk: [5.68166], target6_pk: [6.01053], target7_pk: [6.49829]},
                loc3_pk: {target1_pk: ['2016-12-18'], target2_pk: [datetime.date(2017, 2, 12)], target3_pk: [5.20003],
                          target4_pk: [2.81366], target5_pk: [3.09968], target6_pk: [3.45232], target7_pk: [3.73339]},
                loc4_pk: {target1_pk: ['2016-11-13'], target2_pk: [datetime.date(2017, 2, 12)], target3_pk: [5.5107],
                          target4_pk: [2.89395], target5_pk: [3.68564], target6_pk: [3.69188], target7_pk: [4.53169]},
                loc5_pk: {target1_pk: ['2016-12-25'], target2_pk: [datetime.date(2017, 2, 12)], target3_pk: [4.31787],
                          target4_pk: [2.11757], target5_pk: [2.4432], target6_pk: [2.76295], target7_pk: [3.182]},
                loc6_pk: {target1_pk: ['2017-01-08'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [9.87589],
                          target4_pk: [4.80185], target5_pk: [5.26955], target6_pk: [6.10427], target7_pk: [8.13221]},
                loc7_pk: {target1_pk: ['2016-12-25'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [6.35948],
                          target4_pk: [2.75581], target5_pk: [3.46528], target6_pk: [4.56991], target7_pk: [5.52653]},
                loc8_pk: {target1_pk: ['2016-12-18'], target2_pk: [datetime.date(2017, 2, 12)], target3_pk: [2.72703],
                          target4_pk: [1.90851], target5_pk: [2.2668], target6_pk: [2.07104], target7_pk: [2.27632]},
                loc9_pk: {target1_pk: ['2016-12-18'], target2_pk: [datetime.date(2016, 12, 25)], target3_pk: [3.30484],
                          target4_pk: [2.83778], target5_pk: [2.68071], target6_pk: [2.9577], target7_pk: [3.03987]},
                loc10_pk: {target1_pk: ['2016-12-11'], target2_pk: [datetime.date(2016, 12, 25)], target3_pk: [3.67061],
                           target4_pk: [2.15197], target5_pk: [3.25108], target6_pk: [2.51434], target7_pk: [2.28634]},
                loc11_pk: {target1_pk: ['2016-12-11'], target2_pk: [datetime.date(2017, 2, 5)], target3_pk: [5.06094],
                           target4_pk: [3.07623], target5_pk: [3.50708], target6_pk: [3.79872], target7_pk: [4.43601]}}}
        act_dict = _timezero_loc_target_pks_to_truth_values(self.forecast_model)
        self.assertEqual(exp_dict, act_dict)


    def test_impetus_log_single_bin_bug(self):
        Score.ensure_all_scores_exist()

        project2 = Project.objects.create()
        time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 4, 23))
        create_thai_units_and_targets(project2)

        forecast_model2 = ForecastModel.objects.create(project=project2)
        csv_file_path = Path('forecast_app/tests/scores/20170423-gam_lag1_tops3-20170525-small.cdc.csv')
        load_cdc_csv_forecast_file(None, forecast_model2, csv_file_path, time_zero2)  # no season_start_year
        load_truth_data(project2, Path('forecast_app/tests/scores/dengue-truths-small.csv'))

        log_single_bin_score = Score.objects.filter(abbreviation='log_single_bin').first()
        log_single_bin_score.update_score_for_model(forecast_model2)

        # test scores themselves
        exp_loc_targ_val = [
            ('TH01', '1_biweek_ahead', -0.8580218237501793),
            ('TH01', '2_biweek_ahead', -1.1026203100656484),
            ('TH01', '3_biweek_ahead', -6.214608098422191),
            ('TH01', '4_biweek_ahead', -0.1660545843300827),
            ('TH01', '5_biweek_ahead', -3.146555163288575),
            ('TH02', '1_biweek_ahead', -0.0397808700118446),
            ('TH02', '2_biweek_ahead', -1.2517634681622845),
            # this row was missing b/c zero bin rows are not accounted for (see
            # [log scores with zero probability should return -Inf, not empty/missing value #119]):
            ('TH02', '3_biweek_ahead', LOG_SINGLE_BIN_NEGATIVE_INFINITY),
            ('TH02', '4_biweek_ahead', -2.864704011147587),
            ('TH02', '5_biweek_ahead', -5.298317366548036)
        ]
        score_values_qs = ScoreValue.objects.filter(score=log_single_bin_score,
                                                    forecast__forecast_model=forecast_model2)
        act_values = list(score_values_qs
                          .order_by('score_id', 'forecast_id', 'unit_id', 'target_id')
                          .values_list('unit__name', 'target__name', 'value'))
        self.assertEqual(10, score_values_qs.count())  # 2 units * 5 targets/unit

        for exp_loc_targ_val, act_loc_targ_val in zip(exp_loc_targ_val, act_values):
            self.assertEqual(exp_loc_targ_val[0], act_loc_targ_val[0])  # unit name
            self.assertEqual(exp_loc_targ_val[1], act_loc_targ_val[1])  # target name
            self.assertAlmostEqual(float(exp_loc_targ_val[2]), act_loc_targ_val[2])  # value

        # b/c this test is already set up, let's sanity-test multi here too
        log_multi_bin_score = Score.objects.filter(abbreviation='log_multi_bin').first()
        log_multi_bin_score.update_score_for_model(forecast_model2)
        score_values_qs = ScoreValue.objects.filter(score=log_multi_bin_score,
                                                    forecast__forecast_model=forecast_model2)
        self.assertEqual(10, score_values_qs.count())  # 2 units * 5 targets/unit


#
# ---- utilities ----
#

def _update_scores_for_all_projects():
    """
    Update all scores for all projects. Useful mainly for tests b/c runs in the calling thread and therefore blocks.
    """
    for score in Score.objects.all():
        for project in Project.objects.all():
            for forecast_model in project.models.all():
                score.update_score_for_model(forecast_model)


def _make_cdc_log_score_project():
    project2 = Project.objects.create()
    make_cdc_units_and_targets(project2)

    time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2016, 10, 30))
    load_truth_data(project2, Path('forecast_app/tests/scores/truths-2016-2017-reichlab-small.csv'))

    forecast_model2 = ForecastModel.objects.create(project=project2, name='test model')
    csv_file_path = Path('forecast_app/tests/scores/20161030-KoTstable-20161114-small.cdc.csv')
    forecast2 = load_cdc_csv_forecast_file(2016, forecast_model2, csv_file_path, time_zero2)

    return project2, forecast_model2, forecast2, time_zero2


def _make_thai_log_score_project():
    project2 = Project.objects.create()
    time_zero2 = TimeZero.objects.create(project=project2, timezero_date=datetime.date(2017, 4, 23))
    create_thai_units_and_targets(project2)

    forecast_model2 = ForecastModel.objects.create(project=project2)
    csv_file_path = Path('forecast_app/tests/scores/20170423-gam_lag1_tops3-20170525-small.cdc.csv')
    forecast2 = load_cdc_csv_forecast_file(None, forecast_model2, csv_file_path, time_zero2)  # no season_start_year

    load_truth_data(project2, Path('forecast_app/tests/scores/dengue-truths-small.csv'))
    return project2, forecast_model2, forecast2, time_zero2


def _load_forecast_(forecast_model, timezero_date, predictions_file):
    """
    :return: new Forecast in forecast_model associated with the project's TimeZero for timezero_date, loaded from
        predictions_file
    """
    time_zero = forecast_model.project.timezeros.filter(timezero_date=timezero_date).first()
    forecast = Forecast.objects.create(forecast_model=forecast_model, source='', time_zero=time_zero)
    with open(predictions_file) as fp:
        json_io_dict_in = json.load(fp)
        load_predictions_from_json_io_dict(forecast, json_io_dict_in, False)
    return forecast


def _make_covid19_project(user):
    """
    :return: 2-tuple: (project, forecast_model). does not load truth or predictions
    """
    covid19_project_name = 'COVID-19 Forecasts'  # from COVID-19_Forecasts-config.json
    found_project = Project.objects.filter(name=covid19_project_name).first()
    if found_project:
        found_project.delete()
    project = create_project_from_json(Path('forecast_app/tests/projects/COVID-19_Forecasts-config.json'), user)
    forecast_model = ForecastModel.objects.create(name='docs forecast model', project=project)
    return project, forecast_model
