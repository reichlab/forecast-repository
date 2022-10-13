import copy
import itertools
import logging

from django.test import TestCase

from forecast_app.models import Target
from utils.make_covid_viz_test_project import _make_covid_viz_test_project
from utils.utilities import get_or_create_super_po_mo_users
from utils.visualization import viz_target_variables, viz_units, viz_available_reference_dates, viz_model_names, \
    viz_targets, viz_data, validate_project_viz_options


logging.getLogger().setLevel(logging.ERROR)


class VisualizationTestCase(TestCase):
    """
    """


    @classmethod
    def setUpTestData(cls):
        _, _, cls.po_user, _, _, _, _, _ = get_or_create_super_po_mo_users(is_create_super=True)
        cls.project, cls.models, cls.forecasts = _make_covid_viz_test_project(cls.po_user)


    def test_viz_targets(self):
        # _make_covid_viz_test_project(): 2 Targets: "1 wk ahead inc death", "2 wk ahead inc death". we create a Target
        # with numeric_horizon > 4, which used to be excluded but is now included. we also include a Target.DAY_RDT
        # target to make sure it's included too
        Target.objects.create(project=self.project, name='mmwr target', type=Target.CONTINUOUS_TARGET_TYPE,
                              is_step_ahead=True, numeric_horizon=5,
                              reference_date_type=Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT)
        Target.objects.create(project=self.project, name='day target', type=Target.CONTINUOUS_TARGET_TYPE,
                              is_step_ahead=True, numeric_horizon=2,
                              reference_date_type=Target.DAY_RDT)
        exp_viz_targets = [self.project.targets.filter(name='1 wk ahead inc death').first(),
                           self.project.targets.filter(name='2 wk ahead inc death').first(),
                           self.project.targets.filter(name='mmwr target').first(),
                           self.project.targets.filter(name='day target').first()]
        act_viz_targets = sorted(viz_targets(self.project), key=lambda _: _.name)
        self.assertEqual(sorted(exp_viz_targets, key=lambda _: _.id), sorted(act_viz_targets, key=lambda _: _.id))


    def test_viz_target_variables(self):
        exp_target_vars = [{'value': 'incident_deaths', 'text': 'incident deaths', 'plot_text': 'incident deaths'}]
        act_target_vars = viz_target_variables(self.project)
        self.assertEqual(exp_target_vars, act_target_vars)


    def test_viz_units(self):
        exp_units = [{'value': 'US', 'text': 'US'},
                     {'value': '48', 'text': 'Texas'}]
        act_units = viz_units(self.project)
        self.assertEqual(sorted(exp_units, key=lambda _: _['value']),
                         sorted(act_units, key=lambda _: _['value']))


    def test_viz_available_reference_dates(self):
        exp_avail_ref_dates = {
            'incident_deaths': ['2022-01-01', '2022-01-08', '2022-01-15', '2022-01-22', '2022-01-29']}
        act_avail_ref_dates = viz_available_reference_dates(self.project)
        self.assertEqual(exp_avail_ref_dates, act_avail_ref_dates)


    def test_viz_model_names(self):
        exp_models = ['COVIDhub-ensemble', 'COVIDhub-baseline']  # NB: no 'oracle'
        act_models = viz_model_names(self.project)
        self.assertEqual(exp_models, act_models)


    def test_viz_truth(self):
        unit_ref_date_to_exp_truth = {
            ('US', '2022-01-01'): {'date': ('2021-12-18', '2021-12-25', '2022-01-01'),
                                   'y': (8520.0, 9950.0, 9283.0)},
            ('US', '2022-01-08'): {'date': ('2021-12-18', '2021-12-25', '2022-01-01', '2022-01-08'),
                                   'y': (8913.0, 9709.0, 8633.0, 11221.0)},
            ('US', '2022-01-15'): {'date': ('2021-12-18', '2021-12-25', '2022-01-01', '2022-01-08', '2022-01-15'),
                                   'y': (8913.0, 9903.0, 8801.0, 10917.0, 12431.0)},
            ('US', '2022-01-22'): {
                'date': ('2021-12-18', '2021-12-25', '2022-01-01', '2022-01-08', '2022-01-15', '2022-01-22'),
                'y': (9233.0, 10182.0, 9153.0, 11283.0, 12877.0, 14224.0)},
            ('US', '2022-01-29'): {'date': ('2021-12-18', '2021-12-25', '2022-01-01', '2022-01-08', '2022-01-15',
                                            '2022-01-22', '2022-01-29'),
                                   'y': (9277.0, 10287.0, 9339.0, 11534.0, 13127.0, 14423.0, 16888.0)},
            ('48', '2022-01-01'): {'date': ('2021-12-18', '2021-12-25', '2022-01-01'),
                                   'y': (478.0, 266.0, 422.0)},
            ('48', '2022-01-08'): {'date': ('2021-12-18', '2021-12-25', '2022-01-01', '2022-01-08'),
                                   'y': (478.0, 266.0, 422.0, 717.0)},
            ('48', '2022-01-15'): {'date': ('2021-12-18', '2021-12-25', '2022-01-01', '2022-01-08', '2022-01-15'),
                                   'y': (478.0, 266.0, 422.0, 717.0, 623.0)},
            ('48', '2022-01-22'): {'date': ('2021-12-18', '2021-12-25', '2022-01-01', '2022-01-08', '2022-01-15',
                                            '2022-01-22'),
                                   'y': (478.0, 266.0, 422.0, 717.0, 623.0, 971.0)},
            ('48', '2022-01-29'): {'date': ('2021-12-18', '2021-12-25', '2022-01-01', '2022-01-08', '2022-01-15',
                                            '2022-01-22', '2022-01-29'),
                                   'y': (478.0, 266.0, 422.0, 717.0, 623.0, 971.0, 1212.0)},
        }

        target_key = viz_target_variables(self.project)[0]['value']  # only one - same for both relevant Targets
        for viz_unit, ref_date in itertools.product([viz_unit['value'] for viz_unit in viz_units(self.project)],
                                                    viz_available_reference_dates(self.project)[target_key]):
            act_truth = viz_data(self.project, False, target_key, viz_unit, ref_date)
            self.assertEqual(unit_ref_date_to_exp_truth[(viz_unit, ref_date)], act_truth)


    def test_viz_forecasts(self):
        unit_ref_date_to_exp_forecasts = {
            ('US', '2022-01-01'): {'COVIDhub-baseline': {'target_end_date': ['2022-01-08', '2022-01-15'],
                                                         'q0.025': [5556.225, 3882.59492394924],
                                                         'q0.25': [8589.75, 7949.31382563826], 'q0.5': [9283, 9283],
                                                         'q0.75': [9976.25, 10634.7540650407],
                                                         'q0.975': [13009.775, 14700.6882841328]},
                                   'COVIDhub-ensemble': {'target_end_date': ['2022-01-08', '2022-01-15'],
                                                         'q0.025': [12406, 15265], 'q0.25': [13559, 18533],
                                                         'q0.5': [14742, 20293], 'q0.75': [15287, 21572],
                                                         'q0.975': [17849, 23541]}},
            ('US', '2022-01-08'): {'COVIDhub-baseline': {'target_end_date': ['2022-01-15', '2022-01-22'],
                                                         'q0.025': [7519.85, 5824.91613491135],
                                                         'q0.25': [10498.5, 9857.54027540275], 'q0.5': [11221, 11221],
                                                         'q0.75': [11943.5, 12588.4745897459],
                                                         'q0.975': [14922.15, 16633.6767415174]},
                                   'COVIDhub-ensemble': {'target_end_date': ['2022-01-15', '2022-01-22'],
                                                         'q0.025': [7941, 8789], 'q0.25': [10412, 11512],
                                                         'q0.5': [11167, 14121], 'q0.75': [12663, 15772],
                                                         'q0.975': [14142, 18190]}},
            ('US', '2022-01-15'): {'COVIDhub-baseline': {'target_end_date': ['2022-01-22', '2022-01-29'],
                                                         'q0.025': [8743.55, 7044.55109576096],
                                                         'q0.25': [11701.25, 11072.6051335513], 'q0.5': [12431, 12431],
                                                         'q0.75': [13160.75, 13799.4364143641],
                                                         'q0.975': [16118.45, 17780.6566360664]},
                                   'COVIDhub-ensemble': {'target_end_date': ['2022-01-22', '2022-01-29'],
                                                         'q0.025': [12266, 12216], 'q0.25': [14320, 14276],
                                                         'q0.5': [15037, 15419], 'q0.75': [16294, 18880],
                                                         'q0.975': [19960, 22399]}},
            ('US', '2022-01-22'): {'COVIDhub-baseline': {'target_end_date': ['2022-01-29', '2022-02-05'],
                                                         'q0.025': [10563.95, 8898.62094320943],
                                                         'q0.25': [13494.25, 12876.7962829628], 'q0.5': [14224, 14224],
                                                         'q0.75': [14953.75, 15576.1372738727],
                                                         'q0.975': [17884.05, 19571.1516740167]},
                                   'COVIDhub-ensemble': {'target_end_date': ['2022-01-29', '2022-02-05'],
                                                         'q0.025': [12524, 11101], 'q0.25': [14376, 14393],
                                                         'q0.5': [15314, 16528], 'q0.75': [16148, 18524],
                                                         'q0.975': [18815, 21528]}},
            ('US', '2022-01-29'): {'COVIDhub-baseline': {'target_end_date': ['2022-02-05', '2022-02-12'],
                                                         'q0.025': [13227.95, 11517.598604986],
                                                         'q0.25': [16137.25, 15492.1192286923], 'q0.5': [16888, 16888],
                                                         'q0.75': [17638.75, 18293.9767347673],
                                                         'q0.975': [20548.05, 22277.8349788498]},
                                   'COVIDhub-ensemble': {'target_end_date': ['2022-02-05', '2022-02-12'],
                                                         'q0.025': [13311, 7624], 'q0.25': [15609, 14845],
                                                         'q0.5': [17050, 17714], 'q0.75': [18956, 21218],
                                                         'q0.975': [22111, 24807]}},
            ('48', '2022-01-01'): {
                'COVIDhub-baseline': {'target_end_date': ['2022-01-08', '2022-01-15'], 'q0.025': [0, 0],
                                      'q0.25': [306.75, 221.623076230762], 'q0.5': [422, 422],
                                      'q0.75': [537.25, 623.255077550776], 'q0.975': [1050.725, 1191.99792697927]},
                'COVIDhub-ensemble': {'target_end_date': ['2022-01-08', '2022-01-15'], 'q0.025': [256, 237],
                                      'q0.25': [370, 417], 'q0.5': [423, 461], 'q0.75': [501, 533],
                                      'q0.975': [731, 798]}},
            ('48', '2022-01-08'): {
                'COVIDhub-baseline': {'target_end_date': ['2022-01-15', '2022-01-22'], 'q0.025': [90.125, 0],
                                      'q0.25': [597.25, 513.028812788128], 'q0.5': [717, 717],
                                      'q0.75': [836.75, 920.253637536375], 'q0.975': [1343.875, 1486.10304653046]},
                'COVIDhub-ensemble': {'target_end_date': ['2022-01-15', '2022-01-22'], 'q0.025': [525, 441],
                                      'q0.25': [753, 1038], 'q0.5': [820, 1200], 'q0.75': [893, 1252],
                                      'q0.975': [1372, 2298]}},
            ('48', '2022-01-15'): {
                'COVIDhub-baseline': {'target_end_date': ['2022-01-22', '2022-01-29'], 'q0.025': [0, 0],
                                      'q0.25': [507.75, 421.49641496415], 'q0.5': [623, 623],
                                      'q0.75': [738.25, 824.923306733067], 'q0.975': [1248.025, 1386.50704307043]},
                'COVIDhub-ensemble': {'target_end_date': ['2022-01-22', '2022-01-29'], 'q0.025': [680, 717],
                                      'q0.25': [835, 1032], 'q0.5': [890, 1198], 'q0.75': [1081, 1315],
                                      'q0.975': [1363, 1744]}},
            ('48', '2022-01-22'): {'COVIDhub-baseline': {'target_end_date': ['2022-01-29', '2022-02-05'],
                                                         'q0.025': [349.675, 210.819212192122],
                                                         'q0.25': [855.75, 766.364243642436], 'q0.5': [971, 971],
                                                         'q0.75': [1086.25, 1175.66223912239],
                                                         'q0.975': [1592.325, 1732.05851683517]},
                                   'COVIDhub-ensemble': {'target_end_date': ['2022-01-29', '2022-02-05'],
                                                         'q0.025': [737, 774], 'q0.25': [898, 993],
                                                         'q0.5': [1026, 1181], 'q0.75': [1158, 1354],
                                                         'q0.975': [1455, 1826]}},
            ('48', '2022-01-29'): {'COVIDhub-baseline': {'target_end_date': ['2022-02-05', '2022-02-12'],
                                                         'q0.025': [590.675, 446.981771067711],
                                                         'q0.25': [1089.25, 1003.57023320233], 'q0.5': [1212, 1212],
                                                         'q0.75': [1334.75, 1421.13823138231],
                                                         'q0.975': [1833.325, 1973.59632346323]},
                                   'COVIDhub-ensemble': {'target_end_date': ['2022-02-05', '2022-02-12'],
                                                         'q0.025': [738, 801], 'q0.25': [1111, 1152],
                                                         'q0.5': [1198, 1321], 'q0.75': [1387, 1494],
                                                         'q0.975': [1727, 1944]}},
        }
        target_key = viz_target_variables(self.project)[0]['value']  # only one - same for both relevant Targets
        for viz_unit, ref_date in itertools.product([viz_unit['value'] for viz_unit in viz_units(self.project)],
                                                    viz_available_reference_dates(self.project)[target_key]):
            act_forecasts = viz_data(self.project, True, target_key, viz_unit, ref_date)
            self.assertEqual(unit_ref_date_to_exp_forecasts[(viz_unit, ref_date)], act_forecasts)


    def test_validate_project_viz_options(self):
        # print(viz_model_names(self.project), viz_target_variables(self.project), viz_units(self.project))
        # ['COVIDhub-ensemble', 'COVIDhub-baseline']
        # [{'value': 'incident_deaths',  'text': 'incident deaths',  'plot_text': 'incident deaths'}]
        # [{'value': 'US',  'text': 'US'},
        #  {'value': '48',  'text': 'Texas'}]

        # blue sky
        viz_options = {
            "initial_target_var": "incident_deaths",
            "initial_unit": "48",
            "intervals": [0, 50, 95],
            "initial_checked_models": ["COVIDhub-baseline", "COVIDhub-ensemble"],
            "models_at_top": ["COVIDhub-ensemble", "COVIDhub-baseline"],
            "disclaimer": "Most forecasts have failed to reliably predict rapid changes ..."
        }
        act_valid = validate_project_viz_options(self.project, viz_options)
        self.assertEqual([], act_valid)

        # test bad key types and missing keys
        for key in {'initial_target_var', 'initial_unit', 'intervals', 'initial_checked_models', 'models_at_top',
                    'disclaimer'}:
            edit_viz_options = copy.deepcopy(viz_options)
            edit_viz_options[key] = 0  # int is invalid for all keys
            act_valid = validate_project_viz_options(self.project, edit_viz_options)
            self.assertEqual(1, len(act_valid))
            self.assertIn('top level field type was not', act_valid[0])

            del (edit_viz_options[key])
            act_valid = validate_project_viz_options(self.project, edit_viz_options)
            self.assertEqual(1, len(act_valid))
            self.assertIn('viz_options keys are invalid', act_valid[0])

        # test extra key
        edit_viz_options = copy.deepcopy(viz_options)
        edit_viz_options['bad key'] = 0
        act_valid = validate_project_viz_options(self.project, edit_viz_options)
        self.assertEqual(1, len(act_valid))
        self.assertIn('viz_options keys are invalid', act_valid[0])

        # test bad model list types (not strings)
        edit_viz_options = copy.deepcopy(viz_options)
        edit_viz_options['initial_checked_models'] = [0]
        act_valid = validate_project_viz_options(self.project, edit_viz_options)
        self.assertEqual(1, len(act_valid))
        self.assertIn('initial_checked_models is invalid', act_valid[0])

        edit_viz_options = copy.deepcopy(viz_options)
        edit_viz_options['models_at_top'] = [0]
        act_valid = validate_project_viz_options(self.project, edit_viz_options)
        self.assertEqual(1, len(act_valid))
        self.assertIn('models_at_top is invalid', act_valid[0])

        # test is_validate_objects
        edit_viz_options = copy.deepcopy(viz_options)
        edit_viz_options['initial_target_var'] = 'bad var'
        edit_viz_options['initial_checked_models'] = ['bad model']
        edit_viz_options['models_at_top'] = ['bad model']
        act_valid = validate_project_viz_options(self.project, edit_viz_options, is_validate_objects=False)
        self.assertEqual(0, len(act_valid))

        # test invalid options, one by one
        key_bad_val = [('initial_target_var', 'bad var'),
                       ('initial_unit', 'bad unit'),
                       ('intervals', []),
                       ('intervals', [-1]),
                       ('intervals', ["one"]),
                       ('initial_checked_models', []),
                       ('initial_checked_models', ['bad model']),
                       ('models_at_top', []),
                       ('models_at_top', ['bad model'])]
        for key, bad_val in key_bad_val:
            edit_viz_options = copy.deepcopy(viz_options)
            edit_viz_options[key] = bad_val
            act_valid = validate_project_viz_options(self.project, edit_viz_options)
            self.assertEqual(1, len(act_valid))
