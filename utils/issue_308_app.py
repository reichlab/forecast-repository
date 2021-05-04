import datetime
import logging

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.project import group_targets

from utils.forecast import json_io_dict_from_forecast, load_predictions_from_json_io_dict

from forecast_app.models import Forecast, Project


logger = logging.getLogger(__name__)

DELETE_NOTE = f"retracted forecast deleted from GitHub"
FILE_NAMES_COMMIT_DATES = {
    ('2020-01-03-Columbia_UNC-SurvCon.csv', '01/08/21-16:34:58', False),
    ('2020-01-24-WalmartLabsML-LogForecasting.csv', '01/25/21-01:43:28', False),
    ('2020-03-16-Imperial-ensemble1.csv', '04/23/20-16:19:22', False),
    ('2020-03-16-Imperial-ensemble2.csv', '04/23/20-16:19:22', False),
    ('2020-03-23-Imperial-ensemble1.csv', '04/23/20-16:19:22', False),
    ('2020-03-23-Imperial-ensemble2.csv', '04/23/20-16:19:22', False),
    ('2020-03-27-IHME-IHME.csv', '04/14/20-17:21:37', False),
    ('2020-03-29-IHME-IHME.csv', '04/14/20-17:21:37', False),
    ('2020-03-30-Imperial-ensemble1.csv', '04/23/20-16:19:22', False),
    ('2020-03-30-Imperial-ensemble2.csv', '04/23/20-16:19:22', False),
    ('2020-03-31-IHME-IHME.csv', '04/14/20-17:21:37', False),
    ('2020-04-01-IHME-IHME.csv', '04/14/20-17:21:37', False),
    ('2020-04-05-IHME-IHME.csv', '04/14/20-17:21:37', False),
    ('2020-04-05-LANL-GrowthRate.csv', '05/14/20-23:20:56', False),
    ('2020-04-06-IHME-CurveFit.csv', '04/21/20-13:35:50', False),
    ('2020-04-06-Imperial-ensemble1.csv', '04/23/20-16:19:22', False),
    ('2020-04-06-Imperial-ensemble2.csv', '04/23/20-16:19:22', False),
    ('2020-04-07-IHME-IHME.csv', '04/14/20-17:21:37', False),
    ('2020-04-08-LANL-GrowthRate.csv', '05/14/20-23:20:56', False),
    ('2020-04-08-LANL-GrowthRate.csv', '04/14/20-10:53:31', False),
    ('2020-04-12-CU-80contact.csv', '06/19/20-14:42:59', False),
    ('2020-04-12-LANL-GrowthRate.csv', '05/14/20-23:20:56', False),
    ('2020-04-13-CU-60contact.csv', '04/28/20-09:23:37', False),
    ('2020-04-13-CU-70contact.csv', '04/28/20-09:23:37', False),
    ('2020-04-13-CU-80contact.csv', '04/28/20-09:23:37', False),
    ('2020-04-13-CU-nointerv.csv', '04/28/20-09:23:37', False),
    ('2020-04-13-IHME-CurveFit.csv', '04/21/20-10:46:21', False),
    ('2020-04-13-Imperial-ensemble1.csv', '04/23/20-16:19:22', False),
    ('2020-04-13-Imperial-ensemble2.csv', '04/23/20-16:19:22', False),
    ('2020-04-13-UMassCoE-ensemble.csv', '04/16/20-14:49:57', False),
    ('2020-04-15-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-04-15-LANL-GrowthRate.csv', '05/14/20-23:20:56', False),
    ('2020-04-16-CU-80contact.csv', '06/19/20-14:42:59', False),
    ('2020-04-16-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-04-17-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-04-19-CU-80contact.csv', '06/19/20-14:42:59', False),
    ('2020-04-19-LANL-GrowthRate.csv', '05/14/20-23:20:56', False),
    ('2020-04-20-Imperial-ensemble1.csv', '04/24/20-14:53:55', False),
    ('2020-04-20-Imperial-ensemble2.csv', '04/24/20-14:53:55', False),
    ('2020-04-21-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-04-22-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-04-22-LANL-GrowthRate.csv', '05/14/20-23:20:56', False),
    ('2020-04-22-MIT_CovidAnalytics-DELPHI.csv', '07/06/20-12:06:25', False),
    ('2020-04-23-CU-80contact.csv', '06/19/20-14:42:59', False),
    ('2020-04-23-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-04-24-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-04-26-CU-80contact.csv', '06/19/20-14:42:59', False),
    ('2020-04-26-IowaStateLW-STEM15.csv', '05/26/20-13:20:17', False),
    ('2020-04-26-LANL-GrowthRate.csv', '05/14/20-23:20:56', False),
    ('2020-04-27-MOBS_NEU-GLEAM_COVID.csv', '04/28/20-10:40:03', False),
    ('2020-04-27-MOBS_NEU-GLEAM-COVID-19_v1.csv', '04/27/20-13:28:29', False),
    ('2020-04-27-NotreDame-FRED.csv', '07/06/20-12:06:25', False),
    ('2020-04-27-UT-Mobility2.csv', '05/07/20-14:53:49', False),
    ('2020-04-28-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-04-29-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-04-29-LANL-GrowthRate.csv', '05/14/20-23:20:56', False),
    ('2020-04-30-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-01-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-03-CU-80contact.csv', '06/19/20-14:42:59', False),
    ('2020-05-03-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-03-CU-80contactw10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-03-JHU_IDD-CovidSPHighDist.csv', '05/20/20-10:16:07', False),
    ('2020-05-03-JHU_IDD-CovidSPModDist.csv', '05/20/20-10:16:07', False),
    ('2020-05-03-JHU-IDD-CovidSP-HighEffectDistancing.csv', '05/05/20-17:46:13', False),
    ('2020-05-03-JHU-IDD-CovidSP-ModEffectDistancing.csv', '05/05/20-17:46:13', False),
    ('2020-05-03-LANL-GrowthRateHosp.csv', '05/15/20-17:35:46', False),
    ('2020-05-04-Auquan-SEIR.csv', '07/06/20-12:06:25', False),
    ('2020-05-04-LANL-GrowthRateHosp.csv', '05/15/20-17:35:46', False),
    ('2020-05-04-UMass-MechBayes.csv', '05/04/20-20:16:49', False),
    ('2020-05-05-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-06-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-06-LANL-GrowthRateHosp.csv', '05/15/20-17:35:46', False),
    ('2020-05-07-CU-80contact.csv', '06/19/20-14:42:59', False),
    ('2020-05-07-CU-80contact.csv', '05/08/20-23:06:40', False),
    ('2020-05-07-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-07-CU-80contact1x10p.csv', '05/08/20-23:06:40', False),
    ('2020-05-07-CU-80contact1x5p.csv', '05/08/20-23:06:40', False),
    ('2020-05-07-CU-80contactw10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-07-CU-80contactw10p.csv', '05/08/20-23:06:40', False),
    ('2020-05-07-CU-80contactw5p.csv', '05/08/20-23:06:40', False),
    ('2020-05-07-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-08-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-10-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-10-CU-80contactw10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-10-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-05-10-USC-SI_kJalpha.csv', '07/01/20-12:33:54', False),
    ('2020-05-11-Auquan-SEIR.csv', '05/20/20-10:16:07', False),
    ('2020-05-12-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-13-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-14-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-14-CU-80contactw10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-14-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-05-14-ERDC-SEIR.csv', '05/20/20-10:16:07', False),
    ('2020-05-14-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-15-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-16-CovidActNow-SEIR_CAN.csv', '05/18/20-18:05:09', False),
    ('2020-05-17-CovidActNow-SEIR_CAN.csv', '05/20/20-13:46:30', False),
    ('2020-05-17-CovidActNow-SEIR_CAN.csv', '05/18/20-17:54:31', False),
    ('2020-05-17-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-17-CU-80contactw10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-17-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-05-17-UA-EpiCovDA.csv', '07/06/20-12:06:25', False),
    ('2020-05-17-USC-SI_kJalpha.csv', '07/01/20-12:33:54', False),
    ('2020-05-18-CU-80contact1x10p.csv', '05/18/20-17:54:39', False),
    ('2020-05-18-CU-80contact1x5p.csv', '05/18/20-17:54:39', False),
    ('2020-05-18-CU-80contactw10p.csv', '05/18/20-17:54:39', False),
    ('2020-05-18-CU-80contactw5p.csv', '05/18/20-17:54:39', False),
    ('2020-05-19-COVIDhub-ensemble.csv', '05/19/20-11:49:35', False),
    ('2020-05-19-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-20-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-21-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-21-CU-80contactw10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-21-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-05-21-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-22-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-23-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-23-ISUandPKU-vSEIdR.csv', '05/26/20-17:33:22', False),
    ('2020-05-24-COVID19Sim-SEIR.csv', '05/27/20-23:02:35', False),
    ('2020-05-24-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-24-CU-80contactw10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-24-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-05-24-USC-SI_kJalpha.csv', '07/01/20-12:33:54', False),
    ('2020-05-25-ISUandPKU-vSEIdR.csv', '07/06/20-12:06:25', False),
    ('2020-05-26-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-27-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-28-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-28-CU-80contactw10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-28-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-05-28-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-05-29-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-05-30-CovidActNow-SEIR_CAN.csv', '06/01/20-09:44:01', False),
    ('2020-05-30-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-05-30-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-05-31-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-05-31-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-05-31-STH-3PU.csv', '06/01/20-15:23:41', False),
    ('2020-05-31-USC-SI_kJalpha.csv', '07/01/20-12:33:54', False),
    ('2020-06-01-CDDEP-GlobalAgentBasedModel.csv', '07/06/20-12:06:25', False),
    ('2020-06-01-CDDEP-GlobalAgentBasedModel.csv', '06/22/20-10:34:35', False),
    ('2020-06-01-ISUandPKU-vSEIdR.csv', '07/06/20-12:06:25', False),
    ('2020-06-01-ISUandPKU-vSEIdR.csv', '06/22/20-10:34:35', False),
    ('2020-06-01-MIT_CovidAnalytics-DELPHI.csv', '06/22/20-10:34:35', False),
    ('2020-06-01-MOBS_NEU-GLEAM_COVID.csv', '06/22/20-10:34:35', False),
    ('2020-06-01-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-01-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-02-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-02-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-03-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-03-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-04-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-06-04-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-06-04-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-04-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-04-SWC-TerminusCM.csv', '06/07/20-13:25:08', False),
    ('2020-06-05-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-05-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-06-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-06-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-07-CU-80contact1x10p.csv', '06/19/20-14:42:59', False),
    ('2020-06-07-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-06-07-Geneva-DeterministicGrowth.csv', '06/22/20-10:34:35', False),
    ('2020-06-07-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-07-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-07-USC-SI_kJalpha.csv', '07/01/20-12:33:54', False),
    ('2020-06-08-MIT_CovidAnalytics-DELPHI.csv', '06/22/20-10:34:35', False),
    ('2020-06-08-MOBS_NEU-GLEAM_COVID.csv', '06/22/20-10:34:35', False),
    ('2020-06-08-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-08-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-09-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-09-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-10-ISUandPKU-vSEIdR.csv', '07/06/20-12:06:25', False),
    ('2020-06-10-ISUandPKU-vSEIdR.csv', '06/22/20-10:34:35', False),
    ('2020-06-10-NotreDame-mobility.csv', '07/06/20-12:06:25', False),
    ('2020-06-10-NotreDame-mobility.csv', '06/22/20-10:34:35', False),
    ('2020-06-10-STH-3PU.csv', '07/06/20-12:06:25', False),
    ('2020-06-10-STH-3PU.csv', '06/22/20-10:34:35', False),
    ('2020-06-11-CU-80contactw5p.csv', '06/18/20-15:45:29', False),
    ('2020-06-14-COVID19Sim-Simulator.csv', '06/16/20-00:00:45', False),
    ('2020-06-14-Geneva-DeterministicGrowth.csv', '06/22/20-10:34:35', False),
    ('2020-06-14-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-06-14-USC-SI_kJalpha.csv', '07/01/20-12:33:54', False),
    ('2020-06-15-ISUandPKU-vSEIdR.csv', '07/06/20-12:06:25', False),
    ('2020-06-15-ISUandPKU-vSEIdR.csv', '06/22/20-10:34:35', False),
    ('2020-06-15-MIT_CovidAnalytics-DELPHI.csv', '06/22/20-10:34:35', False),
    ('2020-06-15-MOBS_NEU-GLEAM_COVID.csv', '06/22/20-10:34:35', False),
    ('2020-06-16-Columbia_UNC-SurvCon.csv', '07/13/20-11:47:04', False),
    ('2020-06-16-Columbia_UNC-SurvCon.csv', '07/06/20-12:06:25', False),
    ('2020-06-16-Columbia_UNC-SurvCon.csv', '06/22/20-10:34:35', False),
    ('2020-06-16-NotreDame-mobility.csv', '06/22/20-16:31:02', False),
    ('2020-06-16-SurvCon-SurvCon.csv', '06/17/20-22:58:20', False),
    ('2020-06-16-SurvCon-SurvivalConvolution.csv', '06/16/20-15:51:48', False),
    ('2020-06-18-CU-high.csv', '06/22/20-10:34:19', False),
    ('2020-06-18-CU-low.csv', '06/22/20-10:34:19', False),
    ('2020-06-18-CU-mid.csv', '06/22/20-10:34:19', False),
    ('2020-06-19-NotreDame-mobility.csv', '06/22/20-16:31:02', False),
    ('2020-06-21-Columbia_UNC-SurvCon.csv', '06/29/20-07:44:02', False),
    ('2020-06-21-COVID19Sim-Simulator.csv', '06/22/20-23:43:03', False),
    ('2020-06-21-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-06-21-USC-SI_kJalpha.csv', '07/01/20-12:33:54', False),
    ('2020-06-22-Columbia_UNC-SurvCon.csv', '06/23/20-15:53:16', False),
    ('2020-06-24-QJHong-Encounter.csv', '07/06/20-12:06:25', False),
    ('2020-06-25-RobertWalraven-ESG.csv', '06/30/20-11:02:49', False),
    ('2020-06-26-QJHong-Encounter.csv', '07/06/20-12:06:25', False),
    ('2020-06-27-QJHong-Encounter.csv', '07/06/20-12:06:25', False),
    ('2020-06-28-Columbia_UNC-SurvCon.csv', '07/13/20-11:47:12', False),
    ('2020-06-28-Columbia_UNC-SurvCon.csv', '07/06/20-12:06:25', False),
    ('2020-06-28-Columbia_UNC-SurvCon.csv', '06/29/20-07:45:33', False),
    ('2020-06-28-Geneva-DetGrowth.csv', '07/06/20-12:06:25', False),
    ('2020-06-28-RobertWalraven-ESG.csv', '07/26/20-22:47:03', False),
    ('2020-06-28-RobertWalraven-ESG.csv', '07/06/20-12:06:25', False),
    ('2020-06-28-RobertWalraven-ESG.csv', '07/01/20-01:31:19', False),
    ('2020-06-29-QJHong-Encounter.csv', '07/06/20-12:06:25', False),
    ('2020-06-30-QJHong-Encounter.csv', '07/06/20-12:06:25', False),
    ('2020-06-30-RobertWalraven-ESG.csv', '07/26/20-22:47:32', False),
    ('2020-06-30-RobertWalraven-ESG.csv', '07/06/20-12:06:25', False),
    ('2020-07-03-RobertWalraven-ESG.csv', '07/06/20-11:05:02', False),
    ('2020-07-04-RobertWalraven-ESG.csv', '07/06/20-22:28:55', False),
    ('2020-07-04-RobertWalraven-ESG.csv', '07/06/20-18:38:10', False),
    ('2020-07-05-UT-Mobility.csv', '07/06/20-12:52:26', False),
    ('2020-07-12-CDDEP-SEIR.csv', '07/13/20-13:27:26', False),
    ('2020-07-12-CDDEP-SEIR.csv', '07/13/20-13:19:45', False),
    ('2020-07-12-CDDEP-SEIR.csv', '07/13/20-12:57:27', False),
    ('2020-07-13-QJHong-Encounter.csv', '07/22/20-12:16:08', True),  # is_force_retract
    ('2020-07-19-CDDEP-SEIR.csv', '07/20/20-16:15:28', False),
    ('2020-07-26-UCM_MESALab-FoGSEIR.xlsx', '07/31/20-09:17:58', False),
    ('2020-08-010-UVA-Ensemble.csv', '08/11/20-10:04:41', False),
    ('2020-08-02-USC-SI_kJalpha copy.csv', '08/09/20-16:41:50', False),
    ('2020-08-10_UVA_Ensemble.csv', '08/11/20-10:04:41', False),
    ('2020-08-22-Yu_Group-CLEP.csv', '08/31/20-19:01:07', False),
    ('2020-08-24-WalmartLabsML-LogForecasting.csv', '08/24/20-05:16:07', False),
    ('2020-08-29-Yu_Group-CLEP.csv', '08/31/20-19:01:29', False),
    ('2020-09-11-IHME-CurveFit.csv', '09/21/20-09:54:06', False),
    ('2020-09-13-JHU_IDD-CovidSP_error.csv', '09/29/20-13:12:51', False),
    ('2020-09-20-JHU_IDD-CovidSP_error.csv', '09/29/20-13:12:51', False),
    ('2020-10-05-JHU_CSSE.csv', '10/06/20-15:05:21', False),
    ('2020-10-05-JHU_CSSE.csv', '10/06/20-14:51:17', False),
    ('2020-10-12-WalmartLabsML-LogForecasting.csv', '10/12/20-01:32:15', False),
    ('2020-10-25-JHU-UNC-GAS-StatMechPool.csv', '10/28/20-06:03:56', False),
    ('2020-10-25-JHU-UNC-GAS-StatMechPool.csv', '10/28/20-05:59:47', False),
    ('2020-10-26-USC-SI_kJalpha.csv', '10/26/20-16:43:30', True),  # is_force_retract
    ('2020-11-01-JHU-UNC-GAS-StatMechPool.csv', '11/02/20-14:32:10', False),
    ('2020-11-07-JHU_CSSE-DECOM.csv', '11/09/20-20:14:19', False),
    ('2020-11-1-BPagano-RtDriven.csv', '11/02/20-15:13:11', False),
    ('2020-11-1-BPagano-RtDriven.csv', '11/02/20-14:30:16', False),
    ('2020-11-15-Covid19Sim-Simulator.csv', '11/23/20-11:23:29', False),
    ('2020-11-22-JHU_UNC_GAS-StatMechPool.csv', '12/07/20-17:35:21', False),
    ('2020-11-22-UCF-AEM.txt', '11/23/20-13:15:26', False),
    ('2020-11-29-JHU_UNC_GAS-StatMechPool.csv', '12/07/20-17:35:11', False),
    ('2020-12-07-USACE-ERDC_SEIR.csv', '12/07/20-12:12:54', True),  # is_force_retract
    ('2020-12-20-JHUAPL-Gecko.csv', '01/24/21-22:38:44', False),
    ('2020-12-27-JHUAPL-Gecko.csv', '01/24/21-22:38:53', False),
    ('20200813-Google-Harvard-CPF.csv', '08/14/20-17:31:18', False),
    ('2021-01-03-JHUAPL-Gecko.csv', '01/24/21-22:39:15', False),
    ('2021-01-10-Google_Harvard-CPF.csv', '01/11/21-21:47:18', False),
    ('2021-01-10-JHUAPL-Gecko.csv', '01/24/21-22:39:26', False),
    ('2021-01-11_BMA_ensemble.csv', '01/11/21-17:33:16', False),
    ('2021-01-17-Columiba_UNC-SurvCon.csv', '01/17/21-17:17:32', False),
    ('2021-01-17-JHUAPL-Gecko.csv', '01/24/21-22:39:40', False),
    ('2021-01-18-WalmartLabsML-LogForecasting.csv', '01/18/21-02:07:06', False),
    ('IQVIA_ACOE-STAN', '08/23/20-11:28:22', False),
    ('Yu_group-CLEP', '08/09/20-13:13:33', False),
}  # input from spreadsheet: 3-tuples: (file_name, commit_date, is_force_retract)


@click.command()
def issue_308_app():
    """
    Implements [delete Zoltar forecasts that were deleted from github #308]. Creates new Forecasts for each input file
    from the first two columns of [COVIDhub-deleted-forecasts](https://docs.google.com/spreadsheets/d/1Sc3ehJDgRYYHDCGiS1WA0fgHKN5_DevWwmTOMZWTNyU/edit#gid=0),
    handling special cases as discussed.
    """
    add_deleted_file_retractions(44, FILE_NAMES_COMMIT_DATES, ['day ahead inc death', 'day ahead cum death'])


def add_deleted_file_retractions(project_pk, file_names_commit_dates, target_group_names):
    """
    Top-level function. Skips file_names that have no corresponding Forecast. Errors if != 1 forecasts for file_name.

    :param project_pk: A Project.pk
    :param file_names_commit_dates: similar to  FILE_NAMES_COMMIT_DATES: list of 3-tuples:
        (file_name, commit_date, is_force_retract)
    :param target_group_names: list of target group names as returned by `group_targets()`'s keys
    :return: list of created Forecasts
    :raises RuntimeError: if != 1 forecasts for source
    """
    from utils.forecast import cache_forecast_metadata  # imported here so that tests can patch via mock:


    # from is_different_old_new_json():
    def sort_key(pred_dict):
        return pred_dict['unit'], pred_dict['target'], pred_dict['class']


    # for testing: delete previous matching DELETE_NOTE
    prev_forecast_qs = Forecast.objects.filter(notes=DELETE_NOTE)
    logger.warning(f"deleting previous forecasts ({prev_forecast_qs.count()}): {[f.pk for f in prev_forecast_qs]}")
    prev_forecast_qs.delete()

    # build forecast_to_source_commit_date_is_force: Forecast -> (source, commit_date, is_force_retract), ...}.
    # commit_date is a datetime.date
    forecast_to_source_commit_date_is_force = {}
    logger.info(f"processing sources: {len(file_names_commit_dates)}")
    for file_name, commit_date, is_force_retract in file_names_commit_dates:
        forecasts = Forecast.objects.filter(source=file_name)
        if len(forecasts) == 0:
            logger.warning(f"no forecasts found with source={file_name!r}. skipping")
            continue
        elif len(forecasts) != 1:
            raise RuntimeError(f"!= 1 forecasts for source: {file_name!r}, {[f.id for f in forecasts]}")

        forecast = forecasts[0]
        # parse commit_date. ex: '01/08/21-16:34:58'
        try:
            commit_date_obj = datetime.date(month=int(commit_date[0:2]), day=int(commit_date[3:5]),
                                            year=int("20" + commit_date[6:8]))
        except ValueError as ve:
            raise RuntimeError(f"invalid commit_date: {commit_date!r}: {ve!r}")

        # set/update forecast if it's new or if this item's date is newer than the current one
        if (forecast not in forecast_to_source_commit_date_is_force) \
                or (forecast_to_source_commit_date_is_force[forecast][1] < commit_date_obj):
            forecast_to_source_commit_date_is_force[forecast] = (file_name, commit_date_obj, is_force_retract)

    # get all_target_group_names
    project = Project.objects.get(pk=project_pk)
    target_groups = group_targets(project.targets.all())  # group_name -> group_targets
    all_target_group_names = []
    for target_group_name in target_group_names:
        if target_group_name not in target_groups:
            raise RuntimeError(f"target group not found: {target_group_name!r}")

        targets = target_groups[target_group_name]
        all_target_group_names.extend([t.name for t in targets])
    logger.info(f"target groups: {len(all_target_group_names)}")

    # create a new forecast for each passed forecast that, for each existing prediction dict, either retracts it (if in
    # the target groups, or if is already a retraction) or a duplicates it o/w
    logger.info(f"processing {len(forecast_to_source_commit_date_is_force)} forecasts")
    new_forecasts = []  # return value. filled next
    for forecast in sorted(forecast_to_source_commit_date_is_force.keys(), key=lambda f: f.id):
        is_force_retract = forecast_to_source_commit_date_is_force[forecast][2]
        new_pred_dicts = []  # filled next then loaded
        num_retract, num_dup = 0, 0
        # NB: is_include_retract=True so that retracted PredictionElements show up:
        pred_dicts = sorted(json_io_dict_from_forecast(forecast, None, True)['predictions'], key=sort_key)
        for pred_dict in pred_dicts:
            if (pred_dict['target'] in all_target_group_names) or is_force_retract:
                num_retract += 1
                new_pred_dict = {'unit': pred_dict['unit'],
                                 'target': pred_dict['target'],
                                 'class': pred_dict['class'],
                                 'prediction': None}
                new_pred_dicts.append(new_pred_dict)
            else:
                num_dup += 1
                new_pred_dicts.append(pred_dict)

        # set new issue date, making sure it doesn't violate any rules
        source_commit_date = forecast_to_source_commit_date_is_force[forecast][1]
        new_issue_date = source_commit_date if source_commit_date > forecast.issue_date \
            else forecast.issue_date + datetime.timedelta(days=1)

        logger.info(f"loading forecast: {forecast.pk},{len(new_pred_dicts)},{forecast.source},"
                    f"{forecast.time_zero.timezero_date.isoformat()},{forecast.issue_date.isoformat()},"
                    f"{source_commit_date.isoformat()},{new_issue_date.isoformat()},"
                    f"{not (source_commit_date > forecast.issue_date)},{num_retract},{num_dup},{is_force_retract}")
        new_forecast = Forecast.objects.create(forecast_model=forecast.forecast_model, source=forecast.source,
                                               time_zero=forecast.time_zero, notes=DELETE_NOTE)
        new_forecast.issue_date = new_issue_date
        new_forecast.save()  # fails if not unique, i.e., if issue_date already has a forecast
        new_forecasts.append(new_forecast)
        logger.info(f"new_forecast={new_forecast}")
        load_predictions_from_json_io_dict(new_forecast, {'meta': {}, 'predictions': new_pred_dicts},
                                           is_validate_cats=False)  # atomic
        cache_forecast_metadata(new_forecast)  # atomic

    # done!
    logger.info(f"* done. new_forecasts={[f.pk for f in new_forecasts]}")
    return new_forecasts


if __name__ == '__main__':
    issue_308_app()
