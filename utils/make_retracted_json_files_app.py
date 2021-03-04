import json
from itertools import groupby
from pathlib import Path

import click
import django
from django.db import connection
from django.shortcuts import get_object_or_404

# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.utilities import YYYY_MM_DD_DATE_FORMAT
from utils.migration_0014_utils import _num_rows_old_data, _pred_dicts_from_forecast_old, \
    PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS, _grouped_version_rows

from forecast_app.models import Project, Forecast, QuantileDistribution, PointPrediction


@click.command()
def main():
    """
    xx
    """
    output_dir = Path('/tmp')
    project = get_object_or_404(Project, pk=44)
    unit_id_to_obj = {unit.pk: unit for unit in project.units.all()}
    target_id_to_obj = {target.pk: target for target in project.targets.all()}

    print(f'starting. project={project}. getting _grouped_version_rows()')
    grouped_version_rows = _grouped_version_rows(project, True)  # is_versions_only
    print(f'starting. #grouped_version_rows={len(grouped_version_rows)}')
    for (fm_id, tz_id), grouper in groupby(grouped_version_rows, key=lambda _: (_[0], _[1])):
        print('*', fm_id, tz_id)
        versions = list(grouper)  # list for zip
        for (_, _, issue_date_1, f_id_1, source_1, created_at_1, rank_1), \
            (_, _, issue_date_2, f_id_2, source_2, created_at_2, rank_2) in zip(versions, versions[1:]):
            point_rows_in_f1_not_in_f2 = _forecast_diff(f_id_1, f_id_2, True)
            quantile_rows_in_f1_not_in_f2 = _forecast_diff(f_id_1, f_id_2, False)
            if not point_rows_in_f1_not_in_f2 and not quantile_rows_in_f1_not_in_f2:  # no rows removed
                continue

            f2 = Forecast.objects.get(pk=f_id_2)
            print('-', f_id_1, f_id_2, '.', len(point_rows_in_f1_not_in_f2), len(quantile_rows_in_f1_not_in_f2), '.',
                  _num_rows_old_data(f2))

            # 1/2 get json for f2
            f2_predictions = _pred_dicts_from_forecast_old(f2)
            # print('  = num predictions existing', len(f2_predictions))

            # 2/2 add json for retracted point and quantile prediction elements
            for unit_id, target_id in point_rows_in_f1_not_in_f2:
                f2_predictions.append({"unit": unit_id_to_obj[unit_id].name,
                                       "target": target_id_to_obj[target_id].name,
                                       "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[PointPrediction],
                                       "prediction": None})
            # print('  = num predictions after points', len(f2_predictions))

            for unit_id, target_id in quantile_rows_in_f1_not_in_f2:
                f2_predictions.append({"unit": unit_id_to_obj[unit_id].name,
                                       "target": target_id_to_obj[target_id].name,
                                       "class": PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS[QuantileDistribution],
                                       "prediction": None})
            # print('  = num predictions after quantiles', len(f2_predictions))

            # output the json file. recall zoltpy functions:
            # - zoltpy.connection.Forecast.delete()
            #   forecast_uri = f'{self.zoltar_connection.host}/api/forecast/{forecast_pk}/'
            #   forecast = Forecast(self.zoltar_connection, forecast_uri)
            #
            # - zoltpy.connection.Model.upload_forecast(self, forecast_json, source, timezero_date, notes='')
            #   model_uri = f'{self.zoltar_connection.host}/api/model/{model_pk}/'
            #   model = Model(self.zoltar_connection, model_uri)
            timezero_str = f2.time_zero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT)
            issue_date_str = f2.issue_date.strftime(YYYY_MM_DD_DATE_FORMAT)
            filename = f'{f2.pk}_{f2.forecast_model.pk}_{timezero_str}_{issue_date_str}_with_retractions.json'
            f2_json = {'meta': {}, 'predictions': f2_predictions}
            with open(output_dir / filename, 'w') as fp:
                print('  writing', output_dir / filename)
                json.dump(f2_json, fp, indent=4)
    print('done')


def _forecast_diff(f_id_1, f_id_2, is_point):
    table_name = 'forecast_app_pointprediction' if is_point else 'forecast_app_quantiledistribution'
    sql = f"""
        WITH except_rows AS (
            SELECT unit_id, target_id
            FROM {table_name}
            WHERE forecast_id = %s
                EXCEPT
            SELECT unit_id, target_id
            FROM {table_name}
            WHERE forecast_id = %s
            ORDER BY unit_id, target_id
        )
        SELECT *
        FROM except_rows;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (f_id_1, f_id_2,))
        return cursor.fetchall()


if __name__ == '__main__':
    main()
