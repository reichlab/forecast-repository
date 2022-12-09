from django.db import models

from utils.utilities import basic_str


#
# ---- PredictionData ----
#

class PredictionData(models.Model):
    """
    Represents the actual prediction data corresponding to a PredictionElement. Note that we store the data as JSON,
    rather than a "sparse" table where every row's has all NULL columns but one (as we used to). We did this because
    the data's shape varies depending on the target type.

    The `data` field is the "prediction" portion of a prediction element (AKA its "prediction_data"). For example,
    this prediction element:

        {"unit": "loc2",
         "target": "pct next week",
         "class": "bin",
         "prediction": {
           "cat": [1.1, 2.2, 3.3],
           "prob": [0.3, 0.2, 0.5]
        }

    has this bin prediction data:

        {"cat": [1.1, 2.2, 3.3],
         "prob": [0.3, 0.2, 0.5]}

    This data is shaped differently for the five different target types, e.g.,
    - bin: { "cat": [1.1, 2.2, 3.3],  "prob": [0.3, 0.2, 0.5] }
    - named: { "family": "norm",  "param1": 1.1,  "param2": 2.2 ,  "param3": 3.3 }
    - point: { "value": 2.1 }
    - sample: { "sample": [0, 2, 5] }
    - quantile: { "quantile": [0.25, 0.75],  "value": [0, 50] }

    Notes:
    - None of the values are transformed in any way. For example, 'family' is not changed to an int.
    - This field is the data that each PredictionElement.data_hash is calculated on.
    """
    pred_ele = models.OneToOneField('PredictionElement', related_name='pred_data', on_delete=models.CASCADE,
                                    primary_key=True)
    data = models.JSONField()


    def __repr__(self):
        return str((self.pk, list(self.data.keys())))


    def __str__(self):  # todo
        return basic_str(self)
