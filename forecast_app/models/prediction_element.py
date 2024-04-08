import hashlib
import json

from django.db import models

from utils.utilities import basic_str


#
# PredictionElement
#

class PredictionElement(models.Model):
    """
    Represents a prediction element as loaded from a "JSON IO dict" (aka 'json_io_dict' by callers).
    """

    # prediction classes. corresponds to json_io_dict's 'class' key
    BIN_CLASS = 0
    NAMED_CLASS = 1
    POINT_CLASS = 2
    SAMPLE_CLASS = 3
    QUANTILE_CLASS = 4
    MEAN_CLASS = 5
    MEDIAN_CLASS = 6
    MODE_CLASS = 7
    PRED_CLASS_CHOICES = (
        (BIN_CLASS, 'bin'),
        (NAMED_CLASS, 'named'),
        (POINT_CLASS, 'point'),
        (SAMPLE_CLASS, 'sample'),
        (QUANTILE_CLASS, 'quantile'),
        (MEAN_CLASS, 'mean'),
        (MEDIAN_CLASS, 'median'),
        (MODE_CLASS, 'mode'),
    )

    forecast = models.ForeignKey('Forecast', related_name='pred_eles', on_delete=models.CASCADE)
    pred_class = models.IntegerField(choices=PRED_CLASS_CHOICES)
    unit = models.ForeignKey('Unit', on_delete=models.CASCADE)
    target = models.ForeignKey('Target', on_delete=models.CASCADE)
    is_retract = models.BooleanField(default=False)

    # A binary MD5 hex hash of input "prediction" dict (converted to a json string), e.g., input dicts like:
    #
    #   {"family": "pois", "param1": 1.1}
    #   {"value": 5}
    #   {"sample": [0, 2, 5]}
    #   {"cat": [0, 2, 50], "prob": [0.0, 0.1, 0.9]}
    #   {"quantile": [0.25, 0.75], "value": [0, 50]}
    #
    # This hash is used by `load_predictions_from_json_io_dict()` to compare prediction elements for equality so that
    # duplicate data can be skipped. The algorithm we use to calculate this hash is as implemented in
    # `hash_for_prediction_data_dict()`. we store '' if is_retract b/c there is no PredictionData and therefore no hash
    data_hash = models.CharField(max_length=32)  # length based on output from hashlib.md5(s).hexdigest()


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.prediction_class_as_str(), self.unit.pk, self.target.pk,
                    self.is_retract, self.data_hash))


    def __str__(self):  # todo
        return basic_str(self)


    def prediction_class_as_str(self):
        return PredictionElement.prediction_class_int_as_str(self.pred_class)


    @classmethod
    def prediction_class_int_as_str(cls, prediction_class_int):
        return PRED_CLASS_INT_TO_NAME.get(prediction_class_int, '!?')


    @classmethod
    def hash_for_prediction_data_dict(cls, prediction_data):
        """
        Top-level method for computing the hash of a json_io_dict's "prediction" value. This function is not meant to be
        general to any dict, just json_io_dict ones. Recall MD5 is 128 bits (16 bytes).

        :param prediction_data: the json_io_dict's "prediction" value, e.g.,
            {"family": "pois", "param1": 1.1}  -> '845e3d041b6be23a381b6afd263fb113'
            {"value": 5}
            {"sample": [0, 2, 5]}
            {"cat": [0, 2, 50], "prob": [0.0, 0.1, 0.9]}
            {"quantile": [0.25, 0.75], "value": [0, 50]}
        :return: MD5 hex hash of `prediction_data` as `str`
        """
        return hashlib.md5(json.dumps(prediction_data, sort_keys=True).encode('utf-8')).hexdigest()


#
# some bidirectional accessors for PredictionElement.PRED_CLASS_CHOICES
#

PRED_CLASS_INT_TO_NAME = {class_int: class_name for class_int, class_name in PredictionElement.PRED_CLASS_CHOICES}
PRED_CLASS_NAME_TO_INT = {class_name: class_int for class_int, class_name in PredictionElement.PRED_CLASS_CHOICES}
