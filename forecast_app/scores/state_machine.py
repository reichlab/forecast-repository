import logging
from abc import ABC, abstractmethod

from collections import namedtuple


logger = logging.getLogger(__name__)

# used to track previous and current inputs - passed to LineProcessingMachine.set_input_tuple()
InputTuple = namedtuple('InputTuple', ['forecast_pk', 'location_pk', 'target_pk',
                                       'bin_start_incl', 'bin_end_notincl',
                                       'predicted_value', 'true_value'])


class LineProcessingMachine(ABC):
    """
    An abstract state machine class that supports bin line processing scores like log and pit. It is used to process
    bin lines by calling set_input_tuple() and then advance() for each line. Lines must be sorted in this order:

        'forecast__id', 'location__id', 'target__id', 'bin_start_incl'

    """


    def __init__(self, score) -> None:
        super().__init__()
        self.score = score

        self.input_tuple_previous = None
        self.input_tuple_current = None

        self.state = None


    @abstractmethod
    def advance(self):
        """
        Decides the state to transition to, executing post-transition (but pre-state change) actions.
        """
        pass


    @abstractmethod
    def transition_to_state(self, new_state):
        """
        Saves the new state and handles running any entry actions.
        """
        pass


    @abstractmethod
    def save_score(self):
        """
        Creates a ScoreValue instance.
        """
        pass


    def set_input_tuple(self, input_tuple):
        # logger.debug('set_input_tuple(): {}'.format(input_tuple))
        self.input_tuple_previous = self.input_tuple_current
        self.input_tuple_current = input_tuple


    def is_match(self):
        if self.input_tuple_current.true_value is None:
            is_truth_in_bin = (self.input_tuple_current.bin_start_incl is None) or \
                              (self.input_tuple_current.bin_end_notincl is None)
        else:
            is_truth_in_bin = self.input_tuple_current.bin_start_incl \
                              <= self.input_tuple_current.true_value \
                              < self.input_tuple_current.bin_end_notincl
        return is_truth_in_bin


    def is_start_new_distribution(self):
        """
        :return: True if starting a new target (i.e., a new predictive distribution). assumes lines are ordered by:
            'forecast__id', 'location__id', 'target__id', 'bin_start_incl' - see _calc_log_bin_score_values()
        """
        return self.input_tuple_current.target_pk != self.input_tuple_previous.target_pk
