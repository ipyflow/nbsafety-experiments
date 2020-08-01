# -*- coding: utf-8 -*-
import random

import numpy as np


class ReplayStatsGroup(object):
    def __init__(self, group_suffix):
        self.group_suffix = group_suffix
        self.num_correct = 0.
        self.num_attempts = 0
        self.pp_micro_den = 0.
        self.pp_macro_sum = 0.
        self.pp_normalized_sum = 0.
        self.num_cells_at_each_attempt = []

    def update(self, cell_id, cell_choices, available_choices):
        if isinstance(cell_choices, int):
            assert not isinstance(available_choices, int)
            cell_choices = set(random.sample(list(available_choices), cell_choices))
            available_choices = len(available_choices)
        if available_choices <= 1:
            return
        was_correct = float(cell_id in cell_choices)
        prob_random_correct = float(len(cell_choices)) / available_choices
        self.pp_micro_den += prob_random_correct
        self.pp_macro_sum += was_correct / prob_random_correct
        self.pp_normalized_sum += ((was_correct / prob_random_correct) - 1.) / (len(cell_choices) - 1.)
        self.num_correct += was_correct
        self.num_attempts += 1
        self.num_cells_at_each_attempt.append(float(len(cell_choices)))

    def make_dict(self):
        ret = {}
        if self.num_attempts == 0:
            return ret

        ret.update({
            f'predictive_power_{self.group_suffix}': self.num_correct / self.pp_micro_den,
            f'macro_predictive_power_{self.group_suffix}': self.pp_macro_sum / self.num_attempts,
            f'normalized_predictive_power_{self.group_suffix}': self.pp_normalized_sum / self.num_attempts,
        })
        if self.group_suffix != 'next_cell':
            ret.update({
                f'avg_num_{self.group_suffix}': np.mean(self.num_cells_at_each_attempt),
                f'median_num_{self.group_suffix}': np.median(self.num_cells_at_each_attempt),
            })
        return ret
