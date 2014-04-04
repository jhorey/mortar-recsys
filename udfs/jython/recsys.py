from math import exp
from collections import defaultdict

@outputSchema("scaled: double")
def logistic_scale(val, logistic_param):
    return -1.0 + 2.0 / (1.0 + exp(-logistic_param * val))

@outputSchema("t: (item_A, item_B, dist: double, raw_weight: double)")
def best_path(paths):
    return sorted(paths, key=lambda t:t[2])[0]

@outputSchema("t: (item_A, item_B, dist: double, raw_weight: double, link_data: map[], linking_item: chararray)")
def best_path_detailed(paths):
    return sorted(paths, key=lambda t:t[2])[0]

@outputSchema("signal_map:map[]")
def aggregate_signal_types(signal_list):
    signal_dict = {}
    for row in signal_list:
        if row[3]:
            if not signal_dict.get(row[3]):
                signal_dict[row[3]] = 0
            signal_dict[row[3]] += 1
    return signal_dict

@outputSchema("signal_map:map[]")
def combine_signals(signal_list):
    signal_dict = {}
    for row in signal_list:
        if row[3]:
            for val in row[3].keys():
                if not signal_dict.get(row[3]):
                    signal_dict[row[3]] = 0
                signal_dict[val] += row[3][val]
    return signal_dict

