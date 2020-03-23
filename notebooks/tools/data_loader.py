import numpy as np


def load_dat(path, as_ndarray=False, start=0, lines=512):
    result_list = []
    with open(path, 'r') as fh:
        for i, line in enumerate(fh):
            if i >= start and lines > 0:
                result_list.append(tuple(line.split()))
                lines -= 1
    return np.array(result_list) if as_ndarray else result_list
