'''
Handle files
'''

import os 
import h5py
import numpy as np
import matplotlib.pyplot as plt


def generate_array(rows=100000, cols=4, max_value = 3000):
    arr = np.random.rand(rows, cols) * max_value
    arr.sort(axis=1)
    arr = arr[:, ::-1]
    
    # enforce strict decrease
    eps = 1e-8
    for i in range(cols):
        arr[:, i] -= i * eps
    
    return arr


def mask_by_column(file, column, filter_range, verbalize=False):
    
    if verbalize:
        print(f"Filtering for column {column} in range {filter_range}.")
    
    # Extract the column, that should be filtered for
    col = file[:,column]
    
    # Build a mask for this column alone.
    lo, hi = filter_range
    col_mask = np.logical_and(col > lo, col < hi)
    
    return file[col_mask]



if __name__ == "__main__":
    
    array = generate_array(rows=100000)
    array_filtered = mask_by_column(array, 1, (100,500))
    
