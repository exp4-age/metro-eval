# -*- coding: utf-8 -*-
"""
Created on Fri May  8 10:27:37 2026

@author: exp4-NiGo-233


Here, functions and classes are defined, which are used for 
postprocessing of data. This includes
    overlapping signal from consecutive excitation pulses
    ...
"""

import numpy as np



def overlap(array, repetition_time, ROI_first, ROI_last, 
                  nPhotons=0, verbalize=False):
    """
    Overlay bunches
    """

    if array.size == 0:
        return array

    # First-particle TOF (used for bunch assignment)
    if array.ndim == 1:
        tof0 = array
    else:
        tof0 = array[:, 0]

    max_time = np.max(array)
    if verbalize:
        print(f"Data max time: {max_time:.1f}ns → auto window [0, {max_time:.1f}]")

    # -------------------------
    # 1. Compute bunch index ONCE for each row
    #   The bunch index is found with help of the ROI_first values
    #   We therefore know where the signal starts in time.
    #   The TOF of the first electron is inspected. When the value is between
    #   ROI_first[0] and ROI_first[0]+repetition_time, we have index 1. 
    #   Analogously, the further bunches are indexed.
    # -------------------------
    
    offset_ROI_first = 0 # default value for the start of a bunch (no offset in time)
    
    if ROI_first:
        offset_ROI_first = ROI_first[0]
    
    bunch_index = ((tof0-offset_ROI_first) // repetition_time).astype(np.int64)
    bunch_offset = bunch_index * repetition_time

    # Discard bunches that would exceed max_time (first bunch has index 0 -> bunch_offset=0)
    valid_bunch = bunch_offset + ROI_last[1] <= max_time

    # -------------------------
    # 2. Slowest-particle filter
    #   The slowest electron (we exclude photons here), is masked for the 
    #   ROI_last.
    # -------------------------
    if array.ndim == 1:
        slowest = array
    else:
        particle_cols = slice(0, -nPhotons) if nPhotons > 0 else slice(None)
        slowest = np.max(array[:, particle_cols], axis=1)

    slow_mask = slowest <= (ROI_last[1] + bunch_offset)

    # -------------------------
    # 3. First-particle filter (ROI_first)
    # -------------------------
    if ROI_first is not None:
        lower, upper = ROI_first
        ROI_first_mask = (
            (tof0 >= lower + bunch_offset) &
            (tof0 <= upper + bunch_offset)
        )
    else:
        ROI_first_mask = True

    # -------------------------
    # 4. Final combined mask
    # -------------------------
    mask = valid_bunch & slow_mask & ROI_first_mask

    # if mask is all False, return empty array
    if not np.any(mask):
        if array.ndim == 1:
            return np.empty((0,))
        else:
            return np.empty((0, array.shape[1]))

    # -------------------------
    # 5. Apply offset subtraction
    # -------------------------
    result = array[mask].copy()

    if result.ndim == 1:
        result -= bunch_offset[mask]
    else:
        result -= bunch_offset[mask][:, None]

    return result



def generate_array(rows=100000, cols=4, max_value = 3000, offset = 0,
                   descending=False):
    arr = np.random.rand(rows, cols) * max_value + offset
    arr.sort(axis=1)
    if descending:
        arr = arr[:, ::-1]
    
    
    return arr


def EP_number_from_string(string):
    if string.isalpha():
        e_amount = string.count("E")
        p_amount = string.count("P")
    else:
        e_index = string.find("E")
        p_index = string.find("P")
        if e_index != -1:
            try:
                e_amount = int(string[:e_index])
                p_amount = int(string[e_index+1:p_index])
            except ValueError:
                print(f"Warning: Could not evaluate {string}.")
    return e_amount, p_amount


if __name__ == "__main__":
    
    length=1000
    offset=200
    repetition_time=1000
    hist_range=((0,offset+2.5*repetition_time),(0,offset+2.5*repetition_time))
    bins=(100,100)
    
    arr1 = generate_array(cols=2, max_value=length, offset=offset)
    arr2 = generate_array(cols=2, max_value=length, offset=(offset+repetition_time))
    arr = np.vstack((arr1, arr2))
    from metro_eval.coinc.plot_functions import plot_2D, bin_2D
    
    coinc, xedges, yedges = bin_2D(arr, (0,1), bins=bins,
                                   range=hist_range)
    
    plot_2D(coinc, xedges, yedges, num="Regular")
    
    arr_merge = overlap(arr, repetition_time, 
                              (offset,offset+length), 
                              (offset,offset+length-1)
                              )
    
    coinc, xedges, yedges = bin_2D(arr_merge, (0,1), bins=bins,
                                   range=hist_range,
                                   )
    
    plot_2D(coinc, xedges, yedges, num="Merged")
    
    key = "0E13P"
    
    e, p = EP_number_from_string(key)




















