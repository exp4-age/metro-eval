'''
Handle files
'''
from __future__ import annotations

import os 
import h5py
import numpy as np
from dataclasses import dataclass



def get_file_type(file_path):
    _, ext = os.path.splitext(file_path)
    return ext.lower()

def get_keys(file_path):
    '''

    Parameters
    ----------
    file_path : string
        path of an h5 file.

    Returns
    -------
    keys : list
        list of group keys in h5 file.

    '''
    
    keys = []
    with h5py.File(file_path, 'r') as f:
        base_group = f['0/0.0']
        keys = list(base_group.keys())
        
    return keys

def read_coinc(file_path, key):
    with h5py.File(file_path, 'r') as f:
        base_group = f['0/0.0']
        if key in base_group:
            data = base_group[key][()]
            data = np.asarray(data) * 0.025
        else:
            print(f"Warning: {key} not in {file_path}")
            return 
    return data

def read_scan(file_path, coincKeys = None) -> ScanData:
    step_list = []
    with h5py.File(file_path, 'r') as f:
        base_group = f['0']
        for step in base_group.keys():
            data_dict = {}
            if coincKeys == None:
                coincKeys = list(base_group[step].keys())
            for key in coincKeys:
                if key == "other":
                    continue
                data_dict[key] = np.asarray(base_group[step][key])*0.025
                # Reshape dimensions of 1D arrays
                if data_dict[key].ndim == 1:
                    data_dict[key] = data_dict[key].reshape(-1, 1)

            spec = ScanSpectrum(scan_value=float(step),
                                data_dict=data_dict)
            step_list.append(spec)
        
    return ScanData(step_list)
            



@dataclass
class ScanSpectrum:
    scan_value: float
    data_dict: dict = None
    x: np.ndarray = None
    y: np.ndarray = None


@dataclass
class ScanData:
    spectra: list[ScanSpectrum]



