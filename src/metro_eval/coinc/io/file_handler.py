'''
Handle files
'''
from __future__ import annotations

import os 
import h5py
import numpy as np
import re
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
            # Reshape dimensions of 1D arrays
            if data.ndim == 1:
                data = data.reshape(-1, 1)
        else:
            print(f"Warning: {key} not in {file_path}")
            return 
    return data

def read_scan(file_path, coincKeys = None, verbalize=True) -> ScanData:
    step_list = []

    with h5py.File(file_path, 'r') as f:
        base_group = f['0']
        for step in base_group.keys():
            if step == "by_idx":
                continue
            if verbalize: print(step)
            if coincKeys == None:
                keyList = list(base_group[step].keys())
            else:
                keyList = coincKeys
            
            data_dict = {}
            for key in keyList:
                if not isProperKey(key):
                    continue
                data_dict[key] = np.asarray(base_group[step][key])*0.025
                # Reshape dimensions of 1D arrays
                if data_dict[key].ndim == 1:
                    data_dict[key] = data_dict[key].reshape(-1, 1)
                if verbalize: print(list(data_dict.keys()))

            spec = ScanSpectrum(scan_value=float(step),
                                data_dict=data_dict)
            step_list.append(spec)
        
    return ScanData(step_list)
            
def isProperKey(s: str) -> bool:
    """
    Allowed patterns:
      - E, EE, EEE, EEEE
      - EP, EEP
      - P, PP
      - 1E, 2E, ..., 9E
      - 1P, 2P, ..., 9P
      - 1E1P, 1E2P, ..., 9E9P  
    
    """
    pattern = re.compile(
        r'^'
        r'(?:'
        r'(?:[1-9]?E{1,4})'      # E-only: E–EEEE or 1E–9E
        r'|'
        r'(?:[1-9]?P{1,2})'      # P-only: P, PP or 1P–9P
        r'|'
        r'(?:E{1,2}P)'           # EP mixed (letters only): EP, EEP
        r'|'
        r'(?:[1-9]E[1-9]P{1,2})'
        r')'
        r'$'
    )
    return bool(pattern.match(s))

@dataclass
class ScanSpectrum:
    scan_value: float
    data_dict: dict = None
    x: np.ndarray = None
    y: np.ndarray = None


@dataclass
class ScanData:
    spectra: list[ScanSpectrum]



