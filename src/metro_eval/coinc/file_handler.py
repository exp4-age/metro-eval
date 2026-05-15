'''
Handle files
'''

import os 
import h5py
import numpy as np



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


