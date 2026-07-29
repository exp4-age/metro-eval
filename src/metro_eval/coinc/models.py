# -*- coding: utf-8 -*-
"""
Created on Thu May  7 14:34:49 2026

@author: exp4-NiGo-233
"""

'''
Contains fitting functions
'''

import numpy as np

def poly_with_offset(x, *coeffs):  # x_offset FIRST optional param!
    """
    Note: x_offset as first argument for curve_fit unpacking
    """
    x_offset = coeffs[0]
    coeffs = coeffs[1:]
    x_shifted = np.asarray(x) - x_offset
    result = np.zeros_like(x_shifted)
    for deg, coeff in enumerate(coeffs):
        result += coeff / (x_shifted ** deg)
    return result



MODELS = {
    "poly_offset" : poly_with_offset,
    }