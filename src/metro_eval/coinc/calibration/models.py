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




def _build_model_entry(func, description, parameter_labels, default_initial_parameters,expression=None):
    """Build a user-facing model definition with metadata for the GUI."""
    return {
        "func": func,
        "description": description,
        "parameter_labels": list(parameter_labels),
        "default_initial_parameters": list(default_initial_parameters),
        "expression": expression if expression else ""
    }


MODELS = {
    "poly_offset": _build_model_entry(
        poly_with_offset,
        (
            "Offset polynomial correction used for TOF-to-energy calibration. "
            "The first value is the x-offset, and the remaining coefficients are "
            "used as inverse powers of x-x_offset."
        ),
        ["x_offset", "c0", "c1", "c2", "c3", "c4"],
        [0,0,0,0,0,0],
        expression = (
            "<span style='font-size: 12pt;'>f(x) &nbsp;=&nbsp; </span>"
            "<span style='font-size: 16pt;'>∑"
            "<sub>k=0</sub><sup>n−1</sup>"
            "</span>"
            "&nbsp;"
            "<span style='font-size: 12pt;'>"
            "c<sub>k</sub>"
            "</span>"
            "&nbsp;/&nbsp;"
            "<span style='font-size: 12pt;'>"
            "(x − x<sub>offset</sub>)<sup>k</sup>"
            "</span>"
        )
    )
}


def get_model_config(model_name: str):
    """Return the registry entry for a model, preserving backward compatibility."""
    model = MODELS.get(model_name)
    if isinstance(model, dict):
        return model
    return {
        "func": model,
        "description": "No description available for this model.",
        "parameter_labels": [f"a{i}" for i in range(6)],
        "default_initial_parameters": [0.0] * 6,
    }