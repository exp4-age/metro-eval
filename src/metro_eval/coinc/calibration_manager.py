# -*- coding: utf-8 -*-
"""
Created on Thu May  7 14:35:10 2026

@author: exp4-NiGo-233
"""

'''
Handles:
    Saving calibrations
    loading calibrations
    searching available calibraitons
'''

import json
import h5py
import os
from pathlib import Path
import numpy as np
from typing import Dict
from dataclasses import dataclass, fields, asdict
from typing import Any
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from scipy.optimize import curve_fit
from scipy.odr import ODR, Model, RealData
from scipy.stats import norm

from matplotlib.widgets import RectangleSelector
from lmfit.models import GaussianModel, ConstantModel
import tkinter as tk
from tkinter import filedialog, ttk
import datetime
import yaml

from . import models
from .file_handler import get_keys, read_coinc

SCRIPT_DIR = Path(__file__).resolve().parent
CALIBRATION_DIR = SCRIPT_DIR / "calibrations"


def save_calibration(calibration_dict):
    experiment = calibration_dict["experiment"]
    setting = calibration_dict["setting"]
    author = calibration_dict["author"]
    version = calibration_dict["version"]
    index = calibration_dict["index"]

    filename = f"{experiment}_{setting}_{index}_{author}_{version}.json"

    filepath = CALIBRATION_DIR / filename

    with open(filepath, "w") as f:
        json.dump(calibration_dict, f, indent=4)


def load_calibration(experiment=None, setting=None, index=None, author=None, 
                     version=None, filename=None):
    if filename is None:
        filename = f"{experiment}_{setting}_{index}_{author}_{version}.json"

    filepath = CALIBRATION_DIR / filename

    with open(filepath, "r") as f:
        return json.load(f)


def list_calibrations():
    return list(CALIBRATION_DIR.glob("*.json"))

@dataclass(slots=True)
class ExperimentMetadata():
    """Metadata"""
    experiment: str = ""
    setting: str = ""
    version: str = ""
    author: str = ""
    index: str = ""
    photon_detector: str = ""
    photon_energy: str = ""
    target: str = ""
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'ExperimentMetadata':
        """Create instance from dict, using available keys only."""
        field_names = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in field_names}
        return cls(**filtered)

class Calibration:
    def __init__(self, calibration_dict):
        
        self.calibration_dict=calibration_dict
        
        if calibration_dict is not None:
            self.load_dict(calibration_dict)
        
        self.populate_metadata(calibration_dict)
        
        if self.method is not None:
            if self.model_func is not None:
                if self.p0 is not None:
                    self.get_conversion(set_values=True)
                    pass
        
    def populate_metadata(self, data):
        self.metadata = ExperimentMetadata.from_dict(data)
    
    def load_dict(self, data_dict: Dict):
        """Load calibration data from dictionary"""
        if 'calibration_points' in data_dict:
            self.x_values = np.asarray(data_dict['calibration_points'])[:,0]
            self.y_values = np.asarray(data_dict['calibration_points'])[:,2]
            self.x_err = np.asarray(data_dict['calibration_points'])[:,1]
            self.y_err = np.asarray(data_dict['calibration_points'])[:,3]
        if 'method' in data_dict:
            self.method = data_dict['method']
        if "initial_parameters" in data_dict:
            self.p0 = data_dict["initial_parameters"]["p0"]
        if 'model_type' in data_dict:
            self.model_func = models.MODELS[data_dict['model_type']]
        if 'bunch_overlap' in data_dict:
            self.bunch_overlap_params = data_dict["bunch_overlap"]
            
            
        if self.x_values is None or self.y_values is None:
            raise ValueError("Dictionary must contain at least 'x_values' and 'y_values'")

    def get_conversion(self, model_func=None, method=None, set_values=False, 
                       verbalize=False, p0=None, **kwargs):
        x_values = self.x_values
        y_values = self.y_values
        x_err = self.x_err
        y_err = self.y_err
        if p0 is None:
            p0 = self.p0
        if model_func is None:
            model_func = self.model_func
        if method is None:
            method = self.method
        if verbalize:
            print("Used fitting method: "+method)

        if set_values:
            self.method = method
            self.model_func = model_func

        if method == 'odr' and (x_err is not None or y_err is not None):
            def model_odr(params, x):
                return model_func(x, *params)
            model = Model(model_odr)
            data = RealData(x_values, y_values, sx=x_err, sy=y_err)
            odr = ODR(data, model, beta0=p0 or np.ones(3), **kwargs)
            output = odr.run()
            if verbalize:
                print("odr fit performed!")
            if set_values:
                self.popt = output.beta
                self.perr = output.sd_beta
                self.pcov = output.cov_beta
            return output.beta, output.sd_beta, output.cov_beta

        sigma = y_err if y_err is not None else None
        popt, pcov = curve_fit(model_func, x_values, y_values, p0=p0, 
                              sigma=sigma, absolute_sigma=True, **kwargs)
        perr = np.sqrt(np.diag(pcov))
        if verbalize:
            print("curve_fit performed!")
        if set_values:
            self.popt = popt
            self.perr = perr
            self.pcov = pcov
        return popt, perr, pcov
    
    def convert(self, x, model_func=None):
        if model_func is None:
            model_func = self.model_func
        
        return model_func(x, *self.popt)

    def get_uncertainty(self, x0=None, stds=1):
        if x0 is None:
            x0 = np.linspace(np.min(self.x_values), np.max(self.x_values), 200)

        y_low, y_high = self.__confidence_bands(x0, stds=stds)
        y_fit = self.model_func(x0, *self.popt)

        return {'x0': x0, 'y_fit': y_fit,
                'y_low': y_low, 'y_high': y_high}

    def __format_parameters(self, param_names=None,
                            precision=1,
                            fmt="e",
                            verbalize=False):
        """Convenience method"""
        if self.popt is None or self.perr is None:
            return "No fit performed"
        else:
            formatting = f"{{:.{precision}{fmt}}}"
            if param_names is None:
                param_names = [f'p{i}' for i in range(len(self.popt))]

            lines = []
            for name, p, e in zip(param_names, self.popt, self.perr):
                p_str = formatting.format(p)
                e_str = formatting.format(e)
                lines.append(f"{name} = {p_str} ± {e_str}")
                if verbalize:
                  print(lines)
            return '\n'.join(lines)

    def plot(self, fig_kws=None,
             xlabel="Electron time of flight / ns",
             ylabel="Electron kinetic energy / eV",
             param_names=None, precision=1,nr_of_bins=200,
             stds=2,verbalize=False,
             **kwargs):
        
        beamtime=self.metadata.experiment
        setting = self.metadata.setting
        # fig = plt.figure(num=f"Calibration_{beamtime}_{setting}", clear=True)
        fig = Figure()
        ax = fig.add_subplot()

        if self.popt is not None:
            grid = np.linspace(self.x_values.min(), self.x_values.max(),nr_of_bins)
            confidence_interval = self.get_uncertainty(x0=grid, stds=stds)
            calibration_curve, = ax.plot(grid, confidence_interval["y_fit"],
                                         label="Calibration curve",
                                         **kwargs)
            # Add parameter box AUTOMATICALLY
            legend_str = self.__format_parameters(param_names, precision, 
                                                  verbalize=verbalize)
            ax.text(0.98, 0.97, legend_str, transform=ax.transAxes,
                    fontsize=9, verticalalignment='top', ha="right",
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

            curve_color = calibration_curve.get_color()

            conf_int = ax.fill_between(grid, confidence_interval["y_low"],
                            confidence_interval["y_high"],
                            color=curve_color,
                            alpha=0.3, 
                            label="Confidence interval")

            
        
        
        cal_points = ax.errorbar(self.x_values,
                    self.y_values,
                    xerr=self.x_err,
                    yerr=self.y_err,
                    label="Calibration points",
                    ls = 'none',
                    **kwargs)
    
        if self.popt is not None:
            handles, labels = ax.get_legend_handles_labels()
            order = [2, 0, 1]  # Points → Curve → Confidence
            ax.legend([handles[i] for i in order], 
                      [labels[i] for i in order])
        ax.set_title(beamtime+", "+setting)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.grid(alpha=0.3)
        return fig, ax

    def calc_jacobian(self, x0, verbalize=False):
        '''
        determines numerical Jacobian with respect to parameters at each x0
        '''
        if self.popt is None:
            print("No fit is performed yet!")
            return
        eps = 1e-8
        J = []
        for xi in x0:
            grad = []
            for k, pk in enumerate(self.popt):
                dp = np.zeros_like(self.popt)
                dp[k] = eps
                grad_k = (self.model_func(xi, *(self.popt + dp)) -
                          self.model_func(xi, *(self.popt - dp))) / (2*eps)
                grad.append(grad_k)
            J.append(grad)
        jacobian = np.asarray(J)              # shape (n_points, n_params)
        if verbalize:
            print(jacobian.shape)
        return jacobian
    
    def __std_deviation(self,x0, verbalize=False):
        J = self.calc_jacobian(x0)
        
        variance = np.einsum("ij,jk,ik->i", J, self.pcov, J)
        sigma = np.sqrt(variance)
        if verbalize:
            print(sigma.shape)
        return sigma
    
    def __confidence_bands(self, x0, stds=2, confidence=None,
                                            **kwargs):
        if confidence is not None:
            stds = norm.ppf(confidence)
        
        sigma = self.__std_deviation(x0)
        y = self.model_func(x0, *self.popt)
        y_low = y - stds*sigma
        y_high = y + stds*sigma
        return y_low, y_high
    
    def calc_uncertainty_of_difference(self, tof1, tof2, ):
        jacobian1 = self.calc_jacobian(tof1)
        jacobian2 = self.calc_jacobian(tof2)
        
        jacobian_diff = jacobian2 - jacobian1
        
        variance_of_difference = jacobian_diff @ self.pcov @ jacobian_diff.T
        
        sigma_of_difference = np.sqrt(variance_of_difference)
        
        return sigma_of_difference


@dataclass
class CalibrationManager:
    author: Any = None
    experiment: Any = None
    setting: Any = None
    index: Any = None
    version: Any = None
    model_type: Any = None
    method: Any = None
    calibration_points: Any = None
    initial_parameters: Any = None
    fitted_parameters: Any = None
    bunch_overlap: Any = None
        
    @classmethod
    def from_dict(cls, params: dict[str, Any]):
        field_names = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in params.items() if k in field_names}
        return cls(**filtered)


class CalibrationView:
    def __init__(self, master, manager):
        self.manager = manager
        self.window = tk.Toplevel(master)
        self.window.title("Calibration GUI")
        
        self.file_label=tk.StringVar(master=master, value="No files selected")
        self.selected_key=tk.StringVar(master=master, value="")
        self.data_loaded_label=tk.StringVar(master=master, value="")
        self.setupView()
        
        
    def setupView(self):
        self.window.geometry("1500x600")
        
        # ---- setup window
        self.window.rowconfigure(0, weight=1)
        self.window.columnconfigure(0, weight=1, minsize=300)
        self.window.columnconfigure(1, weight=2, minsize=600)
        self.window.columnconfigure(2, weight=2, minsize=600)
        
        
        info_wdw = ttk.Frame(self.window, padding=10, relief ="solid",)
        info_wdw.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        
        plot_wdw = ttk.Frame(self.window, padding=10, relief ="solid")
        plot_wdw.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")
        
        right_wdw = ttk.Frame(self.window, padding=10, relief ="solid")
        right_wdw.grid(row=0, column=2, padx=5, pady=5, sticky="nsew")

        
        # ---- info window - info
        info_wdw.columnconfigure(0, weight=1)
        info_wdw.rowconfigure(0, weight=1, minsize=100)
        
        data = asdict(self.manager)
        info = {k: v for k, v in data.items() if k != "calibration_points"}
        points= data["calibration_points"]
        
        info_frame = ttk.Frame(info_wdw, padding=10, relief="solid")
        info_frame.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        
        
        text = tk.Text(info_frame, width=40, height=15)
        text.grid(row=0, column=0, padx=0, pady=5, sticky="nsew")
        import json
        formatted = json.dumps(info, indent=4)
        text.insert("1.0", formatted)
        
        text.config(state="disabled")
        
        # ---- info window - points
        
        points_frame = ttk.Frame(info_wdw, padding=10, relief="solid",
                                 width=40)
        points_frame.grid(row=1, column=0, padx=5, pady=5, sticky="nsew")
        
        columns = ("x", "x_err", "y", "y_err")
        
        points = points or [] # handles empty array or None
        
        tree = ttk.Treeview(
                points_frame,
                columns=columns,
                show="headings"
            )
        column_width=1
        # Define headings
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=column_width)

        # Insert rows
        for row in points:
            tree.insert("", tk.END, values=row)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(
            points_frame,
            orient="vertical",
            command=tree.yview
        )
        
        tree.configure(yscrollcommand=scrollbar.set)
        
        # Layout
        tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # ---- central window
        plot_wdw.columnconfigure(0, weight=1)
        plot_wdw.rowconfigure(0, weight=1)
        plot_wdw.rowconfigure(1, weight=3)
        plot_wdw.rowconfigure(2, weight=2)
        
        
        
        # ---- file selection
        file_browser = ttk.Frame(plot_wdw, padding=10, relief="solid")
        file_browser.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        
        file_browser.columnconfigure(0,weight=2)
        file_browser.columnconfigure(1,weight=2)
        file_browser.columnconfigure(2,weight=2)
        file_browser.columnconfigure(3,weight=2)
        file_browser.rowconfigure(0,weight=1)
        
        # Display chosen files
        file_lbl = ttk.Label(file_browser, 
                              textvariable=self.file_label, width=20)
        file_lbl.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        
        # Browse button, select data
        browse_btn = ttk.Button(file_browser, 
                               text="Browse File", 
                               command=self.browse_file)
        browse_btn.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        
        # ---- calibration plot
        calib_view = ttk.Frame(plot_wdw, padding=10, relief="solid")
        calib_view.grid(row=2, column=0, padx=5, pady=5, sticky="nsew")
        
        
        
        
        
        
        
        
    def browse_file(self):
        '''

        Look up a file and store the file_path and file_label. Afterwards, 
        populate the available keys for the coincidence key dropdown menu.

        '''
        file_path = filedialog.askopenfilenames()
        if not file_path:
            return

        self.file_path = list(file_path)
        label = ""
        for file in file_path:
            label += os.path.basename(file)+", "
        label = label[:-2]
        self.file_label.set(label)

        
    



if __name__ == "__main__":
    info = load_calibration(filename=list_calibrations()[1].name)#load_calibration("FinEstBeAMS_202211", "ret4", "1", "NiGo", "v1")
    calib = Calibration(info)
    calib.plot()
    print(calib.bunch_overlap_params)
    manager = CalibrationManager.from_dict(info)
    root = tk.Tk()
    CalibrationView(root, manager)
    root.mainloop()
    
    # A=load_calibration(filename=list_calibrations()[1].name)
    
    