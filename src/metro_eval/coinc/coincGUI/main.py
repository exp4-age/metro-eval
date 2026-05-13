# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 11:11:31 2026

@author: exp4-NiGo-233
"""
import tkinter as tk
from tkinter import filedialog, ttk
import os
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import numpy as np

from file_handler import get_keys, read_coinc
from mask_functions import mask_by_column
from plot_functions import plot_1D, bin_2D, plot_2D
from calibration_manager import list_calibrations, Calibration, load_calibration
import calibration_manager as calman
from postprocessing import overlap


'''
Issues:
    status panel needs to update better
    
    calibration functions: 
        Needs to do preprocessing first
        Needs to change status in panel
        Removal must be fixed
        General development
    
    Use the Run dataclass to handle the data??
    
    Removing calibration/filters/overlap doesnt completely work yet
    
    Maybe: Not apply everything by button pressing, but use checkboxes to change 
    the things that should be executed before plotting
    
'''

class DataApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Coincidence GUI")

        # --- Stored state ---
        self.file_path = None
        self.data = None
        self.current_data = None
        self.selected_key = tk.StringVar(master=self.root)
        self.data_loaded_label = tk.StringVar(master=self.root,)
        self.file_label = tk.StringVar(master=self.root,value="No file selected")
        self.result_label = tk.StringVar(master=self.root,)
        self.filter_rows_column = []
        self.column_masks = []
        self.column_masks_status = []
        self.bunch_overlap = tk.BooleanVar(master=self.root, value=False)
        self.bunch_overlap_status = tk.StringVar(master=self.root,
                                                 value="False")
        self.bunch_overlap_parameters = {}
        self.calibration_info: Calibration = None
        self.calibrated = tk.BooleanVar(master=self.root, value=False)
        self.calibration_status = tk.StringVar(master=self.root,
                                                 value="False")
        self.edit_calibration_parameters = {}
        
        self.choice_plot_dim = tk.StringVar(master=self.root, 
                                            value="1D")
        self.plot_settings_1D = {}
        self.status_panel = None
        # --- UI setup ---
        self.setup_ui()

    #### GUI Setup functions
    def setup_ui(self):
        '''
        Sets the windows and widgets for the start page of the GUI.

        '''
        
        # Size of window
        self.root.geometry("1200x800")
        
        #### Assign Frames: window has 2 columns and 2 rows
        self.root.rowconfigure(0, weight=6)
        self.root.rowconfigure(1, weight=1)
        self.root.columnconfigure(0, weight=1)
        self.root.columnconfigure(1, weight=1)
        
        # Frame for the configurations of the data
        window_upper_left = ttk.Frame(self.root, padding=10)
        window_upper_left.grid(row=0, column=0, sticky="nsew")
        
        # Frame for the plot settings of the data
        window_upper_right = ttk.Frame(self.root, padding=10)
        window_upper_right.grid(row=0, column=1, sticky="nsew")
        
        # Frame for the exit/reset button
        window_bottom = ttk.Frame(self.root, padding=10)
        window_bottom.grid(row=1, column=0, columnspan=2, sticky="nsew")
        
        
        #### Assign subframes
        # Upper left
        window_upper_left.rowconfigure(0, weight=1, minsize=50)
        window_upper_left.rowconfigure(1, weight=1)
        window_upper_left.rowconfigure(2, weight=1)
        window_upper_left.columnconfigure(0, weight=1)
        
        file_browser = ttk.LabelFrame(window_upper_left, 
                                      text="Data selection",
                                      padding=0, relief="solid")
        file_browser.grid(row=0, column=0, sticky="nsew", padx=0, pady=0)
        postprocessing = ttk.LabelFrame(window_upper_left, 
                                        text="Postprocessing (Just testing for now)",
                                        padding=0, relief="solid")
        postprocessing.grid(row=1, column=0, sticky="nsew", padx=0, pady=0)
        masking = ttk.LabelFrame(window_upper_left, 
                                 text="Masking",
                                 padding=0, relief="solid")
        masking.grid(row=2, column=0, sticky="nsew", padx=0, pady=0)
        
        
        # Upper right
        window_upper_right.rowconfigure(0, weight=1, minsize=50)
        window_upper_right.rowconfigure(1, weight=1)
        window_upper_right.columnconfigure(0, weight=1)
        
        data_status = ttk.LabelFrame(window_upper_right, 
                                     text="Data status",
                                     padding=10, relief="solid")
        data_status.grid(row=0, column=0, sticky="new", padx=0, pady=0,)
        
        plotting = ttk.LabelFrame(window_upper_right, 
                                     text="Plot settings",
                                     padding=10, relief="solid")
        plotting.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        
        
        # Bottom
        window_bottom.rowconfigure(0, weight=1)
        window_bottom.columnconfigure(0, weight=1)
        
        app_exit = ttk.Frame(window_bottom, padding=10, relief="solid")
        app_exit.grid(row=0, column=0, sticky="nsew", padx=0, pady=0)
        
        
        #### Add buttons and widgets
        
        #### file_browser
        file_browser.columnconfigure(0,weight=2)
        file_browser.columnconfigure(1,weight=2)
        file_browser.columnconfigure(2,weight=2)
        file_browser.columnconfigure(3,weight=2)
        file_browser.rowconfigure(0,weight=1)
        
        # Display chosen file
        file_lbl = ttk.Label(file_browser, 
                              textvariable=self.file_label, width=20)
        file_lbl.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        
        # Browse button, select data
        browse_btn = ttk.Button(file_browser, 
                               text="Browse File", 
                               command=self.browse_file)
        browse_btn.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        
        # Dropdown menu for coinc selection
        
        self.dropdown = ttk.Combobox(file_browser, textvariable=self.selected_key,
                                state="readonly")
        self.dropdown.grid(row=0, column=2, sticky="ew", padx=5, pady=5)
        
        self.dropdown.bind("<<ComboboxSelected>>", self.on_select)

        # Selected key display
        tk.Label(file_browser,
                 textvariable=self.data_loaded_label).grid(row=1,
                                                      column=0, columnspan=3,
                                                      sticky="nsew", padx=5, pady=5)
        load_data_btn = ttk.Button(file_browser, text="Load data",
                                  command=self.load_data)
        load_data_btn.grid(row=0, column=3, sticky="ew", padx=5, pady=5)
        
        
        #### Postprocessing
        postprocessing.columnconfigure(0, weight=1)
        postprocessing.columnconfigure(1, weight=1)
        postprocessing.rowconfigure(0, weight=1)
        
        overlap_frame = ttk.LabelFrame(postprocessing, 
                                       text="Overlap bunches (manual)",
                                       padding=10, relief="solid")
        overlap_frame.grid(column=1, row=0, padx=5, pady=5, sticky="nes")
        
        self.setup_overlap_manual(overlap_frame)
        
        calibration_frame = ttk.LabelFrame(postprocessing,
                                           text="Calibration",
                                           padding=10, relief="solid")
        calibration_frame.grid(column=0, row=0, padx=5, pady=5, sticky="nsew")
        
        self.setup_calibration_controls(calibration_frame)
        
        
        #### Masking window
        masking.columnconfigure([0,1], weight=1)
        masking.rowconfigure(0, weight=3)
        masking.rowconfigure(1, weight=1)
        
        # ---- Masking by column
        masking_column = ttk.LabelFrame(masking, text="Masking by column",
                                        padding=5, relief="solid")
        masking_column.grid(column=0, row=0, sticky="nsew", padx=5,pady=5)
        
        masking_column.columnconfigure(0, weight=1)
        masking_column.rowconfigure(0, weight=1)
        
        filter_area = ttk.Frame(masking_column)
        filter_area.grid(column=0, row=0, sticky="nsew", padx=5, pady=5)

        canvas = tk.Canvas(filter_area, height=150)
        scrollbar = tk.Scrollbar(filter_area, orient="vertical", 
                                 command=canvas.yview)

        self.scrollable_frame = tk.Frame(canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        add_btn = ttk.Button(masking_column, text="Add Filter", command=self.add_column_filter)
        add_btn.grid(column=0, row=1, sticky="nsew", padx=5, pady=5)
        
        # ---- Advanced masking
        masking_advanced= ttk.LabelFrame(masking, text="Advanced masking",
                                         padding=5, relief="solid")
        masking_advanced.grid(column=1, row=0, sticky="nsew", padx=5,pady=5)
        
        adv_masking_btn = ttk.Button(masking_advanced, text="Advanced options",
                                     command=self.open_advanced_masking)
        adv_masking_btn.pack(side="left", padx=5, pady=5)
        
        # ---- Apply, Reset, Disable all buttons
        
        masking_btns = ttk.Frame(masking, padding=10)
        masking_btns.grid(row=1, column=0, columnspan=2, sticky="nsew", 
                          padx=5, pady=5)
        
        ttk.Button(masking_btns, text="Apply filters", 
                  command=self.apply_filters).pack(side="left", pady=5)
        
        ttk.Button(masking_btns, text="Remove filters", 
                  command=self.remove_filters).pack(side="left", pady=5)
        
        # Exit button
        ttk.Button(app_exit, text="Exit", command=self.close_app).pack()
        
        #### Status panel
        data_status.rowconfigure(0, weight=1)
        data_status.columnconfigure(0, weight=1)
        # self.status_panel = StatusPanel(data_status).grid(
        #     row=0, column=0,
        #     sticky="nsew",
        #     padx=5, pady=5)
        
        ttk.Label(data_status, text="File:").grid(row=0, column=0, sticky="w")
        ttk.Label(data_status, textvariable=self.file_label).grid(
            row=0, column=1, sticky="e")
        
        ttk.Label(data_status, text="Coincidence:").grid(row=1, column=0, sticky="w")
        ttk.Label(data_status, textvariable=self.selected_key).grid(
            row=1, column=1, sticky="e")
        
        ttk.Label(data_status, text="Postprocessing:").grid(row=2, column=0, 
                                                            columnspan=2,
                                                            sticky="w")
        
        ttk.Label(data_status, text="bunch overlap: ").grid(row=3, column=0, sticky="w")
        ttk.Label(data_status, textvariable=self.bunch_overlap_status).grid(
            row=3, column=1, sticky="e")
        
        ttk.Label(data_status, text="Calibrated: ").grid(row=4, column=0, sticky="w")
        ttk.Label(data_status, textvariable=str(self.calibration_status.get())).grid(
            row=4, column=1, sticky="e")
        
        # ttk.Label(data_status, text="Masks:").grid(row=5, column=0, 
        #                                                     columnspan=2,
        #                                                     sticky="w")
        # starting_row=6
        # i = 0
        # for col, mask in self.column_masks:
        #     ttk.Label(data_status, 
        #               text=self.status_from_column_mask(col, mask)).grid(
        #                   row=starting_row+i, column=0, sticky="e")
            # i+=1
        
        # ---- Plotting window
        plotting.rowconfigure(0, weight=1)
        plotting.rowconfigure(1, weight=1)
        plotting.columnconfigure(0, weight=1)
        
        # Divide the plotting window into a Frame for 1D histograms and 2D
        # coincidence maps
        
        plot_1d_frame = ttk.LabelFrame(plotting, 
                                       text="1D Spectra", relief="solid")
        plot_1d_frame.grid(row=0, column=0, sticky="nsew")
        
        plot_2d_frame = ttk.LabelFrame(plotting, 
                                       text="2D coincidence map", relief="solid")
        plot_2d_frame.grid(row=1, column=0, sticky="nsew")
        
        # Design for the 1D part
        self.setup_plot_1d_controls(plot_1d_frame)
        
        # Design for the 2D part
        self.setup_plot_2d_controls(plot_2d_frame)
        
        self.update_plot_columns()
        
    def setup_plot_1d_controls(self, plot_1d_frame):
        '''
         Design for the 1D part of the plot window

        '''
        plot_1d_frame.rowconfigure([0,1,2], weight=1)
        plot_1d_frame.columnconfigure([0,1,2,3], weight=1)
        
        
        ttk.Label(plot_1d_frame, text="Column:").grid(row=0, column=0, sticky="new")
        ttk.Label(plot_1d_frame, text="range:").grid(row=0, column=1, 
                                                     columnspan=2, sticky="new")
        ttk.Label(plot_1d_frame, text="bins:").grid(row=0, column=3, sticky="new")
        
        self.column_cb_plot_1d = ttk.Combobox(plot_1d_frame, width=2)
        range_lo = ttk.Entry(plot_1d_frame, width=5)
        range_hi = ttk.Entry(plot_1d_frame, width=5)
        bin_nr = ttk.Entry(plot_1d_frame, width=5)
            
        self.column_cb_plot_1d.grid(row=1, column=0, sticky="new", padx=5, pady=0)
        range_lo.grid(row=1, column=1, sticky="new", padx=5, pady=0)
        range_hi.grid(row=1, column=2, sticky="new", padx=5, pady=0)
        bin_nr.grid(row=1, column=3, sticky="new", padx=5, pady=0)
        ttk.Button(plot_1d_frame, text="Plot",
                   command=self.plot_1d_spec).grid(row=2, column=2, 
                                                   columnspan=2, 
                                                   sticky="new", padx=5, pady=5)
        ttk.Button(plot_1d_frame, text="Advanced settings",
                   command=self.open_window_advanced_plot_1d).grid(
                       row=2, column=0, 
                       columnspan=2, 
                       sticky="new", padx=5, pady=5)
                
        self.plot_settings_1D = {
            "column" : self.column_cb_plot_1d,
            "range_lo" : range_lo,
            "range_hi" : range_hi,
            "bin_nr" : bin_nr,
            }
        
        
    def setup_plot_2d_controls(self, plot_2d_frame):
        plot_2d_frame.rowconfigure([0,1,2,3], weight=1)
        plot_2d_frame.columnconfigure([0,1,2,3], weight=1)
        
        ttk.Label(plot_2d_frame, text="Columns:").grid(row=0, column=0, sticky="new")
        ttk.Label(plot_2d_frame, text="ranges:").grid(row=0, column=1, 
                                                     columnspan=2, sticky="new")
        ttk.Label(plot_2d_frame, text="bins:").grid(row=0, column=3, sticky="new")
        
        self.column_cb_plot_2d_col1 = ttk.Combobox(plot_2d_frame, width=2)
        range_lo_col1 = ttk.Entry(plot_2d_frame, width=5)
        range_hi_col1 = ttk.Entry(plot_2d_frame, width=5)
        bin_nr_col1 = ttk.Entry(plot_2d_frame, width=5)
            
        self.column_cb_plot_2d_col1.grid(row=1, column=0, sticky="new", padx=5, pady=0)
        range_lo_col1.grid(row=1, column=1, sticky="new", padx=5, pady=0)
        range_hi_col1.grid(row=1, column=2, sticky="new", padx=5, pady=0)
        bin_nr_col1.grid(row=1, column=3, sticky="new", padx=5, pady=0)
        
        self.column_cb_plot_2d_col2 = ttk.Combobox(plot_2d_frame, width=2)
        range_lo_col2 = ttk.Entry(plot_2d_frame, width=5)
        range_hi_col2 = ttk.Entry(plot_2d_frame, width=5)
        bin_nr_col2 = ttk.Entry(plot_2d_frame, width=5)
            
        self.column_cb_plot_2d_col2.grid(row=2, column=0, sticky="new", padx=5, pady=0)
        range_lo_col2.grid(row=2, column=1, sticky="new", padx=5, pady=0)
        range_hi_col2.grid(row=2, column=2, sticky="new", padx=5, pady=0)
        bin_nr_col2.grid(row=2, column=3, sticky="new", padx=5, pady=0)
        
        
        
        
        ttk.Button(plot_2d_frame, text="Advanced settings",
                   command=self.open_window_advanced_plot_1d).grid(
                       row=3, column=0, 
                       columnspan=2, 
                       sticky="ew", padx=5, pady=5)
        ttk.Button(plot_2d_frame, text="Plot",
                   command=self.plot_2d_map).grid(row=3, column=2, 
                                                   columnspan=2, 
                                                   sticky="ew", padx=5, pady=5)
                
        self.plot_settings_2D = {
            "column_1" : self.column_cb_plot_2d_col1,
            "range_lo_1" : range_lo_col1,
            "range_hi_1" : range_hi_col1,
            "bin_nr_1" : bin_nr_col1,
            "column_2" : self.column_cb_plot_2d_col2,
            "range_lo_2" : range_lo_col2,
            "range_hi_2" : range_hi_col2,
            "bin_nr_2" : bin_nr_col2,
            }
        
    def setup_calibration_controls(self, frame):
        
        frame.rowconfigure([0,1,2], weight=1)
        frame.columnconfigure([0,1,2,3], weight=1)
        
        # Add label
        ttk.Label(frame, text="Select calibration").grid(
            row=0, column=0, columnspan=4, sticky="w", padx=5, pady=5)
        
        dropdown_calibrations = ttk.Combobox(frame, width=50)
        dropdown_calibrations.grid(column=0, row=1, columnspan=4, 
                                   sticky="ew", padx=5, pady=5)
        dropdown_calibrations['values'] = [file.name for file in list_calibrations()]
        
        ttk.Button(frame, text="Apply", command=self.apply_calibration).grid(
            row=2, column=0, sticky="ew", padx=5, pady=5)
        
        ttk.Button(frame, text="Open", command=self.open_calibration).grid(
            row=2, column=1, sticky="ew", padx=5, pady=5)
        
        ttk.Button(frame, text="New", command=self.new_calibration).grid(
            row=2, column=2, sticky="ew", padx=5, pady=5)
        
        ttk.Button(frame, text="Remove", command=self.remove_calibration).grid(
            row=2, column=3, sticky="ew", padx=5, pady=5)
        
        self.calibration_choice = dropdown_calibrations
        
        
        
    def setup_overlap_manual(self, frame):
        frame.rowconfigure([0,1,2,3], weight=1)
        frame.columnconfigure([0,1,2], weight=1)
        
        # Add labels
        ttk.Label(frame, text="Repetition time (ns)").grid(
            row=0, column=0, sticky="w", padx=5, pady=5)
        
        ttk.Label(frame, text="ROI_first (ns)").grid(
            row=1, column=0, sticky="w", padx=5, pady=5)
        
        ttk.Label(frame, text="ROI_last (ns)").grid(
            row=2, column=0, sticky="w", padx=5, pady=5)
        
        
        # Add Entry boxes for the repetition time and the ROIs needed for 
        # creating overlap
        rep_time = ttk.Entry(frame, width=6)
        rep_time.grid(row=0, column=1, sticky="ew",
                                           padx=5, pady=5)
        
        roi_first_1 = ttk.Entry(frame, width=6)
        roi_first_1.grid(row=1, column=1, sticky="ew",
                                           padx=5, pady=5)
        
        roi_first_2 = ttk.Entry(frame, width=6)
        roi_first_2.grid(row=1, column=2, sticky="ew",
                                           padx=5, pady=5)
        
        roi_last_1 = ttk.Entry(frame, width=6)
        roi_last_1.grid(row=2, column=1, sticky="ew",
                                           padx=5, pady=5)
        
        roi_last_2 = ttk.Entry(frame, width=6)
        roi_last_2.grid(row=2, column=2, sticky="ew",
                                           padx=5, pady=5)
        
        # Add buttons for applying/disabling the overlap
        
        ttk.Button(frame, text="Apply overlap", command=self.overlap_bunches).grid(
            row=3, column=0, sticky="w", padx=5, pady=5)
        ttk.Button(frame, text="Reset overlap", command=self.overlap_bunches_reset).grid(
            row=3, column=1, columnspan=2, sticky="w", padx=5, pady=5)
        
        self.bunch_overlap_parameters = {
            "repetition_time" : rep_time,
            "ROI_first_1" : roi_first_1,
            "ROI_first_2" : roi_first_2,
            "ROI_last_1" : roi_last_1,
            "ROI_last_2" : roi_last_2,
            }
        
    
        
    def setup_calibration_view(self):
        
        # ---- setup window
        calibration_view_wdw = tk.Toplevel(self.root)
        calibration_view_wdw.geometry("1200x600")
        calibration_view_wdw.rowconfigure(0, weight=1)
        calibration_view_wdw.columnconfigure(0, weight=1, minsize=300)
        calibration_view_wdw.columnconfigure(1, weight=2, minsize=600)
        
        info_wdw = ttk.Frame(calibration_view_wdw, padding=10, relief ="solid")
        info_wdw.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        info_wdw.columnconfigure(0, weight=1)
        info_wdw.rowconfigure(0, weight=1, minsize=100)
        
        plot_wdw = ttk.Frame(calibration_view_wdw, padding=10, relief ="solid")
        plot_wdw.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")
        
        # ---- info window - info
        data = self.calibration_info.calibration_dict
        info = {k: v for k, v in data.items() if k != "calibration_points"}
        points= data["calibration_points"]
        
        info_frame = ttk.Frame(info_wdw, padding=10, relief="solid")
        info_frame.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        
        
        text = tk.Text(info_frame, width=50, height=15)
        text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        import json
        formatted = json.dumps(info, indent=4)
        text.insert("1.0", formatted)
        
        text.config(state="disabled")
        
        # ---- info window - points
        
        points_frame = ttk.Frame(info_wdw, padding=10, relief="solid",
                                 width=40)
        points_frame.grid(row=1, column=0, padx=5, pady=5, sticky="nsew")
        
        columns = ("x", "x_err", "y", "y_err")
        
        tree = ttk.Treeview(
                points_frame,
                columns=columns,
                show="headings"
            )
        column_width=4
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
        
        
        # ---- plot window
        fig, ax = self.calibration_info.plot(nr_of_bins=2000)
        
        canvas = FigureCanvasTkAgg(fig, master=plot_wdw)
        canvas.draw()
        canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        toolbar = NavigationToolbar2Tk(canvas, plot_wdw)
        toolbar.update()
        
        
    def setup_calibration_params_window(self, param_wdw):
        # ---- entries
        entry_frame = ttk.Frame(param_wdw, padding=5, relief="solid")
        entry_frame.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        entry_frame.columnconfigure([0,1], weight=1)
        
        
        author_lbl = ttk.Label(entry_frame, text="author")
        author_lbl.grid(row=0, column=0, sticky="w", pady=10)
        
        experiment_lbl = ttk.Label(entry_frame, text="experiment\n(Beamline_yyyymm)")
        experiment_lbl.grid(row=1, column=0, sticky="w", pady=10)
        
        setting_lbl = ttk.Label(entry_frame, text="setting\n(e.g. ret4: -4V\nor acc10: +10V) ")
        setting_lbl.grid(row=2, column=0, sticky="w", pady=10)
        
        index_lbl = ttk.Label(entry_frame, text="index \n(In case of doublings)")
        index_lbl.grid(row=3, column=0, sticky="w", pady=10)
        
        version_lbl = ttk.Label(entry_frame, text="version (integer)")
        version_lbl.grid(row=4, column=0, sticky="w", pady=10)
        
        
        author_entry = ttk.Entry(entry_frame, width=20)
        author_entry.grid(row=0, column=1, sticky="e", pady=10)
        
        experiment_entry = ttk.Entry(entry_frame, width=20)
        experiment_entry.grid(row=1, column=1, sticky="e", pady=10)
        
        setting_entry = ttk.Entry(entry_frame, width=20)
        setting_entry.grid(row=2, column=1, sticky="e", pady=10)
        
        index_entry = ttk.Entry(entry_frame, width=20)
        index_entry.grid(row=3, column=1, sticky="e", pady=10)
        
        version_entry = ttk.Entry(entry_frame, width=20)
        version_entry.grid(row=4, column=1, sticky="e", pady=10)
        
        
        self.edit_calibration_parameters = {
            "author": author_entry,
            "experiment": experiment_entry,
            "setting": setting_entry,
            "index": index_entry,
            "version": version_entry,
            }
        
        # ---- buttons
        button_frame = ttk.Frame(param_wdw, padding=10, relief="solid")
        button_frame.grid(row=1, column=0, padx=5, pady=5, sticky="nsew")
        
        new_btn = ttk.Button(button_frame, text="New", 
                                 command=self.edit_calibration)
        new_btn.pack(side="right", padx=5, pady=5)
        
        
        
    # ---- Button functions
    
    
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

        keys= self.load_keys_from_file()
        
        # Populate dropdown menu:
        if self.dropdown:
            self.dropdown["values"] = keys
            self.dropdown.set("")
    
    def load_data(self):
        '''
        
        Read the data of the selected file, from the selected coincidence
        key into self.data
        Also store it in self.current_data and self.data_postproc
        
        '''
        arrays = []
        key = self.selected_key.get()
        
        for path in self.file_path:
            arr = read_coinc(path, key)
            arrays.append(arr)
        self.data = np.concatenate(arrays, axis=0)
        
        self.current_data = self.data
        self.data_postproc = self.data
        print(f"Data {self.selected_key.get()} has been loaded.")
        self.data_loaded_label.set(f"Loaded {self.selected_key.get()} data from {self.file_label.get()}.")
        
        self.update_plot_columns()
    
    def apply_calibration(self):
        if self.calibration_choice.get() is not None:
            self.calibration_info = Calibration(load_calibration(
                filename=self.calibration_choice.get()))
            if self.calibration_info.bunch_overlap_params is not None:
                self.populate_bunch_overlap_parameters_from_calibration(self.calibration_info)
                self.overlap_bunches()
                self.current_data = self.calibration_info.convert(self.current_data)
                self.calibrated.set(True)
                self.calibration_status.set(self.calibration_choice.get())
    
    def open_calibration(self):
        if self.calibration_choice.get() is not None:
            self.calibration_info = Calibration(load_calibration(
                filename=self.calibration_choice.get()))
            # self.calibration_info.plot()
            self.setup_calibration_view()
            
        
        #     #TODO
        #     # Create new window
        #     info_window = tk.Toplevel(root)
        #     info_window.title("Calibration info")
        #     info_window.geometry("800x400")
            
        #     info_window.rowconfigure(0, weight=1)
        #     info_window.rowconfigure(1, weight=8)
        #     info_window.columnconfigure([0,1], weight=1)
            
        #     # Header: Choose calibration from dropdown
        #     header = ttk.Frame(info_window, padding=10, relief="solid")
        #     header.grid(row=0, column=0, columnspan=2, sticky="nsew", padx=0, pady=0)
            
        #     ttk.Combobox(header, width=50, 
        #                  values=[file.name for file in list_calibrations()]).pack(
        #                      side="left")
        pass
    
    def new_calibration(self):
    
        # ---- window
        param_wdw = tk.Toplevel(self.root)
        param_wdw.geometry("300x400")
        param_wdw.title("New calibration")
        
        param_wdw.rowconfigure(0, weight=6)
        param_wdw.rowconfigure(1, weight=1)
        param_wdw.columnconfigure(0, weight=1)
    
        self.setup_calibration_params_window(param_wdw)
        
    def edit_calibration(self):
        parameters = self.edit_calibration_parameters
        parameters = {k: v.get() for k,v in parameters.items()}
        manager = calman.CalibrationManager.from_dict(parameters)
        self.calview = calman.CalibrationView(self.root, manager)
        
        
        
    
    def remove_calibration(self):
        self.calibration_info = None
        self.current_data = self.data_postproc
        self.calibrated.set(False)
        self.bunch_overlap_parameters = {}
    
    
    def overlap_bunches(self):
        
        # Get the overlap parameters from self.bunch_overlap_parameters.
        # Check, whether these are regular floats (from Calibration file) or 
        # tkinter objects. Calibration is preferred.
        try:
            rep_time = float(self.bunch_overlap_parameters['repetition_time'])
        except:
            rep_time=self.safe_get_float(
                self.bunch_overlap_parameters['repetition_time'], 
                default=20000)
        try: 
            roi_first_1 = float(self.bunch_overlap_parameters['ROI_first_1'])
        except:
            roi_first_1 = self.safe_get_float(
                self.bunch_overlap_parameters['ROI_first_1'], 
                default=0)
        try: 
            roi_first_2 = float(self.bunch_overlap_parameters['ROI_first_2'])
        except:
            roi_first_2 = self.safe_get_float(
                self.bunch_overlap_parameters['ROI_first_2'], 
                default=0)
        try: 
            roi_last_1 = float(self.bunch_overlap_parameters['ROI_last_1'])
        except:
            roi_last_1 = self.safe_get_float(
                self.bunch_overlap_parameters['ROI_last_1'], 
                default=0)
        try: 
            roi_last_2 = float(self.bunch_overlap_parameters['ROI_last_2'])
        except:
            roi_last_2 = self.safe_get_float(
                self.bunch_overlap_parameters['ROI_last_2'], 
                default=0)
        
        roi_first = (roi_first_1, roi_first_2)
        roi_last = (roi_last_1, roi_last_2)
        e_amount, p_amount = self.EP_number_from_string(self.selected_key.get())
        # print("The number of E and P in the current coincidence set is: "+
        #       f"{e_amount} and {p_amount}.")
        self.data_postproc = overlap(self.data, rep_time, 
                                     roi_first, roi_last, nPhotons=p_amount)
        self.current_data = self.data_postproc
        # print("Hi, I overlap the bunches according to the set parameters, as soon as I am implemented")
        self.bunch_overlap_status.set("True")
        
        
    def overlap_bunches_reset(self):
        print("Hi, I change the current_data back to self.data")
        self.bunch_overlap_status.set("False")
        self.current_data = self.data
        self.data_postproc = self.data
    
    def add_column_filter(self):
        '''
        Add a frame to the scrollable frame, which is a row that holds information
        on the masks that should be applied.

        '''
        row_frame = tk.Frame(self.scrollable_frame, bd=1, relief="solid",
                             padx=5, pady=5)
        row_frame.pack(fill="x", pady=2)
        
        enabled = tk.BooleanVar(value=True)
        
        available_columns=[]
        if self.current_data is not None:
            for i in range(self.current_data.shape[1]):
                available_columns.append(str(int(i+1)))
        else:
            available_columns=[str(i+1) for i in range(10)]
        
        
        column= ttk.Combobox(row_frame, values=available_columns, width=2)
        mask_lo = ttk.Entry(row_frame, width=8)
        mask_hi = ttk.Entry(row_frame, width=8)
        
        def toggle():
            state = "normal" if enabled.get() else "disabled"
            column.configure(state=state)
            mask_lo.configure(state=state)
            mask_hi.configure(state=state)
            
            
        ttk.Checkbutton(row_frame, variable=enabled, 
                       command=toggle).pack(side="left")
        column.pack(side="left", padx=5, anchor="w")
        mask_lo.pack(side="left", padx=5)
        mask_hi.pack(side="left", padx=5)
        
        row_data = {
            "enabled" : enabled,
            "column" : column,
            "mask_lo" : mask_lo,
            "mask_hi" : mask_hi,
            "frame" : row_frame,
            }
        
        self.filter_rows_column.append(row_data)
        
        
        
        
        def remove():
            '''
            Remove the frame of the masking row

            '''
            row_frame.destroy()
            self.filter_rows_column.remove(row_data)
        
        ttk.Button(row_frame, text="Remove", command=remove).pack(side="right")
    
    def apply_filters(self):
        '''
        Apply the masks, which are currently chosen. This takes the 
        self.data_postproc data and apply the corresponding masks. The returned 
        data is then stored in self.current_data.

        '''
        self.apply_column_filters()
        
        data = self.current_data
        for col, mask in self.column_masks:
            data = mask_by_column(data, col, mask)
            
        self.current_data = data
        print("Filters are being applied.")
    
    
    def remove_filters(self):
        '''
        Removes all current filters and makes the self.current_data the 
        self.data_postproc, so the data after preprocessing.

        '''
        self.current_data = self.data_postproc
        self.column_masks = []
        self.column_masks_status = []
        print("Filters are being removed")
    
    def plot_2d_map(self):
        
        col_idx1 = int(self.plot_settings_2D['column_1'].get())-1
        col_idx2 = int(self.plot_settings_2D['column_2'].get())-1
        
        bins_1 = self.safe_get_int(self.plot_settings_2D['bin_nr_1'], default=100)
        range_lo_1 = self.safe_get_float(self.plot_settings_2D['range_lo_1'], default=0)
        range_hi_1 = self.safe_get_float(self.plot_settings_2D['range_hi_1'], default=200)
        range_1 = (range_lo_1, range_hi_1)
        
        bins_2 = self.safe_get_int(self.plot_settings_2D['bin_nr_2'], default=100)
        range_lo_2 = self.safe_get_float(self.plot_settings_2D['range_lo_2'], default=0)
        range_hi_2 = self.safe_get_float(self.plot_settings_2D['range_hi_2'], default=200)
        range_2 = (range_lo_2, range_hi_2)
        hist_kwargs=None
        
        coinc, xedges, yedges = bin_2D(self.current_data, (col_idx1, col_idx2), 
               range=(range_1, range_2), 
                bins=(bins_1, bins_2),
                hist_kwargs=hist_kwargs)
        
        plot_kwargs={}
        
        if self.calibrated.get() == True:
            xlabel=f"Particle {col_idx1+1} kinetic energy (eV)"
            ylabel=f"Particle {col_idx2+1} kinetic energy (eV)"
        else:
            xlabel=f"Particle {col_idx1+1} TOF (ns)"
            ylabel=f"Particle {col_idx2+1} TOF (ns)"
        
        plot_2D(coinc, xedges, yedges, xlabel=xlabel, ylabel=ylabel,
                **plot_kwargs)
        
    def plot_1d_spec(self):
        '''
        Take the information from self.plot_settings_1d and plot a 1D histogram
        using plot_functions.plot_1D

        '''
        
        column = int(self.plot_settings_1D['column'].get())-1
        bins = self.safe_get_int(self.plot_settings_1D['bin_nr'], default=100)
        range_lo = self.safe_get_float(self.plot_settings_1D['range_lo'], default=0)
        range_hi = self.safe_get_float(self.plot_settings_1D['range_hi'], default=200)
        
        #TODO
        ## Here, we need to add the extraction for the other values in the
        ## self.plot_settings_1d dictionary, which can be added in the 
        ## advanced options later
        
        hist_kwargs={}
        plot_kwargs={}
        
        plot_1D(self.current_data, column, range=(range_lo, range_hi), 
                bins=bins,
                hist_kwargs=hist_kwargs,
                plot_kwargs=plot_kwargs)
    
    def open_window_advanced_plot_1d(self):
        print("Hi, I open the advanced plotting option, when I am implemented")
        
    def open_advanced_masking(self):
        print("Hi, I open the advanced masking option, when I am implemented")
    
    
    
    
    
    
    def close_app(self):
        self.root.quit()
        self.root.destroy()
     
    @staticmethod
    def close(window):
        window.quit()
        window.destroy()
     
     
     
     
     
    # ---- Helper functions
    
    @staticmethod
    def safe_get_int(entry, default=1):
        """Safely convert Entry.get() to int, return default if empty/invalid"""
        value = entry.get().strip()
        if not value:
            return default
        try:
            return int(float(value))  # Allows "100.0" → 100
        except ValueError:
            return default
    @staticmethod
    def safe_get_float(entry, default=None):
        """Safely convert Entry.get() to float, return default if empty/invalid"""
        value = entry.get().strip()
        if not value:
            return default
        try:
            return float(value)
        except ValueError:
            return default
    @staticmethod
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

    def populate_bunch_overlap_parameters_from_calibration(self, calib):
        
        params = calib.bunch_overlap_params
        
        self.bunch_overlap_parameters['repetition_time'] = params["repetition_time"]
        self.bunch_overlap_parameters['ROI_first_1'] = params["ROI_first"][0]
        self.bunch_overlap_parameters['ROI_first_2'] = params["ROI_first"][1]
        self.bunch_overlap_parameters['ROI_last_1'] = params["ROI_last"][0]
        self.bunch_overlap_parameters['ROI_last_2'] = params["ROI_last"][1]

        
    def apply_column_filters(self):
        '''
        From the entries in the enabled column filter settings, extract the 
        values and transform them from string to useful values.
        Store the resulting masks in self.column_masks.

        '''
        filters = []
        
        for row in self.filter_rows_column:
            if not row["enabled"].get():
                continue
            
            col=row["column"].get()
            mask_lo = row["mask_lo"].get()
            mask_hi = row["mask_hi"].get()
    
            try:
                col = int(col)-1 # I added 1, so that the numbering starts with 1
                                # Here, I need to substract 1.
            except ValueError:
                pass
            
            try:
                mask_lo = float(mask_lo)
            except ValueError:
                pass
            
            try:
                mask_hi = float(mask_hi)
            except ValueError:
                pass
            
            filters.append((col, (mask_lo, mask_hi)))
        self.column_masks = filters
    
    
    
        
    def status_from_column_mask(col, mask):
        '''
        Gives a tk.Stringvar object from a given mask.

        Input
        -------
        col : integer
        Column number (starting with 0, here transferred to 1) of applied filter
        mask : 2-tuple
        Contains the limits for masking in the first and second entry.

        Returns
        -------
        status : tkinter.StringVar object

        '''
        status = tk.StringVar(f"Column: {col+1} masked to [{mask[0]}, {mask[1]}]")
        return status
        

    def load_keys_from_file(self):
        '''
        
        Get the available keys from the h5 file
        
        '''
        list_of_key_lists = []
        for file in self.file_path:
            list_of_key_lists.append(get_keys(file))
        
        key_list = []
        for lst in list_of_key_lists:
            key_list += lst
            
        key_set = list(dict.fromkeys(key_list))
        
        return key_set

    def on_select(self, event):
        '''
        
        Select the coincidence key from the dropdown menu and save it in
        self.selected_key
        
        '''
        key = self.dropdown.get()
        if key:
            self.selected_key.set(key)
            
    
    def update_plot_columns(self):
        '''
        Updates the number of available columns, that are shown in the 
        1d plot window
        
        '''
        if self.current_data is None:
            cols=list(str(i+1) for i in range(10))
        else:
            cols=list(str(i+1) for i in range(self.current_data.shape[1]))
        self.column_cb_plot_1d['values'] = cols
        self.column_cb_plot_2d_col1['values'] = cols
        self.column_cb_plot_2d_col2['values'] = cols
        

class StatusPanel(ttk.Frame):
    def __init__(self, master):
        super().__init__(master, relief="solid", padding=10)
        self.vars = {
            "status": tk.StringVar(value="Ready"),
            "file": tk.StringVar(value="-"),
            "processed": tk.StringVar(value="False"),
            "calibrated": tk.StringVar(value="False")
        }
        self.columnconfigure([0,1], weight=1)
        ttk.Label(self, text="Status:").grid(row=0, column=0, sticky="w")
        ttk.Label(self, textvariable=self.vars["status"]).grid(
            row=0, column=1, sticky="e", padx=5, pady=5)
        
        ttk.Label(self, text="File:").grid(row=1, column=0, sticky="w")
        ttk.Label(self, textvariable=self.vars["file"]).grid(
            row=1, column=1, sticky="e", padx=5, pady=5)
        
        ttk.Label(self, text="Processed:").grid(row=2, column=0, sticky="w")
        ttk.Label(self, textvariable=self.vars["processed"]).grid(
            row=2, column=1, sticky="e", padx=5, pady=5)
    
    def update(self, **kwargs):
        for key, value in kwargs.items():
            if key in self.vars:
                self.vars[key].set(str(value))


# --- Entry point ---
if __name__ == "__main__":
    root = tk.Tk()
    app = DataApp(root)
    app.root.mainloop()