"""
Module containing methods to access and 
download GEFS.wave probability data 
"""
# Standard Imports
import requests
import os
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional

# Third-Party Imports
import numpy as np
import pygrib
import xarray as xr
import pandas as pd
import dask.array 

# Relative Imports
from .constants import run_date, run_time

# Set constants
# NOAA NOMDAS SERVER FORECAST (TAU) SCHEMA
SEGMENT_ONE = range(0, 243, 3) 
SEGMENT_TWO = range(246, 390, 6)
# A complete list of f{taus} for a given run
SERVER_TAU_LIST = list(SEGMENT_ONE) + list(SEGMENT_TWO) 
 # Paramter IDs
PARAM_INDX = list(range(1, 73, 1))

class GefsClient():
    def __init__(self, 
                 grib_dir:str, 
                 idx_list: list = [7, 8, 9, 38, 40],
                 forecast_hours: tuple =  (0, 243, 3),
                 ):
        
        """
        Python Class to interact with the GEFS probability data via HTTPS

        https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/
        
        Params:
        -------
        grib_dir: str
            directory to store the GRIBS 
            (recommend an empty dir. for processing)
        
        idx_list: list[int]
            list of index values to decode the .grib
            paramters
        
        forecast_hours: tuple(start, end, step)
            The forecast range to select from the
            latest run. 
            Note: 3 hour intervals up to the 240th tau
            then goes to every 6 hours to 384th tau
            (raises error if 243rd tau is set: DNE)
        """

        # Handle kwargs
        # Use a set to efficiently check if any index is not valid
        invalid_indices = [i for i in idx_list if i not in PARAM_INDX]

        if invalid_indices:
            raise IndexError(f"Invalid Parameter Index value(s) passed: {invalid_indices}. Expecting any in {PARAM_INDX}")
        
        self.idx_list = idx_list # Init the idx list

        start, end, step = forecast_hours  # Unpack the tuple
        self.forecast_hours_list = range(start, end, step)
        self.forecast_hours = sorted(f"{i:03d}" for i in self.forecast_hours_list)

        # Check for invalid forecast hours
        invalid_hours = [hour for hour in self.forecast_hours_list if hour not in SERVER_TAU_LIST]

        if invalid_hours:
            raise ValueError(f"Invalid forecast hours: {invalid_hours}. Expecting values in {SERVER_TAU_LIST}")
        
        
        # Get the grib dir
        self.grib_dir = Path(grib_dir)
        
        # Create the metadata df
        self.metadata = self.gefs_metadata()
        # Filter the init index for the var_name
        self.filtered_df = self.metadata.loc[self.idx_list]
        self.sel_columns = self.filtered_df[['var_name', 'grb_shortName']]
        # Convert to a dictionary with the idx_ID as keys and (var_name, grb_shortName) as values
        self.var_dict =self.sel_columns.to_dict('index')

        # Define URL paths based on current datetime logic
        base_url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/gefs.{run_date}/{run_time}/wave/gridded/"
        file_urls = [f"gefs.wave.t{run_time}z.prob.global.0p25.f{hour}.grib2" for hour in self.forecast_hours]

        # Combine base + file_urls 
        self.target_urls = [f"{base_url}{url}" for url in file_urls]

        # Add an attribute to check if downloaded
        self.check_download = False

    def run_GEFS_pipeline(self,
                          save_file: Optional[str] = None, 
                          return_ds: bool = False,
                          clear_gribs: bool = False,
                          ) -> None | xr.Dataset:
        """
        Method to run the main/whole GEFS pipeline
        using class methods 


        """
        if not self.check_download:
            print("Starting GEFS download")
            self.download_gefs_prob_data()

        print("Preprocessing files..this might take some time")
        ds = self.process_gefs()

        if save_file:
            if save_file.endswith(".nc"):
                ds.to_netcdf(save_file)
            else:
                ds.to_zarr(save_file, mode='w')

        if clear_gribs:
            print("Removing dowloaded gribs")
            for file in Path(self.grib_dir).glob("gefs.wave*.grib2"):
                file.unlink()  # Efficient way to remove files
                self.check_download = False # Update download bool (files DNE)

        if return_ds:
            return ds 
        

    def download_gefs_prob_data(self):
        """
        Download the gribs to the grib_dir
        """
    
        # Check for server status
        for target in self.target_urls:
            response = requests.get(target)
            time.sleep(1)
            if response.status_code == 200:
                file_name = target.split("/")[-1]
                file_path = self.grib_dir / file_name
                with open(file_path, "wb") as file:
                    file.write(response.content)
                    print(f"Downloaded: {file_path}")
                    time.sleep(1.5)
            else:
                print(f"Failed to download {target} (status code: {response.status_code})")
        
        self.check_download = True

    def process_gefs(self):
        """
        Method to extract the index (parameters) from the native gefs
        files and write to netcdf format for further processing
        """

        datasets = []

        if not self.check_download:
            self.download_gefs_prob_data()

        # Loop through all GRIB files in the directory
        for filename in sorted(os.listdir(self.grib_dir)):
            if filename.startswith("gefs.wave"):
                data_arrays = []
                tau = filename[-9:-6]  # Slice for the tau string       
                input_filepath = os.path.join(self.grib_dir, filename)
                print(f'Processing {input_filepath}...')
                
                # Use pygrib to extract data
                with pygrib.open(input_filepath) as grbs:
                    for target_index in self.idx_list:
                        grb = grbs.message(target_index)
                        print(f"Target idx: {target_index}, Name: {grb.shortName}, Tau:{tau}")

                        values = self.var_dict[target_index] # Extract the values from the dict
                        var_name = values['var_name'] # Get the var string
                        grb_name = values['grb_shortName'] # Get the grb_var string 
                        data = grb.values # get the np.array data
                        data_3d = np.expand_dims(data, axis=0) # Need to expand it to get the proper shape
                        lats, lons = grb.latlons() # Get the lat,lon data

                        # Extract valid_time and ref_time for this iteration
                        valid_time = grb.validDate
                        ref_time = grb.analDate

                        df = self.metadata.loc[target_index] # Metadata df

                        # Use dask.array for the data
                        dask_data = dask.array.from_array(
                            data_3d,
                            chunks='auto'
                        )

                        if grb.shortName == grb_name:
                            print(f"Creating DataArray for {var_name}")
                            da = xr.DataArray(
                                dask_data,  
                                coords={
                                    "latitude": (["latitude"], lats[:, 0]),
                                    "longitude": (["longitude"], lons[0, :]),
                                    "valid_time": [valid_time],  # Wrap valid_time to define it as a dimension
                                    "ref_time": ref_time,
                                },
                                dims=["valid_time", "latitude", "longitude"],
                                name=var_name,
                                attrs={
                                    "grb_shortName": grb_name,
                                    "grb_Param_Index": target_index,
                                    "Description": df['parameter'],
                                    "units": df['unit'],
                                    "limit": df['threshold'],
                                }
                            )
                            data_arrays.append(da)
                
                # Merge the DataArrays into a Dataset for this file
                merged_da = xr.merge(data_arrays)
                datasets.append(merged_da)

        # Concatenate all datasets along the 'valid_time' dimension
        final_dataset = xr.concat(datasets, dim='valid_time')

        # Edit some metadata 
        del final_dataset.attrs['grb_shortName']
        del final_dataset.attrs['units']
        del final_dataset.attrs['limit']
        
        final_dataset.attrs['Description'] = "GEFS Probability Data from NOAA NOMADS HTTPS"
        final_dataset.attrs['grb_Param_Index'] = [idx for idx in self.idx_list]
        for idx in self.idx_list:
            param, threshold = self.metadata.loc[idx, ['parameter', 'threshold']]
            final_dataset.attrs[f'Parameter_{idx}'] = (param, threshold)

        print('All files processed successfully!')
        return final_dataset

    def gefs_metadata(self)->pd.DataFrame:
        """
        Return the pandas dataframe containing the GEFS.prob parameters 
        """

        # Dynamically resolve the path to the metadata file
        current_dir = Path(__file__).parent  # Directory of the current script
        metadata_path = current_dir / "static" / "gefs_idxinfo.txt"

        df = pd.read_csv(metadata_path,  delimiter=':')
        
        df.set_index("idx_ID", inplace=True)

        return df