"""
Module containing methods to access and 
download GEFS.wave probability data 
"""

import requests
import os
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

import numpy as np
import pygrib
import xarray as xr
import pandas as pd


from .function_tools import monitor_resources

# Forecast hours
FORECAST_HOURS = [f"{i:03d}" for i in range(0, 243, 3)]

# Set datetime logic
current_time = datetime.now(tz=timezone.utc)
current_hour = current_time.hour

# Determine the most recent run time based on the current hour
if 1 <= current_hour < 7:
    run_time = "18"
elif 7 <= current_hour < 9:
    run_time = "00"
elif 10 <= current_hour < 16:
    run_time = "06"
else:
    run_time = "12"

# Determine the date for the selected run
if run_time == "18":
    run_date = (current_time - timedelta(days=1)).strftime("%Y%m%d")

elif run_time == "12" and current_hour < 1:
    run_date = (current_time - timedelta(days=1)).strftime("%Y%m%d")

else:
    run_date = current_time.strftime("%Y%m%d")

# Define URL paths based on datetime logic
base_url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/gefs.{run_date}/{run_time}/wave/gridded/"
file_urls = [f"gefs.wave.t{run_time}z.prob.global.0p25.f{hour}.grib2" for hour in FORECAST_HOURS]

# Combine base + file_urls 
target_urls = [f"{base_url}{url}" for url in file_urls]

class GefsClient():
    """
    Python Class to interact with the GEFS probabIlity data via HTTPS

    https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/
    
    """ 
    def __init__(self, 
                 grib_dir:str, 
                 idx_list: list = [5, 6, 7, 8, 38, 39, 40],
                 output_filepath: str ="gefs_main.nc"):
        """
        Initialize the object

        Params:
        -------
        grib_dir: str
            directory to store the GRIBS 
            (recommend an empty dir. for processing)
        
        idx_list: list[int]
            list of index values to parse the .grib
            paramters

        output_filepath: str
            path to store resulting nc file
        """

        self.grib_dir = Path(grib_dir)
        
        self.idx_list = idx_list
        self.metadata = self.gefs_metadata()

        self.output_filepath = output_filepath

        # Add an attribute to check if downloaded
        self.check_download = False

        # Filter the init index for the var_name
        self.filtered_df = self.metadata.loc[self.idx_list]
        self.sel_columns = self.filtered_df[['var_name', 'grb_shortName']]

        # Convert to a dictionary with the idx_ID as keys and (var_name, grb_shortName) as values
        self.var_dict =self.sel_columns.to_dict('index')

    def download_gefs_prob_data(self):
        """
        Download the gribs to the grib_dir
        """
    
        # Check for server status
        for target in target_urls:
            response = requests.get(target)
            if response.status_code == 200:
                file_name = target.split("/")[-1]
                file_path = self.grib_dir / file_name
                with open(file_path, "wb") as file:
                    file.write(response.content)
                    print(f"Downloaded: {file_path}")
                    time.sleep(0.5)
            else:
                print(f"Failed to download {target} (status code: {response.status_code})")
        
        self.check_download = True

    # def process_gefs(self):
    #     """
    #     Method to extract the index (parameters) from the native gefs
    #     files and write to netcdf format for further processing
    #     """

    #     if not self.check_download:
    #         self.download_gefs_prob_data()

    #     # Loop through all GRIB files in the directory
    #     for filename in sorted(os.listdir(self.grib_dir)):
    #         if filename.startswith(f"gefs.wave"):
    #             tau = filename[-9:-6] # Slice for the tau string       
    #             input_filepath = os.path.join(self.grib_dir, filename)
    #             print(f'Processing {input_filepath}...')
                
    #             # Use pygrib to extract data
    #             with pygrib.open(input_filepath) as grbs:
    #                 for target_index in self.idx_list:
    #                         # Extract the target parameter
    #                         grb = grbs.message(target_index)
    #                         print(f"Target idx: {target_index}, Name:{grb.shortName}")

    #                         # Find matching variable in self.var_dict
    #                         if target_index in self.var_dict:
    #                             values = self.var_dict[target_index]
    #                             var_name = values['var_name']
    #                             grb_name = values['grb_shortName']
    #                             data = grb.values
    #                             lats, lons = grb.latlons()
    #                             valid_time = grb.validDate   # Valid time as datetime object
    #                             ref_time = grb.analDate      # Reference (analysis) time as datetime object

    #                             # Check if the extracted parameter matches the expected shortName
    #                             if grb.shortName == grb_name:
    #                                 output_filename = (
    #                                     f"GEFS_{var_name}_{filename.split('.')[0]}_idx{target_index}_f{tau}.nc"
    #                                 )
    #                                 output_filepath = os.path.join(self.grib_dir, output_filename)

    #                                 # Create and write to a new NetCDF file
    #                                 # Need to pivot away from pygrib when passing
    #                                 # to xarray as this may create memory issues 
    #                                 # and segmentation faults
    #                                 with Dataset(output_filepath, mode='w', format='NETCDF4') as ncfile:
    #                                     # Define dimensions
    #                                     ncfile.createDimension('latitude', lats.shape[0])
    #                                     ncfile.createDimension('longitude', lons.shape[1])
    #                                     ncfile.createDimension('valid_time', None)  # Unlimited time dimension for valid_time and ref_time
    #                                     ncfile.createDimension('ref_time', None)


    #                                     # Define coordinate variables
    #                                     latitudes = ncfile.createVariable('latitude', np.float32, ('latitude',))
    #                                     longitudes = ncfile.createVariable('longitude', np.float32, ('longitude',))
    #                                     valid_times = ncfile.createVariable('valid_time', 'f8', ('valid_time',))
    #                                     ref_times = ncfile.createVariable('ref_time', 'f8',('ref_time',))

    #                                     # Define the data variable
    #                                     data_var = ncfile.createVariable(var_name, np.float32, ('valid_time', 'latitude', 'longitude'))

    #                                     # Write coordinate values
    #                                     latitudes[:] = lats[:, 0]        # Latitude values
    #                                     longitudes[:] = lons[0, :]       # Longitude values

    #                                     # Use CF-compliant time units and calendar
    #                                     time_units = "hours since 1970-01-01 00:00:00"
    #                                     valid_times[0] = date2num(valid_time, units=time_units, calendar="standard")
    #                                     ref_times[0] = date2num(ref_time, units=time_units, calendar="standard")

    #                                     # Add units and calendar attributes for compatibility
    #                                     valid_times.units = time_units
    #                                     valid_times.calendar = "standard"
    #                                     ref_times.units = time_units
    #                                     ref_times.calendar = "standard"


    #                                     # Write the data values
    #                                     data_var[0, :, :] = data

    #                                     # Add metadata
    #                                     data_var.units = grb.units
    #                                     data_var.long_name = grb.name
    #                                     data_var.standard_name = grb.shortName

    #                                     print(f"Saved to {output_filepath}")

           
    #     print('All files processed successfully!')

    def process_gefs(self):
        """
        Method to extract the index (parameters) from the native gefs
        files and write to netcdf format for further processing
        """

        if not self.check_download:
            self.download_gefs_prob_data()

        # Loop through all GRIB files in the directory
        for filename in sorted(os.listdir(self.grib_dir)):
            if filename.startswith(f"gefs.wave"):
                tau = filename[-9:-6] # Slice for the tau string       
                input_filepath = os.path.join(self.grib_dir, filename)
                print(f'Processing {input_filepath}...')
                
                # Use pygrib to extract data
                with pygrib.open(input_filepath) as grbs:
                    for target_index in self.idx_list:
                            # Extract the target parameter
                            grb = grbs.message(target_index)
                            print(f"Target idx: {target_index}, Name:{grb.shortName}")

                            # Find matching variable in self.var_dict
                            if target_index in self.var_dict:
                                values = self.var_dict[target_index]
                                var_name = values['var_name']
                                grb_name = values['grb_shortName']
                                data = grb.values
                                data_3d = np.expand_dims(data, axis=0)
                                lats, lons = grb.latlons()
                                valid_time = grb.validDate   # Valid time as datetime object
                                ref_time = grb.analDate      # Reference (analysis) time as datetime object
                                df = self.metadata.loc[target_index]

                                # Check if the extracted parameter matches the expected shortName
                                if grb.shortName == grb_name:
                                    output_filename = (
                                        f"GEFS_{var_name}_{filename.split('.')[0]}_idx{target_index}_f{tau}.nc"
                                    )
                                    output_filepath = os.path.join(self.grib_dir, output_filename)

                                    # Create and write to a new NetCDF file
                                    # Need to pivot away from pygrib when passing
                                    # to xarray as this may create memory issues 
                                    # and segmentation faults
                                    # # Create a DataArray with time as an additional dimension
                                    data_array = xr.DataArray(
                                        data_3d,  
                                        coords={
                                            "latitude": (["latitude", "longitude"], lats),
                                            "longitude": (["latitude", "longitude"], lons),
                                            "valid_time": valid_time,                                            
                                            "ref_time": ref_time
                                        },
                                        dims=["valid_time", "latitude", "longitude"],
                                        name = var_name,
                                        attrs={
                                            "grb_shortName" : grb_name,
                                            "grb_Param_Index": target_index,
                                            "Description": df['parameter'],
                                            "units": df['unit'],
                                            "Limit": df['threshold'],
                                        }
                                    )
                                    data_array.to_netcdf(output_filepath)
                                    print(f"Saved to {output_filepath}")

        
        print('All files processed successfully!')

    def _merge_data(self) -> xr.Dataset:
        """
        Helper function to pass the grib files to xarray 
        for concatenation and merging

        Returns
        -------
        xr.Dataset
            The xarray dataset
        """


        # Init the datasets
        datasets = []

        # Get a list of all nc files in the GRIB dir
        data_files = sorted(os.path.join(self.grib_dir, f) for f in os.listdir(self.grib_dir) if f.endswith(".nc"))

        # Open all GRIB2 files for this variable and create a dataset
        with xr.open_mfdataset(
            data_files,
            combine='nested',
            concat_dim='valid_time',
            chunks='auto'
        ) as ds:
            # Append the dataset to the list
            datasets.append(ds)

        # Concatenate all datasets along the `valid_time` dimension
        return xr.concat(datasets, dim='valid_time')

    @monitor_resources    
    def run_GEFS_pipeline(self,
                          save_file: bool = True, 
                          return_ds: bool = False
                          ) -> None | xr.Dataset:

        print("Starting GEFS download")
        self.download_gefs_prob_data()

        print("Preprocessing files")
        self.process_gefs()

        print("Merging results")
        ds = self._merge_data()

        if save_file:
            if save_file.endswith(".nc"):
                ds.to_netcdf(self.output_filepath)
            else:
                ds.to_zarr(self.output_filepath, mode='w')

        if return_ds:
            return ds 
        

    def gefs_metadata(self)->pd.DataFrame:
        """
        Return the pandas dataframe containing the GEFS.prob parameters 
        """

        # Dynamically resolve the path to the metadata file
        current_dir = Path(__file__).parent  # Directory of the current script
        metadata_path = current_dir / "docs" / "gefs_idxinfo.txt"

        df = pd.read_csv(metadata_path,  delimiter=':')
        
        df.set_index("idx_ID", inplace=True)

        return df