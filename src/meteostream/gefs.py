"""
Module containing methods to access and 
download GEFS.wave probability data 
"""

import requests
import os
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pygrib
import xarray as xr
import pandas as pd

# Forecast hours
FORECAST_HOURS = [f"{i:03d}" for i in range(0, 243, 3)]

# Set the HTTPS agent in case of 403 error code. 
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36'
}

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
                    time.sleep(1.5)
            else:
                print(f"Failed to download {target} (status code: {response.status_code})")
        
        self.check_download = True

    # def extract_fields(self,
    #         remove_input: bool = True,
    #         open_dataset: bool = False
    #         ) -> None | xr.Dataset:


    #     # Loop through all GRIB files in the directory
    #     for filepath in sorted(self.grib_dir):
    #         if filepath.endswith(".grib2"):
    #             print(f'Processing {filepath}...')

    #             with pygrib.open(str(filepath)) as grbs:
    #                 for target_index in self.idx_list:
    #                     # Extract the target parameter
    #                     grb = grbs.message(target_index)
    #                     param_name = grb.parameterName  # Optional: Extract parameter name for the file

    #                     # Create a unique output filename for each parameter
    #                     output_filename = f'selected_{target_index}_{filepath}'
    #                     output_filepath = self.grib_dir / output_filename

    #                     if remove_input:
    #                         output_filepath.unlink()
                            
    #                     print(f'Processed field {target_index}({param_name})')


            # # Open the GRIB file
            # with pygrib.open(str(filepath)) as grbs:
            #     for target_index in self.idx_list:
            #         # Extract the target parameter
            #         grb = grbs.message(target_index)
            #         param_name = grb.parameterName  # Optional: Extract parameter name for the file

            #         # Create a unique output filename for each parameter
            #         output_filename = f'selected_{target_index}_{filepath}.grib2'
            #         output_filepath = self.grib_dir / output_filename

            #         # Write the extracted parameter to a new GRIB file
            #         with output_filepath.open('wb') as output_file:
            #             output_file.write(grb.tostring())

            #         if remove_input:
            #             output_filename.unlink()
                        
            #         print(f'Processed field {target_index}({param_name})')

            # # Remove the input file after processing all indices
            
            # filepath.unlink()
            # print(f'Removed processed file: {filepath}')
    
    def process_gefs(self,
                 remove_input: bool = True
                 ):

        if not self.check_download:
            self.download_gefs_prob_data()

        # Loop through all GRIB files in the directory
        for filename in sorted(os.listdir(self.grib_dir)):
            for tau in FORECAST_HOURS:
                if filename.endswith(f"f{tau}.grib2"):
                    input_filepath = os.path.join(self.grib_dir, filename)
                    print(f'Processing {input_filepath}...')

                    try:
                        with pygrib.open(input_filepath) as grbs:
                            for target_index in self.idx_list:
                                try:
                                    # Extract the target parameter
                                    grb = grbs.message(target_index)
                                    print(f"Target idx: {target_index}, Name:{grb.shortName}")

                                    # Find matching variable in self.var_dict
                                    if target_index in self.var_dict:
                                        values = self.var_dict[target_index]
                                        var_name = values['var_name']
                                        grb_name = values['grb_shortName']

                                        # Check if the extracted parameter matches the expected shortName
                                        if grb.shortName == grb_name:
                                            output_filename = (
                                                f"GEFS_{var_name}_{filename.split('.')[0]}_idx{target_index}_f{tau}.grib2"
                                            )
                                            output_filepath = os.path.join(self.grib_dir, output_filename)

                                            # Write to a separate file for each matched parameter
                                            with open(output_filepath, 'wb') as output_file:
                                                output_file.write(grb.tostring())

                                            print(f"Saved to {output_filepath}")

                                except Exception as e:
                                    print(f"Error processing index {target_index} in file {filename}: {e}")

                    except Exception as e:
                        print(f"Error opening file {filename}: {e}")

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

        for idx, values in self.var_dict.items():
                var_name, grb_shortName = values['var_name'], values['grb_shortName']

                # Get a list of all GRIB2 files
                grib_files = sorted(os.path.join(self.grib_dir, f) for f in os.listdir(self.grib_dir) if f.startswith(f"GEFS_{var_name}"))
                for file in grib_files:    
                    print(file )
                    print(f"Processing {grb_shortName} in {file}")
                    ds = xr.open_dataset(file, engine='cfgrib', 
                                            backend_kwargs = {"indexpath": None})
                    
                    # Rename the varibale for xarray to 
                    # merge and concat effectively
                    ds = ds.rename({grb_shortName:var_name})
                    # Drop unnecessary variables and rename dimensions
                    print(f"Swapping vars: {grb_shortName} with {var_name}")
                    ds = ds.drop_vars('step').rename({'time': 'ref_time'})
                    datasets.append(ds)

                    print(f"Removing {file}")
                    os.remove(file)

        # Open and concatenate all GRIB2 files
        # Combine all datasets along the `valid_time` dimension
        combined_ds = xr.concat(datasets, dim='valid_time')

        return combined_ds 


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
            ds.to_netcdf(self.output_filepath)

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