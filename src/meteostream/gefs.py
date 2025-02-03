"""
Module containing methods to access and 
download GEFS.wave probability data 
"""

import requests
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pygrib
import xarray as xr
import pandas as pd

# Forecast hours
FORECAST_HOURS = [f"{i:03d}" for i in range(0, 241, 6)]

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
            else:
                print(f"Failed to download {target} (status code: {response.status_code})")
        
        self.check_download = True

    def extract_fields(self,
            remove_input: bool = True,
            open_dataset: bool = False
            ) -> None | xr.Dataset:


        # Loop through all GRIB files in the directory
        for filepath in sorted(self.grib_dir):
            if filepath.endswith(".grib2"):
                print(f'Processing {filepath}...')

                with pygrib.open(str(filepath)) as grbs:
                    for target_index in self.idx_list:
                        # Extract the target parameter
                        grb = grbs.message(target_index)
                        param_name = grb.parameterName  # Optional: Extract parameter name for the file

                        # Create a unique output filename for each parameter
                        output_filename = f'selected_{target_index}_{filepath}'
                        output_filepath = self.grib_dir / output_filename

                        if remove_input:
                            output_filepath.unlink()
                            
                        print(f'Processed field {target_index}({param_name})')


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
    
    def run_gefs(self,
                 remove_input: bool = True
                 ):

        if not self.check_download:
            self.download_gefs_prob_data()

        # Loop through all GRIB files in the directory
        for filename in sorted(os.listdir(self.grib_dir)):
            if filename.endswith('.grib2'):
                input_filepath = os.path.join(self.grib_dir, filename)
                print(f'Processing {input_filepath}...')

                try:
                    with pygrib.open(input_filepath) as grbs:
                        for target_index in self.idx_list:
                            try:
                                # Extract the target parameter
                                grb = grbs.message(target_index)
                                param_name = grb.parameterName  # Optional: Extract parameter name for the file

                                # Create a unique output filename for each parameter
                                output_filename = f'selected_{target_index}_{filename}'
                                output_filepath = os.path.join(self.grib_dir, output_filename)

                                # Write the extracted parameter to a new GRIB file
                                with open(output_filepath, 'wb') as output_file:
                                    output_file.write(grb.tostring())

                                print(f'Saved field {target_index} ({param_name}) to {output_filepath}')

                                if remove_input:
                                    output_filepath.unlink()

                            except Exception as e:
                                print(f'Failed to process index {target_index} in {filename}: {e}')

                    # Remove the input file after processing all indices
                    os.remove(input_filepath)
                    print(f'Removed processed file: {input_filepath}')

                except Exception as e:
                    print(f'Failed to process file {input_filepath}: {e}')

        print('All files processed successfully!')



    def gefs_metadata(self)->pd.DataFrame:
        """
        Return the pandas dataframe containing the GEFS.prob parameters 
        """

        # Dynamically resolve the path to the metadata file
        current_dir = Path(__file__).parent  # Directory of the current script
        metadata_path = current_dir / "docs" / "gefs_idxinfo.txt"

        df = pd.read_csv(metadata_path,  delimiter=':', header=None)

        # Assign column names based on the structure of your data
        df.columns = ["Index_ID", "Parameter", "Unit", "Threshold"]
        
        df.set_index("Index_ID", inplace=True)

        return df