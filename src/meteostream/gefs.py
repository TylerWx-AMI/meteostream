"""
Module containing methods to access and 
download GEFS.wave probability data 
"""

import requests
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
if 1 <= current_hour < 4:
    run_time = "18"
elif 4 <= current_hour < 10:
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


    def download_gefs_prob_data(self):
        """
        Download the gribs to the grib_dir
        """
        # Ingest the dir
        output_dir = Path(self.grib_dir)

        # Create the directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)

        # Check for server status
        for target in target_urls:
            response = requests.get(target)
            if response.status_code == 200:
                file_name = target.split("/")[-1]
                file_path = output_dir / file_name
                with open(file_path, "wb") as file:
                    file.write(response.content)
                    print(f"Downloaded: {file_path}")
            else:
                print(f"Failed to download {target} (status code: {response.status_code})")

    def extract_fields_to_grib(self,
            output_dir: str, 
            remove_input: bool = True
            ) -> None:

        output_dir = Path(output_dir)

        # Create the directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)

        # Loop through all GRIB files in the directory
        for filepath in sorted(self.grib_dir.glob("*.grib2")):
            print(f'Processing {filepath}...')

            # Open the GRIB file
            with pygrib.open(str(filepath)) as grbs:
                for target_index in self.idx_list:
                    # Extract the target parameter
                    grb = grbs.message(target_index)
                    param_name = grb.parameterName  # Optional: Extract parameter name for the file

                    # Create a unique output filename for each parameter
                    output_filename = f'selected_{target_index}_gefs'
                    output_filepath = output_dir / output_filename

                    # Write the extracted parameter to a new GRIB file
                    with output_filepath.open('wb') as output_file:
                        output_file.write(grb.tostring())

                    with xr.open_dataset(output_filepath,
                                         engine="cfgrib",
                                         backend_kwargs={"indexpath": None}) as ds:
                        ds.to_netcdf(f"gefs_{target_index}_{param_name}.nc")
                    
                    if remove_input:
                        output_filepath.unlink()
                        
                    print(f'Saved field {target_index} ({param_name}) to nectdf')

            # Remove the input file after processing all indices
            
            filepath.unlink()
            print(f'Removed processed file: {filepath}')

    def gefs_metadata()->pd.DataFrame:
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

    def run_gefs_pipeline(
            self,
            save_file: bool = True,
            open_dataset: bool = False
            ) -> None | xr.Dataset :

        # Get the dir and file objects
        files = list(Path(self.grib_dir).glob("*.grib2"))

        if not files:
            raise FileNotFoundError(f"No .grib2 files found in {self.grib_dir}")

        with xr.open_mfdataset(
                [str(file) for file in files],
                combine="nested",
                concat_dim="time",
                engine="cfgrib",
                backend_kwargs={"indexpath": None}) as ds:

            # Save the dataset to NetCDF if needed
            if save_file:
                ds.to_netcdf(self.output_filepath)

            # Return the dataset if open_dataset is True
            if open_dataset:
                return ds
        
        return None