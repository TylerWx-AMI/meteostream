from ftplib import FTP
from datetime import datetime, timedelta, timezone

import os
import sys
import pygrib
import xarray as xr

# FTP server details
BASE_URL = "ftp.ncep.noaa.gov"
BASE_DIR = "/pub/data/nccf/com/gens/prod/gefs"
CHILD_DIR = "grib"
OUTPUT_DIR = 'selected_grib'

def download():
    # Ensure the output directory exists
    os.makedirs(CHILD_DIR, exist_ok=True)

    # Calculate the most recent run time (00z, 06z, 12z, 18z)
    current_hour = datetime.now(timezone.utc).hour
    if current_hour < 4:
        RUN_TIME = "18"
    elif current_hour < 10:
        RUN_TIME = "00"
    elif current_hour < 16:
        RUN_TIME = "06"
    else:
        RUN_TIME = "12"

    # Determine the date for the selected run
    if RUN_TIME == "18" and current_hour < 4:
        run_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
    else:
        run_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    # Target forecast hours (e.g., every 6 hours from 000 to 240 or 10 days)
    FORECAST_HOURS = [f"{fh:03d}" for fh in range(0, 241, 6)]

    # Connect to the FTP server
    ftp = FTP(BASE_URL)
    ftp.login()  # Anonymous login

    # Loop through forecast hours and download files
    for fh in FORECAST_HOURS:
        file_name = f"gefs.wave.t{RUN_TIME}z.prob.global.0p25.f{fh}.grib2"
        target_dir = f"{BASE_DIR}.{run_date}/{RUN_TIME}/wave/gridded"
        target_path = f"{target_dir}/{file_name}"

        try:
            # Change to the target directory
            ftp.cwd(target_dir)
            
            # Local file path
            local_file_path = os.path.join(CHILD_DIR, file_name)
            
            # Open a local file for writing
            with open(local_file_path, "wb") as f:
                print(f"Downloading {file_name}...")
                ftp.retrbinary(f"RETR {file_name}", f.write)
                print(f"Downloaded {file_name} to {CHILD_DIR}/")
        except Exception as e:
            print(f"Failed to download {file_name}: {e}")

    # Close the FTP connection
    ftp.quit()

def extract():
    # Directory containing GRIB files
    input_dir = CHILD_DIR
    output_dir = OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)  # Ensure output directory exists

    # Index list for target parameters
    idx_list = [5, 6, 7, 8, 38, 39, 40]

    # Loop through all GRIB files in the directory
    for filename in sorted(os.listdir(input_dir)):
        if filename.endswith('.grib2'):
            input_filepath = os.path.join(input_dir, filename)
            print(f'Processing {input_filepath}...')

    
        with pygrib.open(input_filepath) as grbs:
            for target_index in idx_list:
                try:
                    # Extract the target parameter
                    grb = grbs.message(target_index)
                    param_name = grb.parameterName  # Optional: Extract parameter name for the file

                    # Create a unique output filename for each parameter
                    output_filename = f'selected_{target_index}_{filename}'
                    output_filepath = os.path.join(output_dir, output_filename)

                    # Write the extracted parameter to a new GRIB file
                    with open(output_filepath, 'wb') as output_file:
                        output_file.write(grb.tostring())

                    print(f'Saved field {target_index} ({param_name}) to {output_filepath}')
                except Exception as e:
                    print(f'Failed to process index {target_index} in {filename}: {e}')
                    sys.exit(1)

        # Remove the input file after processing all indices
        os.remove(input_filepath)
        print(f'Removed processed file: {input_filepath}')

def aggregate(input_dir, output_file = "gefs_prob.nc"):
    '''
    This function aggregates the selected GRIB files into a single NetCDF file.
    '''
     # Ensure output_file has a directory path
    if os.path.dirname(output_file):
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Define the index-to-variable name mapping
    index_to_varname = {
        5: 'comb_height_4m',
        6: 'comb_height_5_5m',
        7: 'comb_height_7m',
        8: 'comb_height_9m',
        38: 'ws_bf8',
        39: 'ws_bf9',
        40: 'ws_bf10',
    }

    # Collect datasets for each variable
    datasets = []

    # Loop through index list and load files for each index
    for index, varname in index_to_varname.items():
        file_list = sorted(
            [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f'selected_{index}_' in f]
        )

        # Open GRIB files for this index as a single dataset
        if file_list:
            ds = xr.open_mfdataset(file_list, engine='cfgrib', combine='nested', concat_dim='valid_time', backend_kwargs={'indexpath': None})

            # Determine the original variable name
            original_varname = 'swh' if index < 38 else 'ws'

            # Rename the variable to the desired name
            ds = ds.rename({original_varname: varname})

            # Drop unnecessary variables and rename dimensions
            ds = ds.drop_vars('step').rename({'time': 'ref_time'})

            # Keep only the relevant variable and coordinates
            ds = ds[[varname]]

            # Append to datasets list
            datasets.append(ds)

    # Combine all datasets along the `valid_time` dimension
    combined_ds = xr.concat(datasets, dim='valid_time')

    # Save to NetCDF
    combined_ds.to_netcdf(output_file)
    print(f'Aggregated data saved to {output_file}')

    # Close datasets
    combined_ds.close()

def main(): 
    download()
    extract()
    aggregate(OUTPUT_DIR)

if __name__ == '__main__':
    main()