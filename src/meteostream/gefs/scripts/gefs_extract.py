
import pygrib
import os
import sys
import xarray as xr

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

    # Directory containing GRIB files
    input_dir = 'grib'
    output_dir = 'selected_grib'
    os.makedirs(output_dir, exist_ok=True)  # Ensure output directory exists

    # Index list for target parameters
    idx_list = [5, 6, 7, 8, 38, 39, 40]

    # Loop through all GRIB files in the directory
    for filename in sorted(os.listdir(input_dir)):
        if filename.endswith('.grib2'):
            input_filepath = os.path.join(input_dir, filename)
            print(f'Processing {input_filepath}...')

            try:
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

            except Exception as e:
                print(f'Failed to process file {input_filepath}: {e}')
                sys.exit(1)

    aggregate(output_dir)

    print('All files processed successfully!')
    sys.exit(0)

if __name__ == '__main__':
    main()

