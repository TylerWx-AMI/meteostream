import os
import pygrib
import xarray as xr

def concat_gribs(output_dir: str, merged_filename: str) -> None:
    """
    Merges all selected GRIB files into one GRIB file.

    Args:
        output_dir (str): Directory containing selected GRIBs.
        merged_filename (str): Output file for merged GRIB.
    """
    merged_filepath = os.path.join(output_dir, merged_filename)

    with open(merged_filepath, "wb") as merged_file:
        for filename in sorted(os.listdir(output_dir)):
            if filename.endswith(".grib2"):
                with open(os.path.join(output_dir, filename), "rb") as grib_part:
                    merged_file.write(grib_part.read())

    print(f"Merged all GRIB files into {merged_filepath}")

def convert_to_netcdf(output_dir: str, netcdf_filename: str) -> None:
    """
    Converts extracted GRIB files to a single NetCDF file.

    Args:
        output_dir (str): Directory containing extracted GRIB files.
        netcdf_filename (str): Output NetCDF file.
    """
    datasets = []

    for filename in sorted(os.listdir(output_dir)):
        if filename.endswith(".grib2"):
            grib_path = os.path.join(output_dir, filename)
            ds = xr.open_dataset(grib_path, engine="cfgrib")
            datasets.append(ds)

    merged_ds = xr.concat(datasets, dim="time")
    merged_ds.to_netcdf(os.path.join(output_dir, netcdf_filename))

    print(f"Converted GRIBs to NetCDF: {netcdf_filename}")

