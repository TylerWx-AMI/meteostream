import os
import shutil
import pygrib
import xarray as xr
from typing import List


class GefsClient:
    """
    A class to handle GEFS probabistic data with GRIB file extraction, organization, and conversion.
    """

    def __init__(self, base_dir: str = "~/data", indices: List[int] = None):
        """
        Initialize paths and parameters.

        Args:
            base_dir (str): Base directory for input/output storage.
            indices (List[int]): List of GRIB indices to extract.
        """
        # Expand user home directory
        self.base_dir = os.path.expanduser(base_dir)

        # Ensure base_dir exists
        os.makedirs(self.base_dir, exist_ok=True)

        # Define subdirectories
        self.input_dir = os.path.join(self.base_dir, "grib")
        self.output_dir = os.path.join(self.base_dir, "selected_grib")
        self.tmp_dir = os.path.join(self.base_dir, "tmp")

        self.indices = indices or [5, 6, 7, 8, 38, 39, 40]  # Default fields

        # Ensure subdirectories exist
        os.makedirs(self.input_dir, exist_ok=True)  # Fix: Ensure input_dir exists
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)

    def _extract_grib_fields(self):
        """Extracts specified fields from GRIB files."""
        for filename in sorted(os.listdir(self.input_dir)):
            if filename.endswith(".grib2"):
                input_filepath = os.path.join(self.input_dir, filename)
                print(f"Processing {input_filepath}...")

                try:
                    with pygrib.open(input_filepath) as grbs:
                        for target_index in self.indices:
                            try:
                                grb = grbs.message(target_index)
                                param_name = grb.parameterName

                                output_filename = f"selected_{target_index}_{filename}"
                                output_filepath = os.path.join(self.output_dir, output_filename)

                                with open(output_filepath, "wb") as output_file:
                                    output_file.write(grb.tostring())

                                print(f"Saved field {target_index} ({param_name}) → {output_filepath}")
                            except Exception as e:
                                print(f"Failed to process index {target_index} in {filename}: {e}")
                except Exception as e:
                    print(f"Error opening {filename}: {e}")

    def _move_old_gribs(self):
        """Moves processed GRIB files to a temporary storage directory."""
        for filename in os.listdir(self.input_dir):
            if filename.endswith(".grib2"):
                src = os.path.join(self.input_dir, filename)
                dst = os.path.join(self.tmp_dir, filename)
                shutil.move(src, dst)
                print(f"Moved {filename} → {self.tmp_dir}")

    def _concat_gribs(self, merged_filename="merged_output.grib2"):
        """Merges all extracted GRIB files into a single GRIB file."""
        merged_filepath = os.path.join(self.output_dir, merged_filename)

        with open(merged_filepath, "wb") as merged_file:
            for filename in sorted(os.listdir(self.output_dir)):
                if filename.endswith(".grib2"):
                    with open(os.path.join(self.output_dir, filename), "rb") as grib_part:
                        merged_file.write(grib_part.read())

        print(f"Merged all GRIB files into {merged_filepath}")

    def _convert_to_netcdf(self, netcdf_filename="merged_output.nc"):
        """Converts extracted GRIB files to a single NetCDF file."""
        datasets = []

        for filename in sorted(os.listdir(self.output_dir)):
            if filename.endswith(".grib2"):
                grib_path = os.path.join(self.output_dir, filename)
                ds = xr.open_dataset(grib_path, engine="cfgrib")
                datasets.append(ds)

        merged_ds = xr.concat(datasets, dim="time")
        netcdf_filepath = os.path.join(self.output_dir, netcdf_filename)
        merged_ds.to_netcdf(netcdf_filepath)

        print(f"Converted GRIBs to NetCDF: {netcdf_filepath}")

    def process_pipeline(self):
        """Runs the full pipeline: extract, move, merge, convert."""
        print("Starting GRIB processing pipeline...")
        self._extract_grib_fields()
        self._move_old_gribs()
        self._concat_gribs()
        self._convert_to_netcdf()
        print(" GRIB processing pipeline completed!")


