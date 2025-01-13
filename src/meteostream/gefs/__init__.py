import os
from .extractor import extract_grib_fields
from .manager import move_old_gribs
from .concat import concat_gribs, convert_to_netcdf

# Default paths (can be overridden)
BASE_DIR = os.path.expanduser("~/data")
INPUT_DIR = os.path.join(BASE_DIR, "grib")
OUTPUT_DIR = os.path.join(BASE_DIR, "selected_grib")
TMP_DIR = os.path.join(BASE_DIR, "tmp")

# Ensure directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

# Default parameters for GRIB extraction
DEFAULT_GRIB_INDICES = [5, 6, 7, 8, 38, 39, 40]

def process_grib_pipeline(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR, tmp_dir=TMP_DIR, indices=None):
    """
    Full pipeline to extract fields, move old files, and merge GRIBs.

    Args:
        input_dir (str): Path to original GRIB files.
        output_dir (str): Path to store extracted GRIB files.
        tmp_dir (str): Path to store old GRIB files.
        indices (list[int]): List of GRIB indices to extract. Uses defaults if None.
    """
    indices = indices or DEFAULT_GRIB_INDICES

    print("Starting GRIB processing pipeline...")
    extract_grib_fields(input_dir, output_dir, indices)
    move_old_gribs(input_dir, tmp_dir)
    concat_gribs(output_dir, "merged_output.grib2")
    convert_to_netcdf(output_dir, "merged_output.nc")
    print("GRIB processing pipeline completed!")

# Define what should be available when the package is imported
__all__ = [
    "extract_grib_fields",
    "move_old_gribs",
    "concat_gribs",
    "convert_to_netcdf",
    "process_grib_pipeline",
    "INPUT_DIR",
    "OUTPUT_DIR",
    "TMP_DIR",
    "DEFAULT_GRIB_INDICES"
]
