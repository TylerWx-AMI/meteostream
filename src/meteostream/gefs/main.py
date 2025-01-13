from .extractor import extract_grib_fields
from .manager import move_old_gribs
from .concat import concat_gribs, convert_to_netcdf
from .config import INPUT_DIR, OUTPUT_DIR, TMP_DIR

def process_grib_pipeline():
    """
    Full pipeline to extract fields, move old files, and merge GRIBs.
    """
    print("Starting GRIB processing pipeline...")

    # Step 1: Extract selected fields
    extract_grib_fields(INPUT_DIR, OUTPUT_DIR, indices=[5, 6, 7, 8, 38, 39, 40])

    # Step 2: Move old GRIB files to temporary storage
    move_old_gribs(INPUT_DIR, TMP_DIR)

    # Step 3: Merge GRIB files
    concat_gribs(OUTPUT_DIR, "merged_output.grib2")

    # Step 4: Convert to NetCDF
    convert_to_netcdf(OUTPUT_DIR, "merged_output.nc")

    print("GRIB processing pipeline completed!")

if __name__ == "__main__":
    process_grib_pipeline()
