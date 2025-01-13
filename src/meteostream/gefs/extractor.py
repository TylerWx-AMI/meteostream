import pygrib
import os
from typing import List

def extract_grib_fields(input_dir: str, output_dir: str, indices: List[int]) -> None:
    """
    Extracts specified fields from GRIB files and saves them as new GRIB files.

    Args:
        input_dir (str): Path to input GRIB files.
        output_dir (str): Path to store extracted GRIB files.
        indices (List[int]): List of GRIB message indices to extract.
    """
    os.makedirs(output_dir, exist_ok=True)

    for filename in sorted(os.listdir(input_dir)):
        if filename.endswith(".grib2"):
            input_filepath = os.path.join(input_dir, filename)
            print(f"Processing {input_filepath}...")

            try:
                with pygrib.open(input_filepath) as grbs:
                    for target_index in indices:
                        try:
                            grb = grbs.message(target_index)
                            param_name = grb.parameterName  # Optional

                            output_filename = f"selected_{target_index}_{filename}"
                            output_filepath = os.path.join(output_dir, output_filename)

                            with open(output_filepath, "wb") as output_file:
                                output_file.write(grb.tostring())

                            print(f"Saved field {target_index} ({param_name}) → {output_filepath}")
                        except Exception as e:
                            print(f"Failed to process index {target_index} in {filename}: {e}")
            except Exception as e:
                print(f"Error opening {filename}: {e}")
