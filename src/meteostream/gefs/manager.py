import os
import shutil

def move_old_gribs(input_dir: str, tmp_dir: str) -> None:
    """
    Moves processed GRIB files to a temporary directory for storage.

    Args:
        input_dir (str): Path to original GRIB files.
        tmp_dir (str): Path to store old GRIB files.
    """
    os.makedirs(tmp_dir, exist_ok=True)

    for filename in os.listdir(input_dir):
        if filename.endswith(".grib2"):
            src = os.path.join(input_dir, filename)
            dst = os.path.join(tmp_dir, filename)

            shutil.move(src, dst)
            print(f"Moved {filename} → {tmp_dir}")

