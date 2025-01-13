import os

# Base directory for GRIB processing
BASE_DIR = os.path.expanduser("~/Ops-Scripts/data")

# Paths for GRIB processing
INPUT_DIR = os.path.join(BASE_DIR, "grib")
OUTPUT_DIR = os.path.join(BASE_DIR, "selected_grib")
TMP_DIR = os.path.join(BASE_DIR, "tmp")

# Ensure directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
