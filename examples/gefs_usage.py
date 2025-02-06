"""
Example python script to run the GEFS Pipeline from NOAA
NOMADAS HTTPS requests at 
https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/
"""
# Import the package
from meteostream import GefsClient

# We will use the default index params 
# and default 10-day forecast
# Variable selection and forecast range can be 
# edited on init of the client (read docs)

# Create a instance of the GefsClient
# Pass a directory to download and process grib2 files
client = GefsClient(grib_dir="/home/vrsops/Ops-Scripts/data/grib")

# Execute the pipeline method
# Flexibilty added to not save to file 
# set save_file = None
# clear the gribs (default is False)
# Or return the dataset object (set return_ds = True)
client.run_GEFS_pipeline(save_file="/home/vrsops/Ops-Scripts/data/netcdf/GEFS_prob.nc",
                         clear_gribs=True
                         )
