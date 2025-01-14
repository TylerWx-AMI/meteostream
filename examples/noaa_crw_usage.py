# Example script of how to use the MeteostreAM package for the
# NOAA CRW sea ice and SST observation 
# https://coralreefwatch.noaa.gov/

from meteostream import get_latest_CRW_data, latlon_point_data, latlon_grid_data
from datetime import datetime

file_path_global="NOAA_CRW.zarr"
file_path="NOAA_CRW.nc"

# We can call get_latest_CRW_data to either lazily load the dataset (xarray)
# Or immediately save to a dataset if passed an output string (uses dask backend)

get_latest_CRW_data(file_path=file_path_global)


# We can specify a lat/lon coordinate pair for a time 
# or range of times (start, end) to return the same as above (xr.ds or save)

latitude = 50
longitude = -150
time = datetime(2025, 1, 12, 12) #y, #m, #d, #h (h: only 12 is available) 

latlon_point_data(
    latitude=latitude, 
    longitude=longitude, 
    time=time,
    file_path=file_path
    )

# or 

time_range = datetime(2025, 1, 11, 12), time

latlon_point_data(
    latitude=latitude, 
    longitude=longitude, 
    time=time_range,
    file_path=file_path
    )

# For a grid of data, we must call the grid function and pass bounds  
# outlining the grid, similar time logic applied

west = -160
east = -150
south = 40
north = 50

latlon_grid_data(
    west = west, 
    east = east, 
    south = south,
    north = north,
    time = time, # or time_range above
    file_path=file_path
)