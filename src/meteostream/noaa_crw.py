# Module to access the Coral Reef Watch (observed) SST and SICE data
import xarray as xr

from siphon.catalog import TDSCatalog
from datetime import datetime
from typing import Union, Tuple, Optional
from dask.diagnostics import ProgressBar 

#Define the URL Constant and Dataset index
CRW_URL: str = "https://pae-paha.pacioos.hawaii.edu/thredds/satellite.xml"
CRW_IDX: int = 0

CAT = TDSCatalog(CRW_URL)
LATEST_DS = CAT.datasets[CRW_IDX]
DATA_VARS = ["CRW_SEAICE", "CRW_SST"]

def get_latest_CRW_data(
        file_path:Optional[str] = None
        )-> Union[xr.Dataset, None]:
    """
    Return an xarray dataset with all variables and global coverage for the latest time 
    available, and save to file (optional). This function uses the OPENDAP access method. 

    Parameters:
    -----------
    file_path: str or None

    Returns:
    --------
    xarray.Dataset:
        The dataset containing the latest data (global) for the latest time
    """

    ds = xr.open_dataset(LATEST_DS.access_urls['OPENDAP'], chunks='auto')
    ds = ds.isel(time=-1)
    ds = ds[DATA_VARS]

    if file_path is not None:
        _save_filepath(ds, file_path)
        return None

    return ds 

def latlon_point_data(
    latitude: Union[float, int], 
    longitude: Union[float, int], 
    time: Union[datetime, Tuple[datetime, datetime]],
    file_path: Optional[str] = None
    ) -> Union[xr.Dataset, None]:
    """
    Return the xarray dataset for a specified lat/lon point for one time or range of times 
    from the Hawaii.edu THREDDS server for CRW data.
  
    Parameters:
    -----------
    Latitude: float
        The latitude value 
    
    Longitude: float 
        The longitude value

    time: datetime or tuple(datetime, datetime)
        The time or tuple of times (start, end)

    Returns:
    --------
    xr.Dataset
        The xarray dataset 
    """
    #Handle lat, lon args to ensure float dtype
    try:
        longitude, latitude = float(longitude), float(latitude)
    except TypeError:
        raise TypeError(f"Invalid longitude or latitude passed, expected float ot int, but detected: {type(longitude).__name__}, {type(latitude).__name__}")
    # Create the default ncss and query objects
   
    #Handle time arg:
    if isinstance(time, datetime):
        try:
            ds = xr.open_dataset(LATEST_DS.access_urls['OPENDAP'])
            ds = ds.sel(time=time, latitude=latitude, longitude=longitude, method='nearest')
            ds = ds[DATA_VARS]

            if file_path is not None:
                _save_filepath(ds, file_path)
                return None
        
        except IndexError:
            raise IndexError("Time out of bounds")

        return ds

    elif isinstance(time, tuple) and len(time) == 2 and all(isinstance(t, datetime) for t in time):
        try:
            start = time[0]
            end = time[1]
            ds = xr.open_dataset(LATEST_DS.access_urls['OPENDAP'])
            ds = ds.sel(time=slice(start, end)) 
            ds = ds.sel(latitude=latitude, longitude=longitude, method='nearest')
            ds = ds[DATA_VARS]

            if file_path is not None:
                _save_filepath(ds, file_path)
                return None
        
        except IndexError:
            raise IndexError("Time out of bounds")
        
        return ds

    else:
        raise TypeError(f"Invalid time arguement for {time}: expected datetime obj, but passed {type(time).__name__}")
    
def latlon_grid_data(
    west: Union[float, int], 
    east: Union[float, int], 
    south: Union[float, int], 
    north: Union[float, int], 
    time: Union[datetime, Tuple[datetime, datetime]],
    file_path: Optional[str] = None
    ) -> Union[xr.Dataset, None]:
    """
    Return the xarray dataset for a specified lat/lon grid (bounds) for one time or range of times
    from the Hawaii.edu THREDDS server for CRW data. 

    Parameters:
    -----------
    west: float
        The western latitude bounds
    
    east: float
        The eastern latitude bounds
    
    south: float
        The southern latitude bounds
    
    north: float
        The northern latitude bounds

    time: datetime or tuple(datetime, datetime)
        The time or tuple of times (start, end)

    Returns:
    --------
    xr.Dataset
        The xarray dataset 
    """

    #Handle lat, lon args to ensure float dtype
    try:
        west, east, south, north = (
            float(west), 
            float(east), 
            float(south), 
            float(north)
            )

    except TypeError:
        raise TypeError(f"Invalid longitude or latitude passed, expected float ot int, but detected: {type(west).__name__}, {type(east).__name__}, {type(south).__name__}, {type(north).__name__}")
    # Create the default ncss and query objects

    #Handle time arg:
    if isinstance(time, datetime):
        try:
            ds = xr.open_dataset(LATEST_DS.access_urls['OPENDAP'])
            ds = ds.sel(time=time, latitude=slice(north, south), longitude=slice(west, east))
            ds = ds[DATA_VARS]

            if file_path is not None:
                _save_filepath(ds, file_path)
                return None
        
        except IndexError:
            raise IndexError("Arguements are out of bounds")
        
        return ds 

    elif isinstance(time, tuple) and len(time) == 2 and all(isinstance(t, datetime) for t in time):
        try:
            start = time[0]
            end = time[1]
            ds = xr.open_dataset(LATEST_DS.access_urls['OPENDAP'])
            ds = ds.sel(time=slice(start, end), latitude=slice(north, south), longitude=slice(west, east))
            ds = ds[DATA_VARS]       

            if file_path is not None:
                _save_filepath(ds, file_path)
                return None

        except IndexError:
            raise IndexError("Arguements are out of bounds")
        return ds

    else:
        raise TypeError(f"Invalid time arguement for {time}: expected datetime obj, but passed {type(time).__name__}")
    
def _save_filepath(ds: xr.Dataset, file_path:str) -> None:
    """
    Internal function to save an xr.Dataset as a static file

    Parameters:
    file_path : str
        The file path string to save to 
    """
    with ProgressBar():
        if file_path.endswith('.nc'):
            ds.to_netcdf(file_path)

        elif file_path.endswith('.h5'):
            ds.to_netcdf(file_path, engine='h5netcdf')

        elif file_path.endswith('.zarr'):
            ds.to_zarr(file_path)

        else:
            raise TypeError("unsupported file type to save, dataset not saved to disk")