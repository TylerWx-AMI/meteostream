# Functions to call the Coral Reef Watch (observed) SST and SICE data
import siphon.ncss
import xarray as xr
import siphon

from siphon.catalog import TDSCatalog
from datetime import datetime, timedelta
from typing import Union, Tuple


#Define the URL Constant and Dataset index
CRW_URL: str = "https://pae-paha.pacioos.hawaii.edu/thredds/satellite.html"
CRW_IDX: int = 0

CAT = TDSCatalog(CRW_URL)
LATEST_DS = CAT(CRW_IDX)
DATA_VARS = ("CRW_SEAICE", "CRW_SST")

def get_latest_CRW()->xr.Dataset:
    """
    Return an xarray dataset with all variables and global coverage for the latest time 
    available. This function uses the OPENDAP access method. 

    Returns:
    --------
    xarray.Dataset:
        The dataset containing the latest data (global) for the latest time
    """

    ds = xr.open_dataset(LATEST_DS.access_urls['OPENDAP'])
    ds = ds.isel(time=-1)

    return ds 

def create_query()->Tuple[siphon.ncss.NCSS, siphon.ncss.NCSSQuery]:
    """
    """
    ncss = LATEST_DS.subset()
    query = ncss.query()
    query.variables(DATA_VARS)

    return ncss, query

def latlon_point_data(
    latitude: Union[float, int], 
    longitude: Union[float, int], 
    time: Union[datetime, Tuple[datetime, datetime]]
    ) -> xr.Dataset:
    """
    Return the xarray dataset for a specified lat/lon point for one time or range of times. 
    This functions calls a NCSS query from the Hawaii.edu THREDDS server for CRW data. Too large
    requests may raise an Exception. Needs to be less than ~100MB. DO NOT USE PARALLEL PROCESSING
    AGAINST THE SERVERS, as this might be discouraged practice from the server-side. 

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
        raise TypeError(
              f"Invalid longitude or latitude passed, 
              expected float ot int, but detected: 
              {type(longitude).__name__}, {type(latitude).__name__}"
              )
    # Create the default ncss and query objects
    ncss, query = create_query()
    query.lonlat_point(longitude, latitude)

    #Handle time arg:
    if isinstance(time, datetime):
        query.time(time)
        try:
            data = ncss.get_data(query)
            ds = xr.open_dataset(data)
        except IndexError:
            raise IndexError("Time out of bounds")
        except Exception:
            raise Exception(f"An unexpected error occured")

        return ds

    elif isinstance(time, tuple) and len(time) == 2 and all(isinstance(t, datetime) for t in time):
        query.time_range(time)
        try:
            data = ncss.get_data(query)
            ds = xr.open_dataset(data)
        except IndexError:
            raise IndexError("Time out of bounds")
        except Exception:
            raise Exception(f"An unexpected error occured")

        return ds

    else:
        raise TypeError(f"Invalid time arguement for {time}:
                        expected datetime obj, but passed {type(time).__name__}")
    
def latlon_grid_data(
    west: Union[float, int], 
    east: Union[float, int], 
    south: Union[float, int], 
    north: Union[float, int], 
    time: Union[datetime, Tuple[datetime, datetime]]
    ) -> xr.Dataset:
    """
    Return the xarray dataset for a specified lat/lon grid (bounds) for one time or range of times. 
    This functions calls a NCSS query from the Hawaii.edu THREDDS server for CRW data. Too large
    requests may raise an Exception. Needs to be less than ~100MB. DO NOT USE PARALLEL PROCESSING
    AGAINST THE SERVERS, as this might be discouraged practice from the server-side. 

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
        raise TypeError(
              f"Invalid longitude or latitude passed, 
              expected float ot int, but detected: 
              {type(west).__name__}, {type(east).__name__},
              {type(south).__name__}, {type(north).__name__}"
              )
    # Create the default ncss and query objects
    ncss, query = create_query()
    query.lonlat_box(west, east, south, north)

    #Handle time arg:
    if isinstance(time, datetime):
        query.time(time)
        try:
            data = ncss.get_data(query)
            ds = xr.open_dataset(data)
        except IndexError:
            raise IndexError("Time out of bounds")
        except Exception:
            raise Exception(f"An unexpected error occured")

        return ds

    elif isinstance(time, tuple) and len(time) == 2 and all(isinstance(t, datetime) for t in time):
        query.time_range(time)
        try:
            data = ncss.get_data(query)
            ds = xr.open_dataset(data)
        except IndexError:
            raise IndexError("Time out of bounds")
        except Exception:
            raise Exception(f"An unexpected error occured")

        return ds

    else:
        raise TypeError(f"Invalid time arguement for {time}:
                        expected datetime obj, but passed {type(time).__name__}")