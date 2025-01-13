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

def get_latest_CRW()->xr.Dataset:
    """
    """

    ds = xr.open_dataset(LATEST_DS.access_urls['OPENDAP'])
    ds = ds.isel(time=-1)

    return ds 

def create_query()->Tuple[siphon.ncss.NCSS, siphon.ncss.NCSSQuery]:
    """
    """
    ncss = LATEST_DS.subset()
    query = ncss.query()

    return ncss, query

