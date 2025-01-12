import pandas as pd
import xarray as xr
import siphon as si 
from siphon.catalog import TDSCatalog
from typing import  Union, List
from datetime import datetime as dt

class HycomClient:
    
    # Define constants for simultaneous IP workers to the THREDDS servers. 
    # IP ban could relult if the below thresholds are exceeded.
    MAX_CONNECTIONS: int = 10
    MAX_NCSS_CONNECTIONS: int = 1  
    active_connections: int = 0
    
    """A client to fetch forecast runs from THREDDS Data Server."""
    
    SST_URL: str = "https://tds.hycom.org/thredds/catalog/FMRC_ESPC-D-V02_t3z/runs/catalog.xml"
    SSU_URL: str = "https://tds.hycom.org/thredds/catalog/FMRC_ESPC-D-V02_u3z/runs/catalog.xml"
    SSV_URL: str = "https://tds.hycom.org/thredds/catalog/FMRC_ESPC-D-V02_v3z/runs/catalog.xml"

    def __init__(self):
        """Initialize the client and retrieve forecast runs."""
        
        self.sst_var_ID: str = 'water_temp'
        self.ssu_var_ID: str = 'water_u'
        self.ssv_var_ID: str = 'water_v'

        self.time_dim = None # Might have to update with the OPENDAP decoder 
        self.level_dim = None # Might have to update this attr if we want to do
                              # mean level calulations

        self.latitude_dim: str = 'lat'
        self.longitude_dim: str = 'lon'
        
        self.url_dict = {
            'sst': self.SST_URL,
            'ssu': self.SSU_URL,
            'ssv': self.SSV_URL
        }
    

        self.forecast_alignment = None # Init an attribute to check if all datasets have the same ref_time (server uploads can cause misbehaviors)

    def get_latest_forecast(self, var:str) -> xr.Dataset:
        pass 

    def _check_dataset_completeness(self, dataset:si.catalog.Dataset) -> bool:
        """
        Check if the `siphon.catalog.Dataset` (aggregated) is complete.

        Parameters
        ----------
        dataset : siphon.catalog.Dataset
            The dataset to check for completeness.

        Returns
        -------
        bool
            True if the dataset is complete, False otherwise.
        """

        with xr.open_dataset(dataset.access_urls['OPENDAP'], decode_times=False) as ds:
            if len(ds['time_offset']) != 65:
                return False
            return True
        
    def get_forecast_df(self) -> pd.DataFrame:
        """
        Retrieve all available forecast timestamps for each variable.
        Returned as a pandas.Dataframe object with the variable, timestamp as the idx.
        Used to check if the dataserver is updated with the latest completed dataset (completeness)

        Returns:
        ----------
        df : pandas.Dataframe
            pandas dataframe object with the variable, forecast time, and completeness (bool)

        """
        timestamps = []
        completeness = []
        vars = []

        for var, url in self.url_dict.items():
            cat = TDSCatalog(url)
            ds_list = cat.datasets

            for time, ds in ds_list.items():
                vars.append(var)

                time_str = time[-20:]
                pd_datetime = pd.to_datetime(time_str)
                timestamp = pd.Timestamp(pd_datetime).strftime('%y-%m-%d %HZ ')
                timestamps.append(timestamp)

                complete = self._check_dataset_completeness(ds)
                completeness.append(complete)

        df = pd.DataFrame({
            "variable":vars, 
            "forecast_run":timestamps, 
            "complete": completeness
            }
        )

        df.set_index(['variable', 'forecast_run'], inplace=True) 

        return df

    def _regrid_data(self, 
                     ds: Union[xr.Dataset, 
                     List[xr.Dataset]]) -> Union[xr.Dataset, List[xr.Dataset]]:
            """Returns an xr.Dataset with a uniform grid (0.25x0.25deg spatial resolution).
               Default HYCOM resolution is 0.125x0.25deg, which is not suitable for the xyz and RAF install.
            """
    
            # Check if ds is a list of datasets
            if isinstance(ds, list):
                regridded_datasets = []  # Use a properly named list
                for dataset in ds:
                    regridded_datasets.append(dataset.isel(lat=slice(None, None, 2)))
                return regridded_datasets  # Return a list of regridded datasets
    
            # If a single dataset is provided, process and return it
            return ds.isel(lat=slice(None, None, 2))
    
    def _decode_dataset_OPENDAP(self, dataset:si.catalog.Dataset)->xr.Dataset:
        """
        """
        ds = xr.open_dataset(dataset.access_urls['OPENDAP'], decode_times=False)
        ds = self._regrid_data(ds)

        return ds

