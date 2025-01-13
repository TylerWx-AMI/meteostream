import pandas as pd
import xarray as xr
import siphon
from siphon.catalog import TDSCatalog
from typing import  Union, List, Optional
from datetime import datetime as dt

class HycomClient:
    
    # Define constants for simultaneous IP workers to the THREDDS servers. 
    # IP ban could relult if the below thresholds are exceeded.
    MAX_CONNECTIONS: int = 10
    MAX_NCSS_CONNECTIONS: int = 1  
    active_connections: int = 0
    
    """
    A client to fetch forecast runs and view current state of 
    the HYCOME THREDDS Data Server.
    """
    
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
    
        self.variable_dict = {
            'sst': self.sst_var_ID,
            'ssu': self.ssu_var_ID,
            'ssv': self.ssv_var_ID
        }

        self.forecast_alignment = None # Init an attribute to check if all datasets have the same ref_time (server uploads can cause misbehaviors)

    def _check_dataset_completeness(self, dataset:siphon.catalog.Dataset) -> bool:
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
    
    def _decode_dataset_OPENDAP(self, dataset:siphon.catalog.Dataset)->xr.Dataset:
        """
        We must decode the dataset using pandas and datetime64 objects - as xarray 
        cannot decode times properly, thus the times objects need to be assigned manually

        Parameters:
        -----------

        dataset : siphon.catalog.Dataset
            The dataset object retrieved by the siphon API

        Returns:
        --------

        dataset : xarray.Dataset
            The xarray dataset decoded using pandas and returning datetime64 objects

        """
        ds_str = dataset.name
        time_str = ds_str[-20:]
        timestamp = pd.to_datetime(time_str)
        timestamp = timestamp.to_datetime64()

        ds = xr.open_dataset(dataset.access_urls['OPENDAP'], decode_times=False)
        time_offset = pd.to_timedelta(ds['time_offset'], unit='hours')
        time_offset = pd.TimedeltaIndex(time_offset)
        ds['valid_time'] = timestamp + time_offset

        ds = ds.swap_dims({'time':'valid_time'})
        ds = ds.drop_vars(['tau','time', 'time_offset']) 
        ds = ds.assign_coords(ref_time=timestamp)
        ds['ref_time'].attrs['long_name'] = "Forecast Run"

        ds = self._regrid_data(ds)

        return ds

    def check_server_alignment(self) -> bool:
        """
        Internal function to check the alignment of the aggregated datasets on the HYCOM servers.
        If the forecast runs are not identical - returns false. If forecast runs are not complete - 
        returns false. Both forecast times and full datasets are required to return True. 

        Returns:
        --------
        self.forecast_alignment : bool 
            Boolean value to indicate if the forecast runs are matching AND complete
        """

        # Get the catalog information as a pandas df 
        df = self.get_forecast_df()

                # Get the latest timestamp per variable
        latest_timestamps = df.groupby(level="variable").head(1)

        # Extract latest timestamps and check uniqueness
        unique_timestamps = latest_timestamps.index.get_level_values("forecast_run").unique()
        all_same_timestamp = len(unique_timestamps) == 1

        # Check if all latest timestamps have complete = True
        all_complete = latest_timestamps["complete"].all()

        if all_same_timestamp and all_complete:
            self.forecast_alignment = True
            return self.forecast_alignment

        else:
            self.forecast_alignment = False
            issues = []  # List to store failed conditions

            if not all_same_timestamp:
                issues.append(f"Different timestamps detected: {unique_timestamps}")

            if not all_complete:
                incomplete_vars = latest_timestamps[latest_timestamps["complete"] == False].index.get_level_values("variable").tolist()
                issues.append(f"Forecast times are identical but are incomplete: {incomplete_vars}")
        
        return self.forecast_alignment

    def get_forecast_df(self) -> pd.DataFrame:
            """
            Retrieve all available forecast timestamps for each variable.
            Returned as a pandas.Dataframe object with the variable, timestamp as the idx.
            Used to check if the dataserver is updated with the latest completed dataset (completeness)

            Returns:
            --------
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

    def get_latest_dataset(self) -> Union[xr.Dataset, None]:
        """
        Get the latest datasets from the HYCOM server (requires proper server aligment).

        Returns:
        --------
        xarray.Dataset
            The latest dataset
        
        or

        None
        """
        dataset_list = []

        # Check server alignment 
        if self.check_server_alignment():
            for var, url in self.url_dict.items():
                    cat = TDSCatalog(url)
                    latest_ds = cat.datasets[0]

                    ds = self._decode_dataset_OPENDAP(latest_ds)
                    dataset_list.append(ds)
            
            ds = xr.merge(dataset_list)
            return ds 

        return print("Server not aligned, cannot return the completed datasets at this time")
        

        
        


        
