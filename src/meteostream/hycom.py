import pandas as pd
import siphon.catalog
import xarray as xr
import siphon
from siphon.catalog import TDSCatalog
from typing import  Union, List, Optional, Tuple
from datetime import datetime
from dask.distributed import Client, progress, LocalCluster


# Define constants for simultaneous IP workers to the THREDDS servers. 
# IP ban could relult if the below thresholds are exceeded.
MAX_WORKERS = 9
MAX_NCSS_CONNECTIONS = 1  

SST_URL = "https://tds.hycom.org/thredds/catalog/FMRC_ESPC-D-V02_t3z/runs/catalog.xml"
SSU_URL = "https://tds.hycom.org/thredds/catalog/FMRC_ESPC-D-V02_u3z/runs/catalog.xml"
SSV_URL = "https://tds.hycom.org/thredds/catalog/FMRC_ESPC-D-V02_v3z/runs/catalog.xml"

class HycomClient:
    
    """
    A client to fetch forecast runs and view current state of 
    the HYCOME THREDDS Data Server.
    """
    

    def __init__(self, n_workers: int = 6, chunks: dict = {"time": 5, "lat": 500, "lon": 500}):
        """Initialize the client and retrieve forecast runs."""

        if n_workers >= MAX_WORKERS:
            raise ValueError(f"Number of workers exceeded {MAX_WORKERS}: {n_workers} was initialized")

        # Initialize the dask backend
        self.dask_cluster = LocalCluster(n_workers=n_workers)
        self.client = Client(self.dask_cluster)
        self.chunks = chunks


        self.sst_var_ID = 'water_temp'
        self.ssu_var_ID = 'water_u'
        self.ssv_var_ID = 'water_v'

        self.SST_URL = SST_URL
        self.SSU_URL = SSU_URL
        self.SSV_URL = SSV_URL

        self.URL_LIST = [self.SST_URL, self.SSU_URL, self.SSV_URL]

        self.url_dict = {
            'sst': self.SST_URL,
            'ssu': self.SSU_URL,
            'ssv': self.SSV_URL
            }

        self.time_dim = None # Might have to update with the OPENDAP decoder 
        self.level_dim = None # Might have to update this attr if we want to do
                              # mean level calulations

        self.latitude_dim: str = 'lat'
        self.longitude_dim: str = 'lon'

        self.sst_ds_idx = 0
        self.ssu_ds_idx = 0 
        self.ssv_ds_idx = 0

        self.url_idx = None
        
        self.forecast_alignment = None # Init an attribute to check if all datasets have the same ref_time (server uploads can cause misbehaviors)

        self.server_df = None # Init the df attribute

        self.latest_forecast_download = None # Init the latest forecast dataset for client awareness

    def _check_dataset_completeness(self,
                                    dataset:siphon.catalog.Dataset
                                    ) -> Tuple[bool, int]:
        """
        Check if the `siphon.catalog.Dataset` (aggregated) is complete.

        Parameters
        ----------
        dataset : siphon.catalog.Dataset
            The dataset to check for completeness.

        Returns
        -------
        Tuple
            bool: True if the dataset is complete, False otherwise.
            int: the length of the taus (65 is complete)
        """

        with xr.open_dataset(dataset.access_urls['OPENDAP'], decode_times=False) as ds:
            n_taus = len(ds['time_offset'])
            if n_taus != 65:
                return False, n_taus
            return True, n_taus

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

        ds = xr.open_dataset(
            dataset.access_urls['OPENDAP'], 
            decode_times=False,
            chunks=self.chunks)

        time_offset = pd.to_timedelta(ds['time_offset'], unit='hours')
        time_offset = pd.TimedeltaIndex(time_offset)
        ds['valid_time'] = timestamp + time_offset

        # ds = ds.swap_dims({'time':'valid_time'})
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
            n_taus = []

            for var, url in self.url_dict.items():
                cat = TDSCatalog(url)
                ds_list = cat.datasets

                for time, ds in ds_list.items():
                    vars.append(var)

                    time_str = time[-20:]
                    timestamps.append(time_str)

                    complete = self._check_dataset_completeness(ds)
                    completeness.append(complete[0])
                    n_taus.append(complete[1])

            df = pd.DataFrame({
                "variable":vars, 
                "forecast_run":timestamps, 
                "complete": completeness,
                "n_taus":n_taus
                }
            )

                        # Add a column for percentage completion
            df["completion_percentage"] = (df["n_taus"] / 65 * 100).apply(lambda x: f"{x:.1f}%")

            df['forecast_run'] = pd.to_datetime(df['forecast_run']).dt.normalize()

            # Add a sub-index within each 'variable' group
            df["sub_index"] = df.groupby("variable").cumcount()

            df.set_index(["variable", "sub_index"], inplace=True) 
            # Add a sub-index within each 'variable' group

            # Store as an attribute 
            self.server_df = df

            return self.server_df

    def get_dataset(
        self, 
        depth: Optional[Union[int, Tuple[int, int]]] = None,
        ref_time: Optional[datetime] = None,
        file_path : Optional[str] = None
        ) -> Union[xr.Dataset, None]:
        """
        Return the xarray dataset for the HYCOM data or save to file path. 

        Parameters:
        --------
        ref_time: Optional[datetime], default None (latest)
            The forecast reftime for the dataset

        file_path: str or None
            The filepath to save the dataset 

        Returns:
        --------
        xr.Dataset or None

        """
        # Handle time kwarg with the var_idx method to update the idx or grab the latest:
        if ref_time:
            self.get_varidx(time=ref_time, set_attrs=True) 
        else:
            self.get_forecast_df() if self.server_df is None else self.server_df
    

        # Check if 'complete' is True for each variable using the stored indices
        all_complete = (
            self.server_df.loc[("sst", self.sst_ds_idx), "complete"]
            and self.server_df.loc[("ssu", self.ssu_ds_idx), "complete"]
            and self.server_df.loc[("ssv", self.ssv_ds_idx), "complete"]
        )

        if not all_complete:
            raise ValueError("Not all datasets selected are complete with data")

        dataset_list = []

        for url, idx in self.url_idx.items():
            # print(url, idx) # for debugging
            cat = TDSCatalog(url)
            dataset = cat.datasets[idx]
            ds = self._decode_dataset_OPENDAP(dataset)
            dataset_list.append(ds)

        # for debugging:        
        # for i, ds in enumerate(dataset_list):
        #     print(f"Dataset {i}: dimensions = {ds.dims}")
        #     print(f"Dataset {i}: time length = {len(ds.coords['time'])}")
        #     print(f"Dataset {i}: time values = {ds.coords['time'].values}")

        # Merge the dataset
        merged_ds = xr.merge(dataset_list)

        if file_path:
            self._save_dataset(merged_ds, file_path)
            return None

        if depth:
            if isinstance(depth, int):
                merged_ds = merged_ds.sel(depth=depth, method='nearest')

            elif isinstance(depth, tuple):
                start = float(depth[0])
                end = float(depth[1])

                merged_ds = merged_ds.sel(depth=slice(start, end))
            else:
                raise ValueError(f"Deptj not successfully parsed, expected int but got type: {type(level).__name__} ")


        return merged_ds


    def get_varidx(self, 
                   time: datetime, 
                   set_attrs: Optional[bool] = False
                   ) -> Union[dict, None]:
        """
        Get the sub-index value for a given forecast_run time for each variable.
        
        Parameters:
        -----------
        time : datetime.datetime 
                The traget time to search the dataserver.
        
        set_attrs: bool
                The bool arg to set client attrs with the dataset idx, default=False
        
        Returns:
        dict: 
        A dictionary where keys are variables and values are sub-index values for the given time.

        or 

        None
        """
        
        # # Handle time kwarg 
        if not isinstance(time, datetime):
             raise TypeError(f"Invalid time dtype passed for: {time}")

        df = self.get_forecast_df() if self.server_df is None else self.server_df

        results = {}

        for variable, sub_df in df.groupby(level=0):  # Group by 'variable'
            # Find rows matching the target_time in the forecast_run column
            matching_rows = sub_df[sub_df["forecast_run"] == time]
            if not matching_rows.empty:
                    sub_index = matching_rows.index.get_level_values(1).item()
                    results[variable] = sub_index
            else:
                sub_index = None
                results[variable] = None

            # Dynamically update the attribute if `set_attrs` is True
            if set_attrs:
                attr_name = f"{variable}_ds_idx"
                setattr(self, attr_name, sub_index if not matching_rows.empty else None)

       # Check if any value in `results` is None
        if any(value is None for value in results.values()):
            raise IndexError("Incomplete alignment of forecast data, time selected returned None")
        
        if set_attrs:
            self.results_idx = results
            self.url_idx = {
                self.SST_URL: self.sst_ds_idx,
                self.SSU_URL: self.ssu_ds_idx,
                self.SSV_URL: self.ssv_ds_idx
            }

            return None
        
        return results
            

    def _save_dataset(self, ds: xr.Dataset, file_path:str) -> None:
        """
        Internal function to save an xr.Dataset as a static file

        Parameters:
        file_path : str
            The file path string to save to 
        """

        if file_path.endswith('.nc'):
            ds.to_netcdf(file_path)
            
        elif file_path.endswith('.h5'):
            ds.to_netcdf(file_path, engine='h5netcdf')
        
        elif file_path.endswith('.zarr'):
            task = ds.to_zarr(file_path, compute=False, consolidated=True, encoding={var: {"compressor": None} for var in ds.data_vars})
            future = self.client.compute(task)
            progress(future)
            future.result()

        else:
            raise TypeError("unsupported file type to save, dataset not saved to disk")
    

    def write_xyz():
        pass    


        

        
        


        
