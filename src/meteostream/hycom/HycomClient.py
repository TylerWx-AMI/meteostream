import pandas as pd
from siphon.catalog import TDSCatalog
from typing import Dict
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
        self.url_dict = {
            'sst': self.SST_URL,
            'ssu': self.SSU_URL,
            'ssv': self.SSV_URL
        }
    
        self.forecast_runs = self._get_forecast_runs()

    def _get_forecast_runs(self) -> Dict[str, List[pd.Timestamp]]:
        """Retrieve all available forecast timestamps for each variable."""
        runs = {}

        for var, url in self.url_dict.items():
            try:
                cat = TDSCatalog(url)
                ds_list = list(cat.datasets)  # Convert dataset collection to a list

                if ds_list:  # Ensure datasets exist
                    timestamps = []
                    
                    # Extract timestamps from all datasets
                    for ds in ds_list:
                        timestamp_str = ds[-20:]  # Extract last 20 characters
                        timestamp = pd.to_datetime(timestamp_str, format='%Y-%m-%d %HZ')
                        timestamps.append(timestamp)

                    runs[var] = timestamps  # Store all timestamps in a list

                else:
                    runs[var] = []  # No datasets available

            except Exception as e:
                runs[var] = f"Error: {e}"

        return runs
    
    def _decode_dataset_OPENDAP(self, ds)
        pass
    
    def get_latest_forecast(self, var):
        pass 
