import pandas as pd
from siphon.catalog import TDSCatalog
from typing import Dict

class HycomClient:
    
  # Define constants for simultaneous IP workers to the THREDDS servers. 
  # IP ban could relult if the below thresholds are exceeded.
  MAX_CONNECTIONS: int = 10
  MAX_NCSS_CONNECTIONS: int = 1  
  active_connections: int = 0

  SST_URL: str = "https://tds.hycom.org/thredds/catalog/FMRC_ESPC-D-V02_t3z/runs/catalog.xml"
  SSU_URL: str = "https://tds.hycom.org/thredds/catalog/FMRC_ESPC-D-V02_u3z/runs/catalog.xml"
  SSV_URL: str = "https://tds.hycom.org/thredds/catalog/FMRC_ESPC-D-V02_v3z/runs/catalog.xml"
  
  def __init__(self):
    self.url_dict ={
                    'sst': SST_URL,
                    'ssu': SSU_URL,
                    'ssv': SSV_URL
                    }
  
  def show_forecast_runs(self) -> Dict:
    runs = {
      
    }
    
    for var, url in self.url_dict:
      cat = TDSCatalog(url)
      ds_list = cat.datasets
      
      
  

  def get_latest_forecast(self, var):
    pass 
      
