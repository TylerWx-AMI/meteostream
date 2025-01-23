import xarray as xr
import earthaccess

# Search for a near-real-time sea ice dataset
search_results = earthaccess.search_data(
    short_name="NSIDC-0718",  # Example: Near-Real-Time SSMIS Daily Sea Ice
    cloud_hosted=False       # Adjust to True if cloud-hosted
)


