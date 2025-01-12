# Example script for hycom client usage in meteostream
from meteostream import HycomClient

client = HycomClient()

df = client.get_forecast_df()