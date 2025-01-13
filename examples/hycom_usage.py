# Example script for hycom client usage in meteostream
import time
import logging
from meteostream import HycomClient

# Set up the logger
log_path = 'hycom.log'

handler = logging.FileHandler(log_path, mode='w')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
	handler,  # Ensures flush on every log entry
        logging.StreamHandler()      # Optional: also log to console
    ],
    force=True  # Reinitialize handlers to avoid conflicts
)

# create a client instance
client = HycomClient()

# construct a while loop to esnure proper alignment
while client.check_server_alignment() == False:
    logging.INFO("Server is not aligned: All data not uploaded, or misaligned forecast ref times")
    time.sleep(3600) # Sleep for an hour

# Set the file path
file_path = "hycom_test.nc"
client.download_dataset(file_path)
logging.INFO(f"Download Complete, saved to {file_path}")