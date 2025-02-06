"""
Constants module to configure variables for the package
"""
from datetime import datetime, timezone, timedelta

# Set datetime logic
current_time = datetime.now(tz=timezone.utc)
current_hour = current_time.hour

# Determine the most recent run time based on the current hour
if 1 <= current_hour < 7:
    run_time = "18"
elif 7 <= current_hour < 9:
    run_time = "00"
elif 10 <= current_hour < 16:
    run_time = "06"
else:
    run_time = "12"

# Determine the date for the selected run
if run_time == "18":
    run_date = (current_time - timedelta(days=1)).strftime("%Y%m%d")

elif run_time == "12" and current_hour < 1:
    run_date = (current_time - timedelta(days=1)).strftime("%Y%m%d")

else:
    run_date = current_time.strftime("%Y%m%d")