from meteostream import GefsClient

client = GefsClient(grib_dir="/home/tylerradebaugh/data/grib")

client.run_GEFS_pipeline(save_file="/home/tylerradebaugh/data/netcdf/GEFS_prob.nc")