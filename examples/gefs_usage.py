from meteostream import GefsClient

client = GefsClient(grib_dir="/home/tylerradebaugh/data/grib", 
                    output_filepath="/home/tylerradebaugh/data/netcdf'GEFS_prob.nc")

client.run_GEFS_pipeline()