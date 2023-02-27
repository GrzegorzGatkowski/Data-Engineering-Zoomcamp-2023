
schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.StringType(), True), 
    types.StructField('dropoff_datetime', types.StringType(), True), 
    types.StructField('PULocationID', types.StringType(), True), 
    types.StructField('DOLocationID', types.StringType(), True), 
    types.StructField('SR_Flag', types.StringType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)])
