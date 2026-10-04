# This file is ingesting drivers files from landing volume and writing them to the bronze layer with ingesting timestamp.

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/02.bronze-helpers


drivers_source_path = f"{landing_folder_path}/drivers.json"
drivers_table_name = f"{catalog_name}.{bronze_schema}.drivers"



from pyspark.sql.types import StructField, StringType, StructType, DateType

inner_schema = StructType([
                StructField('givenName', StringType(), True),
                StructField('familyName',StringType(), True)
])
drivers_schema = StructType([
                StructField('driverId', StringType(), True),
                StructField('name', inner_schema, True),
                StructField('dateOfBirth', DateType(), True),
                StructField('nationality', StringType(),True),
                StructField('url', StringType(),True)
])



drivers_df = (
    
    spark.read
                .format('json')
                .option('header', True)
                .option('mode','FAILFAST')   
                .schema(drivers_schema)
                .load(drivers_source_path)
    )



drivers_final_df = add_ingestion_data(drivers_df)


(
    drivers_final_df
    .write
    .format('delta')
    .mode('overWrite')
    .saveAsTable(drivers_table_name)

)
