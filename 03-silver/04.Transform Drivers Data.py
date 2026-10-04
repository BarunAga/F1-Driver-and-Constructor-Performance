# This file is transformating and filtering drivers table from bronze layer and writing them to the silver layer.

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config


table_name = 'Drivers'
bronze_drivers_table = f"{catalog_name}.{bronze_schema}.{table_name}"
silver_drivers_table = f"{catalog_name}.{silver_schema}.{table_name}"



drivers_df = spark.table(bronze_drivers_table)



from pyspark.sql import functions as F
drivers_final_df = (
                    drivers_df.
                    drop('url').
                    withColumn('name', F.initcap(F.concat_ws(' ', F.col('name.givenName'), F.col('name.familyName')))).
                    withColumn('nationality',F.initcap(F.col('nationality'))).
                    withColumnsRenamed({'DriverId':'driver_id','dateOfbirth':'date_of_birth','name':'driver_name'}).
                    dropna(subset=['driver_id']).
                    dropDuplicates()
                    


)



(
    drivers_final_df.write.
    format('delta').
    mode('overwrite').
    saveAsTable(silver_drivers_table)

)
