# This file is transformating and filtering results table from bronze layer and writing them to the silver layer.
# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config



table_name = 'results'
bronze_results_table = f"{catalog_name}.{bronze_schema}.{table_name}"
silver_results_table = f"{catalog_name}.{silver_schema}.{table_name}"


results_df = (spark.table(bronze_results_table))


from pyspark.sql import functions as F
results_final_df = (
                        results_df.
                        drop('url').
                        withColumnsRenamed({'constructorId':'constructor_id',
                                            'driverId':'driver_id',
                                            'raceName':'race_name',
                                            'positionText':'finish_position_text',
                                            'date':'race_date',
                                            'grid':'grid_position',
                                            'laps':'completed_laps',
                                            'number':'car_number',
                                            'position':'finish_position',}).
                        dropna(subset=['season','round','constructor_id','driver_id']). # car_number
                        dropDuplicates().
                        withColumn('race_name',F.initcap(F.col('race_name')))

)


(
    results_final_df.write.
    format('delta').
    mode('overwrite').
    saveAsTable(silver_results_table)

)
