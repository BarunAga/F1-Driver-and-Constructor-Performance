# This file is transformating and filtering sprints table from bronze layer and writing them to the silver layer.
# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config

table_name = 'sprints'
bronze_sprints_table = f"{catalog_name}.{bronze_schema}.{table_name}"
silver_sprints_table = f"{catalog_name}.{silver_schema}.{table_name}"



sprints_df = spark.table(bronze_sprints_table)


from pyspark.sql import functions as F
sprints_final_df = (sprints_df.
                    drop('url').
                    withColumnsRenamed({'constructorId':'constructor_id',
                                        'driverId':'driver_id',
                                        'raceName':'race_name',
                                        'positionText':'finish_position_text',
                                        'date':'race_date',
                                        'grid':'grid_position',
                                        'laps':'completed_laps',
                                        'number':'car_number',
                                        'position':'finish_position'
                                        }).
                    dropna(subset=['season','round','constructor_id','driver_id']).
                    dropDuplicates().
                    withColumn('race_name',F.initcap(F.col('race_name')))
                    
                    
                    
                    
                    )

(
    sprints_final_df.
    write.
    format('delta').
    mode('overwrite').
    saveAsTable(silver_sprints_table)


)
