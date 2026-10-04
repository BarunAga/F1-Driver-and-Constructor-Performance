# This file is transformating and filtering circuits table from bronze layer and writing them to the silver layer.

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config


table_name = 'circuits'
bronze_circuits_table = f"{catalog_name}.{bronze_schema}.{table_name}"
silver_circuits_table = f"{catalog_name}.{silver_schema}.{table_name}"
circuits_df = (
                spark.
                read.
                table(bronze_circuits_table)
                
)

from pyspark.sql import functions as F
circuits_final_df = (circuits_df.
               drop("url").
               withColumnsRenamed({'circuitId':'circuit_id', 'circuitName': 'circuit_name', 'lat': 'latitude', 'long': 'longitude'}).
               dropna(subset=['circuit_id']).
               dropDuplicates().
               withColumns({'circuit_name': F.initcap(F.col('circuit_name')), 'locality': F.initcap(F.col('locality'))})
               )

(       circuits_final_df.
        write.
        format('delta').
        mode('overWrite').
        saveAsTable(silver_circuits_table)
)
