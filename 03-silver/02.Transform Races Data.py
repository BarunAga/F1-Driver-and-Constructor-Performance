# This file is transformating and filtering races table from bronze layer and writing them to the silver layer. 
# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config


table_name = 'races'
bronze_races_table = f"{catalog_name}.{bronze_schema}.{table_name}"
silver_races_table = f"{catalog_name}.{silver_schema}.{table_name}"


races_df = (spark.table(bronze_races_table))

from pyspark.sql import functions as F
races_final_df = (races_df. 
                  drop("url").
                  withColumnsRenamed({'raceName':'race_name','circuitId':'circuit_id','date':'race_date'}).
                  dropna(subset=['season','round']).
                  dropDuplicates().
                  withColumn('race_name',F.initcap(F.col('race_name')))  
)


(   races_final_df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(silver_races_table)
                
)
