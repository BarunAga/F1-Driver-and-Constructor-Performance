# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config



silver_races_table = f"{catalog_name}.{silver_schema}.races"
silver_circuits_table = f"{catalog_name}.{silver_schema}.circuits"



gold_races_dim = f"{catalog_name}.{gold_schema}.races_dim"



races_df = spark.table(silver_races_table)
circuits_df = spark.table(silver_circuits_table)



from pyspark.sql import functions as F



races_dim = (
                races_df.
                join(  
                       circuits_df,
                       races_df.circuit_id == circuits_df.circuit_id,
                       'inner',).
                select(
                       races_df.season,
                       races_df.round,
                       races_df.race_name,
                       races_df.race_date,
                       circuits_df.circuit_name,
                       circuits_df.circuit_id,
                       circuits_df.country)
                

)



(races_dim.write.
format('delta').
mode('overwrite').
saveAsTable(gold_races_dim))
