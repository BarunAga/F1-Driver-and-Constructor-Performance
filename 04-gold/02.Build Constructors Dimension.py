
# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config


gold_constructors_dim = f"{catalog_name}.{gold_schema}.constructors_dim"



silver_constructors_table = f"{catalog_name}.{silver_schema}.constructors"
gold_region_reference_table = f"{catalog_name}.{gold_schema}.ref_nationality_region"



constructors_silver_df = spark.table(silver_constructors_table)
region_reference_df = spark.table(gold_region_reference_table) 


constructors_dim = (constructors_silver_df.
                        join(region_reference_df,
                             constructors_silver_df.nationality == region_reference_df.nationality,
                             'left').
                        select(constructors_silver_df.constructor_id,
                               constructors_silver_df.constructor_name,
                               constructors_silver_df.nationality,
                               region_reference_df.region.alias('nationality_region'))
                   
                        
                        
                        )

(constructors_dim.write.
 format('delta').
 mode('overwrite').
 saveAsTable(gold_constructors_dim))
