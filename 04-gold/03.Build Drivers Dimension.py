
# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config



gold_drivers_dim = f"{catalog_name}.{gold_schema}.drivers_dim"



silver_drivers_table = f"{catalog_name}.{silver_schema}.drivers"
gold_region_reference_table = f"{catalog_name}.{gold_schema}.ref_nationality_region"



silver_drivers_df = spark.table(silver_drivers_table)
region_reference_df = spark.table(gold_region_reference_table) 


drivers_dim = (silver_drivers_df.
               join(region_reference_df,
                    silver_drivers_df.nationality == region_reference_df.nationality,
                    'left').
               select(silver_drivers_df.driver_id,
                      silver_drivers_df.driver_name,
                      silver_drivers_df.date_of_birth,
                      silver_drivers_df.nationality,
                      region_reference_df.region.alias('nationality_region'))
               
               
               )


(
    drivers_dim.write.
    format('delta').
    mode('overwrite').
    saveAsTable(gold_drivers_dim)
)
