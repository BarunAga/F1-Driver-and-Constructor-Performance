# This file is ingesting constructors files from landing volume and writing them to the bronze layer with ingesting timestamp.

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/02.bronze-helpers



constructors_source_path = f"{landing_folder_path}/constructors.json"
constructors_table_name = f"{catalog_name}.{bronze_schema}.constructors"



constructors_schema = """   constructorId STRING,
                        name STRING,
                        nationality STRING,
                        url STRING
                  """

constructors_df = (
    
    spark.read
                .format('json')
                .option('header', True)
                .option('mode','FAILFAST')   
                .schema(constructors_schema)
                .load(constructors_source_path)
    )

constructors_final_df = add_ingestion_data(constructors_df)

(
    constructors_final_df
    .write
    .format('delta')
    .mode('overWrite')
    .saveAsTable(constructors_table_name)

)
