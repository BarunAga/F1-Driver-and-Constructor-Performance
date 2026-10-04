# This file is ingesting circuits files from landing volume and writing them to the bronze layer with ingesting timestamp.

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/02.bronze-helpers

circuits_source_path = f"{landing_folder_path}/circuits.csv"
circuits_table_name = f"{catalog_name}.{bronze_schema}.circuits"


from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DoubleType

circuits_schema = StructType(
    [
        StructField('circuitId', StringType(), True),
        StructField('url', StringType(), True),
        StructField('circuitName', StringType(), True),
        StructField('lat', DoubleType(), True),
        StructField('long', DoubleType(), True),
        StructField('locality', StringType(), True),
        StructField('country', StringType(), True)
    ]
)

circuits_df = (
    
    spark.read
                .format('csv')
                .option('header', True)
                .option('mode','FAILFAST')   
                .schema(circuits_schema)
                .load(circuits_source_path)
    )



circuits_final_df = add_ingestion_data(circuits_df)



(
    circuits_final_df
    .write
    .format('delta')
    .mode('overWrite')
    .saveAsTable(circuits_table_name)

)
