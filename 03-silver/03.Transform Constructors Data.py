# This file is transformating and filtering constructors table from bronze layer and writing them to the silver layer.

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config



table_name = 'constructors'
bronze_constructors_table = f"{catalog_name}.{bronze_schema}.{table_name}"
silver_constructors_table = f"{catalog_name}.{silver_schema}.{table_name}"



constructors_df = spark.table(bronze_constructors_table)




from pyspark.sql import functions as F

constructors_final_df = (
                            constructors_df.
                            drop('url').
                            withColumnsRenamed({'constructorId':'constructor_id','name':'constructor_name'}).
                            dropna(subset=['constructor_id']).
                            dropDuplicates().
                            withColumn('nationality',F.initcap(F.col('nationality')))


)


(
    constructors_final_df.write.
    format('delta').
    mode('overwrite').
    saveAsTable(silver_constructors_table)


)
