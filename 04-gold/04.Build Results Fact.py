
# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config



fact_session_results_table = f"{catalog_name}.{gold_schema}.fact_session_results"



results_silver_table = f"{catalog_name}.{silver_schema}.results"
sprints_silver_table = f"{catalog_name}.{silver_schema}.sprints"



results_df = spark.table(results_silver_table)


sprints_df = spark.table(sprints_silver_table)



from pyspark.sql import functions as F
results_df = (results_df.select(
                                    results_df.season,
                                    results_df.round,
                                    results_df.constructor_id,
                                    results_df.driver_id,
                                    results_df.grid_position,
                                    results_df.completed_laps,
                                    results_df.car_number,
                                    results_df.points,
                                    results_df.finish_position,
                                    results_df.finish_position_text,
                                    results_df.status
                                ).
                                withColumn('session_type',F.lit('RACE'))
                                
                                 )

sprints_df = (sprints_df.select(
                                    sprints_df.season,
                                    sprints_df.round,
                                    sprints_df.constructor_id,
                                    sprints_df.driver_id,
                                    sprints_df.grid_position,
                                    sprints_df.completed_laps,
                                    sprints_df.car_number,
                                    sprints_df.points,
                                    sprints_df.finish_position,
                                    sprints_df.finish_position_text,
                                    sprints_df.status
                                ).withColumn('session_type',F.lit('SRPINT'))
                                
                                 )



fact_session_results_df = (
                            results_df.unionByName(sprints_df,allowMissingColumns = True).
                            withColumn('is_win',F.when(F.col('finish_position') == 1,F.lit(True)).otherwise(F.lit(False))).
                            withColumn('is_podium',F.when((F.col('finish_position') >= 1) & (F.col('finish_position') <= 3),F.lit(True)).otherwise(F.lit(False))).
                            withColumn('has_points',F.when(F.col('points') != 0,F.lit(True)).otherwise(F.lit(False)))
)



(fact_session_results_df.write.
 format('delta').
 mode('overwrite').
 saveAsTable(fact_session_results_table) 
 )
