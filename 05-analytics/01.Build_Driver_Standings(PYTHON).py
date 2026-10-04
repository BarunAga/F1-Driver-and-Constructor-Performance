
# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config



fact_session_results = f"{catalog_name}.{gold_schema}.fact_session_results"
dim_drivers = f"{catalog_name}.{gold_schema}.drivers_dim"



driver_standings_table = f"{catalog_name}.{gold_schema}.driver_standing"



dim_drivers_df = spark.read.table(dim_drivers).select('driver_id',
                                                      'driver_name',
                                                      'nationality')



fact_session_results_df = spark.table(fact_session_results).select('driver_id',
                                                                   'season',
                                                                   'points',
                                                                   'finish_position',
                                                                   'grid_position'
                                                                   )




driver_standings_joined_df = (
                        fact_session_results_df
                        .join(dim_drivers_df,
                              'driver_id',
                              'left')
)


from pyspark.sql import functions as F
from pyspark.sql.window import Window



driver_standings_final_df = (
    
    driver_standings_joined_df
    .groupBy('driver_id','season','driver_name','nationality')
    .agg(F.sum(F.col('points')).alias('total_points'),
        F.sum((F.col('finish_position') == 1).cast('int')).alias('number_of_wins'),
        F.sum(((F.col('finish_position') <= 3) & (F.col('finish_position') >= 1)).cast('int')).alias('number_of_podiums'),
        F.sum((F.col('grid_position') != 0).cast('int')).alias('race_starts')
        )
    .withColumn('standing_position',F.rank().over(Window.partitionBy('season').orderBy(
        F.col('total_points').desc(),
        F.col('number_of_wins').desc()
    )
                                 )))

