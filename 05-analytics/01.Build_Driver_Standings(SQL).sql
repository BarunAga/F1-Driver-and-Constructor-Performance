-- Databricks notebook source
SELECT 
    r.driver_id,
    season, 
    driver_name,
    nationality,
    SUM(points) AS total_points, 
    SUM(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS number_of_wins,
    SUM(CASE WHEN finish_position BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS number_of_podiums,
    COUNT(*) AS race_start,
    DENSE_RANK() OVER (PARTITION BY season ORDER BY SUM(points) DESC, SUM(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) DESC) AS standing_position
    
      
     
FROM formula1.gold.fact_session_results r
JOIN formula1.gold.drivers_dim d
ON r.driver_id = d.driver_id
GROUP BY season, r.driver_id, driver_name, nationality;



-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.view_driver_standings AS
SELECT 
    r.driver_id,
    r.season, 
    d.driver_name,
    d.nationality,
    SUM(r.points) AS total_points, 
    COUNT_IF(r.is_win) AS number_of_wins,
    COUNT_IF(r.is_podium) AS number_of_podiums,
    COUNT(*) AS race_start,
    RANK() OVER (PARTITION BY season ORDER BY SUM(points) DESC, COUNT_IF(r.is_win) DESC) AS standing_position
    
     
     
FROM formula1.gold.fact_session_results r
JOIN formula1.gold.drivers_dim d
ON r.driver_id = d.driver_id
GROUP BY season, r.driver_id, driver_name, nationality;




-- COMMAND ----------

SELECT * FROM formula1.gold.view_driver_standings;

-- COMMAND ----------

