-- Databricks notebook source
CREATE OR REPLACE VIEW formula1.gold.view_constructor_standings
AS
SELECT r.season,
       r.constructor_id,
       c.constructor_name,
       c.nationality,
       COUNT(*) AS race_start,
       SUM(r.points) AS total_points,
       COUNT_IF(r.is_win) AS number_of_wins,
       COUNT_IF(r.is_podium) AS number_of_podiums,
       RANK() OVER (PARTITION BY r.season ORDER BY SUM(r.points) DESC, COUNT_IF(r.is_win) DESC) AS standing_position

 


FROM formula1.gold.fact_session_results r
JOIN formula1.gold.constructors_dim c
ON r.constructor_id = c.constructor_id
GROUP BY r.season,
         r.constructor_id,
         c.constructor_name,
         c.nationality;

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.view_constructor_standings
AS
WITH constructor_standings AS(

SELECT r.season,
       r.constructor_id,
       c.constructor_name,
       c.nationality,
       COUNT(*) AS race_start,
       SUM(r.points) AS total_points,
       COUNT_IF(r.is_win) AS number_of_wins,
       COUNT_IF(r.is_podium) AS number_of_podiums

FROM formula1.gold.fact_session_results r
JOIN formula1.gold.constructors_dim c
ON r.constructor_id = c.constructor_id
GROUP BY r.season,
         r.constructor_id,
         c.constructor_name,
         c.nationality)

SELECT season,
       constructor_id,
       constructor_name,
       nationality,
       race_start,
       total_points,
       number_of_wins,
       number_of_podiums,
       RANK() OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing_position
 FROM constructor_standings;
