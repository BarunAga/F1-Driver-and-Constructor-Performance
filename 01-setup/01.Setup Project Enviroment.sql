--This file creates External Location, Unity Catalog, Schemas and Volumes for this project. In this project Storage Credential created manually.


-- Creating External Location
     -- EXTERNAL_LOCATION is an URL that you can get at Azure Portal
CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dlbrn_formula1
URL 'EXTERNAL_LOCATION'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the formula1 container';

--Create catalog named formula1
CREATE CATALOG  IF NOT EXISTS  formula1
     MANAGED LOCATION 'EXTERNAL_LOCATION' 
     COMMENT 'Formula1 catalog has been created.';


--Create schemas named landing(holds files to perform transactions), bronze, silver, gold
CREATE SCHEMA  IF NOT EXISTS  formula1.landing;
CREATE SCHEMA  IF NOT EXISTS  formula1.bronze
MANAGED LOCATION 'EXTERNAL_LOCATION/bronze'
     COMMENT 'Bronze schema has been created.';
CREATE SCHEMA  IF NOT EXISTS  formula1.silver
MANAGED LOCATION 'EXTERNAL_LOCATION/silver'
     COMMENT 'Silver schema has been created.';
CREATE SCHEMA  IF NOT EXISTS  formula1.gold
MANAGED LOCATION 'EXTERNAL_LOCATION/gold'
     COMMENT 'Gold schema has been created.';


--Create volume files(this is required for accessing landing files through Azure storage)
CREATE  EXTERNAL  VOLUME  IF NOT EXISTS  formula1.landing.files
     LOCATION 'EXTERNAL_LOCATION/landing'
     COMMENT 'Landing volume has been created.'
