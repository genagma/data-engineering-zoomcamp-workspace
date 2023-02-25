-- Count befor change the variable is_test_run to true
--staging
--select count(1) 
--from `taxi_rides_ny_dbt.stg_green_tripdata`; --100, 6304783


--select count(1) 
--from `taxi_rides_ny_dbt.stg_yellow_tripdata`; --100, 56100630

--prodcution
-- select count(1) 
-- from `production.stg_green_tripdata`; --100, 6304783


--select count(1) 
-- from `production.stg_yellow_tripdata`; --100, 56100630

-- Question 1
-- What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)
-- You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. You should find the views and models for querying in your DWH.
-- 61584787
select count(1)
from `production.fact_trips`
WHERE CAST(pickup_datetime AS DATE) >= CAST('2019/01/01' AS DATE FORMAT 'YYYY/MM/DD')
AND CAST(pickup_datetime AS DATE) <= CAST('2020/12/31' AS DATE FORMAT 'YYYY/MM/DD');

-- Question 3:
-- What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)
-- Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false). Filter records with pickup time in year 2019.

SELECT count(1) FROM `dez-20230113.staging.stg_fhv_tripdata`; -- 43244696


-- Question 4:
-- What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)
--Create a core model for the stg_fhv_tripdata joining with dim_zones. Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

SELECT count(1) FROM `dez-20230113.production.fact_fhv_trips`; --22998722
