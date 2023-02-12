-- Q1
SELECT COUNT(*) 
FROM `learning-370118.dezoomcamp.fhv`;

-- Q2
SELECT COUNT(DISTINCT(Affiliated_base_number)) 
FROM `learning-370118.dezoomcamp.fhv_external`;

-- Q2
SELECT COUNT(DISTINCT(Affiliated_base_number)) 
FROM `learning-370118.dezoomcamp.fhv`;

-- Q3
SELECT COUNT(*) 
FROM `learning-370118.dezoomcamp.fhv`
WHERE PUlocationID IS NULL AND
DOlocationID IS NULL;

-- Q5
SELECT COUNT(DISTINCT(Affiliated_base_number)) 
FROM `learning-370118.dezoomcamp.fhv`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

-- Q5
CREATE OR REPLACE TABLE `learning-370118.dezoomcamp.fhv_partitioned`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS (
  SELECT * FROM `learning-370118.dezoomcamp.fhv`
);

-- Q5
SELECT COUNT(DISTINCT(Affiliated_base_number)) 
FROM `learning-370118.dezoomcamp.fhv_partitioned`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
