SELECT COUNT(*)
FROM green_taxi
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-01-15'
AND CAST(lpep_dropoff_datetime AS DATE) = '2019-01-15';

SELECT CAST(lpep_pickup_datetime AS DATE)
FROM green_taxi
WHERE trip_distance = (SELECT MAX(trip_distance) FROM green_taxi);

SELECT passenger_count, COUNT(passenger_count)
FROM green_taxi
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-01-01'
GROUP BY passenger_count;

SELECT dz."Zone", gt.tip_amount
FROM green_taxi gt
LEFT JOIN zones pz 
ON gt."PULocationID" = pz."LocationID"
LEFT JOIN zones dz 
ON gt."DOLocationID" = dz."LocationID"
WHERE  pz."Zone" = 'Astoria'
ORDER BY tip_amount DESC
LIMIT 1;