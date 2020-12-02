select count(*)
from `bigquery-public-data`.london_bicycles.cycle_hire
group by  EXTRACT(YEAR FROM start_date) 
;

CREATE OR REPLACE MODEL db_public.bicycle_model_linear
OPTIONS(input_label_cols=['duration'], model_type='linear_reg')
AS
SELECT 
  duration
  , start_station_name
  , IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 'weekday', 'weekend') as dayofweek
  , FORMAT('%02d', EXTRACT(HOUR FROM start_date)) AS hourofday
FROM `bigquery-public-data`.london_bicycles.cycle_hire
WHERE DATE(start_date) > DATE '2015-12-01'
;
SELECT * FROM ML.PREDICT(MODEL db_public.bicycle_model_linear,(
  SELECT
  'Vauxhall Cross, Vauxhall' AS start_station_name
  , 'weekend' as dayofweek
  , '17' AS hourofday)
)
;

SELECT * FROM ML.PREDICT(MODEL db_public.bicycle_model_linear,(
  SELECT
  'Vauxhall Cross, Vauxhall' AS start_station_name
  , 'weekend' as dayofweek
  , '17' AS hourofday)
)
;
SELECT * FROM ML.WEIGHTS(MODEL db_public.bicycle_model_linear,


CREATE OR REPLACE MODEL db_public.bicycle_model OPTIONS(input_label_cols=['duration'],         model_type='linear_reg’) 
TRANSFORM(
SELECT * EXCEPT(start_date),
CAST(EXTRACT(dayofweek from start_date) AS STRING)         as dayofweek,
CAST(EXTRACT(hour from start_date) AS STRING)as hourofday
)
AS SELECT  duration, start_station_name, start_date 
FROM   `bigquery-public-data.london_bicycles.cycle_hire`
;

CREATE OR REPLACE MODEL  bike_model.model_bucketized
TRANSFORM(* 
EXCEPT(start_date),  
IF (EXTRACT(dayofweek FROM start_date) BETWEEN 2 AND 6,      'weekday','weekend') AS dayofweek,

ML.BUCKETIZE(EXTRACT(HOUR FROM  start_date),[5, 10, 17]) AS hourofday )
OPTIONS  (input_label_cols=['duration'],    model_type='linear_reg') AS

SELECT  duration,  start_station_name,  start_date

FROM  `bigquery-public-data`.london_bicycles.cycle_hire 
;

CREATE OR REPLACE MODEL bike_model.model_fc_geo
 TRANSFORM(duration
       , ML.FEATURE_CROSS(STRUCT(
           IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 
              'weekday', 'weekend') as dayofweek, 
           ML.BUCKETIZE(EXTRACT(HOUR FROM start_date), 
              [5, 10, 17]) AS hr
         )) AS dayhr
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 4) AS start_station_loc4
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 8) AS start_station_loc8
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 16) AS start_station_loc16
)
OPTIONS(input_label_cols=['duration'], model_type='linear_reg')
AS SELECT    duration  , latitude   , longitude   , start_date
FROM `bigquery-public-data`.london_bicycles.cycle_hire
JOIN `bigquery-public-data`.london_bicycles.cycle_stations
ON cycle_hire.start_station_id = cycle_stations.id
;
