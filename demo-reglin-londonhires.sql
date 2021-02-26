--======================================================================================================================
-- Formation BigQuery 
-- Demo ML durée location de velo
-- 10/2020
--======================================================================================================================

-- on regarde quels mois il y a en 2017


--select EXTRACT(YEAR FROM start_date) as annee,EXTRACT(MONTH FROM start_date) as mois,count(*)
--from eu_dgr.public_london_cycle_hire
--WHERE DATE(start_date) > DATE '2017-01-01'
--where substr(cast(rental_id as string),7,2)='00'
--group by  1,2
--order by 1,2
;
-- 438 k lignes en juin 2017 dernier mois
-- MODEL M1
-- on apprend sur le dernier mois 
-- les variables sont le nom de la station et jour semaine vs we et la tranche horaire
DECLARE date_deb DATE default '2017-06-01';

CREATE OR REPLACE MODEL eu_dgr.bicycle_model_linear
OPTIONS(input_label_cols=['duration'], model_type='linear_reg',DATA_SPLIT_METHOD = 'RANDOM' ,DATA_SPLIT_EVAL_FRACTION = 0.2)
AS
SELECT 
  duration
  , start_station_name
  , IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 'weekday', 'weekend') as dayofweek
  , FORMAT('%02d', EXTRACT(HOUR FROM start_date)) AS hourofday
FROM eu_dgr.public_london_cycle_hire
WHERE DATE(start_date) >  date_deb
;
-- prédiction
SELECT * FROM ML.PREDICT(MODEL eu_dgr.bicycle_model_linear,(
  SELECT
  'Vauxhall Cross, Vauxhall' AS start_station_name
  , 'weekend' as dayofweek
  , '17' AS hourofday)
)
;
-- examiner les coefficients : il y en a beaucoup! (804)
SELECT * FROM ML.WEIGHTS(MODEL eu_dgr.bicycle_model_linear);
DECLARE date_deb DATE default '2017-06-01';

-- trop de variables 800 et erreur en va de 890 

-- MODEL M1 on écrase le précédent
-- modele avec regularisation ridge et lasso, plus le coef est eleve plus c'est fort
CREATE OR REPLACE MODEL eu_dgr.bicycle_model_linear_m1
OPTIONS(input_label_cols=['duration'], model_type='linear_reg', DATA_SPLIT_METHOD = 'RANDOM',DATA_SPLIT_EVAL_FRACTION = 0.2 ,l1_reg=0.1,l2_reg=0.1)
AS
SELECT 
  duration
  , start_station_name
  , IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 'weekday', 'weekend') as dayofweek
  , FORMAT('%02d', EXTRACT(HOUR FROM start_date)) AS hourofday
FROM eu_dgr.public_london_cycle_hire
WHERE DATE(start_date) >  date_deb
;

-- MODEL M2 on écrase le précédent
-- le learning rate est  elevé, fixons le à une petite valeur sans qu'il puisse changer
-- on laisse le early stop car en le changeant ca nameliore pas forcément
CREATE OR REPLACE MODEL eu_dgr.bicycle_model_linear_m2
OPTIONS(input_label_cols=['duration'], model_type='linear_reg',DATA_SPLIT_METHOD = 'RANDOM',DATA_SPLIT_EVAL_FRACTION = 0.2 , EARLY_STOP = TRUE,MAX_ITERATIONS=10,l1_reg=0.1,l2_reg=0.1,
LEARN_RATE_STRATEGY ='CONSTANT',LEARN_RATE =0.05)
AS
SELECT 
  duration
  , start_station_name
  , IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 'weekday', 'weekend') as dayofweek
  , FORMAT('%02d', EXTRACT(HOUR FROM start_date)) AS hourofday
FROM eu_dgr.public_london_cycle_hire
WHERE DATE(start_date) >  date_deb
;


-- les metriques descendent MAE 778 etmedian 401
SELECT * FROM ML.WEIGHTS(MODEL eu_dgr.bicycle_model_linear_m2);

-- mettons en forme les coef qui sont dans un STRUCT
select expanded.category, expanded.weight
from  ML.WEIGHTS(MODEL eu_dgr.bicycle_model_linear_m2) as t0
Cross join unnest(t0.category_weights)  as expanded;

select count(*)
from  ML.WEIGHTS(MODEL eu_dgr.bicycle_model_linear_m2) as t0
Cross join unnest(t0.category_weights)  as expanded
where  expanded.weight<>0;

-- MODEL M2 on écrase le précédent avec le transform
-- TRANSFORM
CREATE OR REPLACE MODEL db_public.bicycle_model_m2
TRANSFORM(* EXCEPT(start_date),
CAST(EXTRACT(dayofweek from start_date) AS STRING)         as dayofweek,
CAST(EXTRACT(hour from start_date) AS STRING)as hourofday
)
OPTIONS(input_label_cols=['duration'],model_type='linear_reg',DATA_SPLIT_METHOD = 'RANDOM' ,DATA_SPLIT_EVAL_FRACTION = 0.2,
 EARLY_STOP = TRUE,MAX_ITERATIONS=10,l1_reg=0.05,l2_reg=0.05,LEARN_RATE_STRATEGY ='CONSTANT',LEARN_RATE =0.05) 

AS SELECT  duration, start_station_name, start_date 
FROM   eu_dgr.public_london_cycle_hire
WHERE DATE(start_date) >  date_deb
;


-- MODEL M3 
-- AJOUTONS DES TRANSFORMATIONS 
CREATE OR REPLACE MODEL  eu_dgr.model_bucketized
TRANSFORM(* 
EXCEPT(start_date)
,IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 AND 6, 'weekday','weekend') AS dayofweek
,ML.BUCKETIZE(EXTRACT(HOUR FROM  start_date),[5, 10, 17]) AS hourofday
)
OPTIONS(input_label_cols=['duration'],model_type='linear_reg',DATA_SPLIT_METHOD = 'RANDOM',DATA_SPLIT_EVAL_FRACTION = 0.5 ,
EARLY_STOP = TRUE,MAX_ITERATIONS=10,l1_reg=0.05,l2_reg=0.05,LEARN_RATE_STRATEGY ='CONSTANT',LEARN_RATE =0.05)  AS
SELECT  duration,  start_station_name,  start_date
FROM   eu_dgr.public_london_cycle_hire
WHERE DATE(start_date) >  date_deb
;


SELECT * FROM ML.PREDICT(MODEL eu_dgr.model_bucketized,(
  SELECT
  'Vauxhall Cross, Vauxhall' AS start_station_name
  ,cast('2015-01-31 09:30:00' as TIMESTAMP) AS start_date)
)

-- MODEL M4 
-- AJOUTONS DES TRANSFORMATIONS sur station name :on prend sa position hashée dans un espace reduit
CREATE OR REPLACE MODEL eu_dgr.model_fc_geo
 TRANSFORM(* EXCEPT(start_date,latitude,longitude)
       , ML.FEATURE_CROSS(STRUCT(
           IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 
              'weekday', 'weekend') as dayofweek, 
           ML.BUCKETIZE(EXTRACT(HOUR FROM start_date), 
              [5, 10, 17]) AS hr
         )) AS dayhr
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 2) AS start_station_loc2
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 4) AS start_station_loc4
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 6) AS start_station_loc6
)
OPTIONS(input_label_cols=['duration'],model_type='linear_reg',DATA_SPLIT_METHOD = 'RANDOM' ,DATA_SPLIT_EVAL_FRACTION = 0.5,
EARLY_STOP = TRUE,MAX_ITERATIONS=10,l1_reg=0.05,l2_reg=0.05,LEARN_RATE_STRATEGY ='CONSTANT',LEARN_RATE =0.05)
AS SELECT    duration  , latitude   , longitude   , start_date
FROM eu_dgr.public_london_cycle_hire as cycle_hire
JOIN `bigquery-public-data`.london_bicycles.cycle_stations
ON cycle_hire.start_station_id = cycle_stations.id
WHERE DATE(start_date) >  date_deb

;
SELECT * FROM ML.WEIGHTS(MODEL eu_dgr.model_fc_geo)
;

-- MODEL CLASSIFICATION
-- Calcul du taux de cible

DECLARE date_deb DATE default '2017-06-01';
select distinct IF(duration>1800,'long','court') as type_trajet , count(*)  FROM eu_dgr.public_london_cycle_hire 
WHERE DATE(start_date) >  date_deb and rand()<0.1 
group by type_trajet   
;

-- Taux de cible : 12,7%

-- MODEL  sur 10% et 90% en validation

CREATE OR REPLACE MODEL eu_dgr.model_typetrajet1
 TRANSFORM(* EXCEPT(start_date,latitude,longitude)
       , ML.FEATURE_CROSS(STRUCT(
           IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 
              'weekday', 'weekend') as dayofweek, 
           ML.BUCKETIZE(EXTRACT(HOUR FROM start_date), 
              [5, 10, 17]) AS hr
         )) AS dayhr
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 2) AS start_station_loc2
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 4) AS start_station_loc4
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 6) AS start_station_loc6
)
OPTIONS(input_label_cols=['type_trajet'],model_type='LOGISTIC_REG',DATA_SPLIT_METHOD = 'RANDOM' ,DATA_SPLIT_EVAL_FRACTION = 0.9,
EARLY_STOP = TRUE,MAX_ITERATIONS=20,l1_reg=1,l2_reg=1)
AS SELECT   
IF(duration>1800,'long','court') as type_trajet, latitude   , longitude   , start_date
FROM eu_dgr.public_london_cycle_hire as cycle_hire
JOIN `bigquery-public-data`.london_bicycles.cycle_stations
ON cycle_hire.start_station_id = cycle_stations.id
WHERE DATE(start_date) >  date_deb

;

--logloss 0.35 auyc 0.7164 

-- Performances globales du modèle avec un seuil
SELECT * FROM ML.EVALUATE (MODEL instacart.model_typetrajet1,
....
STRUCT(0.127))

-- Poids des variables
SELECT * from ML.WEIGHTS(MODEL instacart.model_typetrajet1);

-- prédiction
SELECT * FROM ML.PREDICT(MODEL eu_dgr.bicycle_model_linear,(
  SELECT
  'Vauxhall Cross, Vauxhall' AS start_station_name
  , 'weekend' as dayofweek
  , '17' AS hourofday)
)
;


-- augmenter les pénalisations
CREATE OR REPLACE MODEL eu_dgr.model_typetrajet2
 TRANSFORM(* EXCEPT(start_date,latitude,longitude)
       , ML.FEATURE_CROSS(STRUCT(
           IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 
              'weekday', 'weekend') as dayofweek, 
           ML.BUCKETIZE(EXTRACT(HOUR FROM start_date), 
              [5, 10, 17]) AS hr
         )) AS dayhr
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 2) AS start_station_loc2
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 4) AS start_station_loc4
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 6) AS start_station_loc6
)
OPTIONS(input_label_cols=['type_trajet'],model_type='LOGISTIC_REG',DATA_SPLIT_METHOD = 'RANDOM' ,DATA_SPLIT_EVAL_FRACTION = 0.9,
EARLY_STOP = TRUE,MAX_ITERATIONS=20,l1_reg=10,l2_reg=10)
AS SELECT   
IF(duration>1800,'long','court') as type_trajet, latitude   , longitude   , start_date
FROM eu_dgr.public_london_cycle_hire as cycle_hire
JOIN `bigquery-public-data`.london_bicycles.cycle_stations
ON cycle_hire.start_station_id = cycle_stations.id
WHERE DATE(start_date) >  date_deb 
;

-- Poids des variables
SELECT * from ML.WEIGHTS(MODEL instacart.model_typetrajet2);


CREATE OR REPLACE MODEL eu_dgr.model_typetrajet3
 TRANSFORM(* EXCEPT(start_date,latitude,longitude)
       , ML.FEATURE_CROSS(STRUCT(
           IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 
              'weekday', 'weekend') as dayofweek, 
           ML.BUCKETIZE(EXTRACT(HOUR FROM start_date), 
              [5, 10, 17]) AS hr
         )) AS dayhr
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 2) AS start_station_loc2
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 4) AS start_station_loc4
       , ST_GeoHash(ST_GeogPoint(latitude, longitude), 6) AS start_station_loc6
)
OPTIONS(input_label_cols=['type_trajet'],model_type='BOOSTED_TREE_CLASSIFIER',DATA_SPLIT_METHOD = 'RANDOM' ,DATA_SPLIT_EVAL_FRACTION = 0.9,
COLSAMPLE_BYNODE =10,
NUM_PARALLEL_TREE = 100, 
EARLY_STOP = TRUE,MAX_ITERATIONS=1,l1_reg=0,l2_reg=0
 TREE_METHOD = 'HIST',SUBSAMPLE = 0.67,MIN_TREE_CHILD_WEIGHT=30,MAX_TREE_DEPTH =8
)
AS SELECT   
IF(duration>1800,'long','court') as type_trajet, latitude   , longitude   , start_date
FROM eu_dgr.public_london_cycle_hire as cycle_hire
JOIN `bigquery-public-data`.london_bicycles.cycle_stations
ON cycle_hire.start_station_id = cycle_stations.id
WHERE DATE(start_date) >  date_deb 
;