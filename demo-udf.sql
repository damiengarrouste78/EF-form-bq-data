--======================================================================================================================
-- Formation BigQuery 
-- UDF
-- 10/2020
--======================================================================================================================
--https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions
--1 UDF temporary
-- fonction qui associe à un dayof week le jour en français
-- on cree un array de jour en francais et on extrait à tout x timestamp, on extrait le jour de la semaine et on extrailordonne sachant que le 1 signifie sunday! (et pas lundi!!
-- ORDINAL attend un indice commencant à 1 contrairement à OFFSET qui commence à0 )

-- test
select ['dimanche','lundi','mardi'][ORDINAL(1)] 
;

select ['dimanche','lundi','mardi','mercredi','jeudi','vendredi','samedi'][ORDINAL(EXTRACT(DAYOFWEEK from CAST("2017-01-01 00:00:00" AS TIMESTAMP)))] 
;

CREATE TEMPORARY FUNCTION dowInFrench(x TIMESTAMP) AS
(
['Dimanche','Lundi','Mardi','Mercredi','Jeudi','Vendredi','Samedi'][ORDINAL(EXTRACT(DAYOFWEEK from x))]
)
;
-- une fonction enn SQL,returns optionnel
-- application
SELECT 
EXTRACT(DAYOFWEEK from start_date) as hire_dow
,dowInFrench(start_date) as jour_semaine
,COUNT(1) as nb_hires
FROM eu_dgr.public_london_cycle_hire
WHERE rand()<0.01
GROUP BY hire_dow,jour_semaine
;


--2 UDF persistante
-- à enregistrer dans un dataset sachant que la location compte
-- elle sappliquera qu'a des dataset de la meme location


CREATE  FUNCTION eu_dgr.dowInFrench(x TIMESTAMP) AS
(
['Dimanche','Lundi','Mardi','Mercredi','Jeudi','Vendredi','Samedi'][ORDINAL(EXTRACT(DAYOFWEEK from x))]
)
;