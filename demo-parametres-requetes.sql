--======================================================================================================================
-- Formation BigQuery 
-- requetes paramétrées
-- 10/2020
-- Exemple google
--======================================================================================================================

-- 1/
-- Tester sa requete sans parametres
-- détails https://cloud.google.com/bigquery/docs/parameterized-queries#bq
-- Durée moyenne par stations sur les réservations longues

SELECT 
      start_station_name
      , AVG(duration) as avg_duration
    FROM 
      eu_dgr.public_london_cycle_hire
    WHERE 
      upper(start_station_name) LIKE CONCAT("%",'HYDE',"%")
      AND duration BETWEEN 1800 AND 7200
	  AND start_date >'2017-06-01 00:00:00'
    GROUP BY start_station_name
	

-- 1'/
-- écrire la requete avec la convention with parametres au debut

-- passser un param, la date est en timestamp car start_date est dans ce type
SELECT CAST("2017-06-01 00:00:00" AS TIMESTAMP) as date_deb;

WITH PARAMS AS (
SELECT CAST("2017-06-01 00:00:00" AS TIMESTAMP) as date_deb
)
SELECT start_station_name      , AVG(duration) as avg_duration
FROM       eu_dgr.public_london_cycle_hire,PARAMS
WHERE UPPER(start_station_name) IN UNNEST(["PARK LANE , HYDE PARK","HYDE PARK CORNER, HYDE PARK"])
AND start_date >PARAMS.date_deb
GROUP BY start_station_name
;


-- 2/      
-- syntaxe generale 
-- ne pas mettre de ' dans la requete car séparé par des 'query'
-- https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_query
--	--parameter=nom:TYPE:VALUE
-- le type peut etre vide alors signifie STRING
-- si param positionnel alors pas de nom


-- 3/
-- named parameters : les paramètres sont nommés
bq query \
--use_legacy_sql=false \
--parameter=station::HYDE \
--parameter=MIN_DURATION:INT64:1800 \
--parameter=MAX_DURATION:INT64:7200 \
'SELECT 
      start_station_name
      , AVG(duration) as avg_duration
    FROM 
      eu_dgr.public_london_cycle_hire
    WHERE 
      UPPER(start_station_name) LIKE CONCAT("%", @STATION, "%")
      AND duration BETWEEN @MIN_DURATION AND @MAX_DURATION
	  AND start_date >"2017-06-01 00:00:00"
    GROUP BY start_station_name
'

-- 4/
-- named parameters : les paramètres sont nommés avec une datetime
-- on en profite pour tester une fonction date
bq query \
--use_legacy_sql=false \
--parameter=station::HYDE \
--parameter=MIN_DURATION:INT64:1800 \
--parameter=MAX_DURATION:INT64:7200 \
--parameter='START_TIME:TIMESTAMP:2017-01-01 00:00:00' \
'
    SELECT 
      start_station_name,
	  EXTRACT(MONTH FROM start_date) as mois,
	  AVG(duration) as avg_duration
    FROM 
     eu_dgr.public_london_cycle_hire
    WHERE 
      UPPER(start_station_name) LIKE CONCAT("%", @STATION, "%")
      AND duration BETWEEN @MIN_DURATION AND @MAX_DURATION
	  AND  start_date > @START_TIME 
    GROUP BY start_station_name,EXTRACT(MONTH FROM start_date)
'

-- 5/
-- positionnal parameter
-- on teste la fonction initcap 
bq query \
--use_legacy_sql=false \
--parameter=:STRING:Hyde \
'
        SELECT 
          start_station_name
          , AVG(duration) as avg_duration
        FROM 
          eu_dgr.public_london_cycle_hire
        WHERE 
          INITCAP(start_station_name) LIKE CONCAT("%", ?, "%")
          AND start_date >"2017-06-01 00:00:00"
		  AND duration BETWEEN 1800 AND 7200
        GROUP BY start_station_name
'

-- 6/
-- paramètre sous forme de liste : la liste doit etre exprimé sous forme de STRUCT

-- On teste la requete d'abord pour comprendre comment on eput utiliser une liste array dans un where
SELECT 
      start_station_name
      , AVG(duration) as avg_duration
    FROM 
      eu_dgr.public_london_cycle_hire
    WHERE 
    UPPER(start_station_name) IN UNNEST(["PARK LANE , HYDE PARK","HYDE PARK CORNER, HYDE PARK"])      
	  AND start_date >"2017-06-01 00:00:00"
    GROUP BY start_station_name
;

-- mais c un struct le parametre donc testons manip struct
--https://stackoverflow.com/questions/44812619/in-biqquery-how-to-filter-an-array-of-struct-on-matching-multiple-fields-in-the
WITH data AS (
  SELECT 
  [STRUCT<x STRING, y STRING>("PARK LANE , HYDE PARK","1"), ("PARK LANE , HYDE PARK","2")]
  # bien mettre des [ ] pour que ce soit un array de struct
  AS SOURCE)
SELECT *
FROM data
WHERE EXISTS (
  SELECT 1 FROM UNNEST(source) AS s 
)
-- La requete avec un array de STRUCT 
SELECT 
      start_station_name
      , AVG(duration) as avg_duration
    FROM 
      eu_dgr.public_london_cycle_hire, UNNEST( [STRUCT<x STRING, y STRING>("PARK LANE , HYDE PARK","1"), ("HYDE PARK CORNER, HYDE PARK","2")]) as s
    WHERE s.x=UPPER(start_station_name)	
	  AND start_date >"2017-06-01 00:00:00"
    GROUP BY start_station_name
;
-- on parametre
bq query \
--use_legacy_sql=false \
--parameter='liste_stations:STRUCT<x STRING, y STRING>:{"x":"PARK LANE , HYDE PARK", "y": "HYDE PARK CORNER, HYDE PARK"}' \
'SELECT 
      start_station_name
      , AVG(duration) as avg_duration
    FROM 
      eu_dgr.public_london_cycle_hire, UNNEST( [STRUCT<x STRING, y STRING>("PARK LANE , HYDE PARK","1"), ("HYDE PARK CORNER, HYDE PARK","2")]) as s
    WHERE s.x=UPPER(start_station_name)
	AND start_date >"2017-06-01 00:00:00"
    GROUP BY start_station_name
   '


bq query \
--use_legacy_sql=false \
--parameter='liste_stations:STRUCT<x STRING, y STRING>:("PARK LANE , HYDE PARK","1"), ("PARK LANE , HYDE PARK","2")' \
 '
 SELECT *
FROM eu_dgr.public_london_cycle_hire
WHERE UPPER(start_station_name) IN (
  SELECT 1 FROM UNNEST(@liste_stations) 
)'