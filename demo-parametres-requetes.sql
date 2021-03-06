--======================================================================================================================
-- Formation BigQuery 
-- requetes param�tr�es
-- 10/2020

--======================================================================================================================

-- Comment passer des param�tres qui ne sont pas �crits dans le programme � l�exec d'une requete 


-- 1/ -- Tester  requete sans parametres
-- d�tails https://cloud.google.com/bigquery/docs/parameterized-queries#bq
-- Dur�e moyenne par stations sur les r�servations longues

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
	

-- 1'/ -- Rappel on peut �crire la requete avec la convention with o� on encapsule les "param�tres" au debut
-- voir �galement la d�mo script pour d�clarer le parametre commeune variable

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


-- 2/    LES  PARAMETRES 
-- syntaxe generale est --	--parameter=nom:TYPE:VALUE
-- le type peut etre vide alors signifie STRING
-- si param positionnel alors pas de nom
-- ne pas mettre de ' dans la requete car s�par� par des 'query'
-- https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_query


-- 3/
-- named parameters : les param�tres sont nomm�s

-- selon 
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
-- named parameters : les param�tres sont nomm�s avec une datetime
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
-- param�tre sous forme de liste : la liste doit etre exprim� sous forme de STRUCT

-- On teste la requete d'abord pour comprendre comment on eput utiliser une liste array dans un where
-- la liste contien deux noms de stations qui sont dans un array
select ["PARK LANE , HYDE PARK","HYDE PARK CORNER, HYDE PARK"];
SELECT * FROM UNNEST(["PARK LANE , HYDE PARK","HYDE PARK CORNER, HYDE PARK"]) 

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


-- On va passer la liste de stations en parametre comme un struct, passons d'abord en parametre avec la clause with
--https://stackoverflow.com/questions/44812619/in-biqquery-how-to-filter-an-array-of-struct-on-matching-multiple-fields-in-the
WITH data AS (
  SELECT 
  [STRUCT<x STRING, y STRING>("PARK LANE , HYDE PARK","station 1"), ("PARK LANE , HYDE PARK","station 2")]
  # bien mettre des [ ] pour que ce soit un array de struct
  AS SOURCE)
SELECT *
FROM data
WHERE EXISTS (
  SELECT 1 FROM UNNEST(source) AS s 
)
;
-- La requete avec un array de STRUCT 
SELECT 
      s.y,
      start_station_name
      , AVG(duration) as avg_duration
    FROM 
      eu_dgr.public_london_cycle_hire, UNNEST( [STRUCT<x STRING, y STRING>("PARK LANE , HYDE PARK","station 1"), ("HYDE PARK CORNER, HYDE PARK","station 2")]) as s
    WHERE s.x=UPPER(start_station_name)	
	  AND start_date >"2017-06-01 00:00:00"
    GROUP BY s.y,start_station_name
;

-- on parametre, on teste dabord sans passer le parametre
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


   -- on parametre, 
bq query \
--use_legacy_sql=false \
--parameter='liste_stations:STRUCT<x STRING, y STRING>:{"x":"PARK LANE , HYDE PARK", "y": "HYDE PARK CORNER, HYDE PARK"}' \
'SELECT 
      start_station_name
      , AVG(duration) as avg_duration
    FROM 
      eu_dgr.public_london_cycle_hire, UNNEST( [@liste_stations]) as s
    WHERE s.x=UPPER(start_station_name)
	AND start_date >"2017-06-01 00:00:00"
    GROUP BY start_station_name
   '
