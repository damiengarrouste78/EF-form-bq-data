--======================================================================================================================
-- Formation BigQuery 
-- Optimisation des perfs des requetes
-- 10/2020
--======================================================================================================================
-- Declarer une variable pour stocker des noms de stations dans un array
DECLARE top_stations ARRAY<STRING>;
-- Remplir l'array avec les 3 plus grosses stations
SET top_stations = (
WITH compte as  ( 
select start_station_name,count(*) as nb_hires
FROM eu_dgr.public_london_cycle_hire
WHERE start_date >'2017-06-01 00:00:00'
GROUP BY start_station_name)
SELECT ARRAY_AGG(start_station_name ORDER BY nb_hires DESC LIMIT 3)
    FROM 
  
	  COMPTE as cpt
    
);
select top_stations;
-- Requete : on filtre sur la liste des top stations 
SELECT
start_station_name, count(*) as nb_hires 
FROM
eu_dgr.public_london_cycle_hire
WHERE start_station_name IN UNNEST(top_stations)
AND start_date >'2017-06-01 00:00:00'
GROUP BY start_station_name
;
-- PS : pour que la variable top stations soit connu il faut éxécuter en meme temps les statements