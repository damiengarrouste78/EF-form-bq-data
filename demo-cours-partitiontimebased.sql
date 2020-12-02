--======================================================================================================================
-- Formation BigQuery 
-- Optimisation des perfs des requetes
-- 10/2020
-- Exemple google
--======================================================================================================================


--1 REQUETE DE DEPART

--https://cloud.google.com/bigquery/docs/creating-column-partitions?hl=en#creating_a_partitioned_table_from_a_query_result
-- Durée moyenne par stations sur les réservations longues
-- Cette requête coute 1 go ce qui est assez énorme

SELECT       start_station_name      , AVG(duration) as avg_duration    FROM       `bigquery-public-data`.london_bicycles.cycle_hire    WHERE       upper(start_station_name) LIKE CONCAT("%",'HYDE',"%")      AND duration BETWEEN 1800 AND 7200	  AND start_date >'2017-06-01 00:00:00'    GROUP BY start_station_name

-- la table fait 24 M delignes de 2015 à 2017 
-- la requete préc comptabilise 1000 Mo car les filtres n'empechent pas la lecture de moins de données
-- créons une table partitionnée pour optimiser sur start_date

--2 TABLE PARTITIONNEE POUR ETRE ENSUITE REQUETEE A VOLONTE
     
bq mk --table --schema=schema --time_partitioning_type=unit_time  project_id:dataset.table

bq \
--location=EU query \
--destination_table epsi-tech-dsc-formation-202005:eu_dgr.public_london_cycle_hire \
--time_partitioning_field start_date \
--time_partitioning_type DAY \
--use_legacy_sql=false \
'SELECT
   start_station_id,start_station_name,end_station_name,duration,start_date,end_date
   FROM       `bigquery-public-data`.london_bicycles.cycle_hire
'

--3 exéc la requete en interactif

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

-- On a que 18 Mo soit 55 x moins !!!