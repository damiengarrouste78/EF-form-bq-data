--======================================================================================================================
-- Formation BigQuery 
-- EPSILON FRANCE damien.garrouste@epsilon-france.com
-- Introduction GIS
-- 2021
--======================================================================================================================


--1/ Représentation cartographique
-- Demo GIS : stations de vélo à londres, passer cette requête dans Geoviz
SELECT
 *, ST_GeogPoint(longitude, latitude)  AS WKT 
FROM
  `bigquery-public-data.london_bicycles.cycle_stations`
  ;

-- dans l'outil , dans l'onglet datadriven on va visualiser les stations selon leur capacité
-- chosiir fonction linear, fiel : docks count
-- dans domain faire 3 tranches : 10,20 et 30
-- dans range donner les couleurs


--2/ Fonctions spatiales 
-- Les jointures sur données spatiales : jointure selon une fonction géographique

--2.1 Table des stations à Londres
-- On va créer une table par station sur les location de vélo le matin en sem vs we
create temporary table hires_wemorning as
WITH PARAMS AS (
SELECT CAST("2017-01-01 00:00:00" AS TIMESTAMP) as date_deb,
 [1,7] AS WEEKEND_DAYS,
  [6,7,8] AS MORNING_TH
)
SELECT start_station_id,start_station_name,
case when EXTRACT(DAYOFWEEK FROM start_date) in unnest(WEEKEND_DAYS) then 1 else 0 end as flag_weekend
,case when EXTRACT(HOUR FROM TIME(start_date)) in unnest(MORNING_TH) then 1 else 0 end as flag_morning
,count(1) as hires
FROM  eu_dgr.public_london_cycle_hire,PARAMS
WHERE 
start_date >PARAMS.date_deb
GROUP BY start_station_id,start_station_name,flag_weekend,flag_morning
ORDER BY start_station_id,start_station_name,flag_weekend, flag_morning
;
-- WINDOW FUNCTION SUR LA FENETRE WE ET SEMAINE
CREATE OR REPLACE TABLE eu_dgr.public_london_matins as
SELECT start_station_id, start_station_name,flag_weekend, flag_morning,hires,hires_we as hires_soustotal, round(hires/hires_we*100) as ratio_morning
from  
(
select start_station_id,start_station_name,flag_weekend, flag_morning,hires,
SUM(hires) OVER(PARTITION BY start_station_id,start_station_name,flag_weekend) as hires_we
from  hires_wemorning
) as sous_req
order by start_station_id,start_station_name,flag_weekend, flag_morning

;

-- Agrégat final par stations 
CREATE OR REPLACE TABLE eu_dgr.public_london_matins as
select start_station_id, start_station_name,
max(case when flag_weekend=0 then ratio_morning else 0 end) as ratio_morning_we,
max(case when flag_weekend=1 then ratio_morning else 0 end) as ratio_morning_semaine,
sum(hires_soustotal) as tot_hires
from eu_dgr.public_london_matins
where flag_morning=1
group by start_station_id, start_station_name
;

-- Table de réf des stations à Londres
CREATE OR REPLACE TABLE eu_dgr.london_stations as
SELECT 
      id as station_id,
      name as station_name,
      docks_count,
      latitude,
	longitude,
	ST_GeogPoint(longitude, latitude)  AS WKT
    FROM 
     `bigquery-public-data.london_bicycles.cycle_stations`
    
	-- AJOUTE DES GEOGRAPHIES
CREATE OR REPLACE TABLE eu_dgr.public_london_matins as
SELECT a.*,b.docks_count, b.WKT
from eu_dgr.public_london_matins as a
INNER JOIN eu_dgr.london_stations as b
ON a.start_station_id = b.station_id
;


--2.2 Visualisation
-- Aller dans GEOVIZ
SELECT
 * 
FROM
  from eu_dgr.public_london_matins
  ;

--2.3 Fonctions Spatiales
-- Jointure avec les quarties de londres
SELECT
    b.name as quartier,
    b.geometry AS polygon,
	a.start_station_name as st
	a.docks_count, a.WKT
FROM
    eu_dgr.public_london_matins as a,
    eu_dgr.london_frontieres as b
WHERE ST_DWithin(a.wkt, b.geometry)

-- On fait la jointure : est ce que la station fait partie d'un des polygones ?
CREATE OR REPLACE TABLE `epsi-tech-dsc-formation-202005.eu_dgr.london_matins_shp` as
SELECT
    b.name as quartier,
    b.geometry AS polygon,
	a.start_station_name as station_name,
	a.docks_count, a.WKT
   
FROM
    eu_dgr.public_london_matins as a
    INNER JOIN
    eu_dgr.london_boundaries as b
    on ST_Within(a.wkt,b.geometry)
	--on ST_DWithin(a.wkt,b.geometry,0)
	order by  quartier
    ;

--  Cela permet de savoir par exemple combien de stations et combien  de docks sur un quartier (CAMDEN)

select quartier,count(*) as nb_stations, sum(docks_count) as nb_places
from eu_dgr.london_matins_shp
where quartier in ('Camden','KensingtonandChelsea')
group by quartier
;
-- pour mettre la geo , il faut faire cette manip
WITH SS_REQ AS (
select quartier,count(*) as nb_stations, sum(docks_count) as nb_places
from eu_dgr.london_matins_shp
where quartier in ('Camden','KensingtonandChelsea')
group by quartier
)
select
ss_req.quartier,nb_stations,nb_places,boundaries.polygon
from SS_REQ
INNER JOIN
eu_dgr.london_matins_shp as boundaries
ON SS_REQ.quartier=boundaries.quartier
;


-- aller dans geoviz
-- choisir fillcolor -> data driven interval sur nb places , mettre par exemple 2000 et 2100 dans domain et dans range du jaune et du rouge

	https://towardsdatascience.com/a-beginners-guide-to-google-s-bigquery-gis-46a1193499ef

	https://towardsdatascience.com/using-bigquerys-new-geospatial-functions-to-interpolate-temperatures-d9363ff19446


-- Distance sur un point précis : rechercher la station la plus proche
-- On se place dans soho 0.1323 51.5157

-- 5 stations max à moins de 500 metres
WITH params AS (
  SELECT ST_GeogPoint(-0.1323, 51.5157) AS center,
         1 AS maxdist_km
)
SELECT
    quartier,station_name, wkt,
    ST_Distance(wkt, params.center) AS dist_meters
  FROM
    eu_dgr.london_matins_shp,
    params
  WHERE ST_DWithin(wkt, params.center, params.maxdist_km*1000)
  order by dist_meters 
  LIMIT 5
  ;





-- récupérer une latitude longitude FR
curl "https://api-adresse.data.gouv.fr/search/?q=55+quai+grenelle+paris&limit=15"
curl "https://api-adresse.data.gouv.fr/search/?q=issy+moulineaux&limit=5"

-- reste la fonction geo hash : divise le territoire en carré de taille variable
select ST_GEOHASH(ST_GEOGPOINT(2.283957, 48.850527),4)
select ST_GEOHASH(ST_GEOGPOINT(2.26287, 48.8239),4)

-- centre du carre
select ST_GEOGPOINTFROMGEOHASH("u09t")
-- carre plus précis
select ST_GEOHASH(ST_GEOGPOINT(2.283957, 48.850527),8)
select ST_GEOHASH(ST_GEOGPOINT(2.26287, 48.8239),8)