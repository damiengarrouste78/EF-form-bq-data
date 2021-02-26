--======================================================================================================================
-- Formation BigQuery 
-- window functions
-- 10/2020
--======================================================================================================================

-------------
PARTIE 1 LONDON hires
-------------
-- flag week et matin : est ce qu'il y a plus de locations en matinée en semaine ?
create temporary table hires_wemorning as
WITH PARAMS AS (
SELECT CAST("2017-01-01 00:00:00" AS TIMESTAMP) as date_deb,
 [1,7] AS WEEKEND_DAYS,
  [6,7,8] AS MORNING_TH
)
SELECT
case when EXTRACT(DAYOFWEEK FROM start_date) in unnest(WEEKEND_DAYS) then 1 else 0 end as flag_weekend
,case when EXTRACT(HOUR FROM TIME(start_date)) in unnest(MORNING_TH) then 1 else 0 end as flag_morning
,count(1) as hires
FROM  eu_dgr.public_london_cycle_hire,PARAMS
WHERE 
start_date >PARAMS.date_deb
GROUP BY flag_weekend,flag_morning
ORDER BY flag_weekend, flag_morning
;


-- WINDOW FUNCTION SUR LA FENETRE WE ET SEMAINE
SELECT flag_weekend, flag_morning,hires,hires_we as hires_soustotal, round(hires/hires_we*100) as ratio_morning
from  
(
select flag_weekend, flag_morning,hires,
SUM(hires) OVER(PARTITION BY flag_weekend) as hires_we
from  hires_wemorning
) as sous_req
order by flag_weekend, flag_morning
;

-------------
PARTIE  2 JEUX OLYMPIQUES
-------------

create or replace table eu_dgr.nage_jo(nageur STRING,epreuve STRING,temps FLOAT64);
INSERT eu_dgr.nage_jo (nageur,epreuve,temps)
VALUES('Andriy Hovorov','50m',21.74),
('Anthony Ervin','50m',21.4),
('Benjamin Proud','50m',	21.68),
('Brad Tandy','50m',	21.79),
('Bruno Fratus','50m',	21.79),
('Caeleb Dressel','100m',	48.02),
('Cameron McEvoy','100m',	48.12),
('Duncan Scott','100m',	48.01),
('Florent Manaudou','50m',	21.41),
('Kyle Charmers','100m',	47.58),
('Marcelo Chierighini','100m',48.41),
('Nathan Adrian','50m',21.49),
('Nathan Adrian','100m',47.85),
('Pieter Timmers','100m',47.8),
('Santo Condorelli','100m',47.88),
('Simonas Bilis','50m',22.08)
;


-- Classement (fonction rank ) par temps (order by) et par epreuve (partition)
SELECT 
nageur, epreuve, temps, 
RANK() OVER(PARTITION BY epreuve ORDER BY temps) as Rang 
FROM eu_dgr.nage_jo 
;
-- percentiles 
SELECT DISTINCT epreuve, Q1, MED, Q3
FROM 
( SELECT 
nageur, epreuve, temps, 
PERCENTILE_CONT(Temps, 0.25) OVER(PARTITION BY epreuve) as Q1,
PERCENTILE_CONT(Temps, 0.50) OVER(PARTITION BY epreuve) as MED,
PERCENTILE_CONT(Temps, 0.75) OVER(PARTITION BY epreuve) as Q3
FROM eu_dgr.nage_jo)
;
