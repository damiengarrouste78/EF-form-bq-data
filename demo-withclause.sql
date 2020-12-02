-- 1
-- WITH SOUS REQUETE

create table instacart.orders_scope as 
with sous_req as (
select distinct user_id
from instacart.orders
where eval_set = 'train')
select orders.* 
from instacart.orders as orders
join sous_req as ss
on ss.user_id=orders.user_id
; 

-- countif ne marche pas booleen ????
--Select 
--COUNTIF(order_dow in unnest([0,6]), 1, 0) AS nb_we
--FROM instacart.orders_scope
--;

-- ca ca marche
Select 
case when order_dow in unnest([0,6]) then 1 else 0 end as weekend,
COUNT(1)
FROM instacart.orders_scope
where order_dow in unnest([0,6])
group by weekend
;
 -- 2 -- Les parametres dans le with pour la lisibilité
-- array donc entre []
WITH PARAMS AS (
  SELECT [0,6] AS WEEKEND_DAYS,
  [7,8,9,10] AS MORNING_TH
)
-- la requete
SELECT 
  case when order_dow in unnest(WEEKEND_DAYS) then 1 else 0 end as weekend,
  case when order_hour_of_day in unnest(MORNING_TH) then 1 else 0 end as morning,
COUNT(1)
FROM instacart.orders_scope
group by weekend,morning ;   -- autre exemple SELECT CAST("2017-06-01 00:00:00" AS TIMESTAMP) as date_deb;

WITH PARAMS AS (
SELECT CAST("2017-06-01 00:00:00" AS TIMESTAMP) as date_deb
)
SELECT start_station_name      , AVG(duration) as avg_duration
FROM       eu_dgr.public_london_cycle_hire,PARAMS
WHERE UPPER(start_station_name) IN UNNEST(["PARK LANE , HYDE PARK","HYDE PARK CORNER, HYDE PARK"])
AND start_date >PARAMS.date_deb
GROUP BY start_station_name
;