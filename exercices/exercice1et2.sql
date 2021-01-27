--======================================================================================================================
-- Formation BigQuery 
-- 10/2020
-- corrigés 1 et 2
--======================================================================================================================


------------------------------------------------------------------------------------------------------------------------
-- Données
------------------------------------------------------------------------------------------------------------------------

-- Données Kaggle : https://www.kaggle.com/c/instacart-market-basket-analysis
-- stockés dans GCP dans GCS gs://epsi-tech-dsc-formation-202005/data/instacart/

-- Chargement dans le dataset instacart via la webUI, avec la reconnaissance automatique de schema 
-- aisles.csv 					==> table instacart.aisles
-- departments.csv				==> table instacart.departments
-- products.csv					==> table instacart.products
-- orders.csv					==> table instacart.orders
-- order_products__prior.csv	==> table instacart.order_products_prior
-- order_products__train.csv	==> table instacart.order_products_train


------------------------------------------------------------------------------------------------------------------------
-- EXERCICE 1 Chargement et  Adaptation des données 
------------------------------------------------------------------------------------------------------------------------

-- PERIMETRE

-- Données d'origine 
-- Dans la table orders 3 types de commandes (colonne eval_set) : 
	-- train : dernière commande des clients du train set 
	-- test : dernière commande des clients du test set
	-- prior : commandes précédentes des clients du train set ou du test set
-- Dans la table order_products_prior : les lignes des commandes de la table orders avec eval_set = prior
-- Dans la table order_products_train : les lignes des commandes de la table orders avec eval_set = train
-- Les lignes des commandes de la table orders avec eval_set = test ne sont pas fournies 

-- EXERCICE 1
-- Modif : Ne garder que les  commandes des clients du train set

-- 1/ Créer un dataset instacart

-- 2/ Créer les tables à partir des fichiers stockés dans GCS (gs://epsi-tech-dsc-formation-202005/data/instacart/)
	-- aisles.csv			==> instacart.aisles
	-- departments.csv		==> instacart.departments		
	-- products.csv			==> instacart.products


create table instacart.orders_scope as 
select *
from instacart.orders
where user_id in 
(select distinct user_id
from instacart.orders
where eval_set = 'train') 
; 

-- équivalent avec WITH CLAUSE (pas plus rapide car stages identiques)
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

-- Modif : Ne garder que les lignes de commandes des clients du train set : deux tables
create table instacart.orders_products_scope as 
select a.*
from instacart.order_products_train as a
inner join instacart.orders_scope as b on a.order_id = b.order_id
union all 
select c.*
from instacart.order_products_prior as c
inner join instacart.orders_scope as d on c.order_id = d.order_id
; 

-- on a pas de date pour partionner , on va donc faire un clustering sur id 

-- on peut pas passer le clustering en ui https://cloud.google.com/bigquery/docs/creating-clustered-tables#console_1
bq query --use_legacy_sql=false \
'CREATE TABLE
   instacart.orders_products_scope2
 CLUSTER BY
   order_id,product_id AS
 SELECT
   *
 FROM
   `instacart.orders_products_scope`'
;
drop table if exists instacart.orders_products_scope;





------------------------------------------------------------------------------------------------------------------------
-- EXERCICE 2
------------------------------------------------------------------------------------------------------------------------


-- Créer une table orders_products en ‘applatissant’ l’array details de la table orders_struct 
-- STRUCTURE DES DONNEES
-- Transformer la table des commandes et des lignes de commandes en un json avec imbrication 
create table instacart.orders_struct as 
select 
a.order_id,
a.user_id, 
a.eval_set, 
a.order_number, 
a.order_dow, 
a.order_hour_of_day, 
a.days_since_prior_order,
ARRAY_AGG(STRUCT(product_id, add_to_cart_order, reordered)) as details
from instacart.orders_scope as a 
left join instacart.orders_products_scope2 as b on a.order_id = b.order_id
group by a.order_id, a.user_id, a.eval_set, a.order_number, a.order_dow, a.order_hour_of_day, a.days_since_prior_order

-- Export de la table instacart.orders_struct en json dans GCS : gs://epsi-tech-dsc-formation-202005/data/instacart/orders_struct.json

create table instacart.orders_products as
select 
	order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order, 
	details.product_id, details.add_to_cart_order, details.reordered
from instacart.orders_struct
cross join unnest(details) as details

