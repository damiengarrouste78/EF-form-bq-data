--======================================================================================================================
-- Formation BigQuery 
-- 10/2020
-- Elodie MARREC 
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
	
	
--3/ créer les tables correspondant au périmètre (les cclients et leurs commandes présents dans le fichier train)
create table instacart.orders_scope as 
select *
from instacart.orders
where user_id in 
(select distinct user_id
from instacart.orders
where eval_set = 'train') 
; 

-- équivalent avec WITH CLAUSE (pas plus rapide car stages identiques)
-- create table instacart.orders_scope as 
-- with sous_req as (
-- select distinct user_id
-- from instacart.orders
-- where eval_set = 'train')
-- select orders.* 
-- from instacart.orders as orders
-- join sous_req as ss
-- on ss.user_id=orders.user_id
; 

-- : Ne garder que les lignes de commandes des clients du train set : 
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

-----------------------------------------------------
-- Export de la table instacart.orders_struct en json dans GCS : gs://epsi-tech-dsc-formation-202005/data/instacart/orders_struct.json


-----------------------------------------------------
-- Remplacer les ? par les bonnes instructions !!!!
create table instacart.orders_products as
select 
	order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order, 
	?
from instacart.orders_struct
cross join ? as details 


------------------------------------------------------------------------------------------------------------------------
-- EXERCICE 3
------------------------------------------------------------------------------------------------------------------------

-- 1/ Créer un flag ‘organic’ dans la table products à partir du product_name 
--	  Compter le nombre de produits associé

select 
	case when ?(product_name) ? '%organic%' then 1 else 0 end as flag_organic
	, count(*)
from instacart.products 
group by flag_organic;

-- 0	44 652
-- 1	 5 036


-- 2/ Isoler les produits du rayon 56 (diapers wipes = couches / lingettes) et ne contenant pas le mot « wipes » puis créer un champ ‘size’ en extrayant les formes « Size 2 » du product_name
--	  Donner la répartition des produits considérés par size

select size, count(*)
from 
	(
	select product_name, regexp_extract(lower(product_name), ?) as size
	from instacart.products
	where ? = 56 and lower(product_name) not like ?
	) 
group by size
order by size

-- size
-- 1	 	 5
-- 2		 8
-- 3		23	
-- 4		20
-- 5		16
-- 6		12
-- null		56


------------------------------------------------------------------------------------------------------------------------
-- EXERCICE 4
------------------------------------------------------------------------------------------------------------------------

-- 1/ Au sein de chaque département, donner le top 3 des produits par nombre de commande
SELECT
qry.*
from
(

	select 
		department,
		product_name, 
		nb_commandes, 
		? over(partition by ? order by ? ?) as rk
	from
		(
		select 
		  c.department, 
			product_name, 
			count(*) as nb_commandes
		from instacart.orders_products as a
		left join instacart.products as b on a.product_id = b.product_id
		left join instacart.departments as c on b.department_id = c.department_id 
		group by department, product_name 
    ) 
     order by department, rk
) as qry
where qry.rk<=3
;


------------------------------------------------------------------------------------------------------------------------
-- EXERCICE 5
------------------------------------------------------------------------------------------------------------------------


-- methode manuelle 1/ Créer une table au niveau client, avec le nb de commandes passées dans les départements 'frozen', 'bakery' et 'produce'
--	  Calculer les quartiles Q1, MED, Q3 du nb de commandes passées par client sur les départements 'frozen', 'bakery' et 'produce' et les enregistrer dans une table
--	  Ajouter à la table client une version en classes ordinales des nb de commandes passées dans les départements 'frozen', 'bakery' et 'produce' à l'aide des quantiless enregistrés 

-- (la version complète permet de créer la source du Use Case Segmentation
--  exporté : gs://epsi-tech-dsc-formation-202005/data/instacart/user_classes.csv)

create table instacart.users as 
select 
	count(distinct case when department = 'frozen' 				then order_id end) as nb_commandes_frozen,
  	count(distinct case when department = 'bakery' 				then order_id end) as nb_commandes_bakery,
  	count(distinct case when department = 'produce' 			then order_id end) as nb_commandes_produce ,
	count(distinct case when department = 'alcohol' 			then order_id end) as nb_commandes_alcohol,
  	count(distinct case when department = 'international' 		then order_id end) as nb_commandes_international,
  	count(distinct case when department = 'beverages' 			then order_id end) as nb_commandes_beverages,
	count(distinct case when department = 'pets' 				then order_id end) as nb_commandes_pets,
  	count(distinct case when department = 'dry goods pasta' 	then order_id end) as nb_commandes_dry_goods_pasta,
  	count(distinct case when department = 'bulk' 				then order_id end) as nb_commandes_bulk,
	count(distinct case when department = 'personal care' 		then order_id end) as nb_commandes_personal_care,
  	count(distinct case when department = 'meat seafood' 		then order_id end) as nb_commandes_meat_seafood,
  	count(distinct case when department = 'pantry' 				then order_id end) as nb_commandes_pantry,
	count(distinct case when department = 'breakfast' 			then order_id end) as nb_commandes_breakfast,
  	count(distinct case when department = 'canned goods' 		then order_id end) as nb_commandes_canned_goods,
  	count(distinct case when department = 'dairy eggs' 			then order_id end) as nb_commandes_dairy_eggs,
	count(distinct case when department = 'household' 			then order_id end) as nb_commandes_household,
  	count(distinct case when department = 'babies' 				then order_id end) as nb_commandes_babies,
  	count(distinct case when department = 'snacks' 				then order_id end) as nb_commandes_snacks,
	count(distinct case when department = 'deli' 				then order_id end) as nb_commandes_deli,
  	count(distinct case when department in ('other','missing') 	then order_id end) as nb_commandes_others
from instacart.orders_products as a
left join instacart.products as b on a.product_id = b.product_id
left join instacart.departments as c on b.department_id = c.department_id 
group by user_id



------------------------------------------------------------------------------------------------------------------------
-- EXERCICE 5 alternative avec un script qui fait les transpositions
------------------------------------------------------------------------------------------------------------------------

-- on cree 1 ligne par commande et departement de produit (pour ne pas compter deux fois si deux produits du meme departement dans la meme commande)


-- et si dans les values il y a des espaces cela fait un bugge car la variable cree nest pas correcte d'ou le replace
create or replace table instacart.users_commandes as 
select 
distinct user_id,order_id, replace(department,' ','_') as departement,1 as commande
from instacart.orders_products as a
left join instacart.products as b on a.product_id = b.product_id
left join instacart.departments as c on b.department_id = c.department_id 
;


-- le script  pivot est une procedure stocke 
--- la proc stck pivot genère une chaine sql qui est ensuite execute 
CALL instacart.pivot(
  'instacart.users_commandes' # source table
  , 'instacart.users_nb_commandes' # destination table
  , ['user_id'] # row_ids
  , 'departement' # pivot_col_name
  , 'commande' # pivot_col_value
  , 30 # max_columns
  , 'SUM' # aggregation
  , '' # optional_limit
);

-- drop table if exists instacart.users_commandes

------------------------------------------------------------------------------------------------------------------------
-- EXERCICE 6
------------------------------------------------------------------------------------------------------------------------

	select distinct 'frozen' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			?(nb_commandes_frozen			, 0.25) over() as cut_1,
			?(nb_commandes_frozen			, 0.50) over() as cut_2,
			?(nb_commandes_frozen			, 0.75) over() as cut_3
		from instacart.users
		)
union all 
			?
;

--- ne pas scroller sinon vous verrez la réponse ...




























------------------------------------------------------------------------------------------------------------------------
-- EXERCICE 6 corrigé sur tous les univers
------------------------------------------------------------------------------------------------------------------------

create or replace table instacart.quantiles as
	select distinct 'frozen' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_frozen			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_frozen			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_frozen			, 0.75) over() as cut_3
		from instacart.users
		)
union all 
	select distinct 'bakery' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_bakery			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_bakery			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_bakery			, 0.75) over() as cut_3
		from instacart.users
		)
union all 
	select distinct 'produce' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_produce			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_produce			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_produce			, 0.75) over() as cut_3
		from instacart.users
		)	

union all 
	select distinct 'alcohol' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_alcohol			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_alcohol			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_alcohol			, 0.75) over() as cut_3
		from instacart.users
		)			
union all 
	select distinct 'international' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_international		, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_international		, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_international		, 0.75) over() as cut_3
		from instacart.users
		)	
union all 
	select distinct 'beverages' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_beverages			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_beverages			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_beverages			, 0.75) over() as cut_3
		from instacart.users
		)			
union all 
	select distinct 'pets' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_pets				, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_pets				, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_pets				, 0.75) over() as cut_3
		from instacart.users
		)		
union all 
	select distinct 'dry_goods_pasta' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_dry_goods_pasta	, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_dry_goods_pasta	, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_dry_goods_pasta	, 0.75) over() as cut_3
		from instacart.users
		)			
union all 
	select distinct 'bulk' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_bulk				, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_bulk				, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_bulk				, 0.75) over() as cut_3
		from instacart.users
		)			
union all 
	select distinct 'personal_care' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_personal_care		, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_personal_care		, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_personal_care		, 0.75) over() as cut_3
		from instacart.users
		)			
union all 
	select distinct 'meat_seafood' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_meat_seafood		, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_meat_seafood		, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_meat_seafood		, 0.75) over() as cut_3
		from instacart.users
		)								
union all 
	select distinct 'pantry' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_pantry			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_pantry			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_pantry			, 0.75) over() as cut_3
		from instacart.users
		)	
union all 
	select distinct 'breakfast' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_breakfast			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_breakfast			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_breakfast			, 0.75) over() as cut_3
		from instacart.users
		)	
union all 
	select distinct 'canned_goods' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_canned_goods		, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_canned_goods		, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_canned_goods		, 0.75) over() as cut_3
		from instacart.users
		)	
union all 
	select distinct 'dairy_eggs' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_dairy_eggs		, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_dairy_eggs		, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_dairy_eggs		, 0.75) over() as cut_3
		from instacart.users
		)	
union all 
	select distinct 'household' as department, cut_1, cut_2, cut_3
	from 
		(
		select 
			percentile_cont(nb_commandes_household			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_household			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_household			, 0.75) over() as cut_3
		from instacart.users	
		)		
union all 	
	select distinct 'babies' as department, cut_1, cut_2, cut_3	
	from 	
		(	
		select 	
			percentile_cont(nb_commandes_babies			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_babies			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_babies			, 0.75) over() as cut_3
		from instacart.users	
		)				
union all 	
	select distinct 'snacks' as department, cut_1, cut_2, cut_3	
	from 	
		(	
		select 	
			percentile_cont(nb_commandes_snacks			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_snacks			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_snacks			, 0.75) over() as cut_3
		from instacart.users	
		)				
union all 	
	select distinct 'deli' as department, cut_1, cut_2, cut_3	
	from 	
		(	
		select 	
			percentile_cont(nb_commandes_deli				, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_deli				, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_deli				, 0.75) over() as cut_3
		from instacart.users	
		)			
union all 	
	select distinct 'others' as department, cut_1, cut_2, cut_3	
	from 	
		(	
		select 	
			percentile_cont(nb_commandes_others			, 0.25) over() as cut_1,
			percentile_cont(nb_commandes_others			, 0.50) over() as cut_2,
			percentile_cont(nb_commandes_others			, 0.75) over() as cut_3
		from instacart.users	
		)		
;

------------------------------------------------------------------------------------------------------------------------
-- EXERCICE 7 corrigé sur tous les univers
------------------------------------------------------------------------------------------------------------------------

create table instacart.users_classes as 
select 

  user_id,

  case
    when nb_commandes_frozen <= frozen.cut_1 then 1
    when nb_commandes_frozen <= frozen.cut_2 then 2
    when nb_commandes_frozen <= frozen.cut_3 then 3
    else 4
  end as nb_commandes_frozen, 

  case
    when nb_commandes_bakery <= bakery.cut_1 then 1
    when nb_commandes_bakery <= bakery.cut_2 then 2
    when nb_commandes_bakery <= bakery.cut_3 then 3
    else 4
  end as nb_commandes_bakery, 

  case
    when nb_commandes_produce <= produce.cut_1 then 1
    when nb_commandes_produce <= produce.cut_2 then 2
    when nb_commandes_produce <= produce.cut_3 then 3
    else 4
  end as nb_commandes_produce  , 

  case
    when nb_commandes_alcohol <= alcohol.cut_1 then 1
    when nb_commandes_alcohol <= alcohol.cut_2 then 2
    when nb_commandes_alcohol <= alcohol.cut_3 then 3
    else 4
  end as nb_commandes_alcohol, 

  case
    when nb_commandes_international <= international.cut_1 then 1
    when nb_commandes_international <= international.cut_2 then 2
    when nb_commandes_international <= international.cut_3 then 3
    else 4
  end as nb_commandes_international, 

  case
    when nb_commandes_beverages <= beverages.cut_1 then 1
    when nb_commandes_beverages <= beverages.cut_2 then 2
    when nb_commandes_beverages <= beverages.cut_3 then 3
    else 4
  end as nb_commandes_beverages, 

  case
    when nb_commandes_pets <= pets.cut_1 then 1
    when nb_commandes_pets <= pets.cut_2 then 2
    when nb_commandes_pets <= pets.cut_3 then 3
    else 4
  end as nb_commandes_pets, 

  case
    when nb_commandes_dry_goods_pasta <= dry_goods_pasta.cut_1 then 1
    when nb_commandes_dry_goods_pasta <= dry_goods_pasta.cut_2 then 2
    when nb_commandes_dry_goods_pasta <= dry_goods_pasta.cut_3 then 3
    else 4
  end as nb_commandes_dry_goods_pasta, 

  case
    when nb_commandes_bulk <= bulk.cut_1 then 1
    when nb_commandes_bulk <= bulk.cut_2 then 2
    when nb_commandes_bulk <= bulk.cut_3 then 3
    else 4
  end as nb_commandes_bulk, 

  case
    when nb_commandes_personal_care <= personal_care.cut_1 then 1
    when nb_commandes_personal_care <= personal_care.cut_2 then 2
    when nb_commandes_personal_care <= personal_care.cut_3 then 3
    else 4
  end as nb_commandes_personal_care, 

  case
    when nb_commandes_meat_seafood <= meat_seafood.cut_1 then 1
    when nb_commandes_meat_seafood <= meat_seafood.cut_2 then 2
    when nb_commandes_meat_seafood <= meat_seafood.cut_3 then 3
    else 4
  end as nb_commandes_meat_seafood, 

  case
    when nb_commandes_pantry <= pantry.cut_1 then 1
    when nb_commandes_pantry <= pantry.cut_2 then 2
    when nb_commandes_pantry <= pantry.cut_3 then 3
    else 4
  end as nb_commandes_pantry, 

  case
    when nb_commandes_breakfast <= breakfast.cut_1 then 1
    when nb_commandes_breakfast <= breakfast.cut_2 then 2
    when nb_commandes_breakfast <= breakfast.cut_3 then 3
    else 4
  end as nb_commandes_breakfast, 

  case
    when nb_commandes_canned_goods <= canned_goods.cut_1 then 1
    when nb_commandes_canned_goods <= canned_goods.cut_2 then 2
    when nb_commandes_canned_goods <= canned_goods.cut_3 then 3
    else 4
  end as nb_commandes_canned_goods, 

  case
    when nb_commandes_dairy_eggs <= dairy_eggs.cut_1 then 1
    when nb_commandes_dairy_eggs <= dairy_eggs.cut_2 then 2
    when nb_commandes_dairy_eggs <= dairy_eggs.cut_3 then 3
    else 4
  end as nb_commandes_dairy_eggs, 

  case
    when nb_commandes_household <= household.cut_1 then 1
    when nb_commandes_household <= household.cut_2 then 2
    when nb_commandes_household <= household.cut_3 then 3
    else 4
  end as nb_commandes_household, 

  case
    when nb_commandes_babies <= babies.cut_1 then 1
    when nb_commandes_babies <= babies.cut_2 then 2
    when nb_commandes_babies <= babies.cut_3 then 3
    else 4
  end as nb_commandes_babies, 

  case
    when nb_commandes_snacks <= snacks.cut_1 then 1
    when nb_commandes_snacks <= snacks.cut_2 then 2
    when nb_commandes_snacks <= snacks.cut_3 then 3
    else 4
  end as nb_commandes_snacks, 

  case
    when nb_commandes_deli <= deli.cut_1 then 1
    when nb_commandes_deli <= deli.cut_2 then 2
    when nb_commandes_deli <= deli.cut_3 then 3
    else 4
  end as nb_commandes_deli, 

  case
    when nb_commandes_others <= others.cut_1 then 1
    when nb_commandes_others <= others.cut_2 then 2
    when nb_commandes_others <= others.cut_3 then 3
    else 4
  end as nb_commandes_others 

from instacart.users as a 
left join instacart.quantiles as frozen 			on frozen.department = 'frozen'
left join instacart.quantiles as bakery 			on bakery.department = 'bakery'
left join instacart.quantiles as produce 			on produce.department = 'produce'
left join instacart.quantiles as alcohol 			on alcohol.department = 'alcohol'
left join instacart.quantiles as international 		on international.department = 'international'
left join instacart.quantiles as beverages 			on beverages.department = 'beverages'
left join instacart.quantiles as pets 				on pets.department = 'pets'
left join instacart.quantiles as dry_goods_pasta 	on dry_goods_pasta.department = 'dry_goods_pasta'
left join instacart.quantiles as bulk 				on bulk.department = 'bulk'
left join instacart.quantiles as personal_care 		on personal_care.department = 'personal_care'
left join instacart.quantiles as meat_seafood 		on meat_seafood.department = 'meat_seafood'
left join instacart.quantiles as pantry 			on pantry.department = 'pantry'
left join instacart.quantiles as breakfast 			on breakfast.department = 'breakfast'
left join instacart.quantiles as canned_goods 		on canned_goods.department = 'canned_goods'
left join instacart.quantiles as dairy_eggs 		on dairy_eggs.department = 'dairy_eggs'
left join instacart.quantiles as household 			on household.department = 'household'
left join instacart.quantiles as babies 			on babies.department = 'babies'
left join instacart.quantiles as snacks 			on snacks.department = 'snacks'
left join instacart.quantiles as deli 				on deli.department = 'deli'
left join instacart.quantiles as others 			on others.department = 'others'
;