--======================================================================================================================
-- Formation BigQuery Dr Pierre Ricaud
-- 11/2020
--======================================================================================================================

------------------------------------------------------------------------------------------------------------------------
-- USE Case segmentation
------------------------------------------------------------------------------------------------------------------------
-- Construction d'une Segmentation des utilisateurs en fonction de leur historique d’achat (mix produits)
-- A partir de la table users_classes qui contient une ligne par client et une colonne par département de produits 
-- avec le n° du quartile du nombre de commandes passées dans le département où se trouve le client

-- A noter :
-- Les méthodes ACP, CAH, Plus proches voisins n'étant pas disponibles sous BQ, seule une K-Means est réalisée


-- Construction de l'échantillon
CREATE TABLE instacart.users_classes_echant as
SELECT * FROM
(SELECT *, rand() as random FROM instacart.users_classes ORDER BY random)
LIMIT 10000

-- Construction du modèle de Kmeans
-- Les options qui peuvent être utilisées dans le CREATE MODEL pour un modèle de Kmeans :
-- MODEL TYPE : pour spécifier le type de modèle utilisé
-- NUM_CLUSTERS : nombre de clusters => si aucune valeur n'est choisie BigQuery ML choisira une valeur par défaut raisonnable en fonction du nombre total de lignes compris dans les données d'entraînement
-- KMEANS_INIT_METHOD : Méthode d'initialisation des centroides => valeur par défaut RANDOM
-- KMEANS_INIT_COL : Colonne utilisée pour identifier les centroides
-- DISTANCE_TYPE : Métrique utilisée pour calcul des distances
-- STANDARDIZE_FEATURES : standardisation des données => valeur par défaut TRUE

-- Premier test de modèle
CREATE OR REPLACE MODEL instacart.Kmeans_MOD1
OPTIONS
  ( MODEL_TYPE='KMEANS') AS
SELECT * EXCEPT(user_id, random)
FROM instacart.users_classes_echant
-- 5 segments en sortie avec peu de différence entre les segments 1 et 2 : segments avec des volumes d’achat fort dans tous les départements
-- et les segments 4 et 5 : segments avec des niveaux intermédiaire d’achat 

-- La requête ML.EVALUATE donne les mêmes résultats que les métriques de l'onglet "Evaluation" dans la table de modèle
SELECT *
FROM ML.EVALUATE(MODEL instacart.Kmeans_MOD1)



-- Deuxième modèle testé : Construction d’un deuxième modèle en faisant varier le nombre de clusters pour déterminer le nombre de clusters optimal

-- On commence à 3 classes
DECLARE NUM_CLUSTERS INT64 DEFAULT 3;
DECLARE MIN_ERROR FLOAT64 DEFAULT 1000.0;
DECLARE BEST_NUM_CLUSTERS INT64 DEFAULT -1;
DECLARE MODEL_NAME STRING;
DECLARE error FLOAT64 DEFAULT 0;

select * from instacart.__TABLES__ limit 1; 

-- max 5 classes
WHILE NUM_CLUSTERS < 6 DO
-- on cree un nom de modele avec le num clusters
SET MODEL_NAME = CONCAT('instacart.Kmeans_MOD2_', 
                        CAST(NUM_CLUSTERS AS STRING));

						-- on lance le CREATE
EXECUTE IMMEDIATE format("""CREATE OR REPLACE MODEL %s --instacart.TEST_MOD
  OPTIONS(model_type='kmeans', 
          num_clusters=(%d), 
          standardize_features = true) AS
  SELECT * EXCEPT(user_id, random)
  FROM instacart.users_classes_echant;""", MODEL_NAME, NUM_CLUSTERS);


EXECUTE IMMEDIATE format("""SELECT davies_bouldin_index FROM ML.EVALUATE(MODEL %s)""", MODEL_NAME) into error;


  IF error < MIN_ERROR THEN
     SET MIN_ERROR = error;
     SET BEST_NUM_CLUSTERS = NUM_CLUSTERS;
  END IF;


SET NUM_CLUSTERS = NUM_CLUSTERS + 1;

select BEST_NUM_CLUSTERS;


END WHILE;


-- Le modèle à sélectionner est celui dont le nombre de clusters est contenu dans BEST_NUM_CLUSTERS (indice Davies Bouldin)



-- Choix du modèle et application sur toute la base
CREATE OR REPLACE TABLE instacart.users_classes_segment AS
SELECT * EXCEPT(nearest_centroids_distance)
FROM ML.PREDICT( MODEL instacart.Kmeans_MOD2_3, (SELECT * FROM instacart.users_classes))

-- Caractérisation statistique des segments (moyennes par segments sur les achats)
CREATE OR REPLACE TABLE instacart.users_classes_segment_detail as
SELECT CENTROID_ID, avg(nb_commandes_frozen) as nb_commandes_frozen, avg(nb_commandes_bakery) as  nb_commandes_bakery,	
                    avg(nb_commandes_produce) as  nb_commandes_produce,	avg(nb_commandes_alcohol) as  nb_commandes_alcohol,	
                    avg(nb_commandes_international) as  nb_commandes_international,	avg(nb_commandes_beverages) as  nb_commandes_beverages,
                    avg(nb_commandes_pets) as  nb_commandes_pets, avg(nb_commandes_dry_goods_pasta) as  nb_commandes_dry_goods_pasta,
                    avg(nb_commandes_bulk) as  nb_commandes_bulk,	avg(nb_commandes_personal_care) as  nb_commandes_personal_care,	
                    avg(nb_commandes_meat_seafood) as  nb_commandes_meat_seafood, avg(nb_commandes_pantry) as  nb_commandes_pantry,
                    avg(nb_commandes_breakfast) as  nb_commandes_breakfast,	avg(nb_commandes_canned_goods) as  nb_commandes_canned_goods,
                    avg(nb_commandes_dairy_eggs) as  nb_commandes_dairy_eggs,	avg(nb_commandes_household) as  nb_commandes_household,
                    avg(nb_commandes_babies) as nb_commandes_babies, avg(nb_commandes_snacks) as  nb_commandes_snacks,
                    avg(nb_commandes_deli) as  nb_commandes_deli, avg(nb_commandes_others) as nb_commandes_others
FROM instacart.users_classes_segment
GROUP BY CENTROID_ID;



------------------------------------------------------------------------------------------------------------------------
-- USE Case score
------------------------------------------------------------------------------------------------------------------------
-- Scoring des clients ayant acheté le produit 30391 (Organic Cucumber) et prédiction s'ils le rachètent ou non lors du dernier achat

----------------------------------
-- Construction du périmètre clients et de la cible
----------------------------------
-- Suppression du dernier achat de la base des achats
CREATE TABLE instacart.HORS_DERNIER_ACHAT AS
SELECT a.*
FROM instacart.orders_products as a left outer join instacart.order_products_train as b
on a.order_id = b.order_id
where b.order_id is null

-- Recherche des clients ayant déjà acheté le produit 30391 
CREATE TABLE instacart.CUST30391 AS
select distinct user_id
from instacart.HORS_DERNIER_ACHAT
where product_id = 30391
group by user_id;
-- 15 646 clients

-- Extraction des derniers achats avec le produit
CREATE TABLE instacart.PDT30391_DERNIERACHAT AS
select distinct order_id, 1 as achat_30391
from instacart.order_products_train
where product_id = 30391;

-- Recherche si achat du produit dans le dernier achat pour construire la cible 1/0
CREATE TABLE instacart.CIBLE_CUST30391 AS
select distinct a.user_id, case when max(c.achat_30391) = 1 THEN 1 ELSE 0 END as cible
from instacart.CUST30391 as a left join (SELECT distinct user_id, order_id FROM instacart.orders_products) as b
on a.user_id = b.user_id
left join instacart.PDT30391_DERNIERACHAT as c
on b.order_id = c.order_id
group by a.user_id;
-- 15 646 clients


----------------------------------
--Construction des variables explicatives
----------------------------------
-- Nombre de commandes, produits et departments
CREATE OR REPLACE TABLE instacart.INFOS_CONSO AS
SELECT a.user_id,
       count(distinct order_id) as nb_commandes_total,
       count(distinct case when b.product_id = 30391 then order_id end) as nb_commandes_30391,
       count(distinct b.product_id) as nb_produits_dist,
       count(*) as volume_produits,
       count(distinct department) as nb_department

FROM instacart.CIBLE_CUST30391 as a inner join instacart.HORS_DERNIER_ACHAT as b
on a.user_id = b.user_id
left join instacart.products as c
on b.product_id = c.product_id
left join instacart.departments as d
on c.department_id = d.department_id
group by a.user_id;
-- 15 646 clients


-- Délai moyen, min et max entre deux commandes
CREATE OR REPLACE TABLE instacart.INFOS_DELAI_GLOBAL AS
SELECT a.user_id,
       avg(days_since_prior_order) as moy_delai_comm, -- la valeur null de la 1ère commande n'est pas prise en compte dans le calcul de la moyenne
       min(days_since_prior_order) as min_delai_comm,
       max(days_since_prior_order) as max_delai_comm
FROM instacart.CIBLE_CUST30391 as a inner join (select distinct user_id, order_id, days_since_prior_order from instacart.HORS_DERNIER_ACHAT) as b
on a.user_id = b.user_id
group by a.user_id, a.cible;
-- 15 646 clients

-- Liste des départements achetés par chaque client et nombre de commandes associées
CREATE OR REPLACE TABLE instacart.INFOS_DEPARTMENT_GLOBAL AS
SELECT distinct a.user_id, a.cible, b.order_id, d.department, 1 as achat
FROM instacart.CIBLE_CUST30391 as a inner join instacart.HORS_DERNIER_ACHAT as b
on a.user_id = b.user_id
left join instacart.products as c
on b.product_id = c.product_id
left join instacart.departments as d
on c.department_id = d.department_id;

CREATE OR REPLACE TABLE instacart.INFOS_DEPARTMENT_PIVOT AS
SELECT distinct user_id, 
                max(case when department = 'frozen' then 1 else 0 end) as flag_frozen, 
                max(case when department = 'dry goods pasta' then 1 else 0 end) as flag_dry_goods_pasta, 
                max(case when department = 'snacks' then 1 else 0 end) as flag_snacks, 
                max(case when department = 'international' then 1 else 0 end) as flag_international, 
                max(case when department = 'beverages' then 1 else 0 end) as flag_beverages, 
                max(case when department = 'breakfast' then 1 else 0 end) as flag_breakfast, 
                max(case when department = 'produce' then 1 else 0 end) as flag_produce, 
                max(case when department = 'dairy eggs' then 1 else 0 end) as flag_dairy_eggs,
                max(case when department = 'canned goods' then 1 else 0 end) as flag_canned_goods, 
                max(case when department = 'pantry' then 1 else 0 end) as flag_pantry, 
                max(case when department = 'meat seafood' then 1 else 0 end) as flag_meat_seafood, 
                max(case when department = 'household' then 1 else 0 end) as flag_household, 
                max(case when department = 'deli' then 1 else 0 end) as flag_deli, 
                max(case when department = 'other' then 1 else 0 end) as flag_other, 
                max(case when department = 'personal care' then 1 else 0 end) as flag_personal_care, 
                max(case when department = 'bakery' then 1 else 0 end) as flag_bakery, 
                max(case when department = 'babies' then 1 else 0 end) as flag_babies, 
                max(case when department = 'missing' then 1 else 0 end) as flag_missing, 
                max(case when department = 'bulk' then 1 else 0 end) as flag_bulk, 
                max(case when department = 'alcohol' then 1 else 0 end) as flag_alcohol, 
                max(case when department = 'pets' then 1 else 0 end) as flag_pets,
                
                count(distinct case when department = 'frozen' then order_id end) as nb_comm_frozen, 
                count(distinct case when department = 'dry goods pasta' then order_id end) as nb_comm_dry_goods_pasta, 
                count(distinct case when department = 'snacks' then order_id end) as nb_comm_snacks, 
                count(distinct case when department = 'international' then order_id end) as nb_comm_international, 
                count(distinct case when department = 'beverages' then order_id end) as nb_comm_beverages, 
                count(distinct case when department = 'breakfast' then order_id end) as nb_comm_breakfast, 
                count(distinct case when department = 'produce' then order_id end) as nb_comm_produce, 
                count(distinct case when department = 'dairy eggs' then order_id end) as nb_comm_dairy_eggs,
                count(distinct case when department = 'canned goods' then order_id end) as nb_comm_canned_goods, 
                count(distinct case when department = 'pantry' then order_id end) as nb_comm_pantry, 
                count(distinct case when department = 'meat seafood' then order_id end) as nb_comm_meat_seafood, 
                count(distinct case when department = 'household' then order_id end) as nb_comm_household, 
                count(distinct case when department = 'deli' then order_id end) as nb_comm_deli, 
                count(distinct case when department = 'other' then order_id end) as nb_comm_other, 
                count(distinct case when department = 'personal care' then order_id end) as nb_comm_personal_care, 
                count(distinct case when department = 'bakery' then order_id end) as nb_comm_bakery, 
                count(distinct case when department = 'babies' then order_id end) as nb_comm_babies, 
                count(distinct case when department = 'missing' then order_id end) as nb_comm_missing, 
                count(distinct case when department = 'bulk' then order_id end) as nb_comm_bulk, 
                count(distinct case when department = 'alcohol' then order_id end) as nb_comm_alcohol, 
                count(distinct case when department = 'pets' then order_id end) as nb_comm_pets
                
                
FROM instacart.INFOS_DEPARTMENT_GLOBAL
group by user_id, cible;
-- 15 646 clients


-- Achat du produit 30391 ou non dans la dernière commande du périmètre de construction des variables explicatives
CREATE OR REPLACE TABLE instacart.INFOS_ACHATDERCOMM AS
select distinct a.user_id, max(case when c.product_id = 30391 Then 1 else 0 end) as Achat_30391_dernierachat
FROM instacart.CIBLE_CUST30391 as a inner join 
(select distinct user_id, max(order_number) as derniere_comm
from instacart.HORS_DERNIER_ACHAT as b
group by user_id) as b
on a.user_id = b.user_id
left join instacart.HORS_DERNIER_ACHAT as c
on b.user_id = c.user_id and b.derniere_comm = c.order_number
group by a.user_id;
-- 15 646 clients


-- Nombre moyen de produits et départements achetés
CREATE OR REPLACE TABLE instacart.INFOS_MOYDEPPDTS AS
SELECT a.user_id,
       avg(nb_department) as nb_department_moy,
       avg(nb_product_id) as nb_produit_moy

FROM instacart.CIBLE_CUST30391 as a inner join 
(SELECT distinct user_id, order_id, count(distinct b.product_id) as nb_product_id, count(distinct department) as nb_department
from instacart.HORS_DERNIER_ACHAT as b left join instacart.products as c
on b.product_id = c.product_id
left join instacart.departments as d
on c.department_id = d.department_id
group by user_id, order_id) as e
on a.user_id = e.user_id
group by a.user_id;
-- 15 646 clients



-- Moyenne, Min et Max du rang du Produit dans chaque commande
CREATE OR REPLACE TABLE instacart.INFOS_RANG AS
SELECT user_id, 
       avg(add_to_cart_order) as moy_rang_comm, 
       min(add_to_cart_order) as min_rang_comm, 
       max(add_to_cart_order) as max_rang_comm
FROM
(SELECT DISTINCT a.user_id, order_id, add_to_cart_order
FROM instacart.CIBLE_CUST30391 as a inner join instacart.HORS_DERNIER_ACHAT as b
on a.user_id = b.user_id
where product_id = 30391)
GROUP BY user_id
-- 15 646 clients


-- Délai depuis dernière commande (en utilisant la variable days_since_prior_order de la toute dernière commande qui est
-- dans la table instacart.order_products_train
CREATE OR REPLACE TABLE instacart.INFOS_DELAI_DER_COMM AS
SELECT DISTINCT a.user_id, b.days_since_prior_order as delai_dercomm
FROM instacart.CIBLE_CUST30391 as a inner join instacart.orders_products as b
on a.user_id = b.user_id
inner join instacart.order_products_train as c
on b.order_id = c.order_id;
-- 15 646 clients



-- Délai depuis dernière commande du produit 30391
CREATE OR REPLACE TABLE instacart.INFOS_DELAI_DERACHAT_30391 AS
SELECT distinct a1.user_id, sum(a1.days_since_prior_order) as delai_derachat_30391
from (select distinct a.user_id, order_id, order_number, days_since_prior_order
from instacart.CIBLE_CUST30391 as a inner join instacart.HORS_DERNIER_ACHAT as b
on a.user_id = b.user_id) as a1
left join 
(select a.user_id, max(order_number) as order_number_max
from instacart.CIBLE_CUST30391 as a inner join instacart.HORS_DERNIER_ACHAT as b
on a.user_id = b.user_id
where b.product_id = 30391
group by a.user_id) as a2
on a1.user_id = a2.user_id
where a2.order_number_max < a1.order_number
group by a1.user_id
order by a1.user_id
-- 11 731 lignes

CREATE OR REPLACE TABLE instacart.INFOS_DELAI_DERACHAT_30391_M AS
SELECT a.user_id, 
       case when delai_derachat_30391>= 0 then delai_derachat_30391 else 0 end as delai_dernier_achat_30391
from instacart.CIBLE_CUST30391 as a left join instacart.INFOS_DELAI_DERACHAT_30391 as b
on a.user_id = b.user_id
-- 15 646 clients


-- Regroupement de toutes les infos
CREATE OR REPLACE TABLE instacart.REGROUP_INFOS AS
select a.*, b.* EXCEPT (user_id), c.* EXCEPT (user_id), d.* EXCEPT (user_id), e.* EXCEPT (user_id), 
       f.* EXCEPT (user_id), g.* EXCEPT (user_id), h.* EXCEPT (user_id), i.* EXCEPT (user_id)
FROM instacart.CIBLE_CUST30391 as a left join instacart.INFOS_CONSO as b on a.user_id = b.user_id
left join instacart.INFOS_DELAI_GLOBAL as c on a.user_id = c.user_id
left join instacart.INFOS_DEPARTMENT_PIVOT as d on a.user_id = d.user_id
left join instacart.INFOS_ACHATDERCOMM as e on a.user_id = e.user_id
left join instacart.INFOS_MOYDEPPDTS as f on a.user_id = f.user_id
left join instacart.INFOS_RANG as g on a.user_id = g.user_id
left join instacart.INFOS_DELAI_DER_COMM as h on a.user_id = h.user_id
left join instacart.INFOS_DELAI_DERACHAT_30391_M as i on a.user_id = i.user_id
-- 15 646 lignes


-- Construction de nouvelles variables et suppression des variables non utilisées pour la modélisation
CREATE OR REPLACE TABLE instacart.REGROUP_INFOS_M AS
select * EXCEPT(nb_commandes_30391, nb_comm_frozen, nb_comm_dry_goods_pasta, nb_comm_snacks, nb_comm_international, nb_comm_beverages, nb_comm_breakfast, nb_comm_produce, 
                nb_comm_dairy_eggs, nb_comm_canned_goods, nb_comm_pantry, nb_comm_meat_seafood, nb_comm_household, nb_comm_deli, nb_comm_other, nb_comm_personal_care,
                nb_comm_bakery, nb_comm_babies, nb_comm_missing, nb_comm_bulk, nb_comm_alcohol, nb_comm_pets),
      nb_commandes_30391 / nb_commandes_total as Part_commandes_30391,
      nb_comm_frozen / nb_commandes_total as Part_comm_frozen, 
      nb_comm_dry_goods_pasta / nb_commandes_total as Part_comm_dry_goods_pasta, 
      nb_comm_snacks / nb_commandes_total as Part_comm_snacks, 
      nb_comm_international / nb_commandes_total as Part_comm_international, 
      nb_comm_beverages / nb_commandes_total as Part_comm_beverages, 
      nb_comm_breakfast / nb_commandes_total as Part_comm_breakfast, 
      nb_comm_produce / nb_commandes_total as Part_comm_produce, 
      nb_comm_dairy_eggs / nb_commandes_total as Part_comm_dairy_eggs,
      nb_comm_canned_goods / nb_commandes_total as Part_comm_canned_goods, 
      nb_comm_pantry / nb_commandes_total as Part_comm_pantry, 
      nb_comm_meat_seafood / nb_commandes_total as Part_comm_meat_seafood, 
      nb_comm_household / nb_commandes_total as Part_comm_household, 
      nb_comm_deli / nb_commandes_total as Part_comm_deli, 
      nb_comm_other / nb_commandes_total as Part_comm_other, 
      nb_comm_personal_care / nb_commandes_total as Part_comm_personal_care, 
      nb_comm_bakery / nb_commandes_total as Part_comm_bakery, 
      nb_comm_babies / nb_commandes_total as Part_comm_babies, 
      nb_comm_missing / nb_commandes_total as Part_comm_missing, 
      nb_comm_bulk / nb_commandes_total as Part_comm_bulk, 
      nb_comm_alcohol / nb_commandes_total as Part_comm_alcohol, 
      nb_comm_pets / nb_commandes_total as Part_comm_pets
FROM instacart.REGROUP_INFOS
-- 15 646 lignes

-- Calcul du taux de cible
select distinct cible, count(*) from instacart.REGROUP_INFOS_M group by cible
-- Taux de cible : 19,2%

DROP TABLE IF EXISTS  instacart.INFOS_ACHATDERCOMM;
DROP TABLE IF EXISTS  instacart.INFOS_DEPARTMENT_GLOBAL;
DROP TABLE IF EXISTS  instacart.INFOS_DELAI_GLOBAL;
DROP TABLE IF EXISTS  instacart.INFOS_CONSO;
DROP TABLE IF EXISTS  instacart.PDT30391_DERNIERACHAT;
DROP TABLE IF EXISTS  instacart.HORS_DERNIER_ACHAT;
DROP TABLE IF EXISTS  instacart.INFOS_MOYDEPPDTS;
DROP TABLE IF EXISTS  instacart.INFOS_ACHATDERCOMM;
DROP TABLE IF EXISTS  instacart.INFOS_RANG;
DROP TABLE IF EXISTS  instacart.INFOS_DELAI_DER_COMM;
DROP TABLE IF EXISTS  instacart.INFOS_DELAI_DERACHAT_30391;
DROP TABLE IF EXISTS  instacart.INFOS_DELAI_DERACHAT_30391_M;
DROP TABLE IF EXISTS  instacart.REGROUP_INFOS;

----------------------------------
-- Modèle logistique : model_type='LOGISTIC_REG'
----------------------------------
-- Construction du modèle en prenant 25% pour les données TEST
---------------- PREMIER MODELE ----------------
CREATE OR REPLACE MODEL
  instacart.MOD_LOGISTIQUE1
OPTIONS
  ( model_type='LOGISTIC_REG',
    DATA_SPLIT_METHOD='RANDOM', -- Méthode d'échantillonage
    DATA_SPLIT_EVAL_FRACTION = 0.75, -- Part dédié à l'évaluation
    input_label_cols=['Cible']
  ) AS
SELECT * EXCEPT(user_id) FROM instacart.REGROUP_INFOS_M
-- Les résultats (ROC, AUC, Matrice de confusion) se trouvent en cliquant sur la table contenant le modèle
-- A noter : en utilisant l'option DATA_SPLIT_METHOD='RANDOM' le logiciel sait quelle partie a été utilisée pour échantillon apprentissage et quelle partie
-- sera à utiliser pour l'échantillon de validation

-- Performances globales du modèle
SELECT *
FROM ML.EVALUATE (MODEL instacart.MOD_LOGISTIQUE1,
                 (SELECT * EXCEPT(user_id) FROM instacart.REGROUP_INFOS_M),
                 STRUCT(0.236))
			-- Performances sur validation, on ne peut pasmettre de seuil
SELECT *
FROM ML.EVALUATE (MODEL instacart.MOD_LOGISTIQUE1)

-- Poids des variables
SELECT * from ML.WEIGHTS(MODEL instacart.MOD_LOGISTIQUE1);

-- Matrice de confusion du modèle sur tout lech
SELECT *
FROM ML.CONFUSION_MATRIX (MODEL instacart.MOD_LOGISTIQUE1,
                          ( SELECT * FROM instacart.REGROUP_INFOS_M),
                          STRUCT(0.236))

-- Indicateurs liés à la courbe ROC du modèle                       
SELECT *
FROM ML.ROC_CURVE (MODEL instacart.MOD_LOGISTIQUE1,
                          ( SELECT * FROM instacart.REGROUP_INFOS_M))


---------------- DEUXIEME MODELE ----------------
CREATE OR REPLACE MODEL
  instacart.MOD_LOGISTIQUEREG
OPTIONS
  ( model_type='LOGISTIC_REG',
    DATA_SPLIT_METHOD='RANDOM', -- Méthode d'échantillonage
    DATA_SPLIT_EVAL_FRACTION = 0.75, -- Part dédié à l'évaluation
    CLASS_WEIGHTS=[('1', 0.5), ('0', 0.5)] ,
	EARLY_STOP = TRUE,MAX_ITERATIONS=30,l1_reg=10,
    input_label_cols=['Cible']
  ) AS
SELECT * EXCEPT(user_id) FROM instacart.REGROUP_INFOS_M

-- Performances globales du modèle
SELECT *
FROM ML.EVALUATE (MODEL instacart.MOD_LOGISTIQUE2,
                 (SELECT * EXCEPT(user_id) FROM instacart.REGROUP_INFOS_M),
                 STRUCT(0.236))

-- Poids des variables
SELECT * from ML.WEIGHTS(MODEL instacart.MOD_LOGISTIQUE2);


-- Matrice de confusion du modèle
SELECT *
FROM ML.CONFUSION_MATRIX (MODEL instacart.MOD_LOGISTIQUE2,
                          ( SELECT * FROM instacart.REGROUP_INFOS_M),
                          STRUCT(0.60))

-- Indicateurs liés à la courbe ROC du modèle                       
SELECT *
FROM ML.ROC_CURVE (MODEL instacart.MOD_LOGISTIQUE2,
                          ( SELECT * FROM instacart.REGROUP_INFOS_M))




----------------------------------
-- Modèle BOOSTED_TREE_CLASSIFIER : model_type='BOOSTED_TREE_CLASSIFIER'
----------------------------------
-- Toutes les options disponibles : https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-create-boosted-tree?hl=fr
---------------- PREMIER MODELE ----------------
CREATE OR REPLACE MODEL instacart.MOD_TREE1
OPTIONS(MODEL_TYPE='BOOSTED_TREE_CLASSIFIER',
        DATA_SPLIT_METHOD='RANDOM', -- Méthode d'échantillonage
        DATA_SPLIT_EVAL_FRACTION = 0.75, -- Part dédié à l'évaluation
        NUM_PARALLEL_TREE = 1, -- Nombre d'arbres parallèles créés à chaque itération (valeur par défaut 1) 
        -- Pour entraîner une forêt d'arbres décisionnels à boosting, définissez cette valeur sur un nombre supérieur à 1.
        MAX_ITERATIONS = 100, -- Nombre maximal de tours du boosting.
        l1_reg=1,
		l2_reg=1,
        LEARN_RATE = 0.1,
        TREE_METHOD = 'HIST', -- Type d'algorithme de création d'arbre. 
        -- HIST est recommandé pour les ensembles de données volumineux afin d'accélérer l'entraînement et de réduire la consommation de ressources.
        EARLY_STOP = TRUE, -- Indique si l'entraînement doit s'arrêter après la première itération pour laquelle l'amélioration de la perte relative 
        --est inférieure à la valeur    spécifiée pour MIN_REL_PROGRESS
        SUBSAMPLE = 0.5, -- Taux de sous-échantillonnage des instances d'entraînement
        INPUT_LABEL_COLS = ['Cible'])
AS SELECT * EXCEPT(user_id) FROM instacart.REGROUP_INFOS_M
;
    

-- Performances globales du modèle
SELECT *
FROM ML.EVALUATE (MODEL instacart.MOD_TREE1,
                 (SELECT * EXCEPT(user_id) FROM instacart.REGROUP_INFOS_M),
                 STRUCT(0.5))

-- Importance des variables
SELECT * from ML.FEATURE_IMPORTANCE(MODEL instacart.MOD_TREE1);

-- Matrice de confusion du modèle
SELECT *
FROM ML.CONFUSION_MATRIX (MODEL instacart.MOD_TREE1,
                          ( SELECT * FROM instacart.REGROUP_INFOS_M),
                          STRUCT(0.5))

-- Indicateurs liés à la courbe ROC du modèle                       
SELECT *
FROM ML.ROC_CURVE (MODEL instacart.MOD_TREE1,
                          ( SELECT * FROM instacart.REGROUP_INFOS_M))


-- Indicateurs liés à la courbe ROC du modèle                       
SELECT *
FROM ML.ROC_CURVE (MODEL instacart.MOD_TREE1,
                          ( SELECT * FROM instacart.REGROUP_INFOS_M))
