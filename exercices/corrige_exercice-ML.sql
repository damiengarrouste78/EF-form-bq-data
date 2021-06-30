--======================================================================================================================
-- Formation BigQuery Dr Pierre Ricaud
-- 11/2020
-- par rapport à exercice ML on a enlevé le datamanagement poour créer la table de scoring
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


-- Le modèle à sélectionner est celui dont le nombre de clusters est contenu dans BEST_NUM_CLUSTERS (indice de davies bouldin)



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
-- table REGROUP_INFOS_M : table contenant l'ensemble des variables features et cible 14k users (échantillon)

-- Calcul du taux de cible
select distinct cible, count(*) from instacart.REGROUP_INFOS_M group by cible
-- Taux de cible : 19,2%



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


---------------- DEUXIEME MODELE : augmenter le lasso et équilibrer les classes ----------------
CREATE OR REPLACE MODEL
  instacart.MOD_LOGISTIQUEREG
OPTIONS
  ( model_type='LOGISTIC_REG',
    DATA_SPLIT_METHOD='RANDOM', -- Méthode d'échantillonage
    DATA_SPLIT_EVAL_FRACTION = 0.75, -- Part dédié à l'évaluation
    CLASS_WEIGHTS=[('1', 0.5), ('0', 0.5)] , -- classes équilibrées
	EARLY_STOP = TRUE,
	MAX_ITERATIONS=30,
	l1_reg=10,
    input_label_cols=['Cible']
  ) AS
SELECT * EXCEPT(user_id) FROM instacart.REGROUP_INFOS_M

CREATE OR REPLACE MODEL
  instacart.MOD_LOGISTIQUE2
OPTIONS
  ( model_type='LOGISTIC_REG',
    DATA_SPLIT_METHOD='RANDOM', -- Méthode d'échantillonage
    DATA_SPLIT_EVAL_FRACTION = 0.75, -- Part dédié à l'évaluation
    CLASS_WEIGHTS=[('1', 0.5), ('0', 0.5)] , -- classes équilibrées
	LEARN_RATE_STRATEGY='constant',
    LEARN_RATE=0.3,
	EARLY_STOP = TRUE,
	MAX_ITERATIONS=30,
	l1_reg=10,
    input_label_cols=['Cible']
  ) AS
SELECT * EXCEPT(user_id) FROM instacart.REGROUP_INFOS_M

-- Performances globales du modèle
SELECT *
FROM ML.EVALUATE (MODEL instacart.MOD_LOGISTIQUEREG,
                 (SELECT * EXCEPT(user_id) FROM instacart.REGROUP_INFOS_M),
                 STRUCT(0.236))

-- Poids des variables
SELECT * from ML.WEIGHTS(MODEL instacart.MOD_LOGISTIQUEREG);


-- Matrice de confusion du modèle
SELECT *
FROM ML.CONFUSION_MATRIX (MODEL instacart.MOD_LOGISTIQUEREG,
                          ( SELECT * FROM instacart.REGROUP_INFOS_M),
                          STRUCT(0.236))

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
        MAX_ITERATIONS = 50, -- Nombre maximal de tours du boosting.
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
