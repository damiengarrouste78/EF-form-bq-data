--======================================================================================================================
-- Formation BigQuery 
-- 11/2020
--======================================================================================================================
-- charger un tf model
-- on exporte le modele sauvegardé dans un bucket

-- ici c'est un modèle public de word embedding dans 20dimensions

-- depuis cloudshell telecharger, décompresser, charger
mkdir swivel
wget --quiet -O swivel.tar.gz  https://tfhub.dev/google/tf2-preview/gnews-swivel-20dim/1?tf-hub-format=compressed
tar xvfz swivel.tar.gz
gsutil -m cp -R swivel gs://epsi-tech-dsc-formation-202005/models


-- BIGQUERY WEBUI
-- créer un modèle depuis un modele externe TF
CREATE OR REPLACE MODEL eu_dgr.swivel_text_embed
OPTIONS(model_type='tensorflow', model_path='gs://epsi-tech-dsc-formation-202005/models/swivel/*')
;


-- fonction pour calculer la dist eucl entre deux documents sur les dimensions embeddings
CREATE TEMPORARY FUNCTION td(a ARRAY<FLOAT64>, b ARRAY<FLOAT64>, idx INT64) AS (
   (a[OFFSET(idx)] - b[OFFSET(idx)]) * (a[OFFSET(idx)] - b[OFFSET(idx)])
);
CREATE TEMPORARY FUNCTION term_distance(a ARRAY<FLOAT64>, b ARRAY<FLOAT64>) AS ((
   SELECT SQRT(SUM( td(a, b, idx))) FROM UNNEST(GENERATE_ARRAY(0, 19)) idx
));



-- soit une phrase type dont on souhaite rechercher des phrases proches dans la base de documents
-- on la prédit
create or replace table eu_dgr.phrase_type as
SELECT output_0 as prediction  FROM 
ML.PREDICT(MODEL eu_dgr.swivel_text_embed,(SELECT "Great game yesterday night, congrats to all players, they were fantastic, see you at the next match !" as sentences))
;

--- soit une base de documents
CREATE TEMPORARY TABLE Phrases (sentences STRING, id INT64);
INSERT INTO Phrases (sentences, id) VALUES("Hope we will see soon such a good game : all men on the floor were awesome",2);
INSERT INTO Phrases (sentences, id) VALUES("The weather is cloudy",2);
INSERT INTO Phrases (sentences, id) VALUES("We assist to a slowdown of the us economy due to the epidemy",2);
INSERT INTO Phrases (sentences, id) VALUES("Keep watching , nexts rounds will be great ",2);
INSERT INTO Phrases (sentences, id) VALUES("Breaking news : in NBA, Lakers win their play-off yesterday evening",2);
INSERT INTO Phrases (sentences, id) VALUES(" Good game this evening,  congratulations to these awesome players, hope to see you for the next one",2);


-- créer une fonction distance
CREATE TEMPORARY FUNCTION td(a ARRAY<FLOAT64>, b ARRAY<FLOAT64>, idx INT64) AS (
   (a[OFFSET(idx)] - b[OFFSET(idx)]) * (a[OFFSET(idx)] - b[OFFSET(idx)])
);
CREATE TEMPORARY FUNCTION term_distance(a ARRAY<FLOAT64>, b ARRAY<FLOAT64>) AS ((
   SELECT SQRT(SUM( td(a, b, idx))) FROM UNNEST(GENERATE_ARRAY(0, 19)) idx
));

-- Prédictions des similarités

SELECT 
"Great game yesterday night, congrats to all players, they were fantastic, see you at the next match !" as phrase_cible,phrase_histo,
  term_distance(prediction, output_0) AS distance_cible_histo
FROM ML.PREDICT(MODEL eu_dgr.swivel_text_embed,(SELECT sentences as phrase_histo, sentences FROM Phrases)),eu_dgr.phrase_type
ORDER By distance_cible_histo ASC
LIMIT 10
;
