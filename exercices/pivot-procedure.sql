-- source Felipe HOFFA developper @google
--https://towardsdatascience.com/easy-pivot-in-bigquery-one-step-5a1f13c6c710


-- une fonction utilisée dans la fonction principale, remplace les car spec par _, ps : il manque les espaces
CREATE FUNCTION eu_dgr.normalize_col_name(col_name STRING) AS 
(
REGEXP_REPLACE(col_name,r'[/+#|]', '_')
)
;

-- Procédure stockée : script contenant des execute immediate qui va generer du code de transposition avec des IF 
CREATE OR REPLACE PROCEDURE eu_dgr.pivot(
  table_name STRING
  , destination_table STRING
  , row_ids ARRAY<STRING>
  , pivot_col_name STRING
  , pivot_col_value STRING
  , max_columns INT64
  , aggregation STRING
  , optional_limit STRING
  )
BEGIN
  DECLARE pivotter STRING;
  EXECUTE IMMEDIATE (
    "SELECT STRING_AGG(' "||aggregation
    ||"""(IF('||@pivot_col_name||'="'||x.value||'", '||@pivot_col_value||', null)) e_'||eu_dgr.normalize_col_name(x.value))
   FROM UNNEST((
       SELECT APPROX_TOP_COUNT("""||pivot_col_name||", @max_columns) FROM `"||table_name||"`)) x"
  ) INTO pivotter 
  USING pivot_col_name AS pivot_col_name, pivot_col_value AS pivot_col_value, max_columns AS max_columns;
  EXECUTE IMMEDIATE (
   'CREATE OR REPLACE TABLE `'||destination_table
   ||'` AS SELECT '
   ||(SELECT STRING_AGG(x) FROM UNNEST(row_ids) x)
   ||', '||pivotter
   ||' FROM `'||table_name||'` GROUP BY '
   || (SELECT STRING_AGG(''||(i+1)) FROM UNNEST(row_ids) WITH OFFSET i)||' ORDER BY '
   || (SELECT STRING_AGG(''||(i+1)) FROM UNNEST(row_ids) WITH OFFSET i)
   ||' '||optional_limit
  );
END;



-- USAGE

CALL eu_dgr.pivot(
  'bigquery-public-data.iowa_liquor_sales.sales' # source table
  , 'fh-bigquery.temp.test' # destination table
  , ['date'] # row_ids
  , 'store_number' # pivot_col_name
  , 'sale_dollars' # pivot_col_value
  , 30 # max_columns
  , 'SUM' # aggregation
  , '' # optional_limit
);

--- la fonction genère une chaine 
-- et si dans les values il y a des espaces cela fait un bugge car la variable cree nest pas correcte
--CREATE OR REPLACE TABLE `instacart.users_nb_commandes` AS
--SELECT user_id, 
--SUM(IF(department="produce", commande, null)) e_produce, 
--SUM(IF(department="dairy eggs", commande, null)) e_dairy eggs,  dairy eggs end eux mots
