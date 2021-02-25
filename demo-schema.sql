-- Commande pour générer un schéma et lexporter
bq show --format prettyjson --schema ch04.college_scorecard > schema.json
-- en SQL
SELECT 
 TO_JSON_STRING(
    ARRAY_AGG(STRUCT( 
      IF(is_nullable = 'YES', 'NULLABLE', 'REQUIRED') AS
mode,
      column_name AS name,
      data_type AS type)
    ORDER BY ordinal_position), TRUE) AS schema
FROM
  instacart.INFORMATION_SCHEMA.COLUMNS
WHERE
  table_name = 'aisles'