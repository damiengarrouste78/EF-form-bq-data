SELECT
  ST_GeogPoint(longitude, latitude)  AS WKT,
 bikes_count 
FROM
  `bigquery-public-data.london_bicycles.cycle_stations`
  ;