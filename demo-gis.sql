SELECT
 *, ST_GeogPoint(longitude, latitude)  AS WKT 
FROM
  `bigquery-public-data.london_bicycles.cycle_stations`
  ;



 SELECT  z.zip_code,  COUNT(*) AS num_stations FROM  `bigquery-public-data.new_york_citibike.citibike_stations` AS s,  `bigquery-public-data.geo_us_boundaries.zip_codes` AS z WHERE  ST_DWithin(z.zcta_geom,    ST_GeogPoint(s.longitude, s.latitude),    1000) -- 1km  AND num_bikes_available > 30 GROUP BY  z.zip_code ORDER BY  num_stations DESC

Use ST_DWITHIN to check if two locations objects are within some distance 
SELECT  z.zip_code,  COUNT(*) AS num_stations FROM  `bigquery-public-data.new_york_citibike.citibike_stations` AS s,  `bigquery-public-data.geo_us_boundaries.zip_codes` AS z WHERE  ST_DWithin(z.zip_code_geom,    ST_GeogPoint(s.longitude, s.latitude),    1000) -- 1km  AND num_bikes_available > 30 GROUP BY  z.zip_code ORDER BY  num_stations DESC


Represent longitude and latitude points as Well Known Text (WKT) using the function ST_GeogPoint 
SELECT  z.zip_code,  COUNT(*) AS num_stations FROM  `bigquery-public-data.new_york_citibike.citibike_stations` AS s,  `bigquery-public-data.geo_us_boundaries.zip_codes` AS z WHERE  ST_DWithin(z.zcta_geom,    ST_GeogPoint(s.longitude, s.latitude),    1000) -- 1km  AND num_bikes_available > 30 GROUP BY  z.zip_code ORDER BY  num_stations DESC 

Represent points with ST_GeogPoint
Represent regions with ST_MakeLine and ST_MakePolygon

