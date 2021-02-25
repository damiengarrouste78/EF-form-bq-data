SELECT
 *, ST_GeogPoint(longitude, latitude)  AS WKT 
FROM
  `bigquery-public-data.london_bicycles.cycle_stations`
  ;



 SELECT  z.zip_code,  COUNT(*) AS num_stations FROM  `bigquery-public-data.new_york_citibike.citibike_stations` AS s,  `bigquery-public-data.geo_us_boundaries.zip_codes` AS z WHERE  ST_DWithin(z.zcta_geom,    ST_GeogPoint(s.longitude, s.latitude),    1000) -- 1km  AND num_bikes_available > 30 GROUP BY  z.zip_code ORDER BY  num_stations DESC

--Use ST_DWITHIN to check if two locations objects are within some distance 
SELECT  z.zip_code,  COUNT(*) AS num_stations FROM  `bigquery-public-data.new_york_citibike.citibike_stations` AS s,  `bigquery-public-data.geo_us_boundaries.zip_codes` AS z WHERE  ST_DWithin(z.zip_code_geom,    ST_GeogPoint(s.longitude, s.latitude),    1000) -- 1km  AND num_bikes_available > 30 GROUP BY  z.zip_code ORDER BY  num_stations DESC


--Represent longitude and latitude points as Well Known Text (WKT) using the function ST_GeogPoint 
SELECT  z.zip_code,  COUNT(*) AS num_stations FROM  `bigquery-public-data.new_york_citibike.citibike_stations` AS s,  `bigquery-public-data.geo_us_boundaries.zip_codes` AS z WHERE  ST_DWithin(z.zcta_geom,    ST_GeogPoint(s.longitude, s.latitude),    1000) -- 1km  AND num_bikes_available > 30 GROUP BY  z.zip_code ORDER BY  num_stations DESC 



-- récupérer une latitude longitude
curl "https://api-adresse.data.gouv.fr/search/?q=55+quai+grenelle+paris&limit=15"
 curl "https://api-adresse.data.gouv.fr/search/?q=issy+moulineaux&limit=5"
-- reste la fonction geo hash : divise le territoire en carré de taille variable
select ST_GEOHASH(ST_GEOGPOINT(2.283957, 48.850527),4)
select ST_GEOHASH(ST_GEOGPOINT(2.26287, 48.8239),4)

-- centre du carre
select ST_GEOGPOINTFROMGEOHASH("u09t")
-- carre plus précis
select ST_GEOHASH(ST_GEOGPOINT(2.283957, 48.850527),8)
select ST_GEOHASH(ST_GEOGPOINT(2.26287, 48.8239),8)