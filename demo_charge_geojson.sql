-- Comment charger un fichier de shapes dans BQ

 -- Le format geojson est utilisé, exemple de format Geojson simple
 {
  "type": "Feature",
  "geometry": {
    "type": "Point",
    "coordinates": [125.6, 10.1]
  },
  "properties": {
    "name": "Dinagat Islands"
  }
}

-- dans la pratique, un fichier json lignes (1 ligne = 1 json)


-- On va charger les frontières des quartiers de Londres

-- copier le fichier sur le shell
 gsutil cp gs://epsi-tech-dsc-formation-202005/data/london_boundaries/london-wards-2014.geojsonl ~/london-wards-2014.geojsonl

-- ce fichier a une erreur 
bq load \
 --source_format=NEWLINE_DELIMITED_JSON \
 --json_extension=GEOJSON \
 --autodetect \
 eu_dgr.london_boundaries \
 gs://epsi-tech-dsc-formation-202005/data/london_boundaries/london-wards-2014.geojsonl

-- ce fichier a une erreur
 bq load \
 --source_format=NEWLINE_DELIMITED_JSON \
 --json_extension=GEOJSON \
 --autodetect \
 eu_dgr.london_boundaries \
 gs://epsi-tech-dsc-formation-202005/data/london_boundaries/london-sport.geojsonl

 # celui la fonctionne https://skgrange.github.io/data.html
 bq load \
 --source_format=NEWLINE_DELIMITED_JSON \
 --json_extension=GEOJSON \
 --autodetect \
 eu_dgr.london_boundaries \
 gs://epsi-tech-dsc-formation-202005/data/london_boundaries/london-boroughs.geojsonl