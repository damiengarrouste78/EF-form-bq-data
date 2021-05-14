CREATE OR REPLACE TABLE eu_dgr.notes(apprenant STRING, formation STRING, date TIMESTAMP, sat FLOAT64)
;
create or replace table eu_dgr.nage_jo(nageur STRING,epreuve STRING,temps FLOAT64,date_maj TIMESTAMP );
INSERT eu_dgr.nage_jo (nageur,epreuve,temps,date_maj)
VALUES('Andriy Hovorov','50m',21.74,"2021-01-01 00:00:00"),
('Anthony Ervin','50m',21.4,"2021-01-01 00:00:00"),
('Benjamin Proud','50m',	21.68,"2021-01-01 00:00:00"),
('Brad Tandy','50m',	21.79,"2021-01-01 00:00:00"),
('Bruno Fratus','50m',	21.79,"2021-01-01 00:00:00"),
('Caeleb Dressel','100m',	48.02,"2021-01-01 00:00:00"),
('Cameron McEvoy','100m',	48.12,"2021-01-01 00:00:00"),
('Duncan Scott','100m',	48.01,"2021-01-01 00:00:00"),
('Florent Manaudou','50m',	21.41,"2021-01-01 00:00:00"),
('Kyle Charmers','100m',	47.58,"2021-01-01 00:00:00"),
('Marcelo Chierighini','100m',48.41,"2021-01-01 00:00:00"),
('Nathan Adrian','50m',21.49,"2021-01-01 00:00:00"),
('Nathan Adrian','100m',47.85,"2021-01-01 00:00:00"),
('Pieter Timmers','100m',47.8,"2021-01-01 00:00:00"),
('Santo Condorelli','100m',47.88,"2021-01-01 00:00:00"),
('Simonas Bilis','50m',22.08,"2021-01-01 00:00:00")
;


-- calcul schedulé : on calcule un comptage sur la table en supposant qu'elle est mis à jour fréquemment

-- écrivons cette requete
SELECT 
@run_time AS time, nageur, epreuve, temps, 
RANK() OVER(PARTITION BY epreuve ORDER BY temps) as Rang 
FROM dgr.nage_jo 
;

-- ouvrir la planification
-- définir custom et taper every 15 minutes
-- définir une dt de début et de fin courtes
-- dans le nom de la table en sortie, on peut utiliser le moment de l'execution pour tracer toutes les exec
-- ajouter_{run_time|"%Y%m%d%H%M"} pour suffixer avec date et heure