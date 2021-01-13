--======================================================================================================================
-- Formation BigQuery 
-- script, proce stock�, 
-- 10/2020
--======================================================================================================================
--test
------------------------------------------------------------------------------------------------------------------------
-- 1 SCRIPT SIMPLE
-- Declarer une variable pour stocker des noms de stations dans un array
BEGIN
DECLARE top_stations ARRAY<STRING>;
DECLARE date_deb TIMESTAMP;
-- Remplir l'array avec les 3 plus grosses stations
SET date_deb='2017-06-01 00:00:00'
SET top_stations = (
WITH compte as  ( 
select start_station_name,count(*) as nb_hires
FROM eu_dgr.public_london_cycle_hire
WHERE start_date >date_deb
GROUP BY start_station_name)
SELECT ARRAY_AGG(start_station_name ORDER BY nb_hires DESC LIMIT 3)
    FROM 
  
	  COMPTE as cpt
    
);
select top_stations;
-- Requete : on filtre sur la liste des top stations 
SELECT
start_station_name, count(*) as nb_hires 
FROM
eu_dgr.public_london_cycle_hire
WHERE start_station_name IN UNNEST(top_stations)
AND start_date >date_deb
GROUP BY start_station_name
;
-- PS : pour que la variable top stations soit connu il faut �x�cuter en meme temps les statements
END;
------------------------------------------------------------------------------------------------------------------------
-- 2 SCRIPT QUI GENERE DU CODE
-- Soit un script qui applique une combinaison lin�aire du type revenu = constante + pente x nb annee exp 
BEGIN
-- DECLARE au d�but
DECLARE constante INT64;
DECLARE pente INT64;
DECLARE revenu FLOAT64;
DECLARE EXPERIENCE INT64;

-- remplir les variables
SET constante = 30000;
SET pente = 1000;
SET EXPERIENCE = 10;

-- Formule revenu  = constante + pente * (coef)
-- EXECUTE IMMEDIATE provoque lexec live , ainsi le resultat est dispo dans les instructions suivantes
-- les variables declarees sont nommees avec @ et on peut en utilise avec using et ?
EXECUTE IMMEDIATE "SELECT @const + @pente  * @EXPERIENCE" INTO revenu USING constante as const, pente as pente, EXPERIENCE as EXPERIENCE ;

-- on ne peut pas melanger les? et @
EXECUTE IMMEDIATE "SELECT ? + ? * ?" INTO revenu USING 30000,1500,10;

END;
------------------------------------------------------------------------------------------------------------------------
-- 3 SCRIPT QUI GENERE DU CODE EN BOUCLE
-- La boucle fait des INSERT
BEGIN
DECLARE exp INT64 default 0;
EXECUTE IMMEDIATE  "CREATE TEMPORARY TABLE Salaires (experience INT64, revenu FLOAT64)";
LOOP
  SET exp = exp + 1;
  IF exp < 20 THEN
    LEAVE;
  ELSE IF exp >= 60 THEN   BREAK;
  ELSE 	EXECUTE IMMEDIATE "INSERT INTO Salaires (experience, revenu) VALUES(@exp1, 30000 + 1000  * @exp1)"  USING exp as exp1;
  END IF;
END LOOP;
SELECT * from Salaires;
END;
BEGIN
DECLARE exp INT64 default 0;
EXECUTE IMMEDIATE  "CREATE TEMPORARY TABLE Salaires (experience INT64, revenu FLOAT64)";
WHILE exp<60 DO
  SET exp = exp + 1;
  IF exp>20 THEN EXECUTE IMMEDIATE "INSERT INTO Salaires (experience, revenu) VALUES(@exp1, 30000 + 1000  * @exp1)"  USING exp as exp1;
  END IF;
END WHILE;
SELECT * from Salaires;
END;
--3/
--------------
--PROCEDURE STOCKEe
-------------
-- on encapsule le script

CREATE OR REPLACE PROCEDURE eu_dgr.ps_topstations()
BEGIN

-- script demarre ici

DECLARE ... ;

SET ... ;

SELECT ... ;

-- script finit ici
END;



--------------
--APPEL PROCEDURE STOCKEe
-------------

CALL eu_dgr.ps_topstations()
