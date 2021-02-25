-- soit un array, la fonction ORDINAL renvoie l'element i
select ['dimanche','lundi','mardi','mercredi','jeudi','vendredi','samedi'][ORDINAL(1)] 

-- soit une date en chaine, extraire le jour de la semaine
select EXTRACT(DAYOFWEEK from DATE('2021-02-01')) as Jour_Semaine

-- et renvoyer une chaine
select CAST(EXTRACT(DAYOFWEEK from DATE('2021-02-01')) as STRING) as Jour_Semaine

-- soit une chaine, on la tokenize, cela renvoie un array
select SPLIT("la formation est top, merci Damien !",' ') 
-- d'abord, on enleve la ponctuation
select REPLACE("la formation est top, merci Damien !",'!','') 
-- mais comment enlever toute la ponctuation ? tous les car sauf a à z et espace
select REGEXP_REPLACE(LOWER("la formation est top , merci Damien !"),'[^a-z\\s]', '')
-- on reteste
select SPLIT(REGEXP_REPLACE(LOWER("la formation est top, merci Damien !"),'[^a-z\\s]', ''),' ') 
-- la fonction NGRAM
select ML.NGRAMS(SPLIT(REGEXP_REPLACE(LOWER("la formation est top, merci Damien !"),'[^a-z\\s]', ''),' ') , [2,2]) 
-- on corrige un petit détail et voilà!
select ML.NGRAMS(SPLIT(RTRIM(REGEXP_REPLACE(LOWER("la formation est top, merci Damien !"),'[^a-z\\s]', '')),' ') , [2,2]) 