--======================================================================================================================
-- Formation BigQuery 
-- EPSILON FRANCE damien.garrouste@epsilon-france.com
-- Introduction aux struct et array
-- 10/2020
--======================================================================================================================

--1 LES STRUCT
-- On cr�e un struct
-- c un conteneur qui va contenir deux champs
-- un champ panier et un champ produit qui est un array
-- on veut g�n�rer deux lignes de donn�es donc on met en array devant le struct contient deux lignes de donn�es



-- sans le array au debut : cela cree deux colonnes et une seule ligne
select STRUCT<panier STRING, produits ARRAY<STRING>>
('fruits',['pomme','poire','banane']),('legumes',['haricots','champignons'])
;
-- avec le array au debut : cela cree une ligne car un seul array qui contient le struct
select [STRUCT<panier STRING, produits ARRAY<STRING>>
('fruits',['pomme','poire','banane']),('legumes',['haricots','champignons'])]
;

-- Voyons comment  structurer dans une table 
-- il y aura une seule ligne de donn�es car on charge un seul ARRAY
create or replace table eu_dgr.mon_struct as
select [STRUCT<panier STRING, produits ARRAY<STRING>>
('fruits',['pomme','poire','banane']),('legumes',['haricots','champignons'])] as monArrayStruct
;



-- commeles donn�es sont un array on doit passer un unnest si on veut generer des lignes, en sortie cela cree deux lignes
create or replace table eu_dgr.paniers as
select expanded.produits, expanded.panier
from eu_dgr.mon_struct as t0
Cross join unnest(t0.monArrayStruct) as expanded
;
-- si on fait un where sur panier, on obtient bien toutes les lignes 
select * from eu_dgr.paniers
where panier='fruits'
;



-- on va maintenant casser completement et d�plier larray produits

--2 LES array
-- si on veut cr�er autant de lignes et casser la struct de l'array
SELECT panier, produits_unnested
FROM eu_dgr.paniers as t1
CROSS JOIN UNNEST(t1.produits) as produits_unnested
;

-- agg  pour recr�er un arrray � partir de la liste de sproduits pour chaque panier
create table instacart.test3 as 
select panier, array_agg(produits_unnested) as produits_nested
from instacart.test2
group by panier
;

-- ANNEXES creer les donn�es en array etpas en struct

-- On cr�e un array de deux �l�ments � partir de deux lignes de donn�es
-- en sortie on aura une colonne panier et produits
-- deux lignes correspondant aux deux arrays
create or replace table instacart.test as 
select 'fruits' as panier, ARRAY(select 'pomme' union all select 'poire' union all select'banane') as produits
union all 
select 'legumes' as panier, ARRAY(select 'haricots' union all select 'champignons') as produits
;


-- unest permet d'�clater l'array
-- en sortie on aura une colonne panier et produits
-- deux lignes correspondant aux deux arrays
create table instacart.test2 as 
select panier, produits_unnested
from instacart.test
Cross join unnest(produits) as produits_unnested
;
