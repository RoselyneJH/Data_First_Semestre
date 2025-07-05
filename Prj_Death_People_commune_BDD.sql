CREATE INDEX idx_num_insee_avant ON commune_mvt(num_insee_avant DESC);

CREATE INDEX idx_num_insee_apres ON commune_mvt(num_insee_apres DESC);


DROP TABLE IF EXISTS evolution_commune;
DROP TABLE IF EXISTS commune_principale_et_ncaledonie;
DROP TABLE IF EXISTS search_num_insee;


CREATE TABLE evolution_commune AS
WITH RECURSIVE chaines_insee AS (
    -- Point de dpart : tous les codes d'origine
    SELECT
        distinct e.num_insee_avant AS origine,
		e.nom_commune_en_clair_avant ,
        e.num_insee_apres AS actuel,
		e.nom_commune_en_clair_apres ,
        1 AS niveau,ARRAY[e.num_insee_avant, e.num_insee_apres]::TEXT[] AS chemin
    FROM commune_mvt e
    UNION ALL
    -- Etapes suivantes : suivre les transformations successives
    SELECT
        c.origine,c.nom_commune_en_clair_avant,
        e.num_insee_apres,e.nom_commune_en_clair_apres,
        c.niveau + 1,c.chemin ||  e.num_insee_apres
    FROM chaines_insee c INNER JOIN commune_mvt e
        ON e.num_insee_avant = c.actuel
	where c.niveau < 5 AND NOT e.num_insee_apres = ANY(c.chemin)
)
-- Resultat final : le dernier code atteint pour chaque origine
SELECT DISTINCT ON (origine)
    origine as num_insee_invalide,nom_commune_en_clair_avant as nom_invalide,
    actuel AS num_insee_valide,nom_commune_en_clair_apres as nom_valide
FROM chaines_insee
ORDER BY origine, niveau DESC;


CREATE INDEX idx_ev_num_insee_invalide ON evolution_commune(num_insee_invalide DESC);


CREATE TABLE commune_principale_et_ncaledonie AS
select distinct coalesce(pri.num_insee,nco.num_insee) as num_insee,
coalesce(pri.nom_commune_postal,nco.commune) as nom_commune_postal  
from commune_nouvelle_caledonie nco full outer join 
commune_principale pri on (nco.num_insee=pri.num_insee)
order by 1 desc
;
CREATE INDEX idx_num_insee_comm_pri_ncal ON commune_principale_et_ncaledonie(num_insee DESC);


CREATE TABLE search_num_insee AS 
---  Table qui permet d'avoir tous les codes insee de france mvt+principal+new caledonie
select coalesce(num_insee,num_insee_invalide) as num_insee_search, 
coalesce(num_insee,num_insee_valide) as num_insee_valide,
coalesce(nom_commune_postal,nom_valide) as commune_valide,
case when num_insee is null then 'PR' else 'MV' end
from
(select num_insee,nom_commune_postal,num_insee_invalide,
num_insee_valide,nom_valide
from commune_principale_et_ncaledonie pr full outer join evolution_commune ev 
on (pr.num_insee=ev.num_insee_invalide ) ) e
order by num_insee_search
;

CREATE INDEX idx_num_insee_search ON search_num_insee(num_insee_search DESC);


