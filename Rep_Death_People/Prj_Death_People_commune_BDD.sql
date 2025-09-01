CREATE INDEX idx_num_insee_avant ON commune_mvt(num_insee_avant DESC);

CREATE INDEX idx_num_insee_apres ON commune_mvt(num_insee_apres DESC);


---------------------------------------------------------------------- evolution_commune
DROP TABLE IF EXISTS evolution_commune;

CREATE TABLE evolution_commune AS
WITH RECURSIVE chaines_insee AS (
    -- Point de depart : tous les codes d'origine
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


---------------------------------------------------------------------- commune_principale_et_agglo
DROP TABLE IF EXISTS commune_principale_et_agglo;

CREATE TABLE commune_principale_et_agglo as 
select coalesce(pri.num_insee,com.num_insee) as num_insee,
coalesce(pri.nom_commune_postal,com.nom_commune) as nom_commune_postal,
coalesce(pri.code_postal,'' ) as code_postal,coalesce(pri.latitude,com.latitude) as latitude,
coalesce(pri.longitude,com.longitude) as longitude,
coalesce(pri.code_commune,com.code_commune) as code_commune,
coalesce(pri.code_departement,coalesce(com.code_departement,'') ) as code_departement,
coalesce(pri.nom_departement,com.nom_departement)  as nom_departement,
coalesce(pri.code_region,coalesce(com.code_region,'')) as code_region,
coalesce(pri.nom_region,com.nom_region) as nom_region  
from public.commune_2020 com full outer join public.commune_principale pri
on (pri.num_insee=com.num_insee) --where com.typecommune <> 'ARM'
;

CREATE INDEX idx_num_insee_comm_pri_agg ON commune_principale_et_agglo(num_insee DESC);


---------------------------------------------------------------------- commune_principale_et_ncaledonie_pays
DROP TABLE IF EXISTS commune_principale_et_ncaledonie_pays;

CREATE TABLE commune_principale_et_ncaledonie_pays AS
WITH
cte_commune_principale_et_ncaledonie AS (select distinct coalesce(pri.num_insee,nco.num_insee) as num_insee,
coalesce(pri.nom_commune_postal,nco.commune) as nom_commune_postal,
coalesce(pri.nom_departement,'Nouvelle-Caledonie') as nom_departement,
coalesce(pri.nom_region,'Nouvelle-Caledonie') as nom_region,
coalesce(pri.latitude,nco.latitude) as latitude,coalesce(pri.longitude,nco.longitude) as longitude,
coalesce(pri.code_departement,null) as code_departement,coalesce(pri.code_region,null) as code_region
from commune_nouvelle_caledonie nco full outer join 
commune_principale_et_agglo pri on (nco.num_insee=pri.num_insee)
), cte_commune_pays AS (select distinct coalesce(co.num_insee,pa.num_insee) as num_insee,
coalesce(co.nom_commune_postal,pa.nom_pays_fr) as nom_commune_postal,
coalesce(co.nom_departement,pa.nom_pays_fr) as nom_departement,
coalesce(co.nom_region,pa.nom_pays_fr) as nom_region,
coalesce(co.latitude,pa.latitude) as latitude,coalesce(co.longitude,pa.longitude) as longitude,
coalesce(co.code_departement,null) as code_departement,coalesce(co.code_region,null) as code_region
from country_cog_lat_lon pa full outer join
cte_commune_principale_et_ncaledonie co on (co.num_insee = pa.num_insee))
select num_insee,nom_commune_postal,nom_departement,nom_region,latitude,longitude,
code_departement,code_region from cte_commune_pays order by 1 desc 
;

CREATE INDEX idx_num_insee_comm_pri_ncal_py ON commune_principale_et_ncaledonie_pays(num_insee DESC);


---------------------------------------------------------------------- search_num_insee
DROP TABLE IF EXISTS search_num_insee;

CREATE TABLE search_num_insee AS 
---  Table qui permet d'avoir tous les codes insee de france mvt+principal+new caledonie
WITH
cte_tous_num_insee AS (select coalesce(num_insee,num_insee_invalide) as num_insee_search, 
coalesce(num_insee,num_insee_valide) as num_insee_valide,
coalesce(nom_commune_postal,nom_valide) as commune_valide,
case when num_insee is null then 'PR' else 'MV' end as origine
,latitude,longitude,code_departement,nom_departement, code_region,nom_region
from (select num_insee,nom_commune_postal,num_insee_invalide,
coalesce(num_insee,num_insee_valide) as num_insee_valide,nom_valide,latitude,longitude,
code_departement,nom_departement,code_region,nom_region
from commune_principale_et_ncaledonie_pays pr full outer join evolution_commune ev 
on (pr.num_insee=ev.num_insee_invalide)) e),cte_ccompletude AS (
select ctni.num_insee_search,ctni.num_insee_valide,ctni.commune_valide,ctni.origine,
cpn.latitude,cpn.longitude,cpn.code_departement,cpn.nom_departement,cpn.code_region,cpn.nom_region
from cte_tous_num_insee ctni left join commune_principale_et_ncaledonie_pays cpn
on (ctni.num_insee_valide = cpn.num_insee ) )
select num_insee_search,num_insee_valide,commune_valide,origine,
latitude,longitude,code_departement,nom_departement,code_region,nom_region from cte_ccompletude 
order by num_insee_search
;

CREATE INDEX idx_num_insee_search ON search_num_insee(num_insee_search DESC);


--------------------------------------------------------------------------------------------



