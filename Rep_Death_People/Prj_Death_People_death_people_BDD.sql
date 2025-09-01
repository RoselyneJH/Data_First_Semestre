CREATE INDEX idx_idligne_death ON death_people(idligne DESC);
CREATE INDEX idx_annee_death ON death_people(annee DESC);

------------------------------------ affectation_insee_site_naissance_death
DROP TABLE IF EXISTS affectation_insee_site_naissance_death CASCADE;

CREATE TABLE affectation_insee_site_naissance_death AS
WITH
cte_naissance AS (select idligne, coalesce(num_insee_valide,num_insee_naissance) as num_insee_naissance,
coalesce(commune_valide,ville_naissance) as ville_naissance
,latitude as latitude_naissance,longitude as longitude_naissance,code_departement as code_departement_naissance, 
nom_departement as nom_departement_naissance,
code_region as code_region_naissance,nom_region as nom_region_naissance
from death_people de left join search_num_insee se
on (de.num_insee_naissance = se.num_insee_search)),cte_death AS (
select idligne, coalesce(num_insee_valide,num_insee_deces) as num_insee_deces,
coalesce(commune_valide,ville_deces) as ville_deces 
,latitude as latitude_deces,longitude as longitude_deces,code_departement as code_departement_deces,
nom_departement as nom_departement_deces, 
code_region as code_region_deces,nom_region as nom_region_deces
from death_people de left join search_num_insee se
on (de.num_insee_deces = se.num_insee_search) )
SELECT c1.idligne as idligne,num_insee_naissance,ville_naissance
,latitude_naissance,longitude_naissance,
code_departement_naissance,nom_departement_naissance, code_region_naissance,nom_region_naissance
,num_insee_deces ,ville_deces
,latitude_deces,longitude_deces,code_departement_deces,nom_departement_deces,
code_region_deces,nom_region_deces
FROM cte_death c2, cte_naissance c1 where c1.idligne=c2.idligne
order by c1.idligne 
; 

CREATE INDEX idx_idligne_affect ON affectation_insee_site_naissance_death(idligne DESC);

		
------------------------------------ death_people_view
DROP VIEW IF EXISTS death_people_view CASCADE;

CREATE VIEW death_people_view AS (
select de.idligne, de.prenom,de.sex,de.date_naissance_dt,af.num_insee_naissance,af.ville_naissance,
de.pays_naissance,af.latitude_naissance,af.longitude_naissance,
af.code_departement_naissance,coalesce(af.nom_departement_naissance,de.pays_naissance) as nom_departement_naissance,
af.code_region_naissance,coalesce(af.nom_region_naissance,de.pays_naissance) as nom_region_naissance
,de.date_deces_dt,af.num_insee_deces ,af.ville_deces
,af.latitude_deces,af.longitude_deces,af.code_departement_deces,af.nom_departement_deces,
af.code_region_deces,af.nom_region_deces,de.age,de.annee,case when af.num_insee_naissance = af.num_insee_deces then 'O' else 'N' end
as origine
from affectation_insee_site_naissance_death af
inner join death_people de on (de.idligne=af.idligne))	
;


