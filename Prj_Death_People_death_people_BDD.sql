CREATE INDEX idx_idligne_death ON death_people(idligne DESC);

DROP TABLE IF EXISTS affectation_insee_site_naissance_death;

CREATE TABLE affectation_insee_site_naissance_death AS
WITH
cte1 AS (select idligne, coalesce(num_insee_valide,num_insee_naissance) as num_insee_naissance,
coalesce(commune_valide,ville_naissance) as ville_naissance 
from death_people de left join search_num_insee se
on (de.num_insee_naissance = se.num_insee_search)),cte2 AS (
select idligne, coalesce(num_insee_valide,num_insee_deces) as num_insee_deces,
coalesce(commune_valide,ville_deces) as ville_deces 
from death_people de left join search_num_insee se
on (de.num_insee_deces = se.num_insee_search) )
SELECT c1.idligne as idligne,num_insee_naissance,ville_naissance,num_insee_deces ,ville_deces
FROM cte2 c2, cte1 c1 where c1.idligne=c2.idligne
order by c1.idligne 
; 

CREATE INDEX idx_idligne_affect ON affectation_insee_site_naissance_death(idligne DESC);
