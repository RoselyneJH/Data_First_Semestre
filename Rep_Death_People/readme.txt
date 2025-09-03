					PROJET DECES

L'objectif est d'effectuer une anlayse sur les décès en France.
Elle porte sur 2 axes principaux :
		La répartition des deces est-elle homogène en France ?
		Quelle est la proportion des français qui choisissent de passer toute leur vie dans leur ville de naissance ?

L'intérêt est d'éclairer les prises de décisions pour les sociétés :
		d'assurances afin de fidéliser leur client 
		de pompes funèbres pour identifier les pics d'activités
		les pouvoirs publics en terme de répartition de la population et de la continuité de l'état

Les données sont fournies par DATA gouv : https://www.data.gouv.fr/datasets/fichier-des-personnes-decedees/
Ils couvrent des données sur plus de 30 ans.

I. Chargement des lieux

Chargement des données dans DWH (PostgreSQL)
Pour cela récupération de fichiers communes de France ainsi que des pays.

	Commune Principale : 
						Fichier principal des communes françaises mais il n'est pas exhaustif sur certains
						numéro Insee. 
						Descriptif : Num Insee / Nom Commune / Département / Region / Latitude / Longitude / ..
						
	Commune 2020 ou arrondissement : 
						plus performant sur les arrondissements de communes (complète le fichier principal)
						Descriptif : Insee / Nom Commune / Département / Region / ...
						
	Commune de la Nouvelle-Calédonie :
						Uniquement des communes de La Nouvelle-Caledonie 
						Descriptif : Num Insee / Nom Commune / Latitude / Longitude / ...
						Par defaut, la Latitude et Longitude ont été calculées manuellement ; elles sont les mêmes pour toutes les communes.
						
	Commune mouvement : 
						Prise en compte des disparitions/créations de commune et de leur codes Insee. Ce fichier
						comporte des données hiérarchiques
						Descriptif : Num Insee_Avant / Nom Commune Avant/ Num Insse_Apres / Nom Commune Après /Type de Mouvement /...
						
	Pays (LAT,LON)		:
						Comporte les données pays
						Descriptif : Code Iso /Nom Pays /Latitude / Longitude /...
	
	Pays (Insee)		:
						Comporte les données pays
						Descriptif : Num Insee /Nom Pays /...

Le module Extract_Load_Transform_Commune_FR.py, effectue l'extraction et le chargement de ces fichiers.

L'objectif est d'avoir une table unique des numéros insee avec le nom de la commune/Pays.
Si elle est francaise, le departement et la region sont mentionnés sinon le pays est renseigné.
5 Tables sont crées : 
	commune_principale
	commune_principale_enrichie 
	commune_mvt
	commune_nouvelle_caledonie
	Country_cog_lat_lon

(Un merge préalable des 2 fichiers Pays est effectués avant chargement dans le DWH.)

(Présence d'une log de chargement)

II. Transformation des données communes/Pays

Une étape de nettoyage des données (suppression des doublons, formattage etc..) est effectué avant chargement.
Apres le chargement des étapes de transformation vont finaliser l'unicité du couple numéro Insee / nom correspondant via la création de tables intermédiaires.

1ere étape :
	Construction d'une table (CTE) qui présente pour un numéro Insee donné, son dernier numero insee valide et le nom de la commune correspondant.
	La diffculté porte sur la presence de données hiérarchiques.
	TABLE evolution_commune 

	Construction d'une table qui présente le merge entre les communes principales et son enrichissement :
	TABLE commune_principale_et_agglo

	Construction d'une table (CTE) qui présente le merge entre les tables commune_principale_et_agglo, Country_cog_lat_lon et commune_nouvelle_caledonie :
	TABLE commune_principale_et_ncaledonie_pays

	Construction d'une table (CTE) qui présente le merge entre commune_principale_et_ncaledonie_pays et la table evolution_commune. 
	TABLE search_num_insee

Les scripts sont présents dans le fichier : Prj_Death_People_commune_BDD.sql



III. Chargement des données personnes

	Les données personnes sont enregistrées sur le site data.gouv par année ou pour l'année en cours par trimestre (données non tratées ici).
	Il faut donc identifier l'année et le nom du fichier correspondant pour le télecharger.
	
	1ere étape :
	Charger tous les URL des fichiers deces
	J'ai choisi d'enregistrer dans notre DwH les URL des fichiers deces et leur année :
	TABLE nom_url
	Descriptif : Annee /Nom Url
	MODULE : Extract_Load_People (1ere Partie)
	
	2ieme étape :
	A partir d'une année selectionnée, un téléchargement du fichier decès est effectué.
	La problématique majeure est que le fichier.txt comporte des apostrophes qui ne permet pas d'identifier les colonnes correctement.
	Il faut donc reconstituer les colonnes correctement :
	Module Extract_Load_People fonction traitement_validation
	
	Un nettoyage puis formattages données est effectué avant le chargement en base :
	MODULE Extract_Load_People FONCTION telechargement_fichier_personne_decedee_selon_annee
	TABLE death_people
	Decription : Identifiant Ligne / Nom /Prenom / Sex /Date Naissance /Date Deces/Num Insee naissance/ Num Insee deces /...
	
	3ieme étape :
	Apres chargement des données, une transformation est opérée afin d'obtenir un couple unique de numéro Insee pour les communes de naissance et de deces.
	Construction d'une table (CTE) qui présente le merge entre l'identifiant personnes decedées et le couple Insee naissance /Deces et nom commune
		TABLE affectation_insee_site_naissance_death
		Description : Identifiant ligne /Num Insee naissance/ Num Insee deces /Latitude deces/Latitude Naissance/Region /...
	
	Construction d'une vue (CTE) avec toutes les données personnes et communes :
		VUE death_people_view
		Description : Identifiant ligne / Nom /Prenom / Sex /Date Naissance /Date Deces/Num Insee naissance/ Num Insee deces /Latitude deces/Latitude Naissance/...
	Cette étape est effectuée par un script :
		Prj_Death_People_people_BDD.sql (Extract_Load_People.creer_base_et_table_personne_decedee) 
	
IV. Transformation
	
	MODULE Transform_People_Death
	
