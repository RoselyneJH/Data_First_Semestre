L'objectif est d'analyser les décès en France.
Données fournies par DATA gouv : https://www.data.gouv.fr/datasets/fichier-des-personnes-decedees/

I. Chargement
Chargement des données dans DWH (PostgreSQL)
Pour cela récupération de fichiers communes de France :

	Commune Principale (fichier principal)
	Ajout des données sur la nouvelle Calédonie (fichier éponyme)
	Prise en compte des disparitions/créations de commune et de leur codes Insee 	différents (fichier des mouvements de commune)

Traitement de formalisation des données, de nettoyage et de suppression de doublon

Creation de script sql pour mieux appréhender l'affectation de code Insee

Chargement des personnes décédées en fonction de l'année sélectionnée

(Log de chargement)

II. Analyse et création de graphe

III. Conclusion