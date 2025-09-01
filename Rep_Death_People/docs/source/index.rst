.. Death_People documentation master file, created by
   sphinx-quickstart on Fri Aug 29 15:08:40 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Death_People documentation
==========================

Add your content using ``reStructuredText`` syntax. See the
`reStructuredText <https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html>`_
documentation for details.


.. toctree::
   :maxdepth: 2
   :caption: Contents:
  
Mon Projet
==========

Bienvenue dans la documentation de mon projet.

# Analyse des décès en France

.. contents:: Table des matières
\:depth: 2
\:local:

## Objectif de l’étude

Cette étude vise à analyser les décès en France selon deux axes principaux :

1. **Homogénéité spatiale** : la répartition des décès est‑elle homogène en France ?
2. **Fidélité territoriale** : quelle est la proportion des Français qui décèdent dans leur **commune de naissance** (ou, à défaut, qui restent dans leur **région d’origine**) ?

## Intérêt pour les acteurs

* **Assurances** : conception d’offres de fidélisation et tarification territorialisée.
* **Pompes funèbres** : identification des pics d’activité et allocation des ressources.
* **Pouvoirs publics** : compréhension des dynamiques démographiques et appui à la planification territoriale (continuité du service public).

## Source des données

* **Fichier des personnes décédées – INSEE / data.gouv.fr** :
  [https://www.data.gouv.fr/datasets/fichier-des-personnes-decedees/](https://www.data.gouv.fr/datasets/fichier-des-personnes-decedees/)
* Période couverte : **plus de 30 ans** (selon millésimes disponibles).

## I. Chargement des lieux

**Objectif** : constituer une *table de référence unique* des **codes INSEE** → *libellé de commune / pays*, avec enrichissement **département** et **région** pour la France.

Dépôts / fichiers d’entrée

```

- **Commune principale** : fichier de base des communes françaises (certains numéros INSEE peuvent être incomplets).
  
  **Champs (exemples)** : *Num INSEE*, *Nom commune*, *Département*, *Région*, *Latitude*, *Longitude*, …

- **Commune 2020 / Arrondissements** : meilleure couverture des arrondissements de communes (complète le fichier principal).
  
  **Champs (exemples)** : *INSEE*, *Nom commune*, *Département*, *Région*, …

- **Communes de Nouvelle‑Calédonie** : liste spécifique NC.
  
  **Champs (exemples)** : *Num INSEE*, *Nom commune*, *Latitude*, *Longitude*, …
  
  .. note:: Par défaut, *Latitude* et *Longitude* ont été calculées manuellement et sont identiques pour toutes les communes.

- **Commune mouvement** : historique des créations / fusions / disparitions avec correspondances de codes INSEE (structure hiérarchique).
  
  **Champs (exemples)** : *INSEE_Avant*, *Nom_Avant*, *INSEE_Après*, *Nom_Après*, *Type de mouvement*, …

- **Pays (LAT/LON)** : référentiel pays avec coordonnées.
  
  **Champs (exemples)** : *Code ISO*, *Nom pays*, *Latitude*, *Longitude*, …

- **Pays (INSEE)** : référentiel pays avec code INSEE.
  
  **Champs (exemples)** : *Num INSEE*, *Nom pays*, …

Processus d’extraction / chargement
```

* **Module** : `Extract_Load_Transform_Commune_FR.py`
* **SGBD cible** : *PostgreSQL* (DWH)
* **Pré‑traitement** : *merge* préalable des deux référentiels **Pays** avant chargement.
* **Journalisation** : présence d’un *log* de chargement.

Tables créées

```

- ``commune_principale``
- ``commune_principale_enrichie``
- ``commune_mvt``
- ``commune_nouvelle_caledonie``
- ``Country_cog_lat_lon``

II. Transformation des données communes / pays
----------------------------------------------

**Objectif** : garantir l’unicité du couple **(Numéro INSEE, Libellé)** en tenant compte des **mouvements de communes**.

Étapes de nettoyage
```

* Dé‑duplication
* Normalisation des libellés (casse, accents, tirets)
* Harmonisation des codes (zéro‑padding, formats homogènes)

Chaîne de transformations (vues / CTE / tables)

```

1. **Évolution des communes** → ``evolution_commune``

   Construire, pour un INSEE donné, le **dernier code INSEE valide** et le **nom courant** de la commune.
   
   .. note:: Le traitement doit gérer la **hiérarchie des mouvements** (fusions successives, changements multiples).

2. **Agrégation des référentiels de communes** → ``commune_principale_et_agglo``

   Jointure / fusion de la base des communes principales et de son enrichissement.

3. **Extension Outre‑mer et pays** → ``commune_principale_et_ncaledonie_pays``

   Jointure entre ``commune_principale_et_agglo``, ``Country_cog_lat_lon`` et ``commune_nouvelle_caledonie``.

4. **Résolution finale des codes** → ``search_num_insee``

   Jointure de ``commune_principale_et_ncaledonie_pays`` avec ``evolution_commune`` pour obtenir le couple **INSEE final ↔ libellé**.

.. admonition:: Scripts SQL

   Les requêtes correspondantes sont regroupées dans :
   
   ``Prj_Death_People_commune_BDD.sql``

III. Chargement des données personnes
-------------------------------------

**Objectif** : ingérer les *fichiers des personnes décédées* (par année, ou par trimestre pour l’année en cours) et reconstituer un schéma propre pour analyse.

1) Index des fichiers disponibles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Table** : ``nom_url``  (*Année*, *Nom_URL*)
- **Module** : ``Extract_Load_People`` *(1re partie)*

2) Téléchargement & parsing
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Sélection d’une **année** → téléchargement du **fichier texte**.
- **Problème majeur** : présence d’**apostrophes** perturbant la détection des colonnes.
- **Solution** : reconstitution robuste des colonnes.
  
  - **Module** : ``Extract_Load_People``
  - **Fonction** : ``traitement_validation``

- **Nettoyage / formatage** avant insertion :
  
  - **Fonction** : ``telechargement_fichier_personne_decedee_selon_annee``
  - **Table** : ``death_people``
  
  **Champs (exemples)** : *Identifiant_ligne*, *Nom*, *Prénom*, *Sexe*, *Date_naissance*, *Date_décès*, *INSEE_naissance*, *INSEE_décès*, …

3) Affectation INSEE naissance / décès
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **But** : obtenir un **couple unique** de codes INSEE pour **lieu de naissance** et **lieu de décès**.
- **Table / CTE** : ``affectation_insee_site_naissance_death``
  
  **Champs (exemples)** : *Identifiant_ligne*, *INSEE_naissance*, *INSEE_décès*, *Latitude_décès*, *Latitude_naissance*, *Région*, …

- **Vue agrégée** : ``death_people_view`` (ensemble *personnes + communes*)
  
  **Champs (exemples)** : *Identifiant_ligne*, *Nom*, *Prénom*, *Sexe*, *Date_naissance*, *Date_décès*, *INSEE_naissance*, *INSEE_décès*, *Latitude_décès*, *Latitude_naissance*, …

.. admonition:: Script SQL

   La création des structures ci‑dessus est pilotée par :
   
   ``Prj_Death_People_people_BDD.sql`` (*via* ``Extract_Load_People.creer_base_et_table_personne_decedee``)

IV. Transformation analytique
-----------------------------

- **Module** : ``Transform_People_Death``
- **Axes d’analyse** :
  
  1. **Homogénéité territoriale** des décès (cartes, densités, indicateurs par région / département / commune).
  2. **Fidélité territoriale** : part des individus décédant dans leur **commune de naissance** (ou **région d’origine**).

Indicateurs et sorties attendus
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Taux de décès par **niveau géographique** (commune, département, région).
- **Cartes choroplèthes** (si raccordées à des géométries IGN/INSEE).
- Taux de **fidélité à la commune de naissance** et **distance naissance→décès** (si géocodage disponible).
- Séries temporelles par année (pics d’activité).

Annexes
-------

Bonnes pratiques
~~~~~~~~~~~~~~~~

- Versionner les scripts (``.py``/``.sql``) et les paramètres.
- Documenter les schémas cibles et *data lineage*.
- Contrôler la qualité : cohérence des dates, codes INSEE valides, doublons, valeurs manquantes.

Arborescence indicative
~~~~~~~~~~~~~~~~~~~~~~~

::

   docs/
     source/
       index.rst
       methodologie.rst
       schema_donnees.rst
   etl/
     Extract_Load_Transform_Commune_FR.py
     Extract_Load_People.py
     Transform_People_Death.py
   sql/
     Prj_Death_People_commune_BDD.sql
     Prj_Death_People_people_BDD.sql

Liens utiles
~~~~~~~~~~~~

- Data.gouv – fichier des personnes décédées :
  https://www.data.gouv.fr/datasets/fichier-des-personnes-decedees/
- INSEE – référentiels de communes et mouvements :
  https://www.insee.fr

.. tip::
   Pour l’intégrer à **Sphinx**, place ce fichier comme page d’accueil (``index.rst``) ou référence‑le depuis un ``.. toctree::`` dans ``index.rst``.

```
