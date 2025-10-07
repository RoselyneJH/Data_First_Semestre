##############################################################################
#                                 CHARGEMENT PARTIE COMMUNE
##############################################################################

## --------------------------------------------------------------------------##
#                                        LIBRAIRIES
## --------------------------------------------------------------------------##

import requests
import pandas as pd
import re
import time
import zipfile
import io

# pd.options.mode.chained_assignment = None
from sqlalchemy import Column, Integer, String, ForeignKey, CheckConstraint
from sqlalchemy.exc import SQLAlchemyError

from configparser import ConfigParser
from typing import Optional
from io import StringIO

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Date,
    text,
    Float,
    MetaData,
    Table,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from typing import List, Dict, Union, Tuple, Literal

from bs4 import BeautifulSoup

from loguru import logger

import os


# URL directe vers le fichier
URL_COMMUNE_2020_ZIP = (
    "https://www.insee.fr/fr/statistiques/fichier/4316069/communes2020-csv.zip"
)
URL_COMMUNE_MVT = (
    "https://www.insee.fr/fr/statistiques/fichier/4316069/mvtcommune2020-csv.zip"
)
URL_COMMUNE = (
    "https://www.data.gouv.fr/fr/datasets/r/dbe8a621-a9c4-4bc3-9cae-be1699c5ff25"
)
URL_NEW_CALEDONIE = (
    "https://fr.wikipedia.org/wiki/Liste_des_communes_de_la_Nouvelle-Cal%C3%A9donie"
)
URL_COUNTRY_LAT_LON = "https://raw.githubusercontent.com/google/dspl/master/samples/google/canonical/countries.csv?utm_source=chatgpt.com"
# URL directe vers le fichier Données Insee
URL_COUNTRY = (
    "https://www.insee.fr/fr/statistiques/fichier/7766585/v_pays_territoire_2024.csv"
)
## -------------------------------------------------------------------------##
#                                  FONCTIONS
## -------------------------------------------------------------------------##


def gestion_path_ini() -> str:
    """
    Utilisation d'un chemin absolu pour la récupération

    des fichiers ini, sql d'une part et la log d'autre part

    Args:

        None

    Returns:

        Path du repertoire des fichiers log,

        Path du repertoire des fichiers ini et sql

    """
    # chemin absolu du répertoire contenant ce fichier __init__.py
    base_dir = os.path.dirname(os.path.abspath(__file__))
    racine_projet = os.path.normpath(os.path.abspath(os.path.join(base_dir, "..")))

    # chemin vers ton fichier.ini
    log_path = os.path.normpath(os.path.join(base_dir, "..", "my_log"))

    return racine_projet, log_path


def configuration_db(
    filename: str = "Fichier_Connexion.ini", section: str = "postgresql"
) -> Dict[str, str]:
    """
    Configuration de la base de donnée

    Args:

        Nom fichier de configuration

        Nom de bdd

    Returns:

        Dictionnaire recupérant les elements de connexion

    """
    # Create a parser
    parser = ConfigParser()
    # Read the configuration file
    parser.read(PATH_RACINE + "/" + filename)
    # Get the information from the postgresql section
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception("Section {0} not found in {1}".format(section, filename))

    return db


def prepare_dataframe_for_sql(df: pd.DataFrame, drop_columns=None) -> pd.DataFrame:
    """
    Nettoie un DataFrame avant insertion SQL :

        Convertit les colonnes de type category en string

        Convertit les colonnes datetime en date (si souhaité)

        Supprime les colonnes spécifiées (ex: Id auto-incrémentée)

        Remplace NaN par None (si utile pour PostgreSQL)

    Args:

        df (pd.DataFrame): le DataFrame à nettoyer

        drop_columns (list): liste des colonnes à supprimer (ex: ["IdLigne"])

    Returns:

        pd.DataFrame: un DataFrame prêt à insérer dans la base

    """
    df_clean = df.copy()

    # Convertir les colonnes de type category en string
    for col in df_clean.select_dtypes(include=["category"]).columns:
        df_clean[col] = df_clean[col].astype(str)

    # Convertir datetime en date (optionnel : ici on garde seulement la date)
    for col in df_clean.select_dtypes(include=["datetime64[ns]"]).columns:
        df_clean[col] = df_clean[col].dt.date

    # Supprimer les colonnes auto-incrémentées (ex: IdLigne)
    if drop_columns:
        df_clean = df_clean.drop(columns=drop_columns, errors="ignore")

    # Remplacer les NaN par None pour PostgreSQL
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    return df_clean


def chargement_df_en_sql(engine: Engine, df: pd.DataFrame, nom_table: str) -> None:
    """
    Effectue le chargement du Dadaframe dans une Table sql

    Args:

        Le moteur de connexion, le dataframe et le nom de la table à charger

    Returns:

        pas de sortie, message de bonne réalisation

    """
    df_clean_sql = prepare_dataframe_for_sql(df)

    df_clean_sql.to_sql(
        name=nom_table,  # nom de la table
        con=engine,  # moteur SQLAlchemy
        if_exists="replace",  # 'replace' = supprime et recrée, 'append' = ajoute
        index=False,  # ne pas écrire l'index comme une colonne
    )

    print("Table", nom_table, "chargee !")


def creation_de_chaine_de_connexion() -> str:
    """
    Permet de créer la chaine de connexion à la bdd

    Args:

        None

    Returns:

        Renvoie un chaine de caractère de la connexion -> url

    """
    # Lecture du fichier ini :
    db = configuration_db()

    # Préparation de la chaine de connexion
    host = db["host"]
    port = db["port"]
    database = db["database"]
    user = db["user"]
    password = db["password"]
    #  Créer l'URL SQLAlchemy
    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
    return url


def ajout_coordonnees_geo(df: pd.DataFrame) -> pd.DataFrame:
    """
    ajout des latitudes et longitudes pour certains cas

    Args:

        Le dataframe dans lequel on souhaite ajouter des données geographique

        pour une meilleur complétude des données.

        Les agglomérations de Paris, Lyon et Marseille sont tellement grandes

        qu'elles apparaissent parfois avec leur code arrondissement ou bien

        sans precision d'arrondissement. C'est l'objet ici de l'ajout de données

        geo pour l'agglomération.

    Returns:

        Un dataframe ayant des données geo (LAT et LON) pour les agglomérations

    """
    df["latitude"] = 0
    df["longitude"] = 0
    df.loc[
        df["num_insee"] == "75056",
        ["latitude", "longitude", "code_region", "nom_region", "nom_departement"],
    ] = [
        48.86,
        2.35,
        "11",
        "ILE-DE-FRANCE",
        "PARIS",
    ]  # PARIS
    df.loc[
        df["num_insee"] == "69123",
        ["latitude", "longitude", "code_region", "nom_region", "nom_departement"],
    ] = [
        45.75,
        4.85,
        "84",
        "AUVERGNE-RHONE-ALPES",
        "RHONE",
    ]  # LYON
    df.loc[
        df["num_insee"] == "13055",
        ["latitude", "longitude", "code_region", "nom_region", "nom_departement"],
    ] = [
        43.3,
        5.4,
        "93",
        "PROVENCE-ALPES-COTE D'AZUR",
        "BOUCHES-DU-RHONE",
    ]  # MARSEILLE

    return df


def recuperation_commune_2020(url: str) -> pd.DataFrame:
    """
    Traitement du fichier des commune_2020

    Args:

        url vers le fichier des commune 2020

    Return:

        Renvoie un Dataframe des communes nettoyées pas de doublon padding des codes insee

    """
    # Ajouter un en-tête User-Agent
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    #
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # Étape 2 : Ouvrir le fichier .zip en mémoire
    # Cette fonction permet d’ouvrir un fichier .zip.
    with zipfile.ZipFile(
        io.BytesIO(response.content)
    ) as z:  # Cela transforme le contenu brut en un objet fichier en mémoire.
        # Afficher les fichiers contenus dans le zip
        print("Contenu du ZIP :", z.namelist())
        # Étape 3 : Choisir un fichier à lire (le premier ici et le seul !) et le transformer en dataframe
        with z.open(z.namelist()[0]) as f:
            le_df = pd.read_csv(f)
            # Je supprime la colonne libelle qui n'apporte pas deplus value :
            df = le_df[
                ["com", "typecom", "reg", "dep", "arr", "ncc", "nccenr", "can"]
            ].copy()
            df.rename(
                columns={
                    "typecom": "typecommune",
                    "com": "num_insee",
                    "reg": "code_region",
                    "dep": "code_departement",
                    "arr": "num_arrondissement",
                    "ncc": "nom_commune",
                    "nccenr": "nom_commune_officiel",
                    "can": "code_arr_national",
                },
                inplace=True,
            )

            # padding sur le code departement
            df["code_departement"] = df["code_departement"].str.zfill(3)
            # Je prepare le rapprochement avec le 2ieme fichier des communes de France :
            df["code_commune"] = df["num_insee"].str[2:]
            # Problème de format
            df["code_region"] = df["code_region"].astype(str)
            # Ajout des coordonnées geo Lat et Lon
            df = ajout_coordonnees_geo(df)

            if df.duplicated().sum() > 0:
                print("Il y a des doublons :", df.duplicated().sum(), "Suppression !")
                df_sans_dbl = df.drop_duplicates()
            else:
                df_sans_dbl = df.copy()

    df_commune_2020 = df_sans_dbl

    return df_commune_2020


# -------------------------------------------------------------------------
#                          COMMUNES PRINCIPALES
# -------------------------------------------------------------------------
def recuperation_commune_exagone(url: str) -> pd.DataFrame:
    """
    Args:

        url vers le fichier des commune en France exagonale

    Returns:

        Renvoie un Dataframe des communes nettoyées

            pas de doublon

            padding des codes insee

            suppression des accents dans le nom

    """
    # Lecture du fichier CSV en DataFrame
    le_df1 = pd.read_csv(url, sep=",", encoding="utf-8")

    # On renomme
    le_df1.rename(columns={"code_commune_INSEE": "code_commune_insee"}, inplace=True)
    # Changement de format & Padding à gauche
    le_df1["code_commune_insee"] = le_df1["code_commune_insee"].astype(str).str.zfill(5)

    le_df1["code_postal"] = le_df1["code_postal"].astype(str).str.zfill(5)

    le_df1["code_commune"] = (
        le_df1["code_commune"].astype(str).str.replace(".0", "", regex=False)
    )
    le_df1["code_commune"] = le_df1["code_commune"].str.zfill(3)

    le_df1["code_departement"] = le_df1["code_departement"].astype(str).str.zfill(3)

    le_df1["code_region"] = (
        le_df1["code_region"].astype(str).str.replace(".0", "", regex=False)
    )
    le_df1["code_region"] = le_df1["code_region"].str.zfill(2)

    # On change les cacactères accentués :
    le_df1["nom_region"] = le_df1["nom_region"].str.replace(r"[éèê]", "e", regex=True)
    le_df1["nom_region"] = le_df1["nom_region"].str.replace(r"[îï]", "i", regex=True)
    le_df1["nom_region"] = le_df1["nom_region"].str.replace(r"[ôö]", "o", regex=True)

    le_df1["nom_departement"] = le_df1["nom_departement"].str.replace(
        r"[éèê]", "e", regex=True
    )
    le_df1["nom_departement"] = le_df1["nom_departement"].str.replace(
        r"[îï]", "i", regex=True
    )
    le_df1["nom_departement"] = le_df1["nom_departement"].str.replace(
        r"[ôö]", "o", regex=True
    )

    # Je prends les colonnes pertinentes dans une copy indépendante :
    le_df_commune_principal = le_df1[
        [
            "code_commune_insee",
            "nom_commune_postal",
            "code_postal",
            "latitude",
            "longitude",
            "code_commune",
            "code_departement",
            "nom_departement",
            "code_region",
            "nom_region",
        ]
    ].copy()
    # Changement de nom de colonne
    le_df_commune_principal.rename(
        columns={"code_commune_insee": "num_insee"}, inplace=True
    )

    if le_df_commune_principal.duplicated().sum() > 0:
        print(
            "Il y a des doublons :",
            le_df_commune_principal.duplicated().sum(),
            "Suppression !",
        )
        le_df_commune_principal_sans_dbl = le_df_commune_principal.drop_duplicates()
    else:
        le_df_commune_principal_sans_dbl = le_df_commune_principal.copy()

    return le_df_commune_principal_sans_dbl


# -------------------------------------------------------------------------
#                          COMMUNES AYANT EVOLUEES
# -------------------------------------------------------------------------
def recuperation_mouvement_commune(url: str) -> pd.DataFrame:
    """

    Args:

        url vers le fichier des commune ayant été modifiée : suppression, fusion, remplacement etc..

    Returns:

        Renvoie un Dataframe des communes nettoyées :

                pas de doublon

                données hierarchiques

                selection des colonnes pertinentes

    """
    # Ajouter un en-tête User-Agent
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    # url = "https://example.com/fichier.zip"  # Remplace par l'URL réelle
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # Étape 2 : Ouvrir le fichier .zip en mémoire
    # Cette fonction permet d’ouvrir un fichier .zip.
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Cela transforme le contenu brut en un objet fichier en mémoire.
        # Afficher les fichiers contenus dans le zip
        print("Contenu du ZIP :", z.namelist())
        # Étape 3 : Choisir un fichier à lire (le premier ici et le seul !)
        # et le transformer en dataframe
        with z.open(z.namelist()[0]) as f:
            le_df = pd.read_csv(f)
            df_mvt = le_df.copy()

    df_mvt.rename(
        columns={
            "MOD": "type_event_commune",
            "TNCC_AVANT": "type_nom_en_clair_avant",
            "NCC_AVANT": "nom_commune_en_clair_avant",
            "NCCENR_AVANT": "nom_commune_riche_en_clair_avant",
            "TNCC_APRES": "type_nom_en_clair_apres",
            "NCC_APRES": "nom_commune_en_clair_apres",
            "NCCENR_APRES": "nom_commune_riche_en_clair_apres",
        },
        inplace=True,
    )
    df_mvt.rename(columns=str.lower, inplace=True)
    # mettre au bon format :
    df_mvt["date_eff"] = pd.to_datetime(df_mvt["date_eff"])
    df_mvt["idligne"] = df_mvt.index.to_list()
    df_mvt.rename(
        columns={
            "id_commune_avant": "num_insee_avant",
            "id_commune_apres": "num_insee_apres",
        },
        inplace=True,
    )

    df_mvt_ = df_mvt[
        [
            "idligne",
            "type_event_commune",
            "date_eff",
            "type_commune_avant",
            "num_insee_avant",
            "type_nom_en_clair_avant",
            "nom_commune_en_clair_avant",
            "nom_commune_riche_en_clair_avant",
            "libelle_avant",
            "type_commune_apres",
            "num_insee_apres",
            "type_nom_en_clair_apres",
            "nom_commune_en_clair_apres",
            "nom_commune_riche_en_clair_apres",
            "libelle_apres",
        ]
    ].copy()

    if df_mvt_.duplicated().sum() > 0:
        print("Il y a des doublons :", df_mvt.duplicated().sum(), "suppression(s) !")
        df_mvt_sans_dbl = df_mvt_.drop_duplicates()
    else:
        df_mvt_sans_dbl = df_mvt_.copy()

    df_mvt_sans_dbl.rename(
        columns={
            "id_commune_avant": "num_insee_avant",
            "id_commune_apres": "num_insee_apres",
        },
        inplace=True,
    )

    return df_mvt_sans_dbl


# -------------------------------------------------------------------------
#                          COMMUNES NOUVELLE-CALEDONIE
# -------------------------------------------------------------------------
def recuperation_commune_nouvelle_caledonie(url: str) -> pd.DataFrame:
    """
    Args:

        url vers le fichier des commune de nouvelle caledonie

    Returns:

        Renvoie un Dataframe des communes nettoyées

                pas de doublon

                pas d'accent sur les noms de communes

    """

    headers = {"User-Agent": "Mozilla/5.0"}  # pour éviter un blocage éventuel
    html = requests.get(url, headers=headers).text
    soup = BeautifulSoup(html, "html.parser")

    # repérer la table — généralement avec class "wikitable sortable"
    table = soup.find("table", {"class": "wikitable"})

    # parcourt les lignes
    rows = []
    for tr in table.find_all("tr")[1:]:  # sauter le header
        cols = tr.find_all(["th", "td"])
        if len(cols) >= 2:
            nom = cols[0].get_text(strip=True)
            code = cols[1].get_text(strip=True)
            population = cols[4].get_text(strip=True)
            rows.append({"commune": nom, "num_insee": code, "population": population})

    df_new_caledonie = pd.DataFrame(rows)

    # Extraire une année à 4 chiffres entre parenthèses
    df_new_caledonie["annee"] = df_new_caledonie["population"].str.extract(
        r"\((\d{4})\)"
    )

    # remplace la parenthèse et ce qu'elle contient ==> on eut retirer cet element
    # via expression regulière
    df_new_caledonie["population"] = df_new_caledonie["population"].str.replace(
        r"\s*\([^)]*\)", "", regex=True
    )

    # On change les cacactères accentués :
    df_new_caledonie["commune"] = df_new_caledonie["commune"].str.replace(
        r"[éèê]", "e", regex=True
    )
    df_new_caledonie["commune"] = df_new_caledonie["commune"].str.replace(
        r"[îï]", "i", regex=True
    )

    # On met les noms des  communes en majuscule :
    df_new_caledonie["commune_valide"] = df_new_caledonie["commune"].str.upper()
    # Je moyenne la position géo pour toutes les communes de NCalédonie pour l'instant
    df_new_caledonie["latitude"] = -21.123889
    df_new_caledonie["longitude"] = 165.846901
    # La nouvelle-caledonie n'est ni un region, ni un departement mais une collectivité
    # attribution du code insee pour le departement et la region.
    df_new_caledonie["code_departement"] = "988"
    df_new_caledonie["nom_departement"] = "NOUVELLE-CALEDONIE"
    df_new_caledonie["code_region"] = "98"
    df_new_caledonie["nom_region"] = "NOUVELLE-CALEDONIE"

    # >Renommer les colonnes :
    df_ncaledonie = df_new_caledonie[
        [
            "num_insee",
            "commune",
            "population",
            "annee",
            "commune_valide",
            "latitude",
            "longitude",
            "code_departement",
            "nom_departement",
            "code_region",
            "nom_region",
        ]
    ].copy()

    if df_new_caledonie.duplicated().sum() > 0:
        print(
            "Il y a des doublons :",
            df_new_caledonie.duplicated().sum(),
            "Suppression !",
        )
        df_new_caledonie_sans_dbl = df_ncaledonie.drop_duplicates()
    else:
        df_new_caledonie_sans_dbl = df_ncaledonie.copy()

    return df_new_caledonie_sans_dbl


def chargement_pays_LAT_LON() -> pd.DataFrame:
    """
    Permet de rapprocher le nom pays et le couple LAT, LON

    Args:

        None

    Returns:

        Dataframe comportant nom pays, Code insee, Latitude, Longitude

    """
    # Lecture du fichier CSV en DataFrame
    df_lat_lon = pd.read_csv(URL_COUNTRY_LAT_LON)
    df_lat_lon.rename(columns={"country": "codeiso_2"}, inplace=True)
    df_lat_lon["name"] = df_lat_lon["name"].str.strip()
    df_lat_lon["nom_pays_en"] = df_lat_lon["name"].str.upper()
    # Incohérence pour la Namibie
    df_lat_lon.loc[df_lat_lon["nom_pays_en"] == "NAMIBIA", "codeiso_2"] = "NA"

    # URL directe vers le fichier Données Insee
    # URL_COUNTRY = "https://www.insee.fr/fr/statistiques/fichier/7766585/v_pays_territoire_2024.csv"

    # Lecture du fichier CSV en DataFrame
    le_df = pd.read_csv(URL_COUNTRY, sep=",", encoding="utf-8")
    le_df["LIBCOG"] = le_df["LIBCOG"].str.strip()
    # On change les cacactères accentués :
    le_df["LIBCOG"] = le_df["LIBCOG"].str.replace(r"[éèê]", "e", regex=True)
    le_df["LIBCOG"] = le_df["LIBCOG"].str.replace(r"[îïÎ]", "i", regex=True)
    le_df["LIBCOG"] = le_df["LIBCOG"].str.replace(r"[â]", "a", regex=True)

    le_df["nom_pays_fr"] = le_df["LIBCOG"].str.upper()
    le_df["num_insee"] = le_df["COG"].astype("str")
    le_df.rename(columns={"CODEISO2": "codeiso_2"}, inplace=True)
    # Incohérence pour la Namibi et le Kosova
    le_df.loc[le_df["nom_pays_fr"] == "NAMIBIE", "codeiso_2"] = "NA"
    le_df.loc[le_df["nom_pays_fr"] == "KOSOVO", "codeiso_2"] = "XK"

    df_insee = le_df[le_df["codeiso_2"].notnull()][
        ["num_insee", "codeiso_2", "nom_pays_fr"]
    ]

    df_insee_lat_lon = pd.merge(df_insee, df_lat_lon, how="inner", on="codeiso_2")

    return df_insee_lat_lon


def chargement_dwh(
    df_commune_2020: str,
    df_mvt_sans_dbl: str,
    df_new_caledonie_sans_dbl: str,
    le_df_commune_principal_sans_dbl: str,
    df_insee_lat_lon: str,
):
    """
    Chargement des données dans le DWH

    Args:

        données sur les mouvement de commune

            sur les communes de la Nouvelle Caledonie

            sur les communes exagonales

    Returns:

        None

    """

    engine = create_engine(creation_de_chaine_de_connexion())

    # Base ORM
    Base = declarative_base()

    class Commune_principale(Base):
        __tablename__ = "commune_principale"
        num_insee = Column(String(5), primary_key=True)
        nom_commune_postal = Column(String(50), nullable=False)
        code_postal = Column(String(5))
        latitude = Column(Float)
        longitude = Column(Float)
        code_commune = Column(String(3))
        code_departement = Column(String(3))
        nom_departement = Column(String(30), nullable=False)
        code_region = Column(String(2))
        nom_region = Column(String(30), nullable=False)
        __table_args__ = (CheckConstraint("LENGTH(code) = 5", name="num_insee"),)
        # ajout d'une contrainte

    class Commune_mvt(Base):
        __tablename__ = "commune_mvt"
        idligne = Column(Integer, primary_key=True, autoincrement=True)
        type_event_commune = Column(Integer)
        date_eff = Column(Date)
        type_commune_avant = Column(String(4), nullable=False)
        num_insee_avant = Column(String(5))
        type_nom_en_clair_avant = Column(String(7))
        nom_commune_en_clair_avant = Column(String(50), nullable=False)
        nom_commune_riche_en_clair_avant = Column(String(50), nullable=False)
        libelle_avant = Column(String(50))
        type_commune_apres = Column(String(4))
        num_insee_apres = Column(String(5))
        type_nom_en_clair_apres = Column(String, nullable=False)
        nom_commune_en_clair_apres = Column(String(50), nullable=False)
        nom_commune_riche_en_clair_apres = Column(String, nullable=False)
        libelle_apres = Column(String, nullable=False)

    class Commune_nouvelle_caledonie(Base):
        __tablename__ = "commune_nouvelle_caledonie"
        IdLigne = Column(Integer, primary_key=True, autoincrement=True)
        num_insee = Column(String(5), primary_key=True)
        commune = Column(String(50))
        population = Column(Integer)
        annee = Column(Integer)
        commune_valide = Column(String, nullable=False)
        latitude = Column(Float)
        longitude = Column(Float)
        code_departement = Column(String(3))
        nom_departement = Column(String(30))
        code_region = Column(String(2))
        nom_region = Column(String(30))

    ##########################################################################
    # Supprimer l’ancienne table si elle existe
    with engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS commune_2020"))
        connection.execute(text("DROP TABLE IF EXISTS commune_mvt"))
        connection.execute(text("DROP TABLE IF EXISTS commune_nouvelle_caledonie"))
        connection.execute(text("DROP TABLE IF EXISTS commune_principale"))
        connection.execute(text("DROP TABLE IF EXISTS country_cog_lat_lon"))  #
        print("Table(s) Supprimée(s)")

    # Création de la table dans la base
    Base.metadata.create_all(engine)

    print(
        "Table 'commune_mvt', 'commune_principale' & 'commune_nouvelle_caledonie' créée(s) avec succès !"
    )

    chargement_df_en_sql(engine, df_commune_2020, "commune_2020")
    chargement_df_en_sql(engine, df_mvt_sans_dbl, "commune_mvt")
    chargement_df_en_sql(
        engine, df_new_caledonie_sans_dbl, "commune_nouvelle_caledonie"
    )
    chargement_df_en_sql(engine, le_df_commune_principal_sans_dbl, "commune_principale")
    chargement_df_en_sql(engine, df_insee_lat_lon, "country_cog_lat_lon")

    ##########################################################################
    # Permet de lancer un script de creation des outils necessaires, table,
    # index etc..

    try:
        with engine.connect() as connection:
            with connection.begin():  # démarre une transaction
                # Charger un script SQL depuis un fichier
                with open(
                    PATH_RACINE + "/" + "Prj_Death_People_commune_BDD.sql", "r"
                ) as f:
                    sql_script = f.read()
                # Execution pas à pas des requetes
                for statement in sql_script.split(";"):
                    statement = statement.strip()
                    if statement:
                        connection.execute(text(statement))

        print("Le Sql_script exécuté avec succès.")

    except SQLAlchemyError as e:
        print("Une erreur est survenue lors de l'exécution du script SQL :")
        print(e)

    # Correct pour vider/fermer le pool de connexions
    engine.dispose()


# -------------------------------------------------------------------------
#                                     MAIN
# -------------------------------------------------------------------------

# Path
PATH_RACINE, PATH_LOG = gestion_path_ini()
# Configurer loguru
logger.add(
    PATH_LOG + "/" + "DownLoad_File_commune_std.log", rotation="500 MB", level="INFO"
)
#
logger.info("DEBUT TRT")

le_df_commune_principal_sans_dbl = recuperation_commune_exagone(URL_COMMUNE)
logger.info("RECUPERATION COMMUNE PRINCIP.")

df_mvt_sans_dbl = recuperation_mouvement_commune(URL_COMMUNE_MVT)
logger.info("RECUPERATION COMMUNE MODIFIEE")

df_new_caledonie_sans_dbl = recuperation_commune_nouvelle_caledonie(URL_NEW_CALEDONIE)
logger.info("RECUPERATION COMMUNE N.Caled.")

df_commune_2020 = recuperation_commune_2020(URL_COMMUNE_2020_ZIP)

df_insee_lat_lon = chargement_pays_LAT_LON()

chargement_dwh(
    df_commune_2020,
    df_mvt_sans_dbl,
    df_new_caledonie_sans_dbl,
    le_df_commune_principal_sans_dbl,
    df_insee_lat_lon,
)
logger.info("CHARGEMENT DWH ")
#
logger.info("FIN  TRT")
