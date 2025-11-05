## --------------------------------------------------------------------------##
#                                        LIBRAIRIES
## --------------------------------------------------------------------------##

from Extract_Load_People_death_FR import (
    telechargement_fichier_personne_decedee_selon_annee,
    existence_bdd_dictionnaire_fichiers_personne_decedee,
    creation_bdd_dictionnaire_fichiers_personne_decedee,
)
from Extract_Load_People_death_FR import creer_base_et_table_personne_decedee

import pandas as pd
from configparser import ConfigParser
from typing import List, Dict, Union, Tuple, Literal
from sqlalchemy import create_engine, Column, Integer, String, Date, text
from sqlalchemy.orm import declarative_base, sessionmaker

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

import matplotlib.ticker as ticker

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
from sqlalchemy.schema import PrimaryKeyConstraint

from functools import reduce
import os

from Connexion_Bdd import ConnexionBdd

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

        Path du répertoire my_module [base]

    """
    # chemin absolu du répertoire contenant ce fichier __init__.py
    base_dir = os.path.dirname(os.path.abspath(__file__))
    racine_projet = os.path.normpath(os.path.abspath(os.path.join(base_dir, "..")))

    # chemin vers ton fichier.ini
    log_path = os.path.normpath(os.path.join(base_dir, "..", "my_log"))

    return racine_projet, log_path, base_dir

'''
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


def select_death_people() -> pd.DataFrame:
    """
        Create une requete afin de recuperer l'url_name en fonction de l'année

    Args:

        None

    Returns:

        Le nom de l'url à telecharger

    """
    # Créer le moteur SQLAlchemy
    engine = create_engine(creation_de_chaine_de_connexion())

    #
    la_query = "SELECT idligne, prenom,sex,date_naissance_dt,num_insee_naissance,"
    la_query = la_query + "ville_naissance,pays_naissance,latitude_naissance,"
    la_query = la_query + "longitude_naissance,code_departement_naissance,"
    la_query = la_query + "nom_departement_naissance, code_region_naissance,"
    la_query = la_query + "nom_region_naissance,date_deces_dt,num_insee_deces,"
    la_query = la_query + "ville_deces,latitude_deces,longitude_deces,"
    la_query = la_query + "code_departement_deces,nom_departement_deces,"
    la_query = la_query + "code_region_deces,nom_region_deces,age,annee,"
    la_query = la_query + "origine_ville,origine_departement,origine_region "
    la_query = la_query + "FROM death_people_view"
    # Non ici il faut faire un select selon l'année choisie
    with engine.connect() as connection:
        result = connection.execute(text(la_query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    engine.dispose()

    return df
'''

def haversine_np(
    lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray
) -> np.ndarray:
    """
    Calcul de la distance de Haversine.

    Cela s'appuie sur la formule de haversine qui permet de déterminer

    la distance du grand cercle entre deux points d'une sphère

    (à partir de leurs longitudes et latitudes en radian).

    Args:

        latitude et longitude d'au moins 2 points en arrays

        lat_a en np.array, lon_a en np.array,

        lat_b en np.array, lon_b en np.array

    Return:

        distance calculée (km)  entre les points a et b

    """
    RAYON_TERRE = 6371  # En kilomètres
    # Conversion en radian des position Lat/Lon
    lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])
    # Calcul du delta d'angle
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    # Formule de Haversine
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    # Renvoie la distance
    return 2 * RAYON_TERRE * np.arcsin(np.sqrt(a))


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

    Return:

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
    # Je prepare les données au chargement :
    df_clean_sql = prepare_dataframe_for_sql(df)

    df_clean_sql.to_sql(
        name=nom_table,  # nom de la table
        con=engine,  # moteur SQLAlchemy
        if_exists="replace",  # 'replace' = supprime et recrée, 'append' = ajoute
        index=False,  # ne pas écrire l'index comme une colonne
    )

    print("Table", nom_table, "chargee !")


def nettoyage_region_departement_latitude(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process  Vérification du format des dates, code département et region

    Args:

        Dataframe à nettoyer

    Returns:

        Dataframe nettoyé

    """
    # Suppression des communes de naissance fictive
    df = df[df["ville_naissance"] != "COMMUNE FICTIVE"]

    # Suppression des enregistrements avec des codes region/departement, latitude non renseignés !
    df_clean = df[
        df["latitude_naissance"].notna()
        & df["latitude_deces"].notna()
        & df["code_departement_naissance"].notna()
        & df["code_departement_deces"].notna()
    ].copy()

    # respect du format
    df_clean["date_naissance_dt"] = pd.to_datetime(df_clean["date_naissance_dt"])
    df_clean["date_deces_dt"] = pd.to_datetime(df_clean["date_deces_dt"])

    # Preparatif pour les graph extraction des mois et jour
    df_clean["month_deces"] = df_clean["date_deces_dt"].dt.strftime(
        "%b"
    )  # # month_name()
    df_clean["day_deces"] = df_clean["date_deces_dt"].dt.day

    return df_clean


def ajout_distance_classe_age_origine(df_clean_: pd.DataFrame) -> pd.DataFrame:
    """
    Process  Ajout des classes d'age et comptabilité liée,
                nombre personne originaire, des distances entre
                ville de naissance et de deces

    Args:

        Dataframe à transformer

    Returns:

        Dataframe transforme

    """
    df_clean = df_clean_.copy()

    # Définition des bornes des classes (attention à bien couvrir tout l’intervalle)
    bins = [0, 30, 60, 90, 130]

    # Noms des classes
    labels = ["0-30", "30-60", "60-90", "90+"]

    df_clean["classe_age"] = pd.cut(
        df_clean["age"], bins=bins, labels=labels, duplicates="drop", right=False
    )

    # Mapping des codes vers des labels men women
    df_clean["sexe"] = df_clean["sex"].map({"1": "Man", "2": "Woman"})

    # Calcul de la distance entre coordonnées de naissance et de mort :
    df_clean["distance"] = list(
        haversine_np(
            np.array(df_clean["latitude_naissance"]),
            np.array(df_clean["longitude_naissance"]),
            np.array(df_clean["latitude_deces"]),
            np.array(df_clean["longitude_deces"]),
        )
    )

    df_clean["distance"] = df_clean["distance"].round(0)

    # Comptage des morts et distance moyenne
    df_clean_nb_deces_age_distance = df_clean.groupby(
        ["annee", "num_insee_deces"], as_index=False
    ).agg(
        nb_deces=("idligne", "count"),
        distance_tot=("distance", lambda x: round(x.sum(), 1)),
    )
    # Recuperation du nombre de mort origianire de cette ville
    df_clean_nb_ville_origine = (
        df_clean.query("origine_ville == 'O'")
        .groupby(["annee", "num_insee_deces"], as_index=False)
        .agg(nb_origine_ville=("idligne", "count"))
    )
    df_clean_nb_departement_origine = (
        df_clean.query("origine_departement == 'O'")
        .groupby(["annee", "num_insee_deces"], as_index=False)
        .agg(nb_origine_departement=("idligne", "count"))
    )
    df_clean_nb_region_origine = (
        df_clean.query("origine_region == 'O'")
        .groupby(["annee", "num_insee_deces"], as_index=False)
        .agg(nb_origine_region=("idligne", "count"))
    )

    # Recuperation du nombre de mort
    df_clean_nb_woman = (
        df_clean[df_clean["sexe"] == "Woman"]
        .groupby(["annee", "num_insee_deces"], as_index=False)
        .agg(nb_woman=("idligne", "count"))
    )

    # Recuperation du nombre de mort par tranche age
    df_clean_nb_0_30 = (
        df_clean.query("classe_age == '0-30'")
        .groupby(["annee", "num_insee_deces"], as_index=False)
        .agg(
            nb_deces_0_30=("idligne", "count"),
            distance_tot_0_30=("distance", lambda x: round(x.sum(), 1)),
        )
    )
    # 30 à 60
    df_clean_nb_30_60 = (
        df_clean.query("classe_age == '30-60'")
        .groupby(["annee", "num_insee_deces"], as_index=False)
        .agg(
            nb_deces_30_60=("idligne", "count"),
            distance_tot_30_60=("distance", lambda x: round(x.sum(), 1)),
        )
    )
    # 60 à 90
    df_clean_nb_60_90 = (
        df_clean.query("classe_age == '60-90'")
        .groupby(["annee", "num_insee_deces"], as_index=False)
        .agg(
            nb_deces_60_90=("idligne", "count"),
            distance_tot_60_90=("distance", lambda x: round(x.sum(), 1)),
        )
    )

    # 90 et plus
    df_clean_nb_plus_90 = (
        df_clean.query("classe_age == '90+'")
        .groupby(["annee", "num_insee_deces"], as_index=False)
        .agg(
            nb_deces_plus_90=("idligne", "count"),
            distance_tot_plus_90=("distance", lambda x: round(x.sum(), 1)),
        )
    )

    # Top prenom :
    df_clean_insee_prenom_woman = (
        df_clean.query("sexe == 'Woman'")
        .groupby(["annee", "num_insee_deces"], as_index=False)["prenom"]
        .agg(lambda x: x.value_counts().index[0])
    )
    df_clean_insee_prenom_woman.rename(columns={"prenom": "name_woman"}, inplace=True)

    df_clean_insee_prenom_man = (
        df_clean.query("sexe == 'Man'")
        .groupby(["annee", "num_insee_deces"], as_index=False)["prenom"]
        .agg(lambda x: x.value_counts().index[0])
    )
    df_clean_insee_prenom_man.rename(columns={"prenom": "name_man"}, inplace=True)

    # Aggregation sur les mois
    df_clean_grp_month = df_clean.groupby(
        ["annee", "num_insee_deces", "month_deces"], as_index=False
    ).agg(nb_deces_month=("idligne", "count"))
    df_clean_month = df_clean_grp_month.pivot_table(
        index=["annee", "num_insee_deces"],
        columns="month_deces",
        values="nb_deces_month",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    # On merge tous ces dataframes :
    dfs = [
        df_clean_nb_deces_age_distance,
        df_clean_nb_ville_origine,
        df_clean_nb_departement_origine,
        df_clean_nb_region_origine,
        df_clean_nb_woman,
        df_clean_nb_0_30,
        df_clean_nb_30_60,
        df_clean_nb_60_90,
        df_clean_nb_plus_90,
        df_clean_insee_prenom_woman,
        df_clean_insee_prenom_man,
        df_clean_month,
    ]

    df_final = reduce(
        lambda left, right: pd.merge(
            left, right, on=["annee", "num_insee_deces"], how="outer"
        ),
        dfs,
    )
    # Ajustements du merge
    df_final = df_final.fillna(0)

    # declaration de colonne à corriger
    cols_a_modifier = [
        "nb_origine_ville",
        "nb_origine_departement",
        "nb_origine_region",
        "nb_woman",
        "nb_deces_0_30",
        "nb_deces_30_60",
        "nb_deces_60_90",
        "nb_deces_plus_90",
    ]

    # Formattage des colonnes
    df_final[cols_a_modifier] = df_final[cols_a_modifier].astype(int)

    return df_final


## -------------------------------------------------------------------------##
#                                    MAIN
## -------------------------------------------------------------------------##

# Path
PATH_RACINE, PATH_LOG, BASE_DIR = gestion_path_ini()

# Instancier la classe d'accès à la base de données
my_bdd = ConnexionBdd(path_racine = PATH_RACINE, filename = "Fichier_Connexion.ini", 
section = "postgresql" 
)
# Creation de l'Url
url_Bdd = my_bdd.creation_de_chaine_de_connexion()

#engine = create_engine(creation_de_chaine_de_connexion())
engine = create_engine(url_Bdd)

# Existence du dictionnaire ? Sinon creation du dictionnaire
if not existence_bdd_dictionnaire_fichiers_personne_decedee(engine, "nom_url"):
    creation_bdd_dictionnaire_fichiers_personne_decedee(url_Bdd)

# ----
dans_la_liste = ["1991",
    "1996",
    "2001",
    "2006",
    "2011",
    "2016",
    "2021",
    "2024" ]
'''
dans_la_liste = [
    "2005",
    "2006",
    "2007",
    "2008",
    "2009",
    "2010",
    "2011",
    "2012",
    "2013",
    "2014",
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
    "2021",
    "2022",
    "2023",
    "2024",
]
'''
# init dataframe
all_df = pd.DataFrame()
#
for une_annee in dans_la_liste:
    #
    le_df = telechargement_fichier_personne_decedee_selon_annee(
        url_Bdd, BASE_DIR, une_annee
    )

    creer_base_et_table_personne_decedee(PATH_RACINE, url_Bdd, le_df)

    #engine = create_engine(creation_de_chaine_de_connexion())
    print("engine ?")
    engine = create_engine(url_Bdd)

    # Possible d'éviter l'itération des colonnes via information_schema de PostgreSQl
    la_query = "SELECT idligne, prenom,sex,date_naissance_dt,num_insee_naissance,"
    la_query = la_query + "ville_naissance,pays_naissance,latitude_naissance,"
    la_query = la_query + "longitude_naissance,code_departement_naissance,"
    la_query = la_query + "nom_departement_naissance, code_region_naissance,"
    la_query = la_query + "nom_region_naissance,date_deces_dt,num_insee_deces,"
    la_query = la_query + "ville_deces,latitude_deces,longitude_deces,"
    la_query = la_query + "code_departement_deces,nom_departement_deces,"
    la_query = la_query + "code_region_deces,nom_region_deces,age,annee,"
    la_query = (
        la_query
        + "origine_ville,origine_departement,origine_region FROM death_people_view"
    )

    # la_query_insee_naissance_deces = "SELECT annee,num_insee_naissance, num_insee_deces, COUNT(idligne)"
    # la_query_insee_naissance_deces = la_query_insee_naissance_deces + " FROM death_people_view"
    # la_query_insee_naissance_deces = la_query_insee_naissance_deces + " WHERE num_insee_naissance<> num_insee_deces"
    # la_query_insee_naissance_deces = la_query_insee_naissance_deces + " AND UPPER(nom_departement_naissance)"
    # la_query_insee_naissance_deces = la_query_insee_naissance_deces + " <> UPPER(nom_departement_deces)"
    # la_query_insee_naissance_deces = la_query_insee_naissance_deces + " GROUP BY annee,num_insee_naissance,"
    # la_query_insee_naissance_deces = la_query_insee_naissance_deces + " num_insee_deces HAVING COUNT(*) >30"
    # la_query_insee_naissance_deces = la_query_insee_naissance_deces + " ORDER BY COUNT DESC"

    with engine.connect() as conn:
        df = pd.read_sql(text(la_query), conn)
        # Deuxième requête
        # df2 = pd.read_sql(text(la_query_insee_naissance_deces), conn)

    df_clean = nettoyage_region_departement_latitude(df)
    # incohérences identifiés dans mvt de commune => pb de nom departement/region
    df_clean_nan = df_clean.dropna(subset=["nom_departement_deces"])

    # Creation des champs classe et taux d'origine :
    df_final = ajout_distance_classe_age_origine(df_clean_nan)

    all_df = pd.concat([all_df, df_final], axis=0)

# engine = create_engine(creation_de_chaine_de_connexion())
engine = create_engine(url_Bdd)

# Base ORM
Base = declarative_base()


# Définition de la table
class Insee_year_death_origine_prenom(Base):
    __tablename__ = "insee_year_death_origine_prenom"
    annee = Column(
        String(4), comment="2ieme elément de la clé : annee de deces", nullable=False
    )
    num_insee_deces = Column(
        String(5),
        comment="1er elément de la clé : Num insee de la ville de deces",
        nullable=False,
    )
    nb_deces = Column(Integer, comment="Nombre de deces dans cette ville")
    nb_woman = Column(Integer, comment="Nombre de deces de femme")
    nb_origine_ville = Column(
        Integer, comment="Nombre de deces originaire de cette ville"
    )
    nb_origine_departement = Column(
        Integer, comment="Nombre de deces originaire de ce departement"
    )
    nb_origine_region = Column(
        Integer, comment="Nombre de deces originaire de cette region"
    )
    nb_deces_0_30 = Column(Integer, comment="Nombre de deces pour 0-30 ans")
    nb_deces_30_60 = Column(Integer, comment="Nombre de deces pour 30-60 ans")
    nb_deces_60_90 = Column(Integer, comment="Nombre de deces pour 60-90 ans")
    nb_deces_plus_90 = Column(Integer, comment="Nombre de deces pour plus de 90 ans")
    distance_tot = Column(
        Float, comment="Cumul distance entre ville de naissance et de deces"
    )
    distance_tot_0_30 = Column(
        Float,
        comment="Cumul distance entre ville de naissance et de deces pour 0-30 ans",
    )
    distance_tot_30_60 = Column(
        Float,
        comment="Cumul distance entre ville de naissance et de deces pour 30-60 ans",
    )
    distance_tot_60_90 = Column(
        Float,
        comment="Cumul distance entre ville de naissance et de deces pour 60-90 ans",
    )
    distance_tot_plus_90 = Column(
        Float,
        comment="Cumul distance entre ville de naissance et de deces pour plus de 90 ans",
    )
    name_woman = Column(
        String(50), comment="Top 1 prenom des personnes decedees dans cette ville (F)"
    )
    name_man = Column(
        String(50), comment="Top 1 prenom des personnes decedees dans cette ville (H)"
    )
    Jan = Column(Integer, comment="Nombre de morts en janvier")
    Fev = Column(Integer, comment="Nombre de morts en fevrier")
    Mar = Column(Integer, comment="Nombre de morts en mars")
    Apr = Column(Integer, comment="Nombre de morts en avril")
    May = Column(Integer, comment="Nombre de morts en mai")
    Jun = Column(Integer, comment="Nombre de morts en juin")
    Jul = Column(Integer, comment="Nombre de morts en juillet")
    Aug = Column(Integer, comment="Nombre de morts en aout")
    Sep = Column(Integer, comment="Nombre de morts en septembre")
    Oct = Column(Integer, comment="Nombre de morts en octobre")
    Nov = Column(Integer, comment="Nombre de morts en novembre")
    Dec = Column(Integer, comment="Nombre de morts en decembre")
    # Définition de la clé primaire composite
    __table_args__ = (
        PrimaryKeyConstraint("annee", "num_insee_deces"),
        {
            "comment": "Table des statitiques sur les communes pour une année Nombre de mort, femme, originaire de la commune, classe d'âge et top prenom"
        },
    )


# Création de la table dans la base
Base.metadata.create_all(engine)

chargement_df_en_sql(engine, all_df, "insee_year_death_origine_prenom")
# a finir :
# chargement_df_en_sql(engine, all_df, 'insee_naissance_death')

# Correct pour vider/fermer le pool de connexions
engine.dispose()
print("Fin TRT")
