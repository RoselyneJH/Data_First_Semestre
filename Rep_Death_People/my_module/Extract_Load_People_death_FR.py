###############################################################################
#                                    CHARGEMENT PARTIE PERSONNE
###############################################################################

## --------------------------------------------------------------------------##
#                                        LIBRAIRIES
## --------------------------------------------------------------------------##

import pandas as pd
import re
import requests
import time

from loguru import logger

from pydantic import BaseModel, Field, ValidationError, constr
from typing import List, Literal
from typing import List, Dict, Union, Tuple, Literal

# pd.options.mode.chained_assignment = None
# pd.set_option("display.max_colwidth", None)

from configparser import ConfigParser

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Date,
    text,
    MetaData,
    Table,
    Float,
    ForeignKey,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

from Connexion_Bdd import ConnexionBdd

# On recharge la table death_people et également le nom de tous les fichiers
# deces depuis 30 ans en Bdd
ETAT_BDD = "NON_CHARGE"  # "DEJA_CHARGE" # "NON_CHARGE"

## -------------------------------------------------------------------------##
#                                  FONCTIONS
## -------------------------------------------------------------------------##


def gestion_path_ini() -> str:
    """
    Utilisation d'un chemin absolu pour la récupération
    des fichiers ini, sql d'une part et la log d'autre part

    Args:

        None

    Return:

        Recherche du repertoire des fichiers log,
        et recherche du repertoire des fichiers ini et sql

    """
    # chemin absolu du répertoire contenant ce fichier __init__.py
    base_dir = os.path.dirname(os.path.abspath(__file__))
    racine_projet = os.path.normpath(os.path.abspath(os.path.join(base_dir, "..")))

    # chemin vers ton fichier.ini
    log_path = os.path.normpath(os.path.join(base_dir, "..", "my_log"))

    return racine_projet, log_path, base_dir


def recuperer_df_name_and_url(html_text: str) -> pd.DataFrame:
    """
    Args:

        Le contenu du site

    Returns:

        Le dataframe contenant les noms de fichier à periodicité annuelle et leur url de téléchargement

    """
    # Étape 1 : Item à chercher
    item_dece = "/deces-"
    item_file = "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/"
    # Étape 2 : Récupérer les noms fichier et urls correspondant
    links_url = []
    links_file_name = []
    # links_periodicite_non_annuel = []

    for line in html_text.splitlines():  # html_text
        if item_file in line and item_dece in line:
            # motif=re.escape(item_dece)
            # Recherche de toutes les occurrences avec leurs indices pour reconstruire le
            for match in re.finditer(item_file, line):
                start = match.start()
                pos_buttee = line.find('.txt","', match.start())
                url_file = line[start : pos_buttee + 4]

                if len(url_file) < 150:
                    # print(" File ",url_file)
                    name_file = url_file[
                        url_file.find(item_dece, 1)
                        + len(item_dece) : url_file.find(".txt", 1)
                    ]
                    if (
                        len(name_file) == 4
                    ):  # uniquement ls années qui nous interessent !
                        links_url.append(url_file)
                        links_file_name.append(name_file)

                    # Création du DataFrame
    df = pd.DataFrame({"annee_file": links_file_name, "url_file": links_url})
    df["url_file"] = df["url_file"].str.strip()
    df["name_file"] = df["annee_file"].str.strip()
    return df


def selection_file_deces_annee(df: pd.DataFrame, annee: str) -> str:
    """
    Args:

        L'année selectionnée et le dataframe qui comporte fichier_annee et url correspondant

    Return:

        Renvoie l'url correspondante  : str

    """
    cette_url = df[df["name_file"].str.contains(annee)]["url_file"]
    if cette_url.empty:  # Si je ne recupère rien
        cette_url = df[df["name_file"].str.contains("2024")]["url_file"]
    # je recupère la partie url de la Serie :
    cette_url = cette_url.iloc[-1]
    return cette_url


# Type personnalisé : chaîne de 5 chiffres, y compris les zéros initiaux
# FiveDigitString = constr(regex=r'^\d{5}$')
FiveCharAlnum = constr(pattern=r"^[A-Za-z0-9]{5}$")
DateString = constr(pattern=r"^\d{8}$")


# classe qui definit le modèle de validation des données
class RowModel(BaseModel):
    # Ellipsis qui sert ici à indiquer que le champ est obligatoire
    nom: str = Field(..., description="champ obligatoire - Nom")
    prenom: str = Field(..., description="champ obligatoire - Prenom")
    sex: Literal["1", "2"] = Field(
        ..., description="champ obligatoire - Sex 1 homme, 2 fille"
    )
    date_naissance: DateString = Field(
        ..., description="champ obligatoire - date de naissance"
    )
    num_insee_naissance: FiveCharAlnum = Field(
        ..., description="champ obligatoire - Num Insee naissance"
    )
    ville_naissance: str = Field(
        ..., description="champ obligatoire - ville de naissance"
    )
    pays_naissance: str = Field(
        "FRANCE", description="champ optionnel - pays de naissance"
    )
    date_deces: DateString = Field(..., description="champ obligatoire - date de deces")
    num_insee_deces: FiveCharAlnum = Field(
        ..., description="champ obligatoire - Num Insee Deces"
    )
    #


# Traitement par lots
def validate_in_batches(data: List[Dict], batch_size: int = 2) -> Tuple:
    """
    Args:

         data
                une liste de dictionnaires ('List[dict]'), où chaque dict représente une ligne à valider avec 'RowModel'.

        batch_size

                combien de lignes traiter en même temps (paquet de 2 par défaut).

                valid_rows contiendra toutes les lignes valides après validation.

                error_log enregistrera les lignes rejetées, avec la raison de l'erreur.

    Return:

             -> Tuple[List[...], List[...]]

    """
    valid_rows = []
    error_log = []
    mes_err = []
    # Parcourt les données **par lots** de taille
    # i est l’indice de début du lot actuel (0, 2, 4, …).
    for i in range(0, len(data), batch_size):
        batch = data[
            i : i + batch_size
        ]  # Extrait un sous-ensemble des données : les lignes de l’indice i jusqu’à i + batch_size (exclu).
        for j, row in enumerate(batch):  # j : indice local dans le lot
            idx = i + j
            try:
                validated = RowModel(**row)
                valid_rows.append(validated.dict())
            except ValidationError as e:
                # Ajoute au journal d’erreurs un dictionnaire avec :
                # index: position dans la liste d’origine,
                # row: la ligne brute en erreur,
                # error: le message d’erreur Pydantic.
                error_log.append({"index": idx, "row": row, "error": str(e)})
                for err in e.errors():
                    mes_err.append(
                        {
                            "ligne": i,
                            "champ": ".".join(str(p) for p in err["loc"]),
                            "type": err["type"],
                            "message": err["msg"],
                        }
                    )

    return valid_rows, error_log, mes_err


def parsing_file(le_df_mal_formate: pd.DataFrame) -> List[Dict]:
    """
    Parsing des données

    Args:

        le dataframe mal formaté issu de la lecture du fichier txt

    Return:

        une liste de dictionnaire ayant les noms des colonnes comme clés enrichis de sa valeur

        Les données sont identifiées selon la position quelles occupent dans le fichier

    """
    # 1. Accumule les lignes sous forme de liste de dictionnaires
    raw_data = []
    logger.info("le_df_mal_formate ")
    # J'itère le dataframe et je décompose la colonne many_cols, row n'a pas d'importance ici
    for idx, row in le_df_mal_formate.iterrows():
        # 2. Processus :
        # Je reconnais les champs :
        chaine = le_df_mal_formate.iloc[idx, -1]
        pos_fin = chaine.find("*")
        partie_nom = chaine[0:pos_fin]
        pos_deb = pos_fin + 1  # +len(partie_nom)
        pos_fin = chaine.find("/")
        partie_prenom = chaine[pos_deb:pos_fin]
        partie_restante = chaine[pos_fin + 1 :]
        partie_restante = partie_restante.strip()  # beaucoup d'espace
        partie_sex = partie_restante[0]
        partie_date_naissance = partie_restante[1:9]
        partie_num_insee_naissance = partie_restante[9:14]
        partie_restante_1 = partie_restante[14:]
        pos_fin = partie_restante_1.find("   ")
        partie_ville_naissance = partie_restante_1[0:pos_fin]

        pos_fin = len(partie_ville_naissance) + pos_fin
        partie_autre = chaine[124:]
        if partie_autre[0].isalpha():
            pos_fin = partie_autre.find("   ")
            partie_pays_naissance = partie_autre[0:pos_fin]
            # parfois le pays de naissance est long et vient toucher le pavé déces, sans espace les separant
            if len(partie_pays_naissance) > 29:
                partie_pays_naissance = partie_pays_naissance[0:30]
            else:  # il ya forcement un espace
                pos_fin = len(partie_pays_naissance) + pos_fin
                partie_autre = partie_autre[pos_fin:].strip()
        else:
            partie_pays_naissance = "FRANCE"
        partie_date_deces = chaine[
            154:162
        ]  # en effet, parfois le pays est long et ne laisse pas d'espace avec la zone decs(date et num insee)
        partie_num_insee_deces = chaine[162:167]  # partie_autre[8:13]
        # 3. Ajout de la ligne

        try:
            raw_data.append(
                {
                    "nom": partie_nom,
                    "prenom": partie_prenom,
                    "sex": partie_sex,
                    "date_naissance": partie_date_naissance,
                    "num_insee_naissance": partie_num_insee_naissance,
                    "ville_naissance": partie_ville_naissance,
                    "pays_naissance": partie_pays_naissance,
                    "date_deces": partie_date_deces,
                    "num_insee_deces": partie_num_insee_deces,
                }
            )
        except:
            logger.info(f"Nom {partie_nom},prenom {partie_prenom},sex {partie_sex} ! ")

    return raw_data


def traitement_validation(chemin_w: str, an: str) -> Tuple:
    """
    Lecture du fichier fichier_deces.txt

    Le fichier étant mal formaté, il faut reconstituer chaque ligne et attribuer les bonnes colonnes

    qui ont été mal identifiées. C'est la présence de double-apostrophe qui fait basculer la lecture

    du fichier => parsing

    Puis, on fait une validation des données selon le modèle BaseModel.

    Args:

        chemin de travail

        année selectionnée

    Return:

        DataFrame corrigé + les erreurs de validaton

    """
    # Reconstitution des colonnes et creation du Dataframe de l'année selectionnée :
    start = time.time()

    fichier_complet = os.path.join(chemin_w, "fichier_deces.txt")
    #print("-------->  chemin_w", fichier_complet)

    # Ouverture du fichier  attention, il faut choisir un encoding latin1, pas de separateur car certains
    # fichiers comportent plusieurs virgules et/ou tabulations + un warn sur les lignes incorrectes
    le_df_mal_formate = pd.read_csv(
        fichier_complet, on_bad_lines="warn", header=None, encoding="latin1"
    )

    # Creation Dataframe final, declaration des colonnes :
    df = pd.DataFrame(
        columns=[
            "nom",
            "prenom",
            "sex",
            "date_naissance",
            "num_insee_naissance",
            "ville_naissance",
            "pays_naissance",
            "date_deces",
            "num_insee_deces",
        ]
    )

    # execution du parsing de colonne, travail d'identification des colonnes dans la colonne
    raw_data = parsing_file(le_df_mal_formate)

    # j'ajoute les data dans le dataframe  Exécution
    valid_data, errors, mes_err = validate_in_batches(raw_data, batch_size=5)
    df = pd.DataFrame(valid_data)

    # Fin du traitement
    end = time.time()
    print(f"Durée : {end - start:.2f} secondes")
    logger.info(f"Durée du traitement de parsing : {end - start:.2f} secondes")

    # Convertir en DataFrame
    df_mes_err = pd.DataFrame(mes_err)

    # Résumé des erreurs par type
    if len(df_mes_err) > 0:
        resume = df_mes_err.groupby("champ").size().reset_index(name="nb_erreurs")
    else:
        resume = []

    logger.info(f"Erreur validation personne : {len(errors):,}".replace(",", " "))
    for i in range(len(resume)):
        logger.info(
            f"Validation ko pour le champs {resume.iloc[i,0]} : {resume.iloc[i,1]} erreur(s) "
        )
    logger.info(f"Volume de personnes conservé ({an}) : {len(df):,}".replace(",", " "))

    return df, errors


def formattage_date(
    la_date: str, mode: str = "CHECK_PAS_DE_DOUBLE_ZERO", annee: str = "0000"
) -> str:
    """
    2 Modes

    Mode "CHECK_PAS_DE_DOUBLE_ZERO" : Certaines date ont des jours et/ou mois et ou année à zero ; il faut les identifier

    Mode "CHECK_ANNEE_DECHARGEMENT" : L'objectif est ici d'identifier que l'année de deces correspond à celle du fichier telechargé

             Nous avons besoin de l'année correspondant au téléchargement

    Args:

       colonne string d'un dataframe + le mode à traiter format string + eventuellement une date format string

    Return:

       flag d'invalidation ko si date invalide format string

    """

    str_date = str(la_date)
    validation = "ok"

    if mode == "CHECK_PAS_DE_DOUBLE_ZERO":
        if str_date[6:8] == "00" or str_date[4:6] == "00" or str_date[:4] == "0000":
            validation = "ko"
    if mode == "CHECK_ANNEE_DECHARGEMENT":
        if str_date[:4] != annee and annee != "0000":
            validation = "ko"

    return validation


def verification_date(df: pd.DataFrame, an: str) -> pd.DataFrame:
    """
    Permet de toper les anomalies sur date de naissance et deces.

    Args:

        Vérification des dates de naissance et deces Dataframe

    Return:

        Nettoyage des dates erronées -> Dataframe

    """

    df["validation_date_nai_ft"] = df.apply(
        lambda row: formattage_date(row["date_naissance"]), axis=1
    )
    tx_annomalie_date_naiss_ft = round(
        df[df["validation_date_nai_ft"] == "ko"]["nom"].count()
        * 100
        / df["validation_date_nai_ft"].count(),
        2,
    )
    # pas propre du tout !!!
    if tx_annomalie_date_naiss_ft < 5:
        logger.info(
            f"Dates naiss. avec mois, jour ou année = 00 : {tx_annomalie_date_naiss_ft}% des enreg. A suprimer."
        )
        df_date_naiss_ok = df.query("validation_date_nai_ft=='ok'").copy()
    else:
        logger.info(
            f"Dates naiss. avec mois, jour ou année = 00 : {tx_annomalie_date_naiss_ft}% des enreg. Anomalie !"
        )
        df_date_naiss_ok = df.copy()

    df_date_naiss_ok["validation_date_dc_ft"] = df_date_naiss_ok.apply(
        lambda row: formattage_date(row["date_deces"]), axis=1
    )
    tx_annomalie_date_de_ft = round(
        df_date_naiss_ok[df_date_naiss_ok["validation_date_dc_ft"] == "ko"][
            "nom"
        ].count()
        * 100
        / df_date_naiss_ok["validation_date_dc_ft"].count(),
        2,
    )

    if tx_annomalie_date_de_ft < 5:
        logger.info(
            f"Dates dece. avec mois, jour ou année = 00 : {tx_annomalie_date_de_ft}% des enreg. A suprimer."
        )
        df_date_naiss_deces_ok = df_date_naiss_ok.query(
            "validation_date_dc_ft=='ok'"
        ).copy()
    else:
        logger.info(
            f"Dates dece. avec mois, jour ou année = 00 : {tx_annomalie_date_de_ft}% des enreg. Anomalie !"
        )
        df_date_naiss_deces_ok = df_date_naiss_ok.copy()

    df_date_naiss_deces_ok["validation_date_dc_te"] = df_date_naiss_deces_ok.apply(
        lambda row: formattage_date(row["date_deces"], "CHECK_ANNEE_DECHARGEMENT", an),
        axis=1,
    )
    tx_annomalie_date_deces = round(
        df_date_naiss_deces_ok[df_date_naiss_deces_ok["validation_date_dc_te"] == "ko"][
            "nom"
        ].count()
        * 100
        / df_date_naiss_deces_ok["validation_date_dc_te"].count(),
        2,
    )

    if tx_annomalie_date_deces < 5:
        logger.info(
            f"Dates deces. diff. de l'année de téléchargement : {tx_annomalie_date_deces}% des enreg. A suprimer."
        )
        df_date_naiss_et_deces_ok = df_date_naiss_deces_ok.query(
            "validation_date_dc_te=='ok'"
        ).copy()
    else:
        logger.info(
            f"Dates deces. diff. de l'année de téléchargement : {tx_annomalie_date_deces}% des enreg. Anomalie !"
        )
        df_date_naiss_et_deces_ok = df_date_naiss_deces_ok.copy()

    # format de date naissance et deces :
    df_date_naiss_et_deces_ok["date_naissance_dt"] = pd.to_datetime(
        df_date_naiss_et_deces_ok["date_naissance"], errors="coerce"
    ).dt.normalize()
    df_date_naiss_et_deces_ok["date_deces_dt"] = pd.to_datetime(
        df_date_naiss_et_deces_ok["date_deces"], errors="coerce"
    ).dt.normalize()

    # Attention certaines date sont anciennes exe 2011 date de naissance = 16450513, la normalisation donne NaT
    df_date_naiss_et_deces_ok = df_date_naiss_et_deces_ok[
        df_date_naiss_et_deces_ok["date_naissance_dt"].notna()
    ]
    df_date_naiss_et_deces_ok = df_date_naiss_et_deces_ok[
        df_date_naiss_et_deces_ok["date_deces_dt"].notna()
    ]

    # recherche de doublons
    if df_date_naiss_et_deces_ok.duplicated().sum() > 0:
        logger.info(
            f"Nombre de doublon : {df_date_naiss_et_deces_ok.duplicated().sum()}"
        )
        df_date_naiss_et_deces_ok_clean = df_date_naiss_et_deces_ok.drop_duplicates(
            keep="first"
        ).copy()
    else:
        df_date_naiss_et_deces_ok_clean = df_date_naiss_et_deces_ok.copy()

    df_date_naiss_et_deces_ok_clean.drop(
        columns={
            "validation_date_nai_ft",
            "validation_date_dc_ft",
            "validation_date_dc_te",
        },
        inplace=True,
    )

    return df_date_naiss_et_deces_ok_clean

'''
def configuration_db(
    chemin_w: str, filename: str = "Fichier_Connexion.ini", section: str = "postgresql"
) -> Dict[str, str]:
    """
    Args:

        Nom du fichier de configuration

        Nom de la base de données

        Chemin racine du projet

    Return:

        Renvoie ud dictionnaire avec les éléments de connexion à Bdd

    """
    # Create a parser
    parser = ConfigParser()
    # Read the configuration file
    parser.read(chemin_w + "/" + filename)
    # Get the information from the postgresql section
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception("Section {0} not found in {1}".format(section, filename))

    return db
'''

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


def incoherence_attribution_ville_pays_naissance(
    df_clean: pd.DataFrame,
) -> pd.DataFrame:
    """
    Args:

         Dataframe comportant des incohérences ; par exemple ville d'Oran située dans le département de France

         alors que c'est une vieille colonnie francaise.

    Returns:

        dataframe corrigé de ces valeurs incorrectes

    """
    df_clean.loc[
        (
            df_clean["ville_naissance"].str.contains("DEPARTEMENT")
            | df_clean["ville_naissance"].str.contains("SUD")
            | df_clean["ville_naissance"].str.contains("ANCIEN")
        )
        & (
            df_clean["ville_naissance"].str.contains("ALGER", na=False)
            | df_clean["ville_naissance"].str.contains("CONSTANTINE", na=False)
            | df_clean["ville_naissance"].str.contains("ORAN", na=False)
            | df_clean["ville_naissance"].str.contains("OUENZA", na=False)
            | df_clean["ville_naissance"].str.contains("SEBAA", na=False)
            | df_clean["ville_naissance"].str.contains("SETIF", na=False)
            | df_clean["ville_naissance"].str.contains("TREAT", na=False)
        ),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["ALGERIE", "99352"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (df_clean["ville_naissance"].str.contains("HONGRIE", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["HONGRIE", "99112"]

    df_clean.loc[
        (
            df_clean["ville_naissance"].str.contains("DEPARTEMENT")
            | df_clean["ville_naissance"].str.contains("ANCIENNE")
        )
        & (df_clean["ville_naissance"].str.contains("IVOIRE", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["COTE D'IVOIRE", "99326"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (
            df_clean["ville_naissance"].str.contains("ROUMANI", na=False)
            | df_clean["ville_naissance"].str.contains("HIDA", na=False)
            | df_clean["ville_naissance"].str.contains("BLAJ", na=False)
        ),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["ROUMANIE", "99114"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (df_clean["ville_naissance"].str.contains("CHILLAN", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["CHILI", "99417"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (
            df_clean["ville_naissance"].str.contains("VIET NAM", na=False)
            | df_clean["ville_naissance"].str.contains("VIETNAM", na=False)
        ),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["VIETNAM", "99243"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (df_clean["ville_naissance"].str.contains("SUEDE", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["SUEDE", "99104"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (df_clean["ville_naissance"].str.contains("ETATS-UNIS", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["ETATS-UNIS", "99404"]

    df_clean.loc[
        (
            df_clean["ville_naissance"].str.contains("DEPARTEMENT")
            | df_clean["ville_naissance"].str.contains("ANCIEN")
        )
        & (
            df_clean["ville_naissance"].str.contains("MBAM", na=False)
            | df_clean["ville_naissance"].str.contains("CAMEROUN", na=False)
        ),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["CAMEROUN", "99322"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (df_clean["ville_naissance"].str.contains("ALLEMAGNE", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["ALLEMAGNE", "99109"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (df_clean["ville_naissance"].str.contains("GRECE", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["GRECE", "99126"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (df_clean["ville_naissance"].str.contains("ESPAGNE", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["ESPAGNE", "99134"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("DEPARTEMENT")
        & (df_clean["ville_naissance"].str.contains("URUGUAY", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["URUGUAY", "99423"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("ANCIEN")
        & (df_clean["ville_naissance"].str.contains("BELGIQUE", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["BELGIQUE", "99131"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("ANCIEN")
        & (df_clean["ville_naissance"].str.contains("GUINEE", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["GUINEE", "99330"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("ANCIEN")
        & (df_clean["ville_naissance"].str.contains("GABON", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["GABON", "99328"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("ANCIEN")
        & (df_clean["ville_naissance"].str.contains("MADAGASCAR", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["MADAGASCAR", "99333"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("ANCIEN")
        & (df_clean["ville_naissance"].str.contains("NIGER", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["NIGER", "99337"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("ANCIEN")
        & (df_clean["ville_naissance"].str.contains("SENEGAL", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["SENEGAL", "99341"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("ANCIEN")
        & (df_clean["ville_naissance"].str.contains("TCHAD", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["TCHAD", "99344"]

    df_clean.loc[
        df_clean["ville_naissance"].str.contains("ANCIEN")
        & (df_clean["ville_naissance"].str.contains("TOGO", na=False)),
        ["pays_naissance", "num_insee_naissance"],
    ] = ["TOGO", "99345"]

    return df_clean


def recuperer_df_name_and_url(html_text: str) -> pd.DataFrame:
    """
    Permet de recupere la liste de tous les fichiers de deces depuis les années 1980

    Args:

        Le nom du site

    Returns:

        Le dataframe contenant les noms de fichier à periodicité annuelle et leur url de téléchargement

    """
    # Étape 1 : Item à chercher
    item_dece = "/deces-"
    item_file = "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/"
    # Étape 2 : Récupérer les noms fichier et urls correspondant
    links_url = []
    links_file_name = []
    # links_periodicite_non_annuel=[]

    for line in html_text.splitlines():  # html_text
        if item_file in line and item_dece in line:
            # motif=re.escape(item_dece)
            # Recherche de toutes les occurrences avec leurs indices pour reconstruire le
            for match in re.finditer(item_file, line):
                start = match.start()
                pos_buttee = line.find('.txt","', match.start())
                url_file = line[start : pos_buttee + 4]

                if len(url_file) < 150:
                    # print(" File ",url_file)
                    name_file = url_file[
                        url_file.find(item_dece, 1)
                        + len(item_dece) : url_file.find(".txt", 1)
                    ]
                    if (
                        len(name_file) == 4
                    ):  # uniquement ls années qui nous interessent !
                        # print(">> ",name_file)
                        links_url.append(url_file)
                        links_file_name.append(name_file)

                    # Création du DataFrame
    df = pd.DataFrame({"annee_file": links_file_name, "url_file": links_url})
    df["url_file"] = df["url_file"].str.strip()
    df["annee_file"] = df["annee_file"].str.strip()

    return df

'''
def creation_de_chaine_de_connexion(chemin_w: str) -> str:
    """
    Creation de la chaine de connexion

    Args:

        Chemin racine du projet

    Return:

        Renvoie un chaine de caractère de la connexion -> url

    """
    # Lecture du fichier ini :
    db = configuration_db(chemin_w)

    # Préparation de la chaine de connexion
    host = db["host"]
    port = db["port"]
    database = db["database"]
    user = db["user"]
    password = db["password"]
    #  Créer l'URL SQLAlchemy
    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
    return url
'''

def get_row_with_fallback(engine: Engine, an: str) -> pd.DataFrame:
    """
    L'objectif est de recuperer en base l'URl du fichier des personnes décédées

    pour l'année selectionée :

    Args:

        la connexion + l'année selectionnée.
        Si l'année selectionnée n'est pas valide ou n'existe pas en base
        on prend l'année par defaut =2024

    Returns:

      un dataframe comportant l'année et l'url + l'année

    """
    AN_PAR_DEFAUT = "2024"

    LA_QUERY_S = "SELECT annee_file, url_file FROM nom_url"

    for current_an in (an, AN_PAR_DEFAUT):
        la_query = LA_QUERY_S + " where annee_file='" + current_an + "' "
        # print("la_query",la_query)
        with engine.connect() as connection:
            result = connection.execute(text(la_query))
            row = result.fetchone()
            # print("row",row)
            if row is not None:
                # print("trouve",type(row))
                return pd.DataFrame([row], columns=result.keys()), current_an
    return None, an


def select_query_annee(url_Bdd: str, an: str = "") -> pd.DataFrame:
    """
    Create une requete afin de recuperer l'url_name en fonction de l'année

    Args:

        L'année selectionnée

        Chemin de travail

    Returns:

        Le nom de l'url à telecharger et l'année par defaut, si celle fournie est

        incorrecte ou non presente dans la bdd

    """
    # Créer le moteur SQLAlchemy
    # avant : engine = create_engine(creation_de_chaine_de_connexion(chemin_w))
    engine = create_engine(url_Bdd)
    #
    # with engine.connect() as conn:
    df, an = get_row_with_fallback(engine, an)

    engine.dispose()

    return df, an


# Fonction qui permet d'enchainer les traitements de validation du fichier des personnes decedées
def telechargement_fichier_personne_decedee_selon_annee(
    url_Bdd: str, base_dir: str, an: str
) -> pd.DataFrame:
    """
    Permet un téléchargement du fichier des personnes decedees de l'année selectionnée.

    Args:
        url Bdd


        année selectionée

    Returns:

     Le fichier des personnes decedées

        Traitement de validation des données :

            Suppression des doublons

            Suppression des deces ne correspondant pas à l'année traitée

            Correction des dates deces et naissance et/ou suppression selon degré d'incohérences

            Respect du format des données

            Taux d'erreur en log

    """
    # Download file
    les_urls, an = select_query_annee(url_Bdd, an)

    resultat = les_urls.loc[les_urls["annee_file"] == an, "url_file"]

    # Plusieurs lignes possibles ou pas :
    if resultat.empty:
        print("Aucune valeur trouvée pour l'annee", an, "; Selection de l'année 2024")
        resultat = les_urls.loc[les_urls["annee_file"] == "2024", "url_file"]
    elif len(resultat) > 1:
        print(
            "Plusieurs valeurs trouvées pour l'annee ",
            an,
            "; Selection de l'année 2024",
        )
        resultat = les_urls.loc[les_urls["annee_file"] == "2024", "url_file"]

    url = resultat.values[0]

    try:
        response = requests.get(url)
        response.raise_for_status()  # Vérifie si la réponse est OK (code 200)
        print("--------> avant open fichier", an)
        # Sauvegarder le fichier si la requête est réussie
        with open("fichier_deces.txt", "wb") as f:
            f.write(response.content)

        print("Fichier téléchargé avec succès.")
        logger.info(f"Fichier ({an}) téléchargé avec succès.")
        # print("---------> fichier base", base_dir)
        df, errors = traitement_validation(base_dir, an)

        df_clean = verification_date(df, an)
        df_clean = incoherence_attribution_ville_pays_naissance(df_clean)

        # Calcul de l'age de la personne :
        df_clean["age"] = (
            (df_clean["date_deces_dt"] - df_clean["date_naissance_dt"]) // 365
        ).dt.days.astype(int)

        # attention il faut que l'age < 0 !
        df_clean["age"] = df_clean["age"].apply(lambda x: x if x > 0 else 0)

        # ajout de la colonne :
        df_clean["id_index"] = df_clean.index.to_list()
        s_padded = an.ljust(10, "0")  # '2024000000'
        seuil = int(s_padded)
        df_clean["idligne"] = df_clean["id_index"] + seuil
        df_clean["annee"] = an  # J'affecte la même année pour ces données
        df_clean["ville_deces"] = ""
        df_clean = df_clean[
            [
                "idligne",
                "nom",
                "prenom",
                "sex",
                "date_naissance_dt",
                "num_insee_naissance",
                "ville_naissance",
                "pays_naissance",
                "date_deces_dt",
                "num_insee_deces",
                "ville_deces",
                "age",
                "annee",
            ]
        ]

        # on peut le supprimer
        os.remove("fichier_deces.txt")

    except requests.exceptions.HTTPError as errh:
        print(f" Erreur HTTP : {errh}")
        logger.info(f" Erreur HTTP : {errh}")

    except requests.exceptions.ConnectionError as errc:
        print(f" Erreur de connexion : {errc}")
        logger.info(f" Erreur de connexion : {errc}")

    except requests.exceptions.Timeout as errt:
        print(f" Temps d’attente dépassé : {errt}")
        logger.info(f" Temps d’attente dépassé : {errt}")

    except requests.exceptions.RequestException as err:
        print(f" Erreur inconnue : {err}")
        logger.info(f" Erreur inconnue : {err}")

    finally:
        logger.info("TELECHARGEMENT DE LA TABLE DECES TERMINE")

    return df_clean


def creer_base_et_table_personne_decedee(chemin_w: str, url_Bdd: str, df_clean: pd.DataFrame) -> None:
    """
    Args:

        Url de la base de données

        Dataframe des personnes à charger en base

    Return:

        None

    """
    # Création du moteur SQLAlchemy - Crée le moteur de connexion à PostgreSQL (via psycopg)
    engine = create_engine(url_Bdd)
    

    # Déclaration de la base ORM
    Base = declarative_base()

    # Définition de la table
    class Personne(Base):
        __tablename__ = "death_people"
        # nom	prenom	sex	date_naissance	num_insee_naissance	ville_naissance	pays_naissance
        # date_deces	num_insee_deces	date_naissance_dt	date_deces_dt	age
        idligne = Column(
            Integer, primary_key=True, autoincrement=True
        )  # auto-incrément !
        nom = Column(String)
        prenom = Column(String)
        sex = Column(Integer)
        date_naissance_dt = Column(Date)
        num_insee_naissance = Column(String)
        ville_naissance = Column(String)
        pays_naissance = Column(String)
        date_deces_dt = Column(Date)
        num_insee_deces = Column(String)
        ville_deces = Column(String)
        age = Column(Integer)
        annee = Column(String)

        def __repr__(self):
            return f"<death_people(id={self.idligne}, nom='{self.nom}')>"

    # Supprimer l’ancienne table si elle existe, avec begin le commit se fait en automatique.
    ma_requete_suppression = "DROP TABLE IF EXISTS death_people CASCADE;"  #

    with engine.begin() as connection:
        connection.execute(text(ma_requete_suppression))  #
        print("Ligne(s) Supprimée(s)")

    # Création de la table dans la base
    Base.metadata.create_all(engine)

    print("Table 'death_people' créée avec succès !")
    # Envoi dans PostgreSQL
    df_clean_sql = prepare_dataframe_for_sql(df_clean)

    df_clean_sql.to_sql(
        name="death_people",  # nom de la table
        con=engine,  # moteur SQLAlchemy
        if_exists="replace",  # 'replace' = supprime et recrée, 'append' = ajoute
        index=False,  # ne pas écrire l'index comme une colonne
    )
    print("Données insérées depuis le DataFrame dans la table PostgreSQL")

    # Permet de lancer un script de creation des outils necessaires, table, index etc..
    try:
        with engine.connect() as connection:
            with connection.begin():  # démarre une transaction
                # Charger un script SQL depuis un fichier
                with open(
                    chemin_w + "/" + "Prj_Death_People_death_people_BDD.sql", "r"
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

'''
# NEW
def chemin_de_travail() -> str:
    """
    Permet d'identifier le path de travail de ce module

    Args:

        None

    Returns:

        Le chemin du path associé à la recupérations des données

    """
    PATH_RACINE, PATH_LOG = gestion_path_ini()
    return PATH_RACINE
'''

## -------------------------------------------------------------------------##
#                                  MAIN
## -------------------------------------------------------------------------##

if __name__ == "__main__":
    # Path
    PATH_RACINE, PATH_LOG, BASE_DIR = gestion_path_ini()

    # Instancier la classe d'accès à la base de données
    my_bdd = ConnexionBdd(
        path_racine=PATH_RACINE, filename="Fichier_Connexion.ini", section="postgresql"
    )
    # Creation de l'Url
    url_Bdd = my_bdd.creation_de_chaine_de_connexion()
    
    if ETAT_BDD == "NON_CHARGE":
        # Configurer loguru
        logger.add(
            PATH_LOG + "/" + "DownLoad_File_death.log", rotation="500 MB", level="INFO"
        )
        #
        logger.info("DEBUT TRT")

        # -------------- 1. Telechargement de fichiers de personnes decedees (1 /an) ----
        # Il existe une trentaine de fichier.
        # L'objectif ici est de charger leur nom+année dans notre DWH.

        # ----------------------------------------------------------------------------
        URL_PERSONNE_DECEDEE = (
            "https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/"
        )
        response = requests.get(URL_PERSONNE_DECEDEE)
        html_text = response.text
        df_url_a_copier = recuperer_df_name_and_url(html_text)

        df_url_a_copier["idligne"] = df_url_a_copier.index.to_list()
        df_url = df_url_a_copier[["idligne", "annee_file", "url_file"]].copy()

        # -------------- 2. Chargement des données en Bdd   ---------------------------

        # Créer le moteur SQLAlchemy
        engine = create_engine(url_Bdd)

        # Create Metadata object
        metadata = MetaData()

        # Définition de la table fact
        nom_url = Table(
            "nom_url",
            metadata,
            Column("idligne", Integer, primary_key=True),
            Column("annee_file", String, nullable=False),
            Column("url_file", String, nullable=False),
        )

        # Supprimer l’ancienne table si elle existe
        with engine.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS nom_url"))
            print("Table Supprimée")

        # Création de la table dans la base
        metadata.create_all(engine)

        print("Table 'nom_url' créée avec succès !")

        # Envoi dans PostgreSQL
        df_clean_sql = prepare_dataframe_for_sql(df_url)

        df_clean_sql.to_sql(
            name="nom_url",  # nom de la table
            con=engine,  # moteur SQLAlchemy
            if_exists="replace",  # 'replace' = supprime et recrée, 'append' = ajoute
            index=False,  # ne pas écrire l'index comme une colonne
        )

        logger.info(
            f"Chargement dans le DWH de Table nom_url {df_clean_sql.shape[0]:,}".replace(
                ",", " "
            )
        )
        print("Données insérées depuis le DataFrame dans la table PostgreSQL")

        engine.dispose()

        ETAT_BDD = "DEJA_CHARGE"

    # ---- 3. Lecture de la Bdd puis recuperation du fichier deces pour parsing --

    an = "2024"

    # Telechargement du fichier :
    df_clean = telechargement_fichier_personne_decedee_selon_annee(
        url_Bdd, BASE_DIR, an
    )

    # avant engine = create_engine(creation_de_chaine_de_connexion())
    engine = create_engine(url_Bdd)

    # Base ORM
    Base = declarative_base()

    # Définition de la table
    class Personne(Base):
        __tablename__ = "death_people"
        idligne = Column(
            Integer, primary_key=True, autoincrement=True
        )  # auto-incrément !
        nom = Column(String)
        prenom = Column(String)
        sex = Column(Integer)
        date_naissance_dt = Column(Date)
        num_insee_naissance = Column(String)
        ville_naissance = Column(String)
        pays_naissance = Column(String)
        date_deces_dt = Column(Date)
        num_insee_deces = Column(String)
        ville_deces = Column(String)
        age = Column(Integer)
        annee = Column(String)

    ma_requete_suppression = "DROP TABLE IF EXISTS death_people CASCADE;"
    # Supprimer l’ancienne table si elle existe
    with engine.begin() as connection:
        connection.execute(text(ma_requete_suppression))
        print("Ligne(s) Supprimée(s)")

    # Création de la table dans la base
    Base.metadata.create_all(engine)

    print("Table 'death_people' créée avec succès !")

    # Envoi dans PostgreSQL
    df_clean_sql = prepare_dataframe_for_sql(df_clean)

    df_clean_sql.to_sql(
        name="death_people",  # nom de la table
        con=engine,  # moteur SQLAlchemy
        if_exists="replace",  # 'replace' = supprime et recrée, 'append' = ajoute
        index=False,  # ne pas écrire l'index comme une colonne
    )

    logger.info(
        f"Chargement dans le DWH de Table deces {df_clean_sql.shape[0]:,}".replace(
            ",", " "
        )
    )
    # {df_clean_sql.shape[0]:,}".replace(",", " ")
    print("Données insérées depuis le DataFrame dans la table PostgreSQL")

    #######################################################################################
    # Permet de lancer un script de creation des outils necessaires, table, index etc..

    try:
        with engine.connect() as connection:
            with connection.begin():  # démarre une transaction
                # Charger un script SQL depuis un fichier
                with open(
                    PATH_RACINE + "/" + "Prj_Death_People_death_people_BDD.sql", "r"
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

    # vider/fermer le pool de connexions
    engine.dispose()
