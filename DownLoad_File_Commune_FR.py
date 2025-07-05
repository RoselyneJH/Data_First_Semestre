###################################################################################################
#                                    CHARGEMENT PARTIE COMMUNE 
###################################################################################################

# Telecharement du fichier 
# https://www.insee.fr/fr/information/4316069 
# Voir aussi les communes de la nouvelles caledonie :
# https://fr.wikipedia.org/wiki/Liste_des_communes_de_la_Nouvelle-Cal%C3%A9donie
#  SITE :
# https://explore.data.gouv.fr/fr/datasets/5ced14688b4c4114480ce688/#/resources/dbe8a621-a9c4-4bc3-9cae-be1699c5ff25

import requests
import pandas as pd
import re
import pandas as pd

import requests
import time
import zipfile
import io

from sqlalchemy import Column, Integer, String, ForeignKey, CheckConstraint
from sqlalchemy.exc import SQLAlchemyError

from configparser import ConfigParser
from typing import Optional
from io import StringIO

from sqlalchemy import create_engine, Column, Integer, String, Date, text,Float
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from typing import List, Dict, Union, Tuple, Literal

from bs4 import BeautifulSoup


# URL directe vers le(s) fichier(s)
url_commune_principale = "https://www.data.gouv.fr/fr/datasets/r/dbe8a621-a9c4-4bc3-9cae-be1699c5ff25"
url_new_caledonie = "https://fr.wikipedia.org/wiki/Liste_des_communes_de_la_Nouvelle-Cal%C3%A9donie"
url_mvt_commune="https://www.insee.fr/fr/statistiques/fichier/4316069/mvtcommune2020-csv.zip"

def configuration_db(filename:str ='Fichier_Connexion.ini',section:str ='postgresql') ->Dict[str, str] :
    # Create a parser
    parser= ConfigParser()
    # Read the configuration file
    parser.read(filename)
    # Get the information from the postgresql section
    db={}
    if parser.has_section(section):
        params=parser.items(section)
        for param in params:
            db[param[0]]= param[1]
    else:
        raise Exception('Section {0} not found in {1}'.format(section,filename))
    
    return db

def prepare_dataframe_for_sql(df: pd.DataFrame, drop_columns=None) ->pd.DataFrame:
    """
    Nettoie un DataFrame avant insertion SQL :
    - Convertit les colonnes de type category en string
    - Convertit les colonnes datetime en date (si souhaité)
    - Supprime les colonnes spécifiées (ex: Id auto-incrémentée)
    - Remplace NaN par None (si utile pour PostgreSQL)

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

def chargement_df_en_sql(engine: Engine, df:pd.DataFrame,nom_table:str) -> None:
    '''
    Effetcue le chargement du Dadaframe dans une Table sql
    Entrée : Le moteur de connexion, le dataframe et le nom de la table à charger
    Sortie : pas de sortie, message de bonne réalisation
    '''
    df_clean_sql=prepare_dataframe_for_sql(df)

    df_clean_sql.to_sql(
        name=nom_table,             # nom de la table
        con=engine,                 # moteur SQLAlchemy
        if_exists='replace',        # 'replace' = supprime et recrée, 'append' = ajoute
        index=False                 # ne pas écrire l'index comme une colonne
    )

    print("Table",nom_table,"chargee !")

def preparation_chargement_commune_principale(url:str) -> pd.DataFrame:
    '''
        Permet d'effectuer un chargement de la table commune principale
        Entree : url de telechargement
        Sortie : Dataframe

        Process :  formatage des code insee, code region, code postal
                   correction de certain caractere accentue 
                   Suppression des doublons 
    '''
    # URL directe vers le fichier
    #url = "https://www.data.gouv.fr/fr/datasets/r/dbe8a621-a9c4-4bc3-9cae-be1699c5ff25"

    # Lecture du fichier CSV en DataFrame
    le_df1 = pd.read_csv(url, sep=',', encoding='utf-8')

    # On renomme
    le_df1.rename(columns={'code_commune_INSEE':'code_commune_insee'},inplace=True)
    # Changement de format & Padding à gauche
    le_df1['code_commune_insee']=le_df1['code_commune_insee'].astype(str).str.zfill(5)

    le_df1['code_postal']=le_df1['code_postal'].astype(str).str.zfill(5)

    le_df1['code_commune']=le_df1['code_commune'].astype(str).str.replace('.0', '', regex=False)
    le_df1['code_commune']=le_df1['code_commune'].str.zfill(3)

    le_df1['code_departement']=le_df1['code_departement'].astype(str).str.zfill(3)

    le_df1['code_region']=le_df1['code_region'].astype(str).str.replace('.0', '', regex=False)
    le_df1['code_region']=le_df1['code_region'].str.zfill(2)

    # On change les cacactères accentués :
    le_df1['nom_region']=le_df1['nom_region'].str.replace(r"[éèê]", "e", regex=True)
    le_df1['nom_region']=le_df1['nom_region'].str.replace(r"[îï]", "i", regex=True)
    le_df1['nom_region']=le_df1['nom_region'].str.replace(r"[ôö]", "o", regex=True)

    le_df1['nom_departement']=le_df1['nom_departement'].str.replace(r"[éèê]", "e", regex=True)
    le_df1['nom_departement']=le_df1['nom_departement'].str.replace(r"[îï]", "i", regex=True)
    le_df1['nom_departement']=le_df1['nom_departement'].str.replace(r"[ôö]", "o", regex=True)

    # Je prends les colonnes pertinentes dans une copy indépendante :
    le_df_commune_principal=le_df1[['code_commune_insee','nom_commune_postal','code_postal','latitude',
    'longitude','code_commune','code_departement','nom_departement','code_region','nom_region']].copy()
    # Changement de nom de colonne
    le_df_commune_principal.rename(columns={'code_commune_insee':'num_insee'},inplace=True)

    if le_df_commune_principal.duplicated().sum()>0:
        print("Il y a des doublons :",le_df_commune_principal.duplicated().sum(),"Suppression !")
        le_df_commune_principal_sans_dbl=le_df_commune_principal.drop_duplicates()
    else:
        le_df_commune_principal_sans_dbl=le_df_commune_principal.copy()
    
    return le_df_commune_principal_sans_dbl


def preparation_chargement_mouvement_commune(url:str) -> pd.DataFrame:
    '''
        Permet d'effectuer un chargement de la table des mouvements de commune 
        Entree : url qui renvoie à un Zip à ouvrir
        Sortie : Dataframe

        Process :  renommage de colonne , formatage de date
                   ajout d'une colonne 
                   Suppression des doublons 
    '''
    # Ajouter un en-tête User-Agent
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    #url = "https://example.com/fichier.zip"  # Remplace par l'URL réelle
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # Étape 2 : Ouvrir le fichier .zip en mémoire
    # Cette fonction permet d’ouvrir un fichier .zip.
    with zipfile.ZipFile(io.BytesIO(response.content)) as z: # Cela transforme le contenu brut en un objet fichier en mémoire.
        # Afficher les fichiers contenus dans le zip
        print("Contenu du ZIP :", z.namelist())
        # Étape 3 : Choisir un fichier à lire (le premier ici et le seul !) et le transformer en dataframe
        with z.open(z.namelist()[0]) as f:
            le_df = pd.read_csv(f)
            df_mvt=le_df.copy()
            
    df_mvt.rename(columns={'MOD':'type_event_commune','TNCC_AVANT':'type_nom_en_clair_avant','NCC_AVANT':'nom_commune_en_clair_avant',
                        'NCCENR_AVANT':'nom_commune_riche_en_clair_avant',
                        'TNCC_APRES':'type_nom_en_clair_apres','NCC_APRES':'nom_commune_en_clair_apres',
                        'NCCENR_APRES':'nom_commune_riche_en_clair_apres'},inplace=True)
    df_mvt.rename(columns=str.lower, inplace=True)
    # mettre au bon format :
    df_mvt['date_eff'] = pd.to_datetime(df_mvt['date_eff'])
    df_mvt['idligne']=df_mvt.index.to_list()
    df_mvt.rename(columns={'id_commune_avant':'num_insee_avant','id_commune_apres':'num_insee_apres'},inplace=True)

    df_mvt_=df_mvt[['idligne','type_event_commune','date_eff','type_commune_avant','num_insee_avant','type_nom_en_clair_avant',
                    'nom_commune_en_clair_avant','nom_commune_riche_en_clair_avant','libelle_avant','type_commune_apres',
                    'num_insee_apres','type_nom_en_clair_apres','nom_commune_en_clair_apres',
                    'nom_commune_riche_en_clair_apres','libelle_apres']].copy()

    if df_mvt_.duplicated().sum()>0:
        print("Il y a des doublons :",df_mvt.duplicated().sum(),"suppression(s) !")
        df_mvt_sans_dbl=df_mvt_.drop_duplicates()
    else:
        df_mvt_sans_dbl=df_mvt_.copy()
        
    df_mvt_sans_dbl.rename(columns={'id_commune_avant':'num_insee_avant','id_commune_apres':'num_insee_apres'},inplace=True)

    return df_mvt_sans_dbl


def preparation_chargement_commune_new_caledonie(url:str) -> pd.DataFrame:
    '''
        Permet d'effetuer un chargement de la table commune de la nouvelle caledonie
        Entree : url 
        Sortie : Dataframe

        Process :  Parsing des donnees, formatage du code insee
                   Extraction de la date de mise à jour
                   Mise en majuscule du noms des communes et suppression des doublons 
    '''
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
            population=cols[4].get_text(strip=True)
            rows.append({"commune": nom, "num_insee": code,'population':population})

    df_new_caledonie = pd.DataFrame(rows)

    # Extraire une année à 4 chiffres entre parenthèses
    df_new_caledonie["annee"] = df_new_caledonie["population"].str.extract(r"\((\d{4})\)")

    # remplace la parenthèse et ce qu'elle contient ==> on peut retirer cet element via expression regulière 
    df_new_caledonie['population']=df_new_caledonie['population'].str.replace(r"\s*\([^)]*\)", "", regex=True)

    # On change les cacactères accentués :
    df_new_caledonie['commune']=df_new_caledonie['commune'].str.replace(r"[éèê]", "e", regex=True)
    df_new_caledonie['commune']=df_new_caledonie['commune'].str.replace(r"[îï]", "i", regex=True)

    # On met les noms des  communes en majuscule :
    df_new_caledonie['commune_valide']=df_new_caledonie['commune'].str.upper()

    # >Renommer les colonnes :
    df_ncaledonie=df_new_caledonie[['num_insee','commune','population','annee','commune_valide']].copy()

    if df_new_caledonie.duplicated().sum()>0:
        print("Il y a des doublons :",df_new_caledonie.duplicated().sum(),"Suppression !")
        df_new_caledonie_sans_dbl=df_ncaledonie.drop_duplicates()
    else:
        df_new_caledonie_sans_dbl=df_ncaledonie.copy()

    return df_new_caledonie_sans_dbl    

class Commune_principale(Base):
    __tablename__ = 'commune_principale'
    #IdLigne = Column(Integer, primary_key=True, autoincrement=True)  # auto-incrément !
    num_insee = Column(String(5), primary_key=True)
    nom_commune_postal  = Column(String(50), nullable=False ) 
    code_postal = Column(String(5))
    latitude = Column(Float)
    longitude = Column(Float)
    code_commune = Column(String(3))
    code_departement = Column(String(3))
    nom_departement = Column(String(30), nullable=False)
    code_region = Column(String(2))
    nom_region = Column(String(30), nullable=False)
    __table_args__ = (
        CheckConstraint('LENGTH(code) = 5', name='num_insee'),    ) # ajout d'une contrainte 
    
class Commune_mvt(Base):
    __tablename__ = 'commune_mvt'
    idligne = Column(Integer, primary_key=True, autoincrement=True)  # auto-incrément !
    type_event_commune = Column(Integer)
    date_eff  = Column(Date ) 
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
    __tablename__ = 'commune_nouvelle_caledonie'
    IdLigne = Column(Integer, primary_key=True, autoincrement=True)  # auto-incrément ! 
    num_insee = Column(String(5), primary_key=True)
    commune = Column(String(50))    
    population =Column(Integer)
    annee   = Column(Integer)
    commune_valide=Column(String, nullable=False)


# MAIN 

le_df_commune_principal_sans_dbl=preparation_chargement_commune_principale(url_commune_principale)

df_mvt_sans_dbl=preparation_chargement_mouvement_commune(url_mvt_commune)

df_new_caledonie_sans_dbl=preparation_chargement_commune_new_caledonie(url_new_caledonie)

#  Chargement du DWH 

# Lecture du fichier ini :
db=configuration_db()

# Préparation de la chaine de connexion
host = db['host']
port = db['port']
database = db['database']
user = db['user']
password = db['password']
#  Créer l'URL SQLAlchemy 
url=f'postgresql+psycopg://{user}:{password}@{host}:{port}/{database}'

# Crée le moteur de connexion à PostgreSQL (via psycopg)
engine = create_engine(url)

# Base ORM
Base = declarative_base()

with engine.connect() as connection:
    #connection.execute(text("DROP TABLE IF EXISTS commune"))
    connection.execute(text("DROP TABLE IF EXISTS commune_mvt"))
    connection.execute(text("DROP TABLE IF EXISTS commune_nouvelle_caledonie")) 
    connection.execute(text("DROP TABLE IF EXISTS commune_principale")) #
    print("Table(s) Supprimée(s)")

# Création de la table dans la base
Base.metadata.create_all(engine)

print("Table 'commune_mvt', 'commune_principale' & 'commune_nouvelle_caledonie' créée(s) avec succès !")

#chargement_df_en_sql(engine,df_sans_dbl,'commune')
chargement_df_en_sql(engine,df_mvt_sans_dbl,'commune_mvt')
chargement_df_en_sql(engine,df_new_caledonie_sans_dbl,'commune_nouvelle_caledonie')
chargement_df_en_sql(engine,le_df_commune_principal_sans_dbl,'commune_principale')

#######################################################################################
# Permet de lancer un script de creation des outils necessaires, table, index etc..

try:
    with engine.connect() as connection:
        with connection.begin():  # démarre une transaction
            # Charger un script SQL depuis un fichier
            with open("Prj_Death_People_commune_BDD.sql", "r") as f:
                sql_script = f.read()
            # Execution pas à pas des requetes
            for statement in sql_script.split(';'):
                statement = statement.strip()
                if statement:
                    connection.execute(text(statement))

    print("Le Sql_script exécuté avec succès.")

except SQLAlchemyError as e:
    print("Une erreur est survenue lors de l'exécution du script SQL :")
    print(e)

# Correct pour vider/fermer le pool de connexions
engine.dispose()

# FIN 
