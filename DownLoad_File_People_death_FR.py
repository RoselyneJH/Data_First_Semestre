###################################################################################################
#                                    CHARGEMENT PARTIE PERSONNE 
###################################################################################################

import requests
import pandas as pd
import re
import pandas as pd
#from fastapi import FastAPI
import requests
import time

pd.set_option("display.max_colwidth", None) 
from loguru import logger

from pydantic import BaseModel, Field, ValidationError, constr
from typing import List, Literal
from typing import List, Dict, Union, Tuple, Literal
pd.options.mode.chained_assignment = None 

from configparser import ConfigParser

from sqlalchemy import create_engine, Column, Integer, String, Date, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError

url_personne = "https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/"

def recuperer_df_name_and_url(html_text:str) -> pd.DataFrame:
    '''
        Entrée : Le contenu du site
        Sortie : Le dataframe contenant les noms de fichier à periodicité annuelle et leur url de téléchargement
    '''
    # Étape 1 : Item à chercher
    item_dece="/deces-"
    item_file="https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/"
    # Étape 2 : Récupérer les noms fichier et urls correspondant
    links_url=[]
    links_file_name=[]
    links_periodicite_non_annuel=[]
    
    for line in html_text.splitlines(): #html_text
        if  item_file in line and item_dece in line :
            #motif=re.escape(item_dece)
            # Recherche de toutes les occurrences avec leurs indices pour reconstruire le 
            for match in re.finditer(item_file, line):
                start=match.start()
                pos_buttee=line.find('.txt","', match.start())
                url_file=line[start:pos_buttee+4] 
                
                if len(url_file)<150:                
                    #print(" File ",url_file)
                    name_file=url_file[url_file.find(item_dece, 1)+len(item_dece) :url_file.find('.txt',1)]
                    if len(name_file)==4: # uniquement ls années qui nous interessent !
                        links_url.append(url_file)
                        links_file_name.append(name_file)
                    
                    
                    # Création du DataFrame
    df = pd.DataFrame({'annee_file': links_file_name,'url_file': links_url})
    df['url_file']=df['url_file'].str.strip()
    df['name_file']=df['annee_file'].str.strip()
    return df

def selection_file_deces_annee(df: pd.DataFrame,annee: str) -> str:
    '''
    Entrée  : L'année selectionnée et le dataframe qui comporte fichier_annee et url correspondant
    Sortie  : Renvoie l'url correspondante  : str 
    '''
    cette_url=df[df['name_file'].str.contains(annee)]['url_file']
    if cette_url.empty : # Si je ne recupère rien
        cette_url=df[df['name_file'].str.contains("2024")]['url_file']       
    # je recupère la partie url de la Serie :    
    cette_url=cette_url.iloc[-1]
    return cette_url
    


# Type personnalisé : chaîne de 5 chiffres, y compris les zéros initiaux
FiveDigitString = constr(regex=r'^\d{5}$')
FiveCharAlnum = constr(regex=r'^[A-Za-z0-9]{5}$')
DateString = constr(regex=r'^\d{8}$')

# classe qui definit le modèle de validation des données 
class RowModel(BaseModel):
    # Ellipsis qui sert ici à indiquer que le champ est obligatoire
    nom: str = Field(..., description="champ obligatoire - Nom")
    prenom: str = Field(..., description="champ obligatoire - Prenom")
    sex: Literal["1", "2"] = Field(..., description="champ obligatoire - Sex 1 homme, 2 fille")
    date_naissance: DateString = Field(..., description="champ obligatoire - date de naissance")
    num_insee_naissance: FiveCharAlnum = Field(..., description="champ obligatoire - Num Insee naissance")
    ville_naissance:str = Field(..., description="champ obligatoire - ville de naissance")
    pays_naissance:str = Field("FRANCE", description="champ optionnel - pays de naissance")
    date_deces :DateString = Field(..., description="champ obligatoire - date de deces")
    num_insee_deces :FiveCharAlnum = Field(..., description="champ obligatoire - Num Insee Deces")
    # 

# Traitement par lots
def validate_in_batches(data: List[Dict], batch_size: int =2) -> Tuple:
    '''
        data : une liste de dictionnaires ('List[dict]'), où chaque dict représente une ligne à valider avec 'RowModel'.
        batch_size : combien de lignes traiter en même temps (paquet de 2 par défaut).
                valid_rows contiendra toutes les lignes valides après validation.
                error_log enregistrera les lignes rejetées, avec la raison de l'erreur.
                -> Tuple[List[...], List[...]]
    '''
    valid_rows = []
    error_log = []
    mes_err =[]
    # Parcourt les données **par lots** de taille
    # i est l’indice de début du lot actuel (0, 2, 4, …).
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]  #Extrait un sous-ensemble des données : les lignes de l’indice i jusqu’à i + batch_size (exclu).
        for j, row in enumerate(batch): # j : indice local dans le lot
            idx = i + j
            try:
                validated = RowModel(**row)
                valid_rows.append(validated.dict())
            except ValidationError as e:
                # Ajoute au journal d’erreurs un dictionnaire avec :
                # index: position dans la liste d’origine,
                # row: la ligne brute en erreur,
                # error: le message d’erreur Pydantic.
                error_log.append({'index': idx, 'row': row, 'error': str(e)})
                for err in e.errors():
                    mes_err.append({
                    "ligne": i,
                    "champ": '.'.join(str(p) for p in err['loc']),
                    "type": err['type'],
                    "message": err['msg']})

    return valid_rows, error_log, mes_err

def parsing_file(le_df_mal_formate: pd.DataFrame) -> List[Dict]:
    '''
    En entrée : le dataframe mal formaté issu de la lecture du fichier txt
    En sortie : une liste de dictionnaire ayant les noms des colonnes comme clés enrichis de sa valeur
                Les données sont identifiées selon la position quelles occupent dans le fichier
    '''
    # 1. Accumule les lignes sous forme de liste de dictionnaires
    raw_data = []
    logger.info("le_df_mal_formate ")  
    # J'itère le dataframe et je décompose la colonne many_cols, row n'a pas d'importance ici
    for idx, row in le_df_mal_formate.iterrows():
        # 2. Processus : 
        # Je reconnais les champs :
        chaine=le_df_mal_formate.iloc[idx,-1]
        pos_fin=chaine.find("*")
        partie_nom=chaine[0:pos_fin]
        pos_deb=pos_fin+1#+len(partie_nom)
        pos_fin=chaine.find("/")
        partie_prenom=chaine[pos_deb:pos_fin]
        partie_restante=chaine[pos_fin+1:]
        partie_restante=partie_restante.strip() # beaucoup d'espace
        partie_sex=partie_restante[0]
        partie_date_naissance=partie_restante[1:9]
        partie_num_insee_naissance=partie_restante[9:14]
        partie_restante_1=partie_restante[14:]
        pos_fin=partie_restante_1.find("   ")
        partie_ville_naissance=partie_restante_1[0:pos_fin]
        
        pos_fin=len(partie_ville_naissance)+pos_fin
        partie_autre=chaine[124:] 
        if partie_autre[0].isalpha():
            pos_fin=partie_autre.find("   ")
            partie_pays_naissance=partie_autre[0:pos_fin]
            # parfois le pays de naissance est long et vient toucher le pavé déces, sans espace les separant
            if len(partie_pays_naissance)>29:
                partie_pays_naissance=partie_pays_naissance[0:30]
            else: # il ya forcement un espace
                pos_fin=len(partie_pays_naissance)+pos_fin
                partie_autre=partie_autre[pos_fin:].strip()
        else:
            partie_pays_naissance="FRANCE"
        partie_date_deces=chaine[154:162]  # en effet, parfois le pays est long et ne laisse pas d'espace avec la zone decs(date et num insee)
        partie_num_insee_deces=chaine[162:167] #partie_autre[8:13]
        # 3. Ajout de la ligne
        

        try:
            raw_data.append({'nom': partie_nom, 'prenom': partie_prenom,'sex': partie_sex, 'date_naissance': partie_date_naissance,
                     'num_insee_naissance': partie_num_insee_naissance,'ville_naissance': partie_ville_naissance,
                     'pays_naissance': partie_pays_naissance, 'date_deces': partie_date_deces,'num_insee_deces': partie_num_insee_deces}) 
        except :
            logger.info(f"Nom {partie_nom},prenom {partie_prenom},sex {partie_sex} ! ")        
    
    return raw_data  

def traitement_validation():
    '''
        Lecture du fichier_deces.txt, il est mal formatté.
        Il faut faire un parsing des colonnes afin de restituer les colonnes
        
    '''
    # Reconstitution des colonnes et creation du Dataframe de l'année selectionnée :
    start = time.time()
    # Ouverture du fichier  attention, il faut choisir un encoding latin1
    le_df_mal_formate = pd.read_csv("fichier_deces.txt", delimiter="\t", header=None, encoding="latin1")  
    
    # Creation Dataframe final, declaration des colonnes :
    df=pd.DataFrame(columns=["nom","prenom","sex","date_naissance","num_insee_naissance","ville_naissance","pays_naissance","date_deces","num_insee_deces"])
    
    # execution du parsing de colonne, travail d'identification des colonnes dans la colonne
    raw_data=parsing_file(le_df_mal_formate)
    
    # j'ajoute les data dans le dataframe  Exécution
    valid_data, errors,mes_err = validate_in_batches(raw_data, batch_size=5)
    df = pd.DataFrame(valid_data)
    
    # Fin du traitement
    end = time.time()
    print(f"Durée : {end - start:.2f} secondes")
    logger.info(f"Durée du traitement de parsing : {end - start:.2f} secondes")
    
    # Convertir en DataFrame
    df_mes_err = pd.DataFrame(mes_err)
    
    # Résumé des erreurs par type
    resume = df_mes_err.groupby("champ").size().reset_index(name="nb_erreurs")    
    
    logger.info(f"Erreur validation personne : {len(errors):,}".replace(",", " "))
    for i in range(len(resume)):
        logger.info(f"Validation ko pour le champs {resume.iloc[i,0]} : {resume.iloc[i,1]} erreur(s) ")
    logger.info(f"Volume de personnes conservé ({an}) : {len(df):,}".replace(",", " "))

    return df,errors
    
def formattage_date(la_date:str,mode="CHECK_PAS_DE_DOUBLE_ZERO",annee="0000") -> str:
    '''
         2 Modes
         Mode "CHECK_PAS_DE_DOUBLE_ZERO" : Certaines date ont des jours et/ou mois et ou année à zero ; il faut les identifier
         Mode "CHECK_ANNEE_DECHARGEMENT" : L'objectif est ici d'identifier que l'année de deces correspond à celle du fichier telechargé
                  Nous avons besoin de l'année correspondant au téléchargement
         Entrée , colonne string d'un dataframe + le mode à traiter format string + eventuellement une date format string
         
         Sortie , flag d'invalidation ko si date invalide format string
    '''
        
    str_date=str(la_date)
    validation="ok"
    
    if mode=="CHECK_PAS_DE_DOUBLE_ZERO":
        if str_date[6:8]=="00" or str_date[4:6]=="00" or str_date[:4]=="0000":
            validation="ko"    
    if mode=="CHECK_ANNEE_DECHARGEMENT":
        if str_date[:4]!=annee and annee!="0000":
            validation="ko"
            
    return validation

def verification_date(df:pd.DataFrame) -> pd.DataFrame :
    '''
        Entrée : Vérification des dates naissance et deces
        Sortie : Nettoyage du dataframe des dates erronées
    '''
    df['validation_date_nai_ft']=df.apply(lambda row: formattage_date(row['date_naissance']), axis=1)
    tx_annomalie_date_naiss_ft=round(df[df['validation_date_nai_ft']=='ko']['nom'].count()*100/df['validation_date_nai_ft'].count(),2) 
    # pas propre du tout !!!
    if tx_annomalie_date_naiss_ft<5:
        logger.info(f"Dates naiss. avec mois, jour ou année = 00 : {tx_annomalie_date_naiss_ft}% des enreg. A suprimer.")
        df_date_naiss_ok=df.query("validation_date_nai_ft=='ok'")
    else:
        logger.info(f"Dates naiss. avec mois, jour ou année = 00 : {tx_annomalie_date_naiss_ft}% des enreg. Anomalie !")
        df_date_naiss_ok=df
        
    df_date_naiss_ok['validation_date_dc_ft']=df_date_naiss_ok.apply(lambda row: formattage_date(row['date_deces']), axis=1)
    tx_annomalie_date_de_ft=round(df_date_naiss_ok[df_date_naiss_ok['validation_date_dc_ft']=='ko']['nom'].count()*100/df_date_naiss_ok['validation_date_dc_ft'].count(),2) 
    
    if tx_annomalie_date_de_ft<5:
        logger.info(f"Dates dece. avec mois, jour ou année = 00 : {tx_annomalie_date_de_ft}% des enreg. A suprimer.")
        df_date_naiss_deces_ok=df_date_naiss_ok.query("validation_date_dc_ft=='ok'")
    else:
        logger.info(f"Dates dece. avec mois, jour ou année = 00 : {tx_annomalie_date_de_ft}% des enreg. Anomalie !")
        df_date_naiss_deces_ok=df_date_naiss_ok
    
    df_date_naiss_deces_ok['validation_date_dc_te']=df_date_naiss_deces_ok.apply(lambda row: formattage_date(row['date_deces'],'CHECK_ANNEE_DECHARGEMENT' ,an), axis=1)
    tx_annomalie_date_deces=round(df_date_naiss_deces_ok[df_date_naiss_deces_ok['validation_date_dc_te']=='ko']['nom'].count()*100/df_date_naiss_deces_ok['validation_date_dc_te'].count(),2)
    
    if tx_annomalie_date_deces<5:
        logger.info(f"Dates deces. diff. de l'année de téléchargement : {tx_annomalie_date_deces}% des enreg. A suprimer.")
        df_date_naiss_et_deces_ok=df_date_naiss_deces_ok.query("validation_date_dc_te=='ok'")
    else:
        logger.info(f"Dates deces. diff. de l'année de téléchargement : {tx_annomalie_date_deces}% des enreg. Anomalie !")
        df_date_naiss_et_deces_ok=df_date_naiss_deces_ok

    # format de date naissance et deces :
    df_date_naiss_et_deces_ok['date_naissance_dt']=pd.to_datetime(df_date_naiss_et_deces_ok["date_naissance"], errors="coerce").dt.normalize() 
    df_date_naiss_et_deces_ok['date_deces_dt']    =pd.to_datetime(df_date_naiss_et_deces_ok["date_deces"],     errors="coerce").dt.normalize() 

    # recherche de doublons
    if df_date_naiss_et_deces_ok.duplicated().sum()>0:
        logger.info(f"Nombre de doublon : {df_date_naiss_et_deces_ok.duplicated().sum()}")
        df_date_naiss_et_deces_ok_clean=df_date_naiss_et_deces_ok.drop_duplicates(keep="first")
    else:
        df_date_naiss_et_deces_ok_clean=df_date_naiss_et_deces_ok
        
    df_date_naiss_et_deces_ok_clean.drop(columns={'validation_date_nai_ft','validation_date_dc_ft','validation_date_dc_te'},inplace=True )
         
    return df_date_naiss_et_deces_ok_clean

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

def prepare_dataframe_for_sql(df, drop_columns=None):
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

def recuperer_df_name_and_url(html_text:str) -> pd.DataFrame:
    '''
        Entrée : Le contenu du site
        Sortie : Le dataframe contenant les noms de fichier à periodicité annuelle et leur url de téléchargement
    '''
    # Étape 1 : Item à chercher
    item_dece="/deces-"
    item_file="https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/"
    # Étape 2 : Récupérer les noms fichier et urls correspondant
    links_url=[]
    links_file_name=[]
    links_periodicite_non_annuel=[]
    
    for line in html_text.splitlines(): #html_text
        if  item_file in line and item_dece in line :
            #motif=re.escape(item_dece)
            # Recherche de toutes les occurrences avec leurs indices pour reconstruire le 
            for match in re.finditer(item_file, line):
                start=match.start()
                pos_buttee=line.find('.txt","', match.start())
                url_file=line[start:pos_buttee+4] 
                
                if len(url_file)<150:                
                    #print(" File ",url_file)
                    name_file=url_file[url_file.find(item_dece, 1)+len(item_dece) :url_file.find('.txt',1)]
                    if len(name_file)==4: # uniquement ls années qui nous interessent !
                    #print(">> ",name_file)
                        links_url.append(url_file)
                        links_file_name.append(name_file)
                    
                    
                    # Création du DataFrame
    df = pd.DataFrame({'annee_file': links_file_name,'url_file': links_url})
    df['url_file']=df['url_file'].str.strip()
    df['annee_file']=df['annee_file'].str.strip()
    
    return df

# Définition de la table
class Nom_url(Base):
    __tablename__ = 'nom_url'
    
    idligne = Column(Integer, primary_key=True, autoincrement=True)  
    annee_file = Column(String, nullable=False)
    url_file = Column(String, nullable=False)

# Définition de la table
class Personne(Base):
    __tablename__ = 'death_people'
    idligne = Column(Integer, primary_key=True, autoincrement=True)  
    nom = Column(String)
    prenom = Column(String)
    sex = Column(Integer)
    date_naissance_dt = Column(Date)
    num_insee_naissance = Column(String)
    ville_naissance = Column(String)
    pays_naissance = Column(String)
    date_deces_dt = Column(Date)
    num_insee_deces = Column(String)
    ville_deces=Column(String)
    age = Column(Integer)

def chargement_DWH(df_clean:pd.DataFrame):
    '''
        Entree : Le dataframe à charger
        Sortie : ras
        Processus: Connexion à la base de donnée, supression si existe et deconnexion
    '''
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

    # Créer le moteur SQLAlchemy
    engine = create_engine(url)
    # Crée le moteur de connexion à PostgreSQL (via psycopg)
    engine = create_engine(url)

    # Base ORM
    Base = declarative_base()

    # Supprimer l’ancienne table si elle existe
    with engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS death_people"))
        print("Table Supprimée")

    # Création de la table dans la base
    Base.metadata.create_all(engine)

    print("Table 'death_people' créée avec succès !")

    # Envoi dans PostgreSQL
    df_clean_sql=prepare_dataframe_for_sql(df_clean)

    df_clean_sql.to_sql(
        name='death_people',        # nom de la table
        con=engine,                 # moteur SQLAlchemy
        if_exists='replace',        # 'replace' = supprime et recrée, 'append' = ajoute
        index=False                 # ne pas écrire l'index comme une colonne
    )

    logger.info(f"Chargement dans le DWH de Table deces {df_clean_sql.shape[0]:,}".replace(",", " "))
    
    try:
        with engine.connect() as connection:
            with connection.begin():  # démarre une transaction
                # Charger un script SQL depuis un fichier
                with open("Prj_Death_People_death_people_BDD.sql", "r") as f:
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
    print(" Données insérées depuis le DataFrame dans la table PostgreSQL")

    # Correct pour vider/fermer le pool de connexions
    engine.dispose()

# MAIN 

# Configurer loguru
logger.add("DownLoad_File_death.log", rotation="500 MB", level="INFO")
logger.info("DEBUT TRT")
#
response = requests.get(url_personne)
html_text = response.text
# Chargement des url des fihciers decedé ainsi que leur année :
df_url_a_copier=recuperer_df_name_and_url(html_text)

df_url_a_copier['idligne']=df_url.index.to_list()
df_url=df_url_a_copier[['idligne','annee_file','url_file']].copy()

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

# Créer le moteur SQLAlchemy
engine = create_engine(url)

# Base ORM
Base = declarative_base()

# Supprimer l’ancienne table si elle existe
with engine.connect() as connection:
    connection.execute(text("DROP TABLE IF EXISTS nom_url"))
    print("Table Supprimée")

# Création de la table dans la base
Base.metadata.create_all(engine)

print("Table 'nom_url' créée avec succès !")

# Envoi dans PostgreSQL
df_clean_sql=prepare_dataframe_for_sql(df_url)

df_clean_sql.to_sql(
    name='nom_url',        # nom de la table
    con=engine,                 # moteur SQLAlchemy
    if_exists='replace',        # 'replace' = supprime et recrée, 'append' = ajoute
    index=False                 # ne pas écrire l'index comme une colonne
)

logger.info(f"Chargement dans le DWH de Table nom_url {df_clean_sql.shape[0]:,}".replace(",", " "))
# {df_clean_sql.shape[0]:,}".replace(",", " ")
print(" Données insérées depuis le DataFrame dans la table PostgreSQL")

engine.dispose()

#----------------------------------------------------------------------------
# soit on lit la table des UrL, 
# Soit on recharge toute la table et on recupère dans le datframe, la date souhaitée
# 
# Download file
an="2016"
url=selection_file_deces_annee(df_url,an)

try:
    response = requests.get(url)
    response.raise_for_status()  # Vérifie si la réponse est OK (code 200)
    
    # Sauvegarder le fichier si la requête est réussie
    with open("fichier_deces.txt", "wb") as f:
        f.write(response.content)
    
    print("Fichier téléchargé avec succès.")
    logger.info(f"Fichier ({an}) téléchargé avec succès.")

    df,errors=traitement_validation()
    df_clean=verification_date(df)

    # Calcule de l'age de la personne :
    df_clean['age']=((df_clean['date_deces_dt']-df_clean['date_naissance_dt'])//365).dt.days.astype(int)
    # attention il faut que l'age > 0 !
    df_clean['age']=df_clean['age'].apply(lambda x: x if x>0 else 0)

    # ajout de la colonne :    
    df_clean['idligne']=df_clean.index.to_list()
    df_clean['ville_deces']=''
    df_clean=df_clean[['idligne','nom','prenom','sex','date_naissance_dt','num_insee_naissance','ville_naissance','pays_naissance','date_deces_dt','num_insee_deces','ville_deces','age']] #=prepare_dataframe_for_sql(df_clean)
    
    chargement_DWH(df_clean)

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
    
    logger.info(" TRT TERMINE ")  

 
