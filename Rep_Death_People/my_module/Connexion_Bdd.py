## --------------------------------------------------------------------------##
#                               IMPORT
## --------------------------------------------------------------------------##
from configparser import ConfigParser
from typing import Dict
import os
import configparser

##############################################################################
#                      CLASSE POUR CONNEXION A LA BDD
##############################################################################


class ConnexionBdd:
    def __init__(self, path_racine: str, filename: str, section: str,
                mode: str="local"):
        """
        Initialise une connexion BDD

        Args:
            db (dict): dictionnaire comportant les éléments de la connexon
            path_racine (str): Chemin du répertoire contenant le fichier INI
            filename (str): Nom du fichier INI
            section (str): Type de bdd
            mode(str): Acces via Cloud ou Local
        """
        self.path_racine = path_racine
        self.filename = filename
        self.section = section
        self.mode = mode
        self.config = {}


    def configuration_db(self) -> Dict[str, str]:
        """
        Configuration de la base de donnée

        Args:

            Nom fichier de configuration

            Nom de bdd

        Returns:

            None 
            mais un Dictionnaire recupérant les elements de connexion est créé

        """
        if self.mode == "cloud":
            # Streamlit Cloud / Supabase            
            self.config["host"] = os.environ["DB_HOST"]
            self.config["database"] = os.environ["DB_NAME"]
            self.config["user"] = os.environ["DB_USER"]
            self.config["password"] = os.environ["DB_PASSWORD"]
            self.config["port"] = os.environ["DB_PORT"]             
        else:
            # Mode local avec fichier ini
            # Create a parser
            parser = ConfigParser()
            # Read the configuration file
            parser.read(self.path_racine + "/" + self.filename)
            # Get the information from the postgresql section
            db = {}
            if parser.has_section(self.section):
                params = parser.items(self.section)
                for param in params:
                    db[param[0]] = param[1]
            else:
                raise Exception(
                    "Section {0} not found in {1}".format(self.section, self.filename)
                )
            self.config = db          
        


    def get_sqlalchemy_url(self,url_deb:str )-> str:
        """
        Renvoie le prefixe de l'url pour sqlalchemy

        Args:

            None

        Returns:

            Renvoie un chaine de caractère pour la connexion -> url

        """
        return "postgresql+psycopg2"+ url_deb

    def get_polars_url(self,url_deb:str )-> str:
        """
        Renvoie le prefixe de l'url pour Polars (pas d'utilisation de
        driver python)

        Args:

            None

        Returns:

            Renvoie un chaine de caractère pour la connexion -> url

        """
        return "postgresql" + url_deb 
        
    def creation_de_chaine_de_connexion(self, choix_system:str = "sqlalchemy") -> str:
        """
        Permet de créer la chaine de connexion à la bdd

        Args:

            None

        Returns:

            Renvoie un chaine de caractère de la connexion -> url

        """
        # Lecture du fichier ini :
        #db = 
        self.configuration_db()
        # Supabase impose :
        # Connexion chiffrée TLS
        # Rejet des connexions sans SSL
        # DONC :
        sslmode = "require" if self.mode == "cloud" else "prefer"  ## !!!

        # Préparation de la chaine de connexion
        host = self.config["host"] 
        database = self.config["database"]  
        user = self.config["user"] 
        password = self.config["password"] 
        port = self.config["port"] 

        #  Créer l'URL 
        url_deb = (f"://{user}:{password}@{host}:{port}/{database}"
              f"?sslmode={sslmode}" )
        
        if choix_system =="sqlalchemy":
            url = self.get_sqlalchemy_url(url_deb)
        else:
            url = self.get_polars_url(url_deb)
        
        return url
    
