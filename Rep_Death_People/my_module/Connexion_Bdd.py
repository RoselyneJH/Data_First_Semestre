## --------------------------------------------------------------------------##
#                               IMPORT
## --------------------------------------------------------------------------##
from configparser import ConfigParser
from typing import Dict

##############################################################################
#                      CLASSE POUR CONNEXION A LA BDD
##############################################################################


class ConnexionBdd:
    def __init__(self, path_racine: str, filename: str, section: str):
        """
        Initialise une connexion BDD

        Args:
            db (dict): dictionnaire comportant les éléments de la connexon
            path_racine (str): Chemin du répertoire contenant le fichier INI
            filename (str): Nom du fichier INI
            section (str): Type de bdd
        """
        self.path_racine = path_racine
        self.filename = filename
        self.section = section

    def configuration_db(self) -> Dict[str, str]:
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

        return db

    def creation_de_chaine_de_connexion(self) -> str:
        """
        Permet de créer la chaine de connexion à la bdd

        Args:

            None

        Returns:

            Renvoie un chaine de caractère de la connexion -> url

        """
        # Lecture du fichier ini :
        db = self.configuration_db()

        # Préparation de la chaine de connexion
        host = db["host"]
        port = db["port"]
        database = db["database"]
        user = db["user"]
        password = db["password"]
        #  Créer l'URL SQLAlchemy
        url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
        return url
