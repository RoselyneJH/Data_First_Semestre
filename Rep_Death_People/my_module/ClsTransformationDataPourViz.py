###############################################################################
#                                   FLOW DATA POUR VIZ
###############################################################################

## --------------------------------------------------------------------------##
#                                      IMPORT
## --------------------------------------------------------------------------##

import pandas as pd

from configparser import ConfigParser
from typing import List, Dict, Union, Tuple, Literal

from sqlalchemy import create_engine, Column, Integer, String, Date, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

import missingno as msno

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# pd.set_option("display.max_colwidth", None)
# pd.set_option("display.max_columns",None)

import matplotlib.ticker as ticker

from functools import reduce

from Connexion_Bdd import ConnexionBdd
import polars as pl

##############################################################################
#                      CLASSE POUR FLOW VISUALISATION DATA
##############################################################################


class TransformationDataPourViz:
    def __init__(self, path_racine: str):
        """
        Initialise une transformation des personnes decedées

        Args:
            path_racine : repertoire cible
            engine : moteur d'accès à la bdd
        """
        # Instancier la classe d'accès à la base de données
        my_bdd = ConnexionBdd(
            path_racine=path_racine,
            filename="Fichier_Connexion.ini",
            section="postgresql",
        )
        # Creation de l'Url
        self.url_Bdd = my_bdd.creation_de_chaine_de_connexion()

        self.engine = create_engine(self.url_Bdd)

    def haversine_np(
        self, lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray
    ) -> np.ndarray:
        """
        Calcul de la distance de Haversine.
        Cela s'appuie sur la formule de haversine qui permet de déterminer
        la distance du grand cercle entre deux points d'une sphère (à partir de leurs longitudes et latitudes en radian).
        Args :  latitude et longitude d'au moins 2 points en arrays
                lat_a en np.array, lon_a en np.array,
                lat_b en np.array, lon_b en np.array
        Return : distance calculée (km)  entre les points a et b
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

    def creation_classe_age(self, df_clean: pd.DataFrame) -> pd.DataFrame:
        """
        Args    : Dataframe à transformer
        Return  : Dataframe avec les classe d'àge

        Process : Ajout des classes d'age
        """
        # création de bins
        bins = [0, 1, 20, 35, 50, 65, 90, 130]

        # Noms des classes
        labels = ["0-1", "1-20", "20-35", "35-50", "50-65", "65-90", "90+"]

        # création de classe
        df_clean["classe_age"] = pd.cut(
            df_clean["age"], bins=bins, labels=labels, duplicates="drop", right=False
        )

        return df_clean

    # def selection_zone(self, df : pd.DataFrame, la_zone : str = 'R', le_champ : str) -> pd.DataFrame:
    #    '''
    #    '''
    #    if la_zone == 'R':
    #        df.groupby(['la_zone']).agg(lat = ('le_champ_lat','mean'))
    #
    #    return df

    def ajout_distance_classe_age_origine(
        self, df_clean_: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Args    : Dataframe à transformer
        Return  : Dataframe transformé
        Process : Ajout des classes d'age et comptabilité liée,
                    nombre personne originaire, des distances entre
                    ville de naissance et de deces
        """
        # bins = [0, 30, 60, 90, 130]

        # Noms des classes
        # labels = ['0-30', '30-60', '60-90', '90+']

        # df_clean['classe_age'] = pd.cut(df_clean['age'],bins=bins,labels=labels,duplicates='drop', right=False)

        df_clean = self.creation_classe_age(df_clean_)

        # Mapping des codes vers des labels men women
        df_clean["sexe"] = df_clean["sex"].map({"1": "Man", "2": "Woman"})

        # Calcul de la distance entre coordonnées de naissance et de mort :
        df_clean.loc[:, "distance"] = list(
            self.haversine_np(
                np.array(df_clean["latitude_naissance"]),
                np.array(df_clean["longitude_naissance"]),
                np.array(df_clean["latitude_deces"]),
                np.array(df_clean["longitude_deces"]),
            )
        )

        df_clean.loc[:, "distance"] = df_clean["distance"].round(0)

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
            .agg(nb_originaire_ville=("idligne", "count"))
        )
        df_clean_nb_departement_origine = (
            df_clean.query("origine_departement == 'O'")
            .groupby(["annee", "num_insee_deces"], as_index=False)
            .agg(nb_originaire_departement=("idligne", "count"))
        )
        df_clean_nb_region_origine = (
            df_clean.query("origine_region == 'O'")
            .groupby(["annee", "num_insee_deces"], as_index=False)
            .agg(nb_originaire_region=("idligne", "count"))
        )

        # Recuperation du nombre de morte
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
        df_clean_insee_prenom_woman.rename(
            columns={"prenom": "name_woman"}, inplace=True
        )

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
            "nb_originaire_ville",
            "nb_originaire_departement",
            "nb_originaire_region",
            "nb_woman",
            "nb_deces_0_30",
            "nb_deces_30_60",
            "nb_deces_60_90",
            "nb_deces_plus_90",
        ]

        # Formattage des colonnes
        df_final[cols_a_modifier] = df_final[cols_a_modifier].astype(int)

        return df_final

    def nettoyage_region_departement_latitude(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Args    : Dataframe à nettoyer
        Return  : Dataframe nettoyé
        Process : Vérification du format des dates et code département et region
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

    def select_with_polars(self, conn_str: str, la_query: str) -> pl.DataFrame | None:
        """
        Lit les données d'une base SQL avec Polars, en gérant proprement les erreurs.

        :param query: La requête SQL à exécuter.
        :param conn_str: La chaîne de connexion (ex: 'sqlite:///mydb.sqlite').
        :return: Un DataFrame Polars ou None si erreur.
        """

        try:
            # Il faut préparer la chaine de connexion pour ConnectorX
            nouveau_conn_str = conn_str.replace("+psycopg", "")
            propre_conn_str = nouveau_conn_str + "?sslmode=disable"
            # On gagne 4*fois plus de temps avec polars
            df = pl.read_database_uri(la_query, propre_conn_str)
            return df

        except pl.exceptions.PolarsError as e:
            print("Erreur Polars détectée :", e)

        except Exception as e:
            print(" Erreur inattendue :", e)

        return None

    def ExtractionDataTableDeathPeopleView(self) -> pd.DataFrame:
        # engine = create_engine(self.url_Bdd)
        la_query = "SELECT idligne, prenom,sex,date_naissance_dt,num_insee_naissance,"
        la_query = la_query + "ville_naissance,pays_naissance,latitude_naissance,"
        la_query = la_query + "longitude_naissance,code_departement_naissance,"
        la_query = la_query + "nom_departement_naissance, code_region_naissance,"
        la_query = la_query + "nom_region_naissance,date_deces_dt,num_insee_deces,"
        la_query = la_query + "ville_deces,latitude_deces,longitude_deces,"
        la_query = la_query + "code_departement_deces,nom_departement_deces,"
        la_query = la_query + "code_region_deces,nom_region_deces,age,annee,"
        la_query = la_query + "origine_ville, origine_departement,"
        la_query = la_query + "origine_region FROM death_people_view"

        df_polars = self.select_with_polars(self.url_Bdd, la_query)

        df = df_polars.to_pandas()

        # Vérification du Format :
        df_clean = self.nettoyage_region_departement_latitude(df)

        df_clean_nan = df_clean.dropna(subset=["nom_departement_deces"])

        # copie
        df_clean_nan = df_clean_nan.copy()
        # methode officielle pour assigner une colonne :
        df_clean_nan.loc[:, "distance"] = list(
            self.haversine_np(
                np.array(df_clean_nan["latitude_naissance"]),
                np.array(df_clean_nan["longitude_naissance"]),
                np.array(df_clean_nan["latitude_deces"]),
                np.array(df_clean_nan["longitude_deces"]),
            )
        )

        df_clean_nan.loc[:, "distance"] = df_clean_nan["distance"].round(0)

        df_person_nais_dece_departement_region = df_clean_nan.copy()

        # Creation des champs classe et taux d'origine :
        # df_final = self.ajout_distance_classe_age_origine( df_clean )

        # return df_final , df_person_nais_dece_departement_region
        return df_person_nais_dece_departement_region
