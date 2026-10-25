import os
from pathlib import Path

import pandas as pd
import polars as pl
import streamlit as st

from my_module.Cls_load_data_pour_viz import ClsLoadDataPourViz

@st.cache_data
def load_dataframe()-> pd.DataFrame:
    """
    Recupération des données issues d'une bdd
    Args :
        None
    Return :
        Dataframes de données provenant d'une bdd
            - dataframe avec agregation selon kpi
            - dataframe standard sans agregation selon kpi
            - dataframe avec rang des kpis selon secteur
            - nb deces hors france
            - nb total décès
    """

    # Chargement des secrets
    for key, value in st.secrets.items():
        os.environ[key] = str(value)

    # Détection de l'environnement
    host = os.environ.get("DB_HOST", "")
    
    if "supabase" in host:
        mode = "cloud"
    else:
        mode = "local"
     
    # Chargement des données
    my_class = ClsLoadDataPourViz(
        path_racine=str(Path.cwd()),
        choix_system="polars",
        mode=mode,
    )

    df_source,nb_total_selection = my_class.ExtractionDataTableDeathPeopleView()
    df = my_class.creation_classe_age(df_source)

    # Nombre de décès hors France
    nb_deces_hors_france = df["nb_hors_france"].mean() 

    # Toutes les personnes sont considérées comme décédées en France
    df["pays_deces"] = "FRANCE"  # cela permet d'ajuster la hierarchie des secteurs de deces

    # Agrégation pour les visualisations
    df_polars = (
        pl.DataFrame(df)
        .group_by(
            [
                "nom_region_deces",
                "nom_departement_deces",
                "ville_deces",
                "code_region_deces",
                "code_departement_deces",
                "age",
                "sex",
                "classe_age",
                "origine_nationale",
                "origine_region",
                "origine_departement",
                "origine_ville",
                "month_deces",
            ]
        )
        .agg(
            [
                pl.col("latitude_deces").mean().alias("lat"),
                pl.col("longitude_deces").mean().alias("lon"),
                pl.col("idligne").count().alias("nb_deces"),
                pl.col("distance").mean().alias("distance_moy"),
            ]
        )
    )

    df_grp = df_polars.to_pandas()

    return df_grp, df, nb_deces_hors_france, nb_total_selection