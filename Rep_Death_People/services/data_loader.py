import os
from pathlib import Path

import pandas as pd
import polars as pl
import streamlit as st

from my_module.Cls_load_data_pour_viz import ClsLoadDataPourViz
# ---- TEST RJ
import time
# import streamlit as st

def diag(message):
    print(
        f"[DIAG {time.strftime('%H:%M:%S')}] {message}",
        flush=True
    )
    st.write(f"🔎 {message}")
# ---- TEST RJ

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
    # RJE 
    from importlib.metadata import version, PackageNotFoundError

    st.subheader("Diagnostic de l'environnement Cloud")

    st.write("Version de Python :", __import__("sys").version)
    st.write("Version de Polars :", pl.__version__)

    # Affiche les dépendances utilisées pour la connexion SQL
    for package in ["connectorx", "adbc-driver-postgresql", "pyarrow"]:
        try:
            st.write(f"{package} :", version(package))
        except PackageNotFoundError:
            st.write(f"{package} : non installé")

    # Affiche aussi les dépendances optionnelles de Polars
    with st.expander("Toutes les versions de Polars"):
        pl.show_versions()
        
    # RJE 

    t = time.perf_counter()    
    diag("1 - Début du script")
    df_source,nb_total_selection = my_class.ExtractionDataTableDeathPeopleView()
    print(f"ExtractionDataTableDeathPeopleView chargées en {time.perf_counter() - t:.2f} s",
        flush=True)

    t = time.perf_counter()
    diag("2 - creation_classe_age terminé ")
    df = my_class.creation_classe_age(df_source)
    print(f"creation_classe_age chargées en {time.perf_counter() - t:.2f} s",
        flush=True)

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