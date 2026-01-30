import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly
from streamlit_plotly_events import plotly_events
import os

import requests

from typing import List, Dict, Union, Tuple

from streamlit_plotly_events import plotly_events
import polars as pl
import numpy as np

from my_module.Cls_load_data_pour_viz import ClsLoadDataPourViz
from pathlib import Path
from my_module.graphs.graph_bar_origine import render_graph_bar_origine as graph_bar_origine

from my_module.graphs.graph_bar_month import render_graph_bar_month as graph_bar_month
from my_module.graphs.graph_bar_month import (
    render_graph_bar_class_age_month as graph_bar_class_age_month,
)
from my_module.graphs.graph_heat_map import (
    render_graph_heat_map_origine as graph_heat_map_origine,
)

from my_module.graphs.graph_scoring import (
    render_graph_score as graph_scoring,
)

# -------------------------------------------------------------------------------------

# Permet de reduire la marge entre side bar et reste de l'écran
# A définir, en premier dans une app. streamlit
st.set_page_config(layout="wide")

# --- Fonction pour charger des fichiers GeoJSON ---
@st.cache_data
def load_geojsons():
    """
    Récupération des fichiers Geo
    Args :
        None
    Return :
        fichier json region
        fichier json departement
    """

    geojson_regions = requests.get(
        "https://france-geojson.gregoiredavid.fr/repo/regions.geojson"
    ).json()
    geojson_departements = requests.get(
        "https://france-geojson.gregoiredavid.fr/repo/departements.geojson"
    ).json()
    return geojson_regions, geojson_departements

# Fonction pour préparer le cumul

# --- Fonction pour récupérer data ---
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
    """
    # Charger secrets
    if hasattr(st, "secrets"):
        for k, v in st.secrets.items():
            os.environ[k] = str(v)
    else:
        print("Pas de secrets définis")

    # Détection environnement par le host
    host = os.environ.get("DB_HOST", "")

    if "supabase" in host:
        mode = "cloud"
    else:
        mode = "local"
    
    # ---  Recupération de mes données via la classe ---
    my_class = ClsLoadDataPourViz(
        path_racine= str(Path.cwd()),
        choix_system="polars",
        mode=mode,
    )
    
    df_person_nais_dece_departement_region = (
        my_class.ExtractionDataTableDeathPeopleView()
    )

    df = my_class.creation_classe_age(df_person_nais_dece_departement_region)

    mon_pl = pl.DataFrame(df)
    df_polars = (
        mon_pl.group_by(
            [
                "nom_region_deces",
                "nom_departement_deces",
                "ville_deces",
                "code_region_deces",
                "code_departement_deces",
                "age",
                "classe_age",
                "origine_nationale",
                "origine_region",
                "origine_departement",
                "origine_ville",
                "month_deces",
            ]
        ).agg(
            [
                pl.col("latitude_deces").mean().alias("lat"),
                pl.col("longitude_deces").mean().alias("lon"),
                pl.col("idligne").count().alias("nb_deces"),
                pl.col("distance").mean().alias("distance_moy"),
            ]
        )
    )
    df_grp = df_polars.to_pandas()

    return df_grp, df

def recherche_dominant_sur_secteur(df_fnl_m: pd.DataFrame, ce_secteur:str,
                                   cette_origine_secteur:str)-> Tuple:
    """
    Recherche des éléments dominants dans le secteur
    pour le prénom (mode), age (moyen), secteur de naissance (mode),
    secteur de deces (mode)
    Args :
        nom du secteur à traiter
        origine du secteur à traiter
    Return :
        Séries sur kpi
    """
    # Age moyen
    age_moyen = str(round(df_fnl_m['age'].mean(),0)).replace(".0", "")

    # Sexe
    serie_sex ='H' # par défaut
    if df_fnl_m['sex'].mode()[0] == '2': # femme
        serie_sex ='F'
        serie_prenom= df_fnl_m[df_fnl_m['sex']=='2']['prenom'].mode()
    else: # Alors homme
        serie_prenom=df_fnl_m[df_fnl_m['sex']=='1']['prenom'].mode()

    # Préparation des éléments dominants dans un secteur donnée
    ce_secteur_naissance = ce_secteur.replace("deces","naissance")
    serie_lieu_naissance= df_fnl_m[ce_secteur_naissance].mode()
    serie_lieu_deces= df_fnl_m[ce_secteur].mode()

    # Originaire :
    nb_originaire = df_fnl_m[df_fnl_m[cette_origine_secteur] =='O'][cette_origine_secteur].count()
    nb_Non_originaire = df_fnl_m[df_fnl_m[cette_origine_secteur] =='N'][cette_origine_secteur].count()
    
    origine_dominante = 'N'
    if nb_originaire>=nb_Non_originaire:
        origine_dominante = 'O'
    
    distance_moy = str(round(df_fnl_m[df_fnl_m[cette_origine_secteur] ==origine_dominante]['distance'].mean(),0)).replace(".0", " ")
    distance_moy = distance_moy +"km"
    
    return age_moyen, serie_sex, serie_prenom, serie_lieu_naissance, serie_lieu_deces, origine_dominante, distance_moy

# Récupération des regions et départements
geojson_regions, geojson_departements = load_geojsons()

# Recupération des datas provenant de la Bdd
df_grp, df = load_dataframe()

# Le titre
st.title("Analyse géographique des décès en France en 2024")

# --- Fond d'écran ---
st.markdown(
    """
    <style>
    /*  fond d'écran */
    .stApp {
        background-color: #ADD8E6; /* bleu ciel */
    }
    
    /* Changer le fond de la sidebar */
    [data-testid="stSidebar"] {
        background-color: #9ec9d7;
   
    }
    /* Changer la couleur du bouton du Tab */

     button[data-baseweb="tab"] {
        color: #444;
    }

    button[data-baseweb="tab"][aria-selected="true"] {
        color: white;
        background-color: #9ec9d7;
        border-radius: 6px 6px 0 0;
    }
    /* Label du metric */
    [data-testid="stMetricLabel"] {
        font-size: 18px;        
        font-family: 'Arial', sans-serif;
        color: #555555;
    }

    /* Valeur principale */
    [data-testid="stMetricValue"] {
        font-size: 18px;        
        color: #1f77b4;
    }
    /* Delta */
    [data-testid="stMetricDelta"] {
        font-size: 16px;
        font-family: 'Courier New', monospace;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ----------------------------- font-weight: bold;
# Sidebars
# -----------------------------

# Widgets dans la sidebar
st.sidebar.header("Filtres")

# --- Combobox Région ---
regions = ["Toutes les régions"] + sorted(df_grp["nom_region_deces"].unique().tolist())
region_selected = st.sidebar.selectbox("Choisis une région :", regions)

# --- Filtrage selon la région sélectionnée ---
if region_selected == "Toutes les régions":
    df_region = df_grp.copy()
    df_rgn = df.copy()
else:
    df_region = df_grp[df_grp["nom_region_deces"] == region_selected]
    df_rgn = df[df["nom_region_deces"] == region_selected]

# -------------------------------------------------------------------------------------
# ComboBox Département
departements = ["Tous les départements"] + sorted(
    df_region["nom_departement_deces"].unique().tolist()
)
departement_selected = st.sidebar.selectbox("Département :", departements)

# Filtrage selon le département
if departement_selected == "Tous les départements":
    df_dept = df_region.copy()
    df_dpt = df_rgn.copy()
else:
    df_dept = df_region[df_region["nom_departement_deces"] == departement_selected]
    df_dpt = df_rgn[df_rgn["nom_departement_deces"] == departement_selected]

# -------------------------------------------------------------------------------------

# ComboBox Ville
villes = ["Toutes les villes"] + sorted(df_dept["ville_deces"].unique().tolist())
ville_selected = st.sidebar.selectbox("Ville :", villes)

# Filtrage selon la ville
if ville_selected == "Toutes les villes":
    df_final_ = df_dept.copy()
    df_fnl_ = df_dpt.copy()
else:
    df_final_ = df_dept[df_dept["ville_deces"] == ville_selected]
    df_fnl_ = df_dpt[df_dpt["ville_deces"] == ville_selected]

# Slider
start, end = st.sidebar.slider("Âge :", 0, 105, (20, 85))

# Extraction des personnes respectant le filtre sur l'age
df_final = df_final_[(df_final_["age"] >= start) & (df_final_["age"] <= end)]

df_fnl = df_fnl_[(df_fnl_["age"] >= start) & (df_fnl_["age"] <= end)]

st.write("Auteur : R.Jean ")

# Test si presence de valeurs apres selection :
if len(df_final) > 0:
    valeur = df_final["nb_deces"].sum()   
    st.write(f"Déces sélectionnés : {valeur:,}".replace(",", " "))
    restitution_des_valeurs = True
else:
    st.warning(
        "Ces valeurs ne renvoient pas de données. Veuillez modifier la dernière valeur sélectionnée."
    )
    restitution_des_valeurs = False

# -------------------------------------------------------------------------------------

# -----------------------------
# Filtres
# -----------------------------

if restitution_des_valeurs:

    # === Détermination du centre de la carte ===
    if not df_final.empty:
        center_lat = df_final["lat"].mean()
        center_lon = df_final["lon"].mean()
    else:
        center_lat, center_lon = 46.6, 2.5  # centre de la France

    # -------------------------------------------------------------------------------------

    if region_selected == "Toutes les régions":
        geojson_filtered = geojson_regions  # toutes les régions
    else:
        # Filtrer les départements correspondant à la région
        deps = (
            df_final[df_final["nom_region_deces"] == region_selected][
                "nom_departement_deces"
            ]
            .unique()
            .tolist()
        )
        geojson_filtered = {
            "type": "FeatureCollection",
            "features": [
                f
                for f in geojson_departements["features"]
                if f["properties"]["nom"] in deps
            ],
        }

    # -------------------------------------------------------------------------------------
    # permet de toper l'affichage max des villes dans un département
    # sur le graphe tx mortalité vs originaire
    nb_energ = 30
    ordre_tri = False
    precision = 3

    # === Préparation des données pour la carte ===
    if ville_selected != "Toutes les villes":
        df_list = df_final.query("ville_deces == @ville_selected")
        # ➜ chaque ligne renvoie un cumul de personnes decedées
        df_map = df_list.groupby(   
            ["ville_deces"], as_index=False
        ).agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})

        hover_col = "ville_deces"
        size_col = "nb_deces"

        # ****** BarPlot *****
        df_bar = df_list.groupby(["ville_deces", "origine_ville"], as_index=False).agg(
            nb_deces=("nb_deces", "sum"),
        )

        df_bar = df_bar.sort_values("nb_deces", ascending=ordre_tri).head(nb_energ)
        nom_secteur = "ville_deces"

        # ****** BarPlot2 *****
        df_bar_cl = (
            df_final.query("ville_deces == @ville_selected")  
            .groupby(
                ["ville_deces", "classe_age", "origine_ville"],
                as_index=False,
                observed=True,
            )
            .agg(
                nb_deces=("nb_deces", "sum"), 
            )
        )
        origine_secteur = "origine_ville"

        # ****** BarPlot3 *****
        df_bar_month = (
            df_final.query("ville_deces == @ville_selected")  
            .groupby(["month_deces", "origine_ville"], as_index=False, observed=True)
            .agg(
                nb_deces=("nb_deces", "sum"),  
            )
        )

        # ****** BarPlot Test *****
        df_bar_month_cl = (
            df_final.query("ville_deces == @ville_selected")
            .groupby(
                ["ville_deces", "month_deces", "classe_age", "origine_ville"],
                as_index=False,
                observed=True,
            )
            .agg(
                nb_deces=("nb_deces", "sum"),
            )
        )
        # ****** Metrics *******
        df_fnl_m = df_fnl.query("ville_deces == @ville_selected")

    elif departement_selected != "Tous les départements":
        # ➜ regroupement par ville
        df_list = df_final.query("nom_departement_deces == @departement_selected")
        df_map = (
            df_list.groupby(  
                ["nom_departement_deces", "ville_deces"]
            )
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "ville_deces"
        size_col = "count"

        # ****** BarPlot *****
        df_bar = df_list.groupby(  
            ["ville_deces", "origine_departement"], as_index=False
        ).agg(
            nb_deces=("nb_deces", "sum"),
            
        )

        df_bar = df_bar.sort_values("nb_deces", ascending=ordre_tri).head(nb_energ)
        nom_secteur = "ville_deces"

        # ****** BarPlot2 *****
        df_bar_cl = (
            df_final.query("nom_departement_deces == @departement_selected")  
            .groupby(
                ["ville_deces", "classe_age", "origine_departement"],
                as_index=False,
                observed=True,
            )
            .agg(
                nb_deces=("nb_deces", "sum"), 
            )
        )
        origine_secteur = "origine_departement"

        # ****** BarPlot3 *****
        df_bar_month = (
            df_final.query("nom_departement_deces == @departement_selected")  
            .groupby(
                ["month_deces", "origine_departement"], as_index=False, observed=True
            )
            .agg(
                nb_deces=("nb_deces", "sum"),  
            )
        )

        # ****** BarPlot Test *****
        df_bar_month_cl = (
            df_final.query("nom_departement_deces == @departement_selected")
            .groupby(
                [
                    "nom_departement_deces",
                    "month_deces",
                    "classe_age",
                    "origine_departement",
                ],
                as_index=False,
                observed=True,
            )
            .agg(
                nb_deces=("nb_deces", "sum"),
            )
        )
        # ****** Metrics *******
        df_fnl_m = df_fnl.query("nom_departement_deces == @departement_selected")

    elif region_selected != "Toutes les régions":
        # ➜ regroupement par département
        df_list = df_final.query("nom_region_deces == @region_selected")
        df_map = (
            df_list.groupby(  
                ["nom_region_deces", "nom_departement_deces"]
            )
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "nom_departement_deces"
        size_col = "count"

        # ****** BarPlot *****
        df_bar = df_list.groupby(
            ["nom_departement_deces", "origine_departement"], as_index=False
        ).agg(
            nb_deces=("nb_deces", "sum"),             
        )

        df_bar = df_bar.sort_values("nb_deces", ascending=ordre_tri).head(nb_energ)

        nom_secteur = "nom_departement_deces"

        # ****** BarPlot2 *****
        df_bar_cl = (
            df_final.query("nom_region_deces == @region_selected") 
            .groupby(
                ["nom_departement_deces", "classe_age", "origine_departement"],
                as_index=False,
                observed=True,
            )
            .agg(
                nb_deces=("nb_deces", "sum"),  
            )
        )
        origine_secteur = "origine_departement"

        # ****** BarPlot3 *****
        df_bar_month = (
            df_final.query("nom_region_deces == @region_selected") 
            .groupby(
                ["month_deces", "origine_departement"], as_index=False, observed=True
            )
            .agg(
                nb_deces=("nb_deces", "sum"),  
            )
        )
        # ****** BarPlot Test *****
        df_bar_month_cl = (
            df_final.query("nom_region_deces == @region_selected")
            .groupby(
                [
                    "nom_departement_deces",
                    "month_deces",
                    "classe_age",
                    "origine_departement",
                ],
                as_index=False,
                observed=True,
            )
            .agg(
                nb_deces=("nb_deces", "sum"),
            )
        )
        # ****** Metrics *******
        df_fnl_m = df_fnl.query("nom_region_deces == @region_selected")

    else:
        # ➜ regroupement par région

        df_map = (
            df_final.groupby(["nom_region_deces"])
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "nom_region_deces"
        size_col = "count"

        # ****** BarPlot *****
        df_bar = df_final.groupby(
            ["nom_region_deces", "origine_nationale"], as_index=False
        ).agg(
            nb_deces=("nb_deces", "sum"),            
        )

        df_bar = df_bar.sort_values("nb_deces", ascending=ordre_tri).head(nb_energ)

        nom_secteur = "nom_region_deces" 

        # ****** BarPlot2 *****    
        df_bar_cl = df_final.groupby(  
            ["nom_region_deces", "classe_age", "origine_nationale"],
            as_index=False,
            observed=True,
        ).agg(
            nb_deces=("nb_deces", "sum"),  
        )
        origine_secteur = "origine_nationale"  

        # ****** BarPlot3 *****
        df_bar_month = df_final.groupby(  
            ["month_deces", "origine_nationale"], as_index=False, observed=True
        ).agg(
            nb_deces=("nb_deces", "sum"),  
        )

        # ****** BarPlot Test *****
        df_bar_month_cl = df_final.groupby(
            ["nom_region_deces", "month_deces", "classe_age", "origine_region"],
            as_index=False,
            observed=True,
        ).agg(
            nb_deces=("nb_deces", "sum"),
        )

        # ****** Metrics *******
        df_fnl_m = df_fnl.query("origine_nationale =='O'")

    # -------------------------------------------------------------------------------------

    # -----------------------------
    # Scatter Mapbox
    # -----------------------------

    # === Création de la carte Plotly ===
    fig = px.scatter_mapbox(
        df_map,
        lat="lat",
        lon="lon",
        size=size_col,
        hover_name=hover_col,
        color=hover_col,
        mapbox_style="carto-positron",
        zoom=4 if region_selected == "Toutes les régions" else 6,
        height=300,
        width=300,
    )
    # Dans scatter_mapbox
    # Le paramètre zoom contrôle le niveau de détail de la carte :
    # zoom=3 → on voit l’Europe
    # zoom=5 → on voit toute la France
    # zoom=7 → on voit une région
    # zoom=9 → on voit une ville
    # zoom=12+ → on voit un quartier

    # --- Appliquer à la carte ----
    fig.update_layout(
        mapbox_style="open-street-map",
        mapbox_layers=[
            {
                "source": geojson_filtered,
                "type": "line",
                "color": "black",
                "line": {"width": 0.3},
            }
        ],
        mapbox_center={"lat": center_lat, "lon": center_lon},
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        showlegend=False,
    )

    # --- Affichage dans Streamlit ---
    st.sidebar.plotly_chart(fig, width="stretch")
    
    # --- Tabulations  ---

    (tabMain, tabAnalyse) = st.tabs(["📌Tableau de Bord","🔍 Analyse"])

    # -----------------------------
    # TAB 1
    # -----------------------------
    with tabMain:
        with st.container(border=True):
            if origine_secteur == 'origine_nationale':
                st.subheader("Portrait moyen du défunt en France")
            else:
                st.subheader("Portrait moyen du défunt sur ce secteur")

            col_sex, col_age, col_pren, col_lieu_nai, col_lieu_dec, col_origine, col_dist = st.columns([1.2,
                                    0.9, 2.6, 2.9,2.9, 1, 1.3])

            age_moyen, serie_sex, serie_prenom, serie_lieu_naissance, serie_lieu_deces, origine_dominante, distance_moy = recherche_dominant_sur_secteur(
                                df_fnl_m, nom_secteur, origine_secteur )            
        
            col_age.metric("Âge moy.", f"{age_moyen} ans")
            # Affichage des icônes SVG dans la colonne `col_sex`
            with col_sex:
                sex = "homme"  # Exemple: ici, tu pourrais avoir une condition qui choisit entre "homme" ou "femme"
                
                if serie_sex[0] == "H":
                    st.image("assets/men.svg", width=120) # Affichage de l'icône homme
                else:
                    st.image("assets/women.svg", width=120)  # Affichage de l'icône femme
            
            col_pren.metric("Prénom dominant", serie_prenom[0])
            col_lieu_nai.metric("Secteur de naissance dominant",serie_lieu_naissance[0] )        
            col_lieu_dec.metric("Secteur de décès dominant",serie_lieu_deces[0] )
            col_origine.metric("Originaire",origine_dominante)
            col_dist.metric("Distance moy.*",distance_moy)
            
            st.caption("Distance moy.* = Distance moyenne entre le lieux de naissance et de décès.")
        
        with st.container(border=True):
            st.subheader("Scoring")
            st.markdown(
                """
                <div style="background-color: #ADD8E6; ">
                Les indicateurs présents dans ces graphes sont relatifs à la fin de vie.\n
                => TAFV et IMD sont calculés sur un département ou une region, le score est 
                relatif a une ville.\n
                📌 Le taux d'attractivité de fin de vie (TAFV) mesure la capacité d'un secteur à accueillir, 
                au moment du décès, des personnes qui n'y sont pas nées.
                Interprétation :<br>
                <b>-</b> TAFV > 0.6 le secteur est très attractif en fin de vie pour les exogènes. Cela peut refléter la présence d'hôpitaux, d'EHPAD
                ou de zones de retraite présidentielle.<br> 
                <b>-</b> TAFV < 0.3 les décès sont majoritairement locaux.\n 
                📌 L'indice de mobilité différentiel (IMD) mesure la mobilité entre originaires et non 
                originaires d'un secteur.<br> 
                <b>-</b> IMD > 0.5 les exogènes sont plus mobiles. <br>
                <b>-</b> IMD < 0.5 les natifs sont plus mobiles.\n
                📌 Le score mesure dans quelle mesure une ville concentre beaucoup de personnes qui y sont originaires
                et qui y terminent leur vie.<br> 
                <b>-</b> Score > 0.5 le secteur “garde ses habitants”. Cela reflète un ancrage territoriale.<br> 
                <b>-</b> Score < 0.5 le Le secteur est attractif pour des personnes venues d’ailleurs.<br> 
                </div>
                
            """,
                unsafe_allow_html=True,
            )

            fig_score, df_score = graph_scoring(df_fnl,nom_secteur,origine_secteur)
            
            with st.container(border=True):         
                st.plotly_chart(fig_score, width="stretch", key="Graphe_score")  
                
             

            #st.dataframe(df_score)


    # -----------------------------
    # TAB 2
    # -----------------------------
    with tabAnalyse:  
        # -----------------------------
        # Barplot selon filtres
        # -----------------------------
        with st.container(border=True):
            
            st.markdown(
                """
                <div style="background-color: #ADD8E6; ">
                Ces graphiques permettent d’observer simultanément la répartition géographique, les classes d'âge 
                de la mortalité couplés à l'origine des populations pour chaque secteur (région, département et ville).\n
                📌 Si la mortalité des originaires est importante, alors la population est très ancrée sur ce secteur.
                C'est un indicateur intéressant pour péreniser le business des sociétés de produits financiers
                (assurances, banques..).
                </div>
            """,
                unsafe_allow_html=True,
            )

        # Preparation de l'alignement des graphes
        # Colonnes côte à côte
        # Mettre un espace entre les différents conteneurs
        col1, col2 = st.columns([3.2, 3.1])
        
        # creation du graphe
        la_fig, list_ordonnee_secteur_sans_dbl = graph_bar_origine(
            df_bar, nom_secteur, origine_secteur
        )

        with col1:
            with st.container(border=True):

                # Ouvre un pop-up
                with st.popover("ℹ️ À propos de ce graphique"):
                    st.markdown(
                        """
                        <div style="background-color: #ADD8E6;
                            padding:12px;
                            border-radius:8px;
                            border-left:4px solid #1f77b4; ">
                        Ce graphique représente : <br>
                        <b>-</b> les décès par secteur (région, département, ville) <br>
                        <b>-</b> ventilé par l'origine des décès <br>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                (tab11,) = st.tabs(["📊 Originaire et Exogène"])

                with tab11:
                    st.plotly_chart(
                        la_fig, width="stretch", key="Graphe_bar_origine"
                    )

        with col2:
            with st.container(border=True):

                # Ouvre un pop-up
                with st.popover("ℹ️ À propos de ce graphique"):
                    st.markdown(
                        """
                        <div style="background-color: #ADD8E6;
                            padding:12px;
                            border-radius:8px;
                            border-left:4px solid #1f77b4; ">
                        Ce graphique représente selon critère sur Origine :<br>
                        <b>-</b> les décès par classe d’âge <br>
                        <b>-</b> ventilés par origine secteur (région, département, ville) <br>
                        <b>-</b> agrégés par secteur <br>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                # Tabs ds Streamlit
                tab21, tab22 = st.tabs(["📊 Originaire", "📈 Exogène"])

                with tab21:
                    # st.subheader("Analyse – Graphe 1")
                    # Affichage dans Streamlit
                    st.plotly_chart(
                        graph_heat_map_origine(
                            df_bar_cl,
                            nom_secteur,
                            origine_secteur,
                            list_ordonnee_secteur_sans_dbl,
                            "O",
                        ),
                        width="stretch",
                        key="Clas_Age_Ori_O",
                    )

                with tab22:
                    # st.subheader("Analyse – Graphe 2")
                    # Affichage dans Streamlit
                    st.plotly_chart(
                        graph_heat_map_origine(
                            df_bar_cl,
                            nom_secteur,
                            origine_secteur,
                            list_ordonnee_secteur_sans_dbl,
                            "N",
                        ),
                        width="stretch",
                        key="Clas_Age_Ori_N",
                    )

        with st.container(border=True):
            st.markdown(
                """
                <div style="background-color: #ADD8E6; ">
                Ces graphiques permettent d’observer la fréquence de la mortalité, selon la classe d'âge et l'origine.\n        
                📌 Cela permet pour les sociétés de pompe funebre, d'anticiper les pics d'activité.
                </div>
            """,
                unsafe_allow_html=True,
            )

        # Mettre un espace entre les différents conteneurs
        col3, col4 = st.columns([3.2, 3.2])

        with col3:
            with st.container(border=True):
                st.plotly_chart(
                    graph_bar_month(df_bar_month, origine_secteur),
                    width="stretch",
                    key="bar_month",
                )

        with col4:
            with st.container(border=True):
                st.plotly_chart(
                    graph_bar_class_age_month(df_bar_month_cl, origine_secteur),
                    width="stretch",
                    key="bar_monthEtClAge",
                )

        # (Optionnel) Afficher le DataFrame filtré
        with st.expander("Voir les données filtrées du df_final "):
            # st.dataframe(df_final)
            st.dataframe(df_final)

    #st.image("assets/logoWm1.svg", width=50)
    
    
