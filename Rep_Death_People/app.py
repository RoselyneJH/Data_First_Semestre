#####################################################################################
#                                                                                   #
#                               A P P L I C A T I O N                               #
#                                 S T R E A M L I T                                 #
#                                                                                   #
#####################################################################################

import streamlit as st
import pandas as pd
import plotly.express as px

import os

import requests

from typing import List, Dict, Union, Tuple

import polars as pl
import numpy as np

from my_module.Cls_load_data_pour_viz import ClsLoadDataPourViz
from pathlib import Path


from my_module.graphs.graph_secteur_score import (  # type: ignore
    ClsGraphScore as graph_score,
)

from my_module.graphs.graph_secteur_score_age import (  # type: ignore
    ClsGraphScoreAge as graph_score_age,
)

# -------------------------------------------------------------------------------------
#

# Permet de reduire la marge entre side bar et reste de l'écran
# A définir, en premier dans une app. streamlit
st.set_page_config(layout="wide")

# --- Etat de pagination ---
if "page" not in st.session_state:
    st.session_state.page = 0


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
def load_dataframe() -> pd.DataFrame:
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
        path_racine=str(Path.cwd()),
        choix_system="polars",
        mode=mode,
    )

    df_person_nais_dece_departement_region = (
        my_class.ExtractionDataTableDeathPeopleView()
    )

    df = my_class.creation_classe_age(df_person_nais_dece_departement_region)
    
    # Nombre de Deces hors france :
    nb_deces_hors_france = df['nb_hors_france'].mean()

    # toutes ces personnes sont mortes en France :
    df['pays_deces'] ='FRANCE' # cela permet d'ajuster la hierarchie des secteurs de deces

    mon_pl = pl.DataFrame(df)
    df_polars = mon_pl.group_by(
        [
            "nom_region_deces",
            "nom_departement_deces",
            "ville_deces",
            "code_region_deces",
            "code_departement_deces",
            "age",
            "sex",  # ajout
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
    df_grp = df_polars.to_pandas()
    
    return df_grp, df, nb_deces_hors_france


def moyenne_ecart_type_national(df_fnl: pd.DataFrame) -> Tuple:
    """
    Args :
        Dataframe national
        valeur dont on doit calculer moyenne et ecart-type
        Ajout d'un filtre age
    Return :
        la moyenne
        l'ecart-type
    """
    mon_pl = pl.DataFrame(df_fnl)

    df_polars = (
        mon_pl.lazy()
    #    .filter(pl.col("pays_naissance").is_in(["FRANCE"]))
        .select(
            [
                ((pl.col("distance") + 1).log())
                .mean()
                .alias("moy_distance"),  # mobilité moyenne
                ((pl.col("distance") + 1).log()).std().alias("std_distance"),
                pl.col("distance")
                .filter(pl.col("origine_departement") == "O")
                .min()
                .alias("distance_dep_min"),
                pl.col("distance")
                .filter(pl.col("origine_departement") == "O")
                .max()
                .alias("distance_dep_max"),
                pl.col("distance")
                .filter(pl.col("origine_nationale") == "O")
                .max()
                .alias("distance_nat_max"),
            ]
        )
        .collect()
    )

    df_polars_age = (
        mon_pl.lazy()
    #    .filter(pl.col("pays_naissance").is_in(["FRANCE"]))
        .group_by("classe_age")
        .agg(
            [
                ((pl.col("distance") + 1).log())
                .mean()
                .alias("moy_distance"),  # mobilité moyenne
                ((pl.col("distance") + 1).log()).std().alias("std_distance"),
            ]
        )  # ecart type de la mobilité
        .collect()
    )

    df = df_polars.to_pandas()
    df_age = df_polars_age.to_pandas()

    return df, df_age


def recherche_dominant_sur_secteur(
    df_fnl_m: pd.DataFrame, ce_secteur: str, cette_origine_secteur: str
) -> Tuple:
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
    age_moyen = str(round(df_fnl_m["age"].mean(), 0)).replace(".0", "")

    # Sexe
    serie_sex = "H"  # par défaut
    if df_fnl_m["sex"].mode()[0] == "2":  # femme
        serie_sex = "F"
        serie_prenom = df_fnl_m[df_fnl_m["sex"] == "2"]["prenom"].mode()
    else:  # Alors homme
        serie_prenom = df_fnl_m[df_fnl_m["sex"] == "1"]["prenom"].mode()

    # Préparation des éléments dominants dans un secteur donnée
    ce_secteur_naissance = ce_secteur.replace("deces", "naissance")
    serie_lieu_naissance = df_fnl_m[ce_secteur_naissance].mode()
    serie_lieu_deces = df_fnl_m[ce_secteur].mode()

    # Originaire :
    nb_originaire = df_fnl_m[df_fnl_m[cette_origine_secteur] == "O"][
        cette_origine_secteur
    ].count()
    nb_Non_originaire = df_fnl_m[df_fnl_m[cette_origine_secteur] == "N"][
        cette_origine_secteur
    ].count()

    origine_dominante = "N"
    if nb_originaire >= nb_Non_originaire:
        origine_dominante = "O"

    distance_moy = str(
        round(
            df_fnl_m[df_fnl_m[cette_origine_secteur] == origine_dominante][
                "distance"
            ].mean(),
            0,
        )
    ).replace(".0", " ")
    distance_moy = distance_moy + "km"

    return (
        age_moyen,
        serie_sex,
        serie_prenom,
        serie_lieu_naissance,
        serie_lieu_deces,
        origine_dominante,
        distance_moy,
    )


def statistique_sur_secteur(
    df_fnl_e: pd.DataFrame, ce_secteur: str, cette_origine_secteur: str
) -> Tuple:
    """
    Produits des Kpis sur ce secteur
    Args :
        nom du secteur à traiter
        origine du secteur à traiter
    Return :
        Séries de kpi :
        Pourcentage d'originaire, Pourcentage d'exogène, Indicateur TAFV, Age Moy Origi,
        Age Moy exo, distance mediane par classe d'âge
    """
    # Dictionnaire
    stat_secteur = {}

    df_fnl_m = df_fnl_e.copy()

    ordre= ["0-1", "1-20", "20-35", "35-50", "50-65", "65-90", "90+"]

    # proportion des classes d'age
    les_proportions_age = (
        df_fnl_m["classe_age"]
        .value_counts(normalize=True)
        .mul(100)
        .round(3)
        .reset_index(name="pourcentage")
    )

    les_proportions_age["classe_age"] = les_proportions_age["classe_age"].cat.reorder_categories(
        ordre,
        ordered=True
    )
    les_proportions_age = les_proportions_age.sort_values("classe_age")

    # Preparation des indicateurs
    nb_originaire = df_fnl_m[df_fnl_m[cette_origine_secteur] == "O"][
        cette_origine_secteur
    ].count()
    nb_exogene = df_fnl_m[df_fnl_m[cette_origine_secteur] == "N"][
        cette_origine_secteur
    ].count()

    # Pourcentage Originaire
    pct_originaire = str(round(100 * nb_originaire / len(df_fnl_m), 2))

    # Pourcentage Exogène
    pct_exogene = str(round(100 * nb_exogene / len(df_fnl_m), 2))

    # Indicateur TAFV
    ind_TAFV = str(round(nb_originaire / (nb_originaire + nb_exogene), 1))

    # Distance
    distance_med_originaire = (
        str(
            df_fnl_m[df_fnl_m[cette_origine_secteur] == "O"]["distance"].median()
        ).replace(".0", " ")
        + " Km"
    )
    distance_med_exogene = (
        str(
            df_fnl_m[df_fnl_m[cette_origine_secteur] == "N"]["distance"].median()
        ).replace(".0", " ")
        + " Km"
    )

    # Age moyen Origi/ Exoge
    age_moy_originaire = str(
        round(df_fnl_m[df_fnl_m[cette_origine_secteur] == "O"]["age"].mean(), 0)
    ).replace(".0", "")
    age_moy_exogene = str(
        round(df_fnl_m[df_fnl_m[cette_origine_secteur] == "N"]["age"].mean(), 0)
    ).replace(".0", "")

    stat_secteur["pct_originaire"] = pct_originaire
    stat_secteur["pct_exogene"] = pct_exogene
    stat_secteur["ind_TAFV"] = ind_TAFV
    stat_secteur["age_moy_originaire"] = age_moy_originaire
    stat_secteur["age_moy_exogene"] = age_moy_exogene
    stat_secteur["distance_med_originaire"] = distance_med_originaire
    stat_secteur["distance_med_exogene"] = distance_med_exogene

    df_fnl_m["distance_av_som"] = 1 + df_fnl_m["distance"]
    df_fnl_m.loc[:, "distance_lng"] = np.log(df_fnl_m["distance_av_som"])
    distance_log_sum_prp = df_fnl_m["distance_lng"].sum() / len(df_fnl_m)

    return (
        pct_originaire,
        pct_exogene,
        ind_TAFV,
        distance_log_sum_prp,
        age_moy_originaire,
        age_moy_exogene,
        distance_med_originaire,
        distance_med_exogene,
        les_proportions_age,
    )


# Récupération des regions et départements
geojson_regions, geojson_departements = load_geojsons()

# Recupération des datas provenant de la Bdd
df_grp, df, nb_deces_hors_france = load_dataframe()

# Chemin relatif pour la recupération des images .svg
BASE_DIR = Path(__file__).resolve().parent
image_path_men = BASE_DIR / "assets" / "men.svg"
image_path_women = BASE_DIR / "assets" / "women.svg"
image_path_carte = BASE_DIR / "assets" / "France.svg"
image_path_pourcentage = BASE_DIR / "assets" / "Pourcentage.svg"
image_path_paysage = BASE_DIR / "assets" / "Paysage.svg"

# Le titre
st.title("Dynamiques et attractivités des territoires ")
st.header("Insights pour assurances et politiques publiques")
st.subheader("Analyse des décès en France (2024)")

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
    
    /* Valeurs du slider (20, 85) */
        .stSlider * {
            color: #00090f !important;
    }
    /* Taille des icone dite Material*/
    span[style*="Material Symbols Rounded"] {
        font-size: 26px !important;  
        vertical-align: -0.25em !important;             
    }

    </style>
    """,
    unsafe_allow_html=True,
)

# ----------------------------- font-weight: bold;
# Sidebars vertical-align: middle !important;
# ----------------------------- vertical-align: -0.15em !important; 

# Widgets dans la sidebar
st.sidebar.header(":material/filter_alt: Filtres")

# --- Combobox Région ---
regions = ["Toutes les régions"] + sorted(df_grp["nom_region_deces"].unique().tolist())
region_selected = st.sidebar.selectbox(":material/distance: Région :", regions)

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
departement_selected = st.sidebar.selectbox(":material/distance: Département :", departements)

# Filtrage selon le département
if departement_selected == "Tous les départements":
    df_dept = df_region.copy()
    df_dpt = df_rgn.copy()
else:
    df_dept = df_region[df_region["nom_departement_deces"] == departement_selected]
    df_dpt = df_rgn[df_rgn["nom_departement_deces"] == departement_selected]

# -------------------------------------------------------------------------------------

# ComboBox Ville
cette_agglomeration=""

with st.sidebar:
    villes = ["Toutes les villes"] + sorted(df_dept["ville_deces"].unique().tolist())
    # pour eviter de selectionner des villes sur des départements différents, j'active l'option disabled
    ville_selected = st.selectbox(
        ":material/distance: Ville / Arrondissement :",
        villes,
        disabled=(departement_selected == "Tous les départements"),
        help="Choisissez un département pour activer la sélection des villes",
    )
    # initialisation case à cocher :
    agglomeration = False # la case à cocher "agglomeration" est non cochée [init]
    
    # boolean sur type de ville standard =(paris 6 ieme, Bordeaux, valencienne, St denis...) ou agglomération (PARIS, LYON, MARSEILLE)
    est_ville_standard = True # Est ce une ville standard ? on initalise à oui [init]

    if (
        ville_selected.startswith("PARIS")
        or ville_selected.startswith("MARSEILLE")
        or ville_selected.startswith("LYON")
    ):
        est_ville_standard = False
        cette_agglomeration = ville_selected.split()[0]
        le_libelle_agg = "Agglomération de " + cette_agglomeration

    if (not est_ville_standard) & (not ville_selected == "Toutes les villes"):
        agglomeration = st.checkbox(label=le_libelle_agg, disabled=est_ville_standard, 
        help ="Regrouper les résultats de l'agglomération")


# Filtrage selon la ville
if ville_selected == "Toutes les villes":
    df_final_ = df_dept.copy()
    df_fnl_ = df_dpt.copy()
elif agglomeration :
    df_final_ = df_dept[df_dept["ville_deces"].str.contains(cette_agglomeration)]
    df_fnl_ = df_dpt[df_dpt["ville_deces"].str.contains(cette_agglomeration)]
else:
    df_final_ = df_dept[df_dept["ville_deces"] == ville_selected]
    df_fnl_ = df_dpt[df_dpt["ville_deces"] == ville_selected]

# Selection des sexes
with st.sidebar:
    # 
    choix_genre = st.radio(
        ":material/person: Genre :",
        [":material/wc: ", ":material/man: ", ":material/woman: "],
        horizontal=True,
        disabled=(departement_selected != "Tous les départements")
        | (region_selected != "Toutes les régions"),
        help="Déselectionner le département et/ou la région pour activer les boutons",
    )

if choix_genre == "H":
    df_final_f = df_final_.query("sex=='1'")
    df_fnl_f = df_fnl_.query("sex=='1'")
elif choix_genre == "F":
    df_final_f = df_final_.query("sex== '2'")
    df_fnl_f = df_fnl_.query("sex== '2'")
else:
    df_final_f = df_final_.copy()
    df_fnl_f = df_fnl_.copy()


# Slider
start, end = st.sidebar.slider(
    ":material/deployed_code_account: Âge :",
    0,
    105,
    (20, 85),
    disabled=(departement_selected != "Tous les départements")
    | (region_selected != "Toutes les régions"),
    help="Déselectionner le département et/ou la région pour activer le slider",
)
# Je dois recalculer au niveau national ecart-type et moyenne de deplacement, je dois obliger
# l'utilisateur à passer par le selectbox Région puis celui de département

# Extraction des personnes respectant le filtre sur l'age
df_final = df_final_f[(df_final_f["age"] >= start) & (df_final_f["age"] <= end)]

df_fnl = df_fnl_f[(df_fnl_f["age"] >= start) & (df_fnl_f["age"] <= end)]
 
# -------------------------------------------------------------------------------------
st.write("Auteur : R.Jean / Source : https://www.insee.fr/fr/statistiques")
# -------------------------------------------------------------------------------------
# Test si presence de valeurs apres selection :
if len(df_final) > 0:
    valeur = df_final["nb_deces"].sum()
    if valeur > 50:
        nb_deces_hors_france = int(nb_deces_hors_france)
        proportion_hf = round(nb_deces_hors_france*100/ valeur,0)
        # creation de container avec colonne * 2 
        with st.sidebar.container(
            border=False,
            height=70,
        ):
            col1, col2 = st.columns(
                [1.2,  0.4]
            )  #
            with col1:
                st.info(f" Sélection : {valeur:,}".replace(",", " "))

            with col2:        
                with st.popover(":material/warning:"):
                    st.write(f"""
                        La sélection comportent une proportion de décès à l'étranger :
                        - Nombre de décès hors hexagone : {nb_deces_hors_france}
                        - Proportion nationale : {proportion_hf} %
                        """)
        #  :material/group:  ℹ️
    else:
        st.sidebar.warning(
            f"Interprétation délicate ! \nSélection décès faible : {valeur:,}".replace(
                ",", " "
            )
        )
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

    # === Préparation des données pour la carte et pour graphes  ===
    if ville_selected != "Toutes les villes":
        
        if not agglomeration:

            df_fnl_m = df_fnl.query("ville_deces == @ville_selected")

            # ➜ chaque ligne renvoie un cumul de personnes decedées
            df_map = df_final.query("ville_deces == @ville_selected").groupby(["ville_deces"], as_index=False).agg(
                {"lat": "mean", "lon": "mean", "nb_deces": "sum"}
            )            
        else:
            df_fnl_m = df_fnl.copy()
            df_fnl_m.drop('ville_deces',  axis='columns', inplace=True)
            df_fnl_m.insert(loc=15, column='ville_deces', value=cette_agglomeration)

            df_map = df_final.query("ville_deces == @ville_selected").groupby(["ville_deces"], as_index=False).agg(
                {"lat": "mean", "lon": "mean", "nb_deces": "sum"}
            ) 

        hover_col = "ville_deces"
        size_col = "nb_deces"

        nom_secteur = "ville_deces"

        origine_secteur = "origine_ville"

    elif departement_selected != "Tous les départements":
        # conserver ces valeurs :
        df_final_sav=df_final
        df_fnl_sav= df_fnl
        # ➜ regroupement par ville
        #df_list = df_final.query("nom_departement_deces == @departement_selected")
        df_map = (
            df_final.query("nom_departement_deces == @departement_selected").groupby(["nom_departement_deces", "ville_deces"])
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "ville_deces"
        size_col = "count"

        nom_secteur = "ville_deces"

        origine_secteur = "origine_departement"

        df_fnl_m = df_fnl.query("nom_departement_deces == @departement_selected")

    elif region_selected != "Toutes les régions":
        # ➜ regroupement par département
        #df_list = df_final.query("nom_region_deces == @region_selected")
        df_map = (
            df_final.query("nom_region_deces == @region_selected").groupby(["nom_region_deces", "nom_departement_deces"])
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "nom_departement_deces"
        size_col = "count"

        nom_secteur = "nom_departement_deces"

        origine_secteur = "origine_departement"

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

        nom_secteur = "nom_region_deces"

        origine_secteur = "origine_nationale"

        # ****** Metrics *******
        df_fnl_m = df_fnl

    # -------------------------------------------------------------------------------------
    # STREAMLIT  SESSION
    # --- Gestion changement de filtres ---
    current_filters = (
        region_selected,
        departement_selected,
        ville_selected,
        start,
        end,
    )

    (
        pc_o,
        pc_e,
        ind_atfv,
        distance_lng_sum_prc,
        moy_age_o,
        moy_age_e,
        dis_med_o,
        dis_med_e,
        les_proportions_age,
    ) = statistique_sur_secteur(df_fnl_m, nom_secteur, origine_secteur)

    if "filters" not in st.session_state:
        st.session_state.filters = current_filters
        if (
            origine_secteur == "origine_nationale"
        ):  # nous sommes sur une posture nationale pour laquelle
            # nous calculons l'ecart-type et la moyenne de la distance (cas de relance de l'application)
            le_df_ecart_type_moy, le_df_ecart_type_moy_age = (
                moyenne_ecart_type_national(df_fnl_m)
            )
            st.session_state.ecart_type_national = le_df_ecart_type_moy["std_distance"]
            st.session_state.moyenne_nationale = le_df_ecart_type_moy["moy_distance"]
            st.session_state.distance_departement_min = le_df_ecart_type_moy[
                "distance_dep_min"
            ]
            st.session_state.distance_departement_max = le_df_ecart_type_moy[
                "distance_dep_max"
            ]
            st.session_state.df_ecart_type_moy_age = le_df_ecart_type_moy_age
            st.proportion_nat_age = les_proportions_age
            


    if current_filters != st.session_state.filters:
        st.session_state.filters = current_filters
        st.session_state.page = 0  # reset pagination
        if (
            origine_secteur == "origine_nationale"
        ):  # nous sommes sur une posture nationale pour laquelle
            # nous calculons l'ecart-type et la moyenne de la distance (cas de modification filtes)
            le_df_ecart_type_moy, le_df_ecart_type_moy_age = (
                moyenne_ecart_type_national(df_fnl_m)
            )
            st.session_state.ecart_type_national = le_df_ecart_type_moy["std_distance"]
            st.session_state.moyenne_nationale = le_df_ecart_type_moy["moy_distance"]
            st.session_state.distance_departement_min = le_df_ecart_type_moy[
                "distance_dep_min"
            ]
            st.session_state.distance_departement_max = le_df_ecart_type_moy[
                "distance_dep_max"
            ]
            st.session_state.df_ecart_type_moy_age = le_df_ecart_type_moy_age
            st.proportion_nat_age = les_proportions_age

    # -------------------------------------------------------------------------------------
    # PAGINATION

    ce_graph_TAFV = graph_score(df_fnl_m, nom_secteur, origine_secteur)

    # je fais apparaitre uniquement dans une vue nat, region ou departement
    if origine_secteur != "origine_ville":

        # Boutons de navigation
        with st.sidebar.container(
            border=False,
            height=50,
        ):
            col1, col12, col3, col13, col4 = st.columns(
                [0.35, 0.1, 0.42, 0.1, 0.35]
            )  #
            with col1:
                be_disabled = (
                    True
                    if st.session_state.page == ce_graph_TAFV.nombre_de_page - 1
                    else False
                )
                if (
                    st.button(
                        "⬅️",
                        disabled=be_disabled,
                        use_container_width=True,
                        help="Secteurs à mortalité faible",
                    )
                    and not be_disabled
                ):
                    st.session_state.page += 1

            with col3:
                st.info(f"{st.session_state.page + 1} / {ce_graph_TAFV.nombre_de_page}")

            with col4:
                be_disabled = True if st.session_state.page == 0 else False
                if st.button(
                    "➡️",
                    disabled=be_disabled,
                    use_container_width=True,
                    help="Secteurs à mortalité forte",
                ):  # type: ignore
                    st.session_state.page -= 1

        st.session_state.page = max(0, st.session_state.page)
        st.session_state.page = min(st.session_state.page, ce_graph_TAFV.nombre_de_page)

    # faire un petit espace pour eviter d'interargir avec la map et les boutons
    st.sidebar.space(size="xxsmall")
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
    (tabMain,) = st.tabs(["🔍 Analyse "])

    # -----------------------------
    # TAB 1
    # -----------------------------
    with tabMain:
        if origine_secteur == "origine_nationale":
            sous_titre_indicateur_personne = "Portrait moyen du défunt en France"
            sous_titre_indicateur_secteur = "Indicateurs nationaux"
        else:
            sous_titre_indicateur_personne = "Portrait moyen du défunt sur ce secteur"
            sous_titre_indicateur_secteur = "Indicateurs territoriaux"

        with st.container(border=True):
            st.subheader("Objectifs :")
            st.markdown(
                """
                <div style="background-color: #ADD8E6; ">
                Cette présentation consiste à distinguer deux types de territoires : ceux qui gagnent des seniors (Attractivité)
                et ceux qui y restent pour leur vie (Ancrage).\n
                
                Cette information est importante pour les sociétés d'assurances et complémentaires santé. <br> Les territoires 
                avec beaucoup de seniors indiquent potentiellement : <br>
                <b>-</b> Des successions plus nombreuses à moyen terme <br>
                <b>-</b> Des transferts d’épargne et d’immobilier <br>
                <b>-</b> Une activation future de contrats d’assurance-vie <br>
                </div>                
                """,
                unsafe_allow_html=True,
            )

        with st.container(border=True):

            st.subheader(sous_titre_indicateur_personne)

            (
                col_sex,
                col_age,
                col_pren,
                col_lieu_nai,
                col_lieu_dec,
                col_origine,
                col_dist,
            ) = st.columns([1.2, 0.9, 2.6, 2.9, 2.9, 1, 1.3])

            (
                age_moyen,
                serie_sex,
                serie_prenom,
                serie_lieu_naissance,
                serie_lieu_deces,
                origine_dominante,
                distance_moy,
            ) = recherche_dominant_sur_secteur(df_fnl_m, nom_secteur, origine_secteur)

            col_age.metric("Âge moy.", f"{age_moyen} ans")
            # Affichage des icônes SVG dans la colonne `col_sex`
            with col_sex:
                sex = "homme"  

                if serie_sex[0] == "H":
                    st.image(image_path_men, width=120 )  # Affichage de l'icône homme 
                else:
                    st.image(image_path_women, width=120)  # Affichage de l'icône femme

            col_pren.metric("Prénom dominant", serie_prenom[0])
            col_lieu_nai.metric(
                "Secteur de naissance dominant", serie_lieu_naissance[0]
            )
            col_lieu_dec.metric("Secteur de décès dominant", serie_lieu_deces[0])
            col_origine.metric("Originaire", origine_dominante)
            col_dist.metric("Distance moy.*", distance_moy)

            st.caption(
                "Distance moy.* = Distance moyenne entre le lieux de naissance et de décès."
            )

        with st.container(border=True):

            st.subheader(sous_titre_indicateur_secteur)

            (
                col_carte,    
                col_pc_originaire,
                col_pc_exogene,
                col_moy_age_ori,
                col_moy_age_exo,
                col_dist_med_o,
                col_dist_med_e,
            ) = st.columns([0.6,0.7, 0.7, 0.9, 0.9, 0.9, 0.9])

            with col_carte:
                if origine_secteur == "origine_nationale":
                    st.image(image_path_carte, width=160 )  # Affichage  
                else:
                    st.image(image_path_paysage, width=160 )  # Affichage 

            col_pc_originaire.metric("Originaire", f"{pc_o} %",border=True)
            col_pc_exogene.metric("Exogène",  f"{pc_e} %",border=True)

            col_moy_age_ori.metric("Âge moy. Originaire", f"{moy_age_o} an(s)",border=True )
            col_moy_age_exo.metric("Âge moy. Exogène",  f"{moy_age_e} an(s)",border=True)
            col_dist_med_o.metric("Distance med.* Originaire", dis_med_o,border=True)
            col_dist_med_e.metric("Distance med.* Exogène", dis_med_e,border=True)

            # calcul d'IMD
            ind_imd = round(
                (st.session_state.moyenne_nationale.loc[0] - distance_lng_sum_prc)
                / st.session_state.ecart_type_national.loc[0],
                2,
            )
            #
            st.caption("Distance med.* = Distance médiane ")

            with st.container(border=True):
                st.subheader("Pourcentage des défunts par classes d'âge")

                col_img,col_1,col_2,col_3,col_4,col_5,col_6,col_7= st.columns([0.6,0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6])
                
                # Pour eviter de répéter et pour boucler :
                cols = [col_1, col_2, col_3, col_4, col_5, col_6, col_7]

                with col_img:
                    st.image(image_path_pourcentage, width=160 )

                if origine_secteur != "origine_nationale":
                    les_proportions_age['delta'] =st.proportion_nat_age['pourcentage'] - les_proportions_age['pourcentage']
                    les_proportions_age['delta']=round(les_proportions_age['delta'],2)
                    print("les proportion_age",les_proportions_age.info())
                    for i, col in enumerate(cols):
                        col.metric(
                            f"{les_proportions_age.iloc[i, 0]} {'an' if i == 0 else 'ans'}",
                            f"{les_proportions_age.iloc[i, 1]:.2f} %",
                            les_proportions_age.iloc[i, 2],
                            border=True
                        )                   
                else:
                    for i, col in enumerate(cols):
                        col.metric(
                            f"{les_proportions_age.iloc[i, 0]} {'an' if i == 0 else 'ans'}",
                            f"{les_proportions_age.iloc[i, 1]:.2f} %",
                            border=True
                        )
                                    
        # RESTITUTION DES GRAPHES
        # instanciation faite précedemment
        fig_score, message_score, df_score = ce_graph_TAFV.render_graph_score(
            page=st.session_state.page
        )
        # Instancie la classe
        ce_graph_TAFV_age = graph_score_age(df_fnl_m, nom_secteur, origine_secteur)

        fig_score_age = ce_graph_TAFV_age.render_graph_score_age(
            page=st.session_state.page
        )

        fig_score_age_Exo = ce_graph_TAFV_age.render_graph_score_age(
            False, page=st.session_state.page
        )

        # Instancie la classe
        ce_graph_IMD = graph_score(
            df_fnl_m,
            nom_secteur,
            origine_secteur,
            st.session_state.ecart_type_national.loc[0],
            st.session_state.moyenne_nationale.loc[0],
        )

        fig_score_IMD, message_score_, df_score_IMD = (
            ce_graph_IMD.render_graph_score_IMD(
                page=st.session_state.page,
            )
        )

        # Instancie la classe
        ce_graph_IMD_age = graph_score_age(
            df_fnl_m,
            nom_secteur,
            origine_secteur,
            st.session_state.df_ecart_type_moy_age,
        )

        fig_score_IMD_age_mobile = ce_graph_IMD_age.render_graph_score_age_IMD(
            False, page=st.session_state.page, indicateur="mobilite_secteur"
        )

        fig_score_IMD_age_inertie = ce_graph_IMD_age.render_graph_score_age_IMD(
            True, page=st.session_state.page, indicateur="mobilite_secteur"
        )

        # fin

        with st.container(border=True):
            col_score_1, col_x, col_score_2 = st.columns([4.9, 0.6, 4.7])
            with col_score_1:
                with st.container(border=False):
                    st.subheader("Taux d'attractivité de fin de vie")
                    col_ind_tafv, _ = st.columns(
                        [0.5, 0.5]
                    )  #
                    col_ind_tafv.metric(
                        "TAFV", ind_atfv
                    )  # 
                    st.markdown(
                        """
                        <div style="background-color: #ADD8E6; ">
                        📌 Le taux d'attractivité de fin de vie (TAFV) mesure la capacité d'un secteur à accueillir,
                         au moment du décès, des personnes qui n'y sont pas nées. \n 
                        <b></b>\n                          
                        """,
                        unsafe_allow_html=True,
                    )
                    with st.popover("ℹ️ Interprétation "):
                        st.markdown(
                            """
                            <div style="background-color: #ADD8E6;
                                padding:12px;
                                border-radius:8px;
                                border-left:4px solid #1f77b4; ">
                                <b>-</b> TAFV < 0.3 le secteur est très attractif en fin de vie pour les exogènes. Cela peut 
                                refléter la présence d'hôpitaux, d'EHPAD ou de zones de retraite résidentielle.<br>
                                <b>-</b> TAFV > 0.6 les décès sont majoritairement locaux (fort ancrage territorial).
                                Cela correspond à une faible mobilité residentielle soulignant une forte identité culturelle.<br>
                            </div>              
                            """,
                            unsafe_allow_html=True,
                        )
            with col_score_2:
                with st.container(border=False):
                    st.subheader("Indice de mobilité différentielle")
                    col_ind_imd, _ = st.columns([0.4, 0.6])  #
                    col_ind_imd.metric("IMD", ind_imd)  #
                    st.markdown(
                        """
                        <div style="background-color: #ADD8E6; ">
                        📌 L’indice de mobilité différentielle permet de répondre à cette question :\n
                        Ce territoire est-il plus ou moins mobile que la moyenne nationale ?\n                        
                        </div>                
                        """,
                        unsafe_allow_html=True,
                    )
                    with st.popover("ℹ️ Interprétation "):
                        st.markdown(
                            """
                        <div style="background-color: #ADD8E6;
                            padding:12px;
                            border-radius:8px;
                            border-left:4px solid #1f77b4; ">
                            <b>-</b> IMD < 0 le secteur a une mobilité plus importante que la moyenne nationale.
                            Cela correspond à des territoires de circulation.<br>
                            <b>-</b> IMD = 0 Le secteur a une mobilité identique à la moyenne du pays. <br>
                            <b>-</b> IMD > 0 le secteur a une mobilité plus faible que la moyenne nationale.
                            Cela peut refléter des territoires d'ancrage. <br>
                        </div>                
                        """,
                            unsafe_allow_html=True,
                        )

            with st.container(border=True):
                # Préparation de l'alignement des graphes 
                # Colonnes côte à côte
                # Mettre un espace entre les différents conteneurs
                #col_TAFV, col_separateur, col_age_TAFV = st.columns([4.9, 0.6, 4.7])

                col_TAFV, col_separateur, col_IMD = st.columns([4.9, 0.6, 4.7])
                with col_TAFV:

                    with st.container(border=True):

                        # Tabs ds Streamlit
                        if not origine_secteur == "origine_ville":
                            # Ouvre un pop-up
                            with st.popover("ℹ️ À propos de ce graphique"):
                                st.markdown(
                                    """
                                    <div style="background-color: #ADD8E6;
                                        padding:12px;
                                        border-radius:8px;
                                        border-left:4px solid #1f77b4; ">
                                    Le taux d'attractivité découpe le graphe en 3 zones : <br>
                                    <b>-</b> Zone à forte présence d'exogènes dans ce secteur <br>
                                    <b>-</b> Zone neutre <br>
                                    <b>-</b> Zone à forte présence d'originaires dans ce secteur <br>
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )
                            (tab11,) = st.tabs(["📊 Poids des secteurs"])
                        else:
                            # Ouvre un pop-up
                            with st.popover("ℹ️ À propos de ce graphique"):
                                st.markdown(
                                    """
                                    <div style="background-color: #ADD8E6;
                                        padding:12px;
                                        border-radius:8px;
                                        border-left:4px solid #1f77b4; ">
                                    Représentation des origines des défunts pour cette ville 
                                        ou arrondissement : <br>
                                    <b>-</b> Proportion des originaires et des éxogènes <br>
                                    <b>-</b> Top 5 de la provenance des exogènes <br>
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )
                            (tab11,) = st.tabs(["📊 Proportion d'originaire/exogène  "])

                        with tab11:
                            st.plotly_chart(
                                fig_score, width="stretch", key="Graphe_score"
                            )
                
                with col_IMD:
                    with st.container(border=True):
                        with st.popover("ℹ️ À propos de ce graphique"):
                                    st.markdown(
                                        """
                                        <div style="background-color: #ADD8E6;
                                            padding:12px;
                                            border-radius:8px;
                                            border-left:4px solid #1f77b4; ">
                                        L'IMD découpe le graphe en 2 zones : <br>
                                        <b>-</b> Zone à forte mobilité <br>
                                        <b>-</b> Zone à forte inertie  <br>
                                        </div>
                                        """,
                                        unsafe_allow_html=True,
                                    )
                    
                        
                        (tab12I,) = st.tabs(["📊 Poids des secteurs"])
                        with tab12I: 
                            st.plotly_chart(
                                fig_score_IMD,
                                width="stretch",
                                key="Graphe_IMD",
                            )


                # message de suppression d'éventuel secteur sans intéret
                st.text(message_score)

            with st.container(border=True):
                # col_TAFV, col_separateur, col_age_TAFV = st.columns([4.9,0.7,4.6])
                #col_IMD, col_separateur, col_age_IMD = st.columns(
                #    [4.9, 0.6, 4.7]
                #) 
                col_age_TAFV, col_separateur, col_age_IMD = st.columns(
                    [4.9, 0.6, 4.7]
                )

                with col_age_TAFV:
                    with st.container(border=True):
                        # Ouvre un pop-up
                        with st.popover("ℹ️ À propos de ces graphiques"):
                            if not origine_secteur == "origine_ville":
                                st.markdown(
                                    """
                                    <div style="background-color: #ADD8E6;
                                        padding:12px;
                                        border-radius:8px;
                                        border-left:4px solid #1f77b4; ">
                                    Ces deux visualisations présentent le top 5 des meilleurs TAFV pour l'ancrage et l'attractivité
                                    des territoires.<br>
                                    <b>-</b> Les 5 meilleures cellules apparaissent avec un cadre noir. 
                                    Leur rang est spécifié dans l'encadré.<br>
                                    <b>-</b> La meilleur classe d'age et le meilleur secteur sont mis en surbrillance.
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.markdown(
                                    """
                                    <div style="background-color: #ADD8E6;
                                        padding:12px;
                                        border-radius:8px;
                                        border-left:4px solid #1f77b4; ">
                                    Ces deux visualisations présentent les classes d'âge des défunts de cette ville en distinguant
                                    les originaires des exogènes.<br>
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )
                        # Tabs ds Streamlit
                        if not origine_secteur == "origine_ville":
                            tab21, tab22 = st.tabs(
                                ["📈 Top 5 des Exogènes", "📊 Top 5 des Originaires"]
                            )
                        else:
                            tab21, tab22 = st.tabs(
                                [
                                    "📈 Classes d'âge exogènes ",
                                    "📊 Classes d'âge originaires ",
                                ]
                            )

                        with tab21:
                            st.plotly_chart(
                                fig_score_age_Exo,
                                width="stretch",
                                key="Graphe_score_age_Exo",
                            )

                        with tab22:
                            st.plotly_chart(
                                fig_score_age,
                                width="stretch",
                                key="Graphe_score_age",
                            )
 
                with col_age_IMD:
                    with st.container(border=True):
                        with st.popover("ℹ️ À propos de ce graphique"):
                                        st.markdown(
                                            """
                                            <div style="background-color: #ADD8E6;
                                                padding:12px;
                                                border-radius:8px;
                                                border-left:4px solid #1f77b4; ">
                                            Le taux d'attractivité découpe le graphe en 3 zones : <br>
                                            <b>-</b> Zone à forte présence d'exogènes dans ce secteur <br>
                                            <b>-</b> Zone neutre <br>
                                            <b>-</b> Zone à forte présence d'originaires dans ce secteur <br>
                                            </div>
                                            """,
                                            unsafe_allow_html=True,
                                        )
                        if not origine_secteur == "origine_ville":
                            tab12I, tab12I_ = st.tabs(
                                ["📈 Top 5 Mobilité", "📊 Top 5 Inertie"]
                            )
                        else:
                            (tab12I,) = st.tabs(["📊 Mobilité"])                        

                        with tab12I: #
                            st.plotly_chart(
                                fig_score_IMD_age_mobile,
                                width="stretch",
                                key="Graphe_age_IMD",
                            )
                        if not origine_secteur == "origine_ville":
                            with tab12I_: #
                                st.plotly_chart(
                                    fig_score_IMD_age_inertie,
                                    width="stretch",
                                    key="Graphe_age_IMD_i",
                                )
