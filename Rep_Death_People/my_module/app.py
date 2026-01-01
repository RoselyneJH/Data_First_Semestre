import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly

from Cls_load_data_pour_viz import ClsLoadDataPourViz
import requests

from streamlit_plotly_events import plotly_events
import polars as pl
import numpy as np

from graphs.graph_bar_origine    import render_graph_bar_origine            as graph_bar_origine
from graphs.graph_scatter_miroir import render_graph_scatter_miroir         as graph_scatter_miroir
from graphs.graph_bar_month      import render_graph_bar_month              as graph_bar_month  
from graphs.graph_bar_month      import render_graph_bar_class_age_month    as graph_bar_class_age_month
from graphs.graph_heat_map       import render_graph_heat_map_origine       as graph_heat_map_origine
# -------------------------------------------------------------------------------------

# Permet de reduire la marge entre side bar et reste de l'écran
# A définir, en premier dans une app. streamlit
st.set_page_config(
    layout="wide"
)

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


# --- Fonction pour récupérer data ---
@st.cache_data
def load_dataframe():
    """
    Recupération des données
    Args :
        None
    Return :
        Dataframe de données provenant d'une classe
    """
    # ---  Recupération de mes données via la classe ---
    my_class = ClsLoadDataPourViz(
        path_racine=r"C:\Users\chokr\Data Projet\Death_People\Rep_Death_People"
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
                #(pl.col("origine_region") == "O").sum().alias("nb_originaire_region"),
                #(pl.col("origine_departement") == "O")
                #.sum()
                #.alias("nb_originaire_departement"),
               # (pl.col("origine_ville") == "O").sum().alias("nb_originaire_ville"),
                pl.col("distance").mean().alias("distance_moy"),
            ]
        )
        # .collect()
    )
    df_grp = df_polars.to_pandas()

    return df_grp, df


# Récupération des regions et départements
geojson_regions, geojson_departements = load_geojsons()

# Recupération des datas provenant de la Bdd
df_grp, df = load_dataframe()

# Le titre
st.title("Mortalité et origine des populations en 2024")

# --- Fond d'écran ---
# E6E6FA; /* lavande */
# B0E0E6 bleu vert
# ADD8E6 bleu ciel         F0F8FF   B0C4DE C0C0C0
st.markdown(
    """
    <style>
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

    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
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

# Test si presence de valeurs apres selection :
if len(df_final) > 0:
    valeur = len(df_final) #.replace(",", " ")
    
    st.write(f"Déces sélectionnés : {valeur:,}".replace(",", " "))
    restitution_des_valeurs = True
else:
    st.warning("Ces valeurs ne renvoient pas de données. Veuillez modifier la dernière valeur sélectionnée.")
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
        df_list =  df_final.query("ville_deces == @ville_selected")
        # ➜ chaque ligne renvoie un cumul de personnes decedées
        df_map = (
            df_list #df_final.query("ville_deces == @ville_selected")
            .groupby(["ville_deces"], as_index=False)
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
        )

        hover_col = "ville_deces"
        size_col = "nb_deces"

        # ****** BarPlot *****
        df_bar = (
            df_list 
            .groupby(["ville_deces","origine_ville"], as_index=False)
            .agg(
                nb_deces=("nb_deces", "sum"),
            )
        )

        df_bar = df_bar.sort_values("nb_deces", ascending=ordre_tri).head(nb_energ)
        nom_secteur = "ville_deces"

        # ****** BarPlot2 *****
        df_bar_cl = (
            df_fnl.query("ville_deces == @ville_selected")
            .groupby(["ville_deces","classe_age","origine_ville"], as_index=False, observed=True)
            .agg(
                nb_deces=("idligne", "count"),
            )        
        )
        origine_secteur = "origine_ville"

        # ****** BarPlot3 *****   
        df_bar_month = (
            df_fnl.query("ville_deces == @ville_selected")
            .groupby(["month_deces","origine_ville"], as_index=False, observed=True)
            .agg(
                nb_deces=("idligne", "count"),
            )        
        )
        
        # ****** BarPlot Test *****   
        df_bar_month_cl = (
            df_final.query("ville_deces == @ville_selected")
            .groupby(["ville_deces","month_deces","classe_age",
                    "origine_ville"], as_index=False, observed=True)
            .agg(
                nb_deces=("nb_deces", "sum"),
            )        
        )

    elif departement_selected != "Tous les départements":
        # ➜ regroupement par ville
        df_list =  df_final.query("nom_departement_deces == @departement_selected")
        df_map = (
            df_list #df_final.query("nom_departement_deces == @departement_selected")
            .groupby(["nom_departement_deces", "ville_deces"])
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "ville_deces"
        size_col = "count"

        # ****** BarPlot *****
        df_bar = (
            df_list #df_final.query("nom_departement_deces == @departement_selected")
            .groupby(["ville_deces","origine_departement"], as_index=False)
            .agg(
                nb_deces=("nb_deces", "sum"),
                # nb_originaire_ville=("nb_originaire_ville", "sum"),
            )
        )

        df_bar = df_bar.sort_values("nb_deces", ascending=ordre_tri).head(nb_energ)
        nom_secteur = "ville_deces"

        # ****** BarPlot2 *****
        df_bar_cl = (
            df_fnl.query("nom_departement_deces == @departement_selected")
            .groupby(["ville_deces","classe_age","origine_departement"], as_index=False, observed=True)
            .agg(
                nb_deces=("idligne", "count"),
            )        
        )
        origine_secteur = "origine_departement"

        # ****** BarPlot3 *****   
        df_bar_month = (
            df_fnl.query("nom_departement_deces == @departement_selected")
            .groupby(["month_deces","origine_departement"], as_index=False, observed=True)
            .agg(
                nb_deces=("idligne", "count"),
            )        
        )
        
        # ****** BarPlot Test *****   
        df_bar_month_cl = (
            df_final.query("nom_departement_deces == @departement_selected")
            .groupby(["nom_departement_deces","month_deces","classe_age",
                    "origine_departement"], as_index=False, observed=True)
            .agg(
                nb_deces=("nb_deces", "sum"),
            )        
        )

    elif region_selected != "Toutes les régions":
        # ➜ regroupement par département
        df_list = df_final.query("nom_region_deces == @region_selected")
        df_map = (
            df_list # df_final.query("nom_region_deces == @region_selected")
            .groupby(["nom_region_deces", "nom_departement_deces"])
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "nom_departement_deces"
        size_col = "count"

        # ****** BarPlot *****
        df_bar = (
            df_list 
            .groupby(["nom_departement_deces","origine_departement"], as_index=False)
            .agg(
                nb_deces=("nb_deces", "sum"),
                #nb_originaire_departement=("nb_originaire_departement", "sum"),
            )
        )

        df_bar = df_bar.sort_values("nb_deces", ascending=ordre_tri).head(nb_energ)

        nom_secteur = "nom_departement_deces"

        # ****** BarPlot2 *****
        df_bar_cl = (
            df_fnl.query("nom_region_deces == @region_selected")
            .groupby(["nom_departement_deces","classe_age","origine_departement"], 
                    as_index=False, observed=True)
            .agg(
                nb_deces=("idligne", "count"),
            )        
        )
        origine_secteur = "origine_departement"

        # ****** BarPlot3 *****   
        df_bar_month = (
            df_fnl.query("nom_region_deces == @region_selected")
            .groupby(["month_deces","origine_departement"], as_index=False, observed=True)
            .agg(
                nb_deces=("idligne", "count"),
            )        
        )
        # ****** BarPlot Test *****   
        df_bar_month_cl = (
            df_final.query("nom_region_deces == @region_selected")
            .groupby(["nom_departement_deces","month_deces","classe_age",
                    "origine_departement"], as_index=False, observed=True)
            .agg(
                nb_deces=("nb_deces", "sum"),
            )        
        )

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
        df_bar = df_final.groupby(["nom_region_deces","origine_region"], as_index=False).agg(
            nb_deces=("nb_deces", "sum"),
            # nb_originaire_region=("nb_originaire_region", "sum"),
        )

        df_bar = df_bar.sort_values("nb_deces", ascending=ordre_tri).head(nb_energ)

        nom_secteur = "nom_region_deces"

        # ****** BarPlot2 *****   , observed=False ,"origine_region"
        df_bar_cl = (
            df_fnl
            .groupby(["nom_region_deces","classe_age","origine_region"], as_index=False, observed=True)
            .agg(
                nb_deces=("idligne", "count"),
            )        
        )
        origine_secteur = "origine_region"

        # ****** BarPlot3 *****   
        df_bar_month = (
            df_fnl
            .groupby(["month_deces","origine_region"], as_index=False, observed=True)
            .agg(
                nb_deces=("idligne", "count"),
            )        
        )
        
        # ****** BarPlot Test *****   
        df_bar_month_cl = (
            df_final
            .groupby(["nom_region_deces","month_deces","classe_age",
                    "origine_region"], as_index=False, observed=True)
            .agg(
                nb_deces=("nb_deces", "sum"),
            )        
        )

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
    st.sidebar.plotly_chart(fig, use_container_width=True)

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
    col1, col2 = st.columns(
        [3.2, 2.9]
    ) 

    la_fig, list_ordonnee_secteur_sans_dbl = graph_bar_origine(df_bar,
                                                            nom_secteur,
                                                            origine_secteur)
    
    with col1:
        with st.container(border=True):
            
            # Ouvre un pop-up
            with st.popover("ℹ️ À propos de ce graphique"):
                st.markdown("""
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
            tab11, = st.tabs(["📊 Originaire et Exogène"])

            with tab11:
                st.plotly_chart(la_fig, 
                        use_container_width=True,
                        key="Graphe_bar_origine")           

    with col2:
        with st.container(border=True):
           
            # Ouvre un pop-up
            with st.popover("ℹ️ À propos de ce graphique"):
                st.markdown("""
                    <div style="background-color: #ADD8E6;
                        padding:12px;
                        border-radius:8px;
                        border-left:4px solid #1f77b4; ">
                    Ce graphique représente :<br>
                    <b>-</b> les décès par classe d’âge <br>
                    <b>-</b> ventilés par origine secteur (région, département, ville) <br>
                    <b>-</b> agrégés par secteur <br>
                    </div>
                    """,
                    unsafe_allow_html=True,
                    )
                
            # Tabs ds Streamlit 
            tab1, tab2 = st.tabs(["📊 Originaire", "📈 Exogène"])

            with tab1:
                #st.subheader("Analyse – Graphe 1")
                # Affichage dans Streamlit  
                st.plotly_chart(graph_heat_map_origine(df_bar_cl,nom_secteur,origine_secteur, 
                                    list_ordonnee_secteur_sans_dbl,'O'),
                                    use_container_width=True,
                                    key="Clas_Age_Ori_O",
                            )
            
            with tab2:
                #st.subheader("Analyse – Graphe 2")
                # Affichage dans Streamlit  
                st.plotly_chart(graph_heat_map_origine(df_bar_cl,nom_secteur,origine_secteur, 
                                    list_ordonnee_secteur_sans_dbl,'N'),
                                    use_container_width=True,
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
    col3, col4 = st.columns(
        [2.9, 2.9]
    )   

    with col3:
        with st.container(border=True):
            st.plotly_chart(graph_bar_month(df_bar_month, origine_secteur), 
                use_container_width=True,
                key="bar_month",
            )

    with col4:
        with st.container(border=True):
            st.plotly_chart(graph_bar_class_age_month(df_bar_month_cl, origine_secteur), 
                use_container_width=True,
                key="bar_monthEtClAge",
            )
    
    # (Optionnel) Afficher le DataFrame filtré
    with st.expander("Voir les données filtrées du df_bar_month_cl "):
        # st.dataframe(df_final)
        st.dataframe(df_bar_month_cl)
    
    # with col3:
    #     with st.container(border=True):
    #         st.plotly_chart(graph_heat_map_origine(df_bar_cl,nom_secteur,origine_secteur, 
    #                             list_ordonnee_secteur_sans_dbl,'N'),
    #                             use_container_width=True,
    #                             key="Clas_Age_Ori_N",
    #                     )

    #with st.container(border=True):
    #    st.plotly_chart(graph_scatter_miroir(df_bar,nom_secteur,origine_secteur),
    #                use_container_width=True, 
    #                key="test2")

    # Création du bart chart 2
    #fig = px.bar(
    #    df_bar_cl,
    #    y="classe_age",
    #    x="nb_deces",
    #    color=origine_secteur,
    #    barmode="group",   # équivalent au hue de seaborn
    #    orientation="h",
    #    color_discrete_map={"N": "steelblue", "O": "darkorange"},
    #    category_orders={"origine": ["N", "O"]},
    #    # color_discrete_map={
    #    #     "nb_deces": "steelblue",  # "#1f77b4",   # bleu  "skyblue"
    #    #     "nb_originaire": "darkorange",  # "#ff7f0e",   # orange
    #    # },
    #)

    #fig.update_layout(
    #    title="Classe d'âge de la mortalité et origine",  # "Mortalité et originaires [total]",
    #    xaxis=dict(title="Mortalité tous secteurs"),
    #    yaxis=dict(title="Classe d'âge", side="left", showgrid=True),
    #    plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
    #    paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
    #    height=600,
    #    width=400,
    #)

