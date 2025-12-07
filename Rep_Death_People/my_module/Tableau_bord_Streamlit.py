import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from ClsTransformationDataPourViz import TransformationDataPourViz
import requests

from streamlit_plotly_events import plotly_events
import polars as pl
import numpy as np

# -------------------------------------------------------------------------------------

# --- Présentation / Titre / ---
st.set_page_config(page_title=r"Carte Régions / Départements / Villes", layout="wide")


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
    my_class = TransformationDataPourViz(
        path_racine=r"C:\Users\chokr\Data Projet\Death_People\Rep_Death_People"
    )

    df_person_nais_dece_departement_region = (
        my_class.ExtractionDataTableDeathPeopleView()
    )

    # my_class.creation_classe_age (df_person_nais_dece_departement_region)

    df = my_class.creation_classe_age(df_person_nais_dece_departement_region)

    # df_grp = df.groupby(['nom_region_deces','nom_departement_deces','ville_deces',
    # 'code_region_deces','code_departement_deces','age'],as_index = False).agg(
    # lat=("latitude_deces", "mean") ,
    # lon=("longitude_deces", "mean"),
    # nb_deces=("idligne", "count"),
    # nb_originaire_region=('origine_region', lambda x: (x == 'O').sum()),
    # nb_originaire_departement=('origine_departement', lambda x: (x == 'O').sum()),
    # nb_originaire_ville=('origine_ville', lambda x: (x == 'O').sum()),
    # distance_moy = ("distance", "mean"),
    # )
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
            ]
        ).agg(
            [
                pl.col("latitude_deces").mean().alias("lat"),
                pl.col("longitude_deces").mean().alias("lon"),
                pl.col("idligne").count().alias("nb_deces"),
                (pl.col("origine_region") == "O").sum().alias("nb_originaire_region"),
                (pl.col("origine_departement") == "O")
                .sum()
                .alias("nb_originaire_departement"),
                (pl.col("origine_ville") == "O").sum().alias("nb_originaire_ville"),
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

st.title("Mortalité et origine des populations en 2024")
# Widgets dans la sidebar
# st.sidebar.title("Menu")
st.sidebar.header("Filtres")

# st.dataframe(df_grp)

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

    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Sidebars
# -----------------------------

# --- Combobox Région ---
regions = ["Toutes les régions"] + sorted(df_grp["nom_region_deces"].unique().tolist())
region_selected = st.sidebar.selectbox("Choisis une région :", regions)

# --- Filtrage selon la région sélectionnée ---
if region_selected == "Toutes les régions":
    df_region = df_grp.copy()
else:
    df_region = df_grp[df_grp["nom_region_deces"] == region_selected]

# -------------------------------------------------------------------------------------
# ComboBox Département
departements = ["Tous les départements"] + sorted(
    df_region["nom_departement_deces"].unique().tolist()
)
departement_selected = st.sidebar.selectbox("Département :", departements)

# Filtrage selon le département
if departement_selected == "Tous les départements":
    df_dept = df_region.copy()
else:
    df_dept = df_region[df_region["nom_departement_deces"] == departement_selected]

# -------------------------------------------------------------------------------------

# ComboBox Ville
villes = ["Toutes les villes"] + sorted(df_dept["ville_deces"].unique().tolist())
ville_selected = st.sidebar.selectbox("Ville :", villes)

# Filtrage selon la ville
if ville_selected == "Toutes les villes":
    df_final_ = df_dept.copy()
else:
    df_final_ = df_dept[df_dept["ville_deces"] == ville_selected]

# Slider
start, end = st.sidebar.slider("Âge :", 0, 105, (20, 85))

# Extraction des personnes respectant le filtre sur l'age
df_final = df_final_[(df_final_["age"] >= start) & (df_final_["age"] <= end)]

if len(df_final) > 0:
    st.write(f"Nombre de lignes : {len(df_final):,}".replace(",", " "))
    restitution_des_valeurs = True
else:
    st.warning("Ces valeurs ne renvoient pas de données. Veuillez modifier la dernière valeur sélectionnée.")
    restitution_des_valeurs = False

# Je teste un select_slider 
# edges = np.arange(0, 105, 10)  # 0-10-20-30-40-50
# bins = [(edges[i], edges[i+1]) for i in range(len(edges)-1)]

# Sélecteur de bin
# selection = st.select_slider(
#     "Sélectionnez un bin ou une plage",
#     options=bins,
#     value=(bins[2], bins[7]),  # plage 10–20 → 30–40
#     format_func=lambda b: f"{b[0]}–{b[1]}"
# )
#print("bins",bins)
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
    nb_energ = 25
    ordre_tri = False
    precision = 3

    # === Préparation des données pour la carte ===
    if ville_selected != "Toutes les villes":
        # ➜ chaque ligne renvoie un cumul de personnes decedées
        df_map = (
            df_final.query("ville_deces == @ville_selected")
            .groupby(["ville_deces"], as_index=False)
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
        )

        hover_col = "ville_deces"
        size_col = "nb_deces"

        # ****** BarPlot *****
        df_bar = (
            df_final.query("ville_deces == @ville_selected")
            .groupby(["ville_deces"], as_index=False)
            .agg(
                nb_deces=("nb_deces", "sum"),
                nb_originaire_ville=("nb_originaire_ville", "sum"),
            )
        )

        df_bar["Tx_originaire"] = (
            df_bar["nb_originaire_ville"] / df_bar["nb_deces"] * 100
        )
        df_bar["Tx_originaire"] = df_bar["Tx_originaire"].round(precision)

        df_bar["Tx_deces"] = df_bar["nb_deces"] / df_bar["nb_deces"].sum() * 100
        df_bar["Tx_deces"] = df_bar["Tx_deces"].round(precision)

        # df_bar = df_bar.assign(
        # max_value=df_bar[["Tx_deces", "Tx_originaire"]].max(axis=1)
        # ).sort_values("max_value", ascending=False)

        df_bar = df_bar.sort_values("Tx_deces", ascending=ordre_tri).head(nb_energ)  #
        nom_secteur = "ville_deces"

        # ****** BarPlot2 *****
        df_bar_gnl = pd.DataFrame(
            {
                "categorie": ["nb_deces", "nb_originaire"],
                "valeur": [
                    df_bar["nb_deces"].sum(),
                    df_bar["nb_originaire_ville"].sum(),
                ],
            }
        )

    elif departement_selected != "Tous les départements":
        # ➜ regroupement par ville
        # df_map = df_final.groupby(["ville_deces", "lat", "lon"]).size().reset_index(name="count")
        df_map = (
            df_final.query("nom_departement_deces == @departement_selected")
            .groupby(["nom_departement_deces", "ville_deces"])
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "ville_deces"
        size_col = "count"

        # ****** BarPlot *****
        df_bar = (
            df_final.query("nom_departement_deces == @departement_selected")
            .groupby(["ville_deces"], as_index=False)
            .agg(
                nb_deces=("nb_deces", "sum"),
                nb_originaire_ville=("nb_originaire_ville", "sum"),
            )
        )
        df_bar["Tx_originaire"] = (
            df_bar["nb_originaire_ville"] / df_bar["nb_originaire_ville"].sum() * 100
        )
        df_bar["Tx_originaire"] = df_bar["Tx_originaire"].round(precision)

        df_bar["Tx_deces"] = df_bar["nb_deces"] / df_bar["nb_deces"].sum() * 100
        df_bar["Tx_deces"] = df_bar["Tx_deces"].round(precision)

        df_bar = df_bar.sort_values("Tx_deces", ascending=ordre_tri).head(nb_energ)
        nom_secteur = "ville_deces"

        # ****** BarPlot2 *****
        df_bar_gnl = pd.DataFrame(
            {
                "categorie": ["nb_deces", "nb_originaire"],
                "valeur": [
                    df_bar["nb_deces"].sum(),
                    df_bar["nb_originaire_ville"].sum(),
                ],
            }
        )

    elif region_selected != "Toutes les régions":
        # ➜ regroupement par département
        df_map = (
            df_final.query("nom_region_deces == @region_selected")
            .groupby(["nom_region_deces", "nom_departement_deces"])
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "nom_departement_deces"
        size_col = "count"

        # ****** BarPlot *****
        df_bar = (
            df_final.query("nom_region_deces == @region_selected")
            .groupby(["nom_departement_deces"], as_index=False)
            .agg(
                nb_deces=("nb_deces", "sum"),
                nb_originaire_departement=("nb_originaire_departement", "sum"),
            )
        )

        df_bar["Tx_originaire"] = (
            df_bar["nb_originaire_departement"]
            / df_bar["nb_originaire_departement"].sum()
            * 100
        )
        df_bar["Tx_originaire"] = df_bar["Tx_originaire"].round(precision)

        df_bar["Tx_deces"] = df_bar["nb_deces"] / df_bar["nb_deces"].sum() * 100
        df_bar["Tx_deces"] = df_bar["Tx_deces"].round(precision)

        df_bar = df_bar.sort_values("Tx_deces", ascending=ordre_tri)

        nom_secteur = "nom_departement_deces"

        # ****** Barplot2 *****
        df_bar_gnl = pd.DataFrame(
            {
                "categorie": ["nb_deces", "nb_originaire"],
                "valeur": [
                    df_bar["nb_deces"].sum(),
                    df_bar["nb_originaire_departement"].sum(),
                ],
            }
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
        df_bar = df_final.groupby(["nom_region_deces"], as_index=False).agg(
            nb_deces=("nb_deces", "sum"),
            nb_originaire_region=("nb_originaire_region", "sum"),
        )
        df_bar["Tx_originaire"] = (
            df_bar["nb_originaire_region"] / df_bar["nb_originaire_region"].sum() * 100
        )

        df_bar["Tx_deces"] = df_bar["nb_deces"] / df_bar["nb_deces"].sum() * 100
        df_bar["Tx_deces"] = df_bar["Tx_deces"].round(precision)

        df_bar["Tx_originaire"] = df_bar["Tx_originaire"].round(precision)

        df_bar = df_bar.sort_values("Tx_deces", ascending=ordre_tri)

        nom_secteur = "nom_region_deces"

        # ****** BarPlot2 *****
        df_bar_gnl = pd.DataFrame(
            {
                "categorie": ["nb_deces", "nb_originaire"],
                "valeur": [
                    df_bar["nb_deces"].sum(),
                    df_bar["nb_originaire_region"].sum(),
                ],
            }
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
    # -----------------------------  padding:15px; border-radius:10px; Voici un message avec une couleur de fond !

    # st.dataframe(df_final)
    # message_tx_mortality_origine = """Ce graphique présente, pour chaque département, deux indicateurs démographiques distincts :
    # le taux de personnes décédées (barplot) et le taux de personnes originaires du secteur (scatter).
    # Il permet d’observer simultanément la répartition géographique de la mortalité et de l'origine des populations :\n
    # 🔍 Si la courbe des originaires est supérieure à la mortalité,
    # alors la population est très ancrée sur ce secteur.\n
    # 📌 Pour toutes sociétés de produits financiers (assurances, banques..),
    # la stabilité et la longevité de ces populations sont des atouts commerciaux. """

    # st.write(message_tx_mortality_origine)
    # st.dataframe(df_bar)

    st.markdown(
        """
    <div style="background-color: #ADD8E6; ">
    Ce graphique permet d’observer simultanément la répartition géographique de la mortalité et l'origine des populations 
    pour chaque secteur à l'aide de ces indicateurs :\n
    📌 Le taux de mortalité\n
    📌 Le taux des personnes originaires\n
    Si le taux des originaires est supérieure à la mortalité, alors la population est très ancrée sur ce secteur.
    C'est un indicateur intéressant pour péreniser le business des sociétés de produits financiers (assurances, banques..).
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Preparation de l'alignement des graphes
    # Colonnes côte à côte
    # Mettre un espace entre le barplot et Pieplot
    col1, col2 = st.columns(
        [3.0, 1]
    )  # plus d’espace entre les colonnes avant  => st.columns(2)

    # fig = go.Figure()

    # Trace barplot
    # fig.add_trace(go.Bar(
    #    x=df_bar[nom_secteur],
    #    y=df_bar["Tx_deces"],
    #    name="Taux de mortalité",
    #    marker_color="skyblue",
    #    yaxis="y",
    #    #orientation="h",
    # ))

    # Trace line
    # fig.add_trace(go.Scatter(
    #    x=df_bar[nom_secteur],
    #    y=df_bar["taux_originaire"],
    #    name="Taux origine",
    #    mode="markers",#"lines+markers",
    #    marker=dict(color="darkorange"),
    #    line=dict(width=3),
    #    yaxis="y",
    # ))

    # *****

    fig = px.bar(
        df_bar,
        y=df_bar[nom_secteur],
        x=[
            df_bar["Tx_deces"],
            df_bar["Tx_originaire"],
        ],  # plusieurs colonnes → barplot groupé
        barmode="group",
        labels={"value": "Proportion (%)", "secteur": nom_secteur},
        title="Proportions par secteur",
        color_discrete_map={"Tx_deces": "steelblue", "Tx_originaire": "darkorange"},
        orientation="h",
    )

    # Mettre le fond transparent
    fig.update_layout(
        title="Relations entre mortalité et origine géographique des personnes ",
        xaxis=dict(title="Taux "),
        yaxis=dict(
            title="secteur",
            side="left",
            showgrid=True,
            categoryorder="array",
            categoryarray=df_bar[nom_secteur].tolist(),
        ),
        yaxis2=dict(
            # title="Taux origine",
            overlaying="y",
            side="right",
            showgrid=True,
        ),
        plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
        paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
        height=650,
        width=400,
    )
    # l’axe vertical est inversé, donc le premier élément du DataFrame se retrouve en haut,
    # et le dernier en bas ; il faut donc inverser :
    fig.update_yaxes(autorange="reversed")

    with col1:
        st.plotly_chart(fig, use_container_width=True)

    # Création du bart chart 2
    fig = px.bar(
        df_bar_gnl,
        x="categorie",
        y="valeur",
        color="categorie",
        color_discrete_map={
            "nb_deces": "steelblue",  # "#1f77b4",   # bleu  "skyblue"
            "nb_originaire": "darkorange",  # "#ff7f0e",   # orange
        },
        # color_discrete_sequence=px.colors.qualitative.Pastel
        # color_discrete_sequence=["#1E88E5", "#42A5F5"],
    )

    fig.update_layout(
        title="Données brutes tous secteurs ",  # "Mortalité et originaires [total]",
        xaxis=dict(title="Tous secteurs"),
        yaxis=dict(title="Mortalité", side="left", showgrid=True),
        plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
        paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
        height=380,
        width=400,
    )

    with col2:
        # Affichage dans Streamlit
        st.plotly_chart(fig, use_container_width=True)

    # (Optionnel) Afficher le DataFrame filtré
    with st.expander("Voir les données filtrées du df_bar"):
        # st.dataframe(df_final)
        st.dataframe(df_bar)

    with st.expander("Voir les données filtrées du df_final"):
        # st.dataframe(df_final)
        st.dataframe(df_final)
