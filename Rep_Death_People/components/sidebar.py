import streamlit as st
import pandas as pd
import plotly.express as px
#from graph import CeGraphTAFV

from my_module.graphs.graph_secteur_score import (  
    ClsGraphScore as graph_score,
)

from services.geojson_loader import load_geojsons

# CONSTANTE
CST_ORIGINE_VILLE = "origine_ville"
CST_ORIGINE_DEPARTEMENT = "origine_departement"
CST_ORIGINE_REGION = "origine_region"
CST_ORIGINE_NATIONAL = "origine_nationale"
agglomeration = False



def afficher_sidebar_ph(
    df_grp: pd.DataFrame,
    df: pd.DataFrame,
):
    
    # Affiche la sidebar et retourne les choix utilisateur.    

    # Utilisation de la bibliothèque Google Material Symbols
    st.sidebar.header(":material/filter_alt: Filtres") 

    # ==========================
    # REGION
    # ==========================

    regions = ["Toutes les régions"] + sorted(df_grp["nom_region_deces"].unique().tolist())
    region_selected = st.sidebar.selectbox(":material/distance: Région :", regions)

    # --- Filtrage selon la région sélectionnée ---
    if region_selected == "Toutes les régions":
        df_region = df_grp.copy()
        df_rgn = df.copy()
    else:
        df_region = df_grp[df_grp["nom_region_deces"] == region_selected]
        df_rgn = df[df["nom_region_deces"] == region_selected]


# --- Filtrage selon la région sélectionnée ---
    if region_selected == "Toutes les régions":
        df_region = df_grp.copy()
        df_rgn = df.copy()
    else:
        df_region = df_grp[df_grp["nom_region_deces"] == region_selected]
        df_rgn = df[df["nom_region_deces"] == region_selected]

    # ==========================
    # DEPARTEMENT
    # ==========================
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

    # ==========================
    # VILLE
    # ==========================
    
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

    # ==========================
    # AGGLOMERATION
    # ==========================

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
        agglomeration = st.sidebar.checkbox(label=le_libelle_agg, disabled=est_ville_standard, 
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
        

    # ==========================
    # GENRE
    # ==========================

    with st.sidebar:
        # illustration :
        CST_GENRE = ":material/person: Genre :"
        CST_HOMME_FEMME = ":material/wc: "
        CST_HOMME = ":material/man: "
        CST_FEMME = ":material/woman: "
        
        choix_genre = st.radio(
            CST_GENRE,
            [CST_HOMME_FEMME, CST_HOMME, CST_FEMME],
            horizontal=True,
            disabled=(departement_selected != "Tous les départements")
            | (region_selected != "Toutes les régions"),
            help="Déselectionner le département et/ou la région pour activer les boutons",
            captions=[
            "Tous ",
            "Hom.",
            "Fem.",
            ],
        ) 
        

        if choix_genre == CST_HOMME:
            df_final_f = df_final_.query("sex=='1'")
            df_fnl_f = df_fnl_.query("sex=='1'")            
        elif choix_genre == CST_FEMME:
            df_final_f = df_final_.query("sex== '2'")
            df_fnl_f = df_fnl_.query("sex== '2'")             
        else:
            df_final_f = df_final_.copy()
            df_fnl_f = df_fnl_.copy()             

    # ==========================
    # AGE
    # ==========================

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

    # Test si presence de valeurs apres selection :
    if len(df_final) > 0:
        valeur = df_final["nb_deces"].sum()
        if valeur > 50:
            
            # creation de container avec colonne * 2 
            with st.sidebar.container(
                border=False,
                height=70,
            ):
                st.info(f"Sélection décès : {valeur:,}".replace(",", " "))
                    
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

    # Récupération des regions et départements
    geojson_regions, geojson_departements = load_geojsons()

    # === Détermination du centre de la carte ===
    if not df_final_f.empty:
        center_lat = df_final_f["lat"].mean()
        center_lon = df_final_f["lon"].mean()
    else:
        center_lat, center_lon = 46.6, 2.5  # centre de la France


    if region_selected == "Toutes les régions":
        geojson_filtered = geojson_regions  # toutes les régions
    else:
        # Filtrer les départements correspondant à la région
        deps = (
            df_final_f[df_final_f["nom_region_deces"] == region_selected][
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
        # pour les filtres
        origine_secteur = CST_ORIGINE_VILLE #

        # pour savoir à quel niveau hierarchique
        niveau_hierarchique = CST_ORIGINE_VILLE

    elif departement_selected != "Tous les départements":
        # conserver ces valeurs :
        # ➜ regroupement par ville
        df_map = (
            df_final.query("nom_departement_deces == @departement_selected").groupby(["nom_departement_deces", "ville_deces"])
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "ville_deces"
        size_col = "count"

        nom_secteur = "ville_deces"

        origine_secteur = CST_ORIGINE_DEPARTEMENT 

        # pour savoir à quel niveau hierarchique
        niveau_hierarchique = CST_ORIGINE_DEPARTEMENT

        df_fnl_m = df_fnl.query("nom_departement_deces == @departement_selected")

    elif region_selected != "Toutes les régions":
        # ➜ regroupement par département
        df_map = (
            df_final.query("nom_region_deces == @region_selected").groupby(["nom_region_deces", "nom_departement_deces"])
            .agg({"lat": "mean", "lon": "mean", "nb_deces": "sum"})
            .reset_index()
            .rename(columns={"nb_deces": "count"})
        )
        hover_col = "nom_departement_deces"
        size_col = "count"

        nom_secteur = "nom_departement_deces"

        origine_secteur = CST_ORIGINE_DEPARTEMENT#"origine_departement"
        # pour savoir à quel niveau hierarchique
        niveau_hierarchique = CST_ORIGINE_REGION

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
        # pour savoir à quel niveau hierarchique
        niveau_hierarchique = CST_ORIGINE_NATIONAL

        # ****** Metrics *******
        df_fnl_m = df_fnl

    # ==========================
    # RETOUR DES FILTRES
    # ==========================

    return {
    "region": region_selected,
    "departement": departement_selected,
    "ville": ville_selected,
    "agglomeration": agglomeration,
    "nom_agglomeration": cette_agglomeration,
    "genre": choix_genre,
    "age_min": start,
    "age_max": end,
    "nom_secteur":nom_secteur,
    "origine_secteur":origine_secteur,
    "niveau_hierarchique" :niveau_hierarchique,
    "df_map":df_map,
    "hover_col":hover_col,
    "size_col":size_col,
    "geojson_filtered":geojson_filtered,
    "center_lat": center_lat,
    "center_lon": center_lon,
    "df_fnl_m": df_fnl_m,
    }
 
def afficher_sidebar_pb(
        df_grp: pd.DataFrame,
        df: pd.DataFrame,
        df_map: pd.DataFrame,
        origine_secteur: str,
        ce_graph_TAFV: graph_score,
        hover_col: str,
        size_col: str,
        region_selected: str,
        geojson_filtered,
        center_lat,
        center_lon,
    ):
    
    # -------------------------------------------------------------------------------------
    # PAGINATION

    # je fais apparaitre uniquement dans une vue nat, region ou departement
    if origine_secteur != CST_ORIGINE_VILLE :

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
                        use_container_width='stretch',
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
                    use_container_width='stretch',
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