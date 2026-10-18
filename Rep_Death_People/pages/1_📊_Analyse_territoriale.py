from pathlib import Path

#import plotly.express as px
import streamlit as st
import pandas as pd
from typing import List, Dict, Union, Tuple
import numpy as np

from analytics.statistics import (
    moyenne_ecart_type_national,
    recherche_dominant_sur_secteur,
    statistique_sur_secteur,
)

from components.filters import render_filters
from components.session import initialize_session
from components.styles import apply_styles

from services.data_loader import load_dataframe
from services.geojson_loader import load_geojsons

from components.sidebar import(
    afficher_sidebar_ph,
    afficher_sidebar_pb,
    CST_ORIGINE_VILLE,
    CST_ORIGINE_VILLE,
    CST_ORIGINE_DEPARTEMENT,
    CST_ORIGINE_REGION,
    CST_ORIGINE_NATIONAL,
)
#from components.sidebar import afficher_sidebar_pb

#from utils.filters import appliquer_filtres

from my_module.graphs.graph_secteur_score import (
    ClsGraphScore as graph_score,
)

from my_module.graphs.graph_secteur_score_age import (
    ClsGraphScoreAge as graph_score_age,
)

from pathlib import Path


# Chemin relatif pour la recupération des images .svg
BASE_DIR = Path(__file__).resolve().parent.parent

image_path_men = BASE_DIR / "assets" / "men.svg"
image_path_women = BASE_DIR / "assets" / "women.svg"
image_path_carte = BASE_DIR / "assets" / "France.svg"
image_path_pourcentage = BASE_DIR / "assets" / "Pourcentage.svg"
image_path_pourcentage_EXO = BASE_DIR / "assets" / "Pourcentage_EXO.svg"
image_path_paysage = BASE_DIR / "assets" / "Paysage.svg"

def quel_est_le_message_info(ind_tafv: str, ind_imd: str, la_classe: graph_score ):
    '''
    Permet de comprendre comment typé le secteur, quelles sont les caractéristiques
    Args :
        tafv dans le secteur
        imd  dans le secteur
    Return :
        restitue un libelle issu d'un calcul   
    '''
    # Il faut convertir :
    ind_tafv = float(ind_tafv)
    ind_imd = float(ind_imd)

    if ind_tafv < 0.5:    # SECTEUR PLUTOT EXOGENE
        if ind_tafv < 0.3:
            pre_libelle = "Secteur très attractif "
        else:
            pre_libelle = "Secteur majoritairement exogène "

        if ind_imd < 0:   # SECTEUR EXOGENE-MOBILE
            lib = pre_libelle +" disposant d'une mobilité plus forte que la moyenne nationale"
        elif ind_imd == 0:# SECTEUR EXOGENE-NEUTRE
            lib = pre_libelle + " disposant d'une mobilité comparable à la moyenne nationale"
        else:             # SECTEUR EXOGENE-STATIQUE
            lib = pre_libelle +" disposant d'une inertie plus forte que la moyenne nationale"
    #---------------------------------------------------
    elif ind_tafv == 0.5:    # SECTEUR EQUILIBRE
        if ind_imd < 0:   # SECTEUR EQUILIBRE-MOBILE
            lib = "Secteur équilibré avec une mobilité plus forte que la moyenne nationale"
        elif ind_imd == 0:# SECTEUR EQUILIBRE-NEUTRE
            lib = "Secteur équilibré avec une mobilité comparable à la moyenne nationale"
        else:             # SECTEUR EQUILIBRE-STATIQUE
            lib = "Secteur équilibré avec une inertie plus forte que la moyenne nationale"
    #---------------------------------------------------
    else:                 # SECTEUR PLUTOT ORIGINAIRE # ind_tafv > 0.5
        if ind_tafv > 0.8:
            pre_libelle = "Secteur avec beaucoup d'originaires (ancrage)"
        else:
            pre_libelle = "Secteur d'originaires"

        if ind_imd < 0:   # SECTEUR ORIGINAIRE-MOBILE
            lib = pre_libelle +" disposant d'une mobilité plus forte que la moyenne nationale"
        elif ind_imd == 0:# SECTEUR ORIGINAIRE-NEUTRE
            lib = pre_libelle +" disposant d'une mobilité comparable à la moyenne nationale"
        else:             # SECTEUR ORIGINAIRE-STATIQUE
            lib = pre_libelle +" disposant d'une inertie plus forte que la moyenne nationale"
    #---------------------------------------------------
    return lib

def secteur_selectionne(s_origine: str = CST_ORIGINE_VILLE):
    '''
    Permet d'identifier le niveau du secteur sélectionné
    Args :
        origine selectionnée
    Return :
        renvoie le type de secteur, le niveau hérarchique
    '''
    if s_origine == CST_ORIGINE_VILLE:
        le_secteur = "commune"
    elif s_origine == CST_ORIGINE_REGION:
        le_secteur ="régional"
    elif s_origine == CST_ORIGINE_DEPARTEMENT:
        le_secteur ="départemental"
    else:
        le_secteur ="national"

    return le_secteur

# ==========================
# FUNCTION 
# ==========================

@st.cache_data
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
    les_proportions_age_EXO = (
        df_fnl_m[df_fnl_m[cette_origine_secteur] == "N"]["classe_age"]
        .value_counts(normalize=True)
        .mul(100)
        .round(3)
        .reset_index(name="pourcentage")
    )

    les_proportions_age = (
        df_fnl_m["classe_age"]
        .value_counts(normalize=True)
        .mul(100)
        .round(3)
        .reset_index(name="pourcentage")
    )

    # ordonne
    les_proportions_age["classe_age"] = les_proportions_age["classe_age"].cat.reorder_categories(
        ordre,
        ordered=True
    )

    les_proportions_age = les_proportions_age.sort_values("classe_age")

    # ordonne ATFV
    les_proportions_age_EXO["classe_age"] = les_proportions_age_EXO["classe_age"].cat.reorder_categories(
        ordre,
        ordered=True
    )

    les_proportions_age_EXO = les_proportions_age_EXO.sort_values("classe_age")
    
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
        les_proportions_age_EXO,
    )
# ==========================
# PGM  
# ==========================

initialize_session()
apply_styles()

# ==========================
# CHARGEMENT DES DONNEES
# ==========================
# Recupération des datas provenant de la Bdd
st.write("🔄 Étape 2.1 : Tentative de chargement des données...")
df_grp, df, nb_deces_hors_france,nb_total_selection  = load_dataframe()
st.write("🔄 Étape 2.2 : Chargement des données...")
geojson_regions, geojson_departements = load_geojsons()

# ==========================
# SIDEBAR PARTIE HAUTE
# ==========================

filtres = afficher_sidebar_ph(
    df_grp=df_grp,
    df=df
)

# ==========================
# APPLICATION DES FILTRES
# ==========================

region_selected = filtres["region"]
departement_selected = filtres["departement"]
ville_selected = filtres["ville"]
start = filtres["age_min"]
end = filtres["age_max"]
origine_secteur = filtres["origine_secteur"]
nom_secteur = filtres["nom_secteur"]
choix_genre = filtres["genre"]
df_map = filtres["df_map"]
hover_col = filtres["hover_col"]
size_col = filtres["size_col"]
geojson_filtered = filtres["geojson_filtered"]
center_lat = filtres["center_lat"] 
center_lon = filtres["center_lon"] 
df_fnl_m =filtres['df_fnl_m']
niveau_hierarchique = filtres['niveau_hierarchique']

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
    ind_tafv,
    distance_lng_sum_prc,
    moy_age_o,
    moy_age_e,
    dis_med_o,
    dis_med_e,
    les_proportions_age,
    les_proportions_EXO_age,
) = statistique_sur_secteur(df_fnl_m, nom_secteur, origine_secteur)

if current_filters != st.session_state.filters or "filters" not in st.session_state:

    st.session_state.filters = current_filters

    st.session_state.page = 0

    # Ici tu refais les calculs nécessaires
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
            st.proportion_nat_age = les_proportions_age.copy(True)
            st.proportion_EXO_nat_age = les_proportions_EXO_age.copy(True)
            st.session_state.genre = choix_genre

# ==========================
# SIDEBAR PARTIE BASSE
# ==========================

# -------------------------------------------------------------------------------------
    # PAGINATION

ce_graph_TAFV = graph_score(df_fnl_m, nom_secteur, origine_secteur,
                            st.session_state.ecart_type_national.loc[0],
                            st.session_state.moyenne_nationale.loc[0])


afficher_sidebar_pb(
    df_grp=df_grp,
    df=df,
    df_map = df_map,
    origine_secteur=origine_secteur,
    ce_graph_TAFV=ce_graph_TAFV,
    hover_col = hover_col,
    size_col = size_col,
    region_selected = region_selected,
    geojson_filtered = geojson_filtered,
    center_lat = center_lat,
    center_lon = center_lon,
) 

# -----------------------------
# L'ECRAN 
# -----------------------------

# --- Tabulations  ---
(tabMain,tabKpi, tabGraphAnalyse) = st.tabs(["🎯 Intro", "📍Vue globale", "🔍 Détail sectoriel"])

# -----------------------------
# TAB 1
# -----------------------------
with tabMain:
    with st.container(border=True):
        st.subheader("Objectifs et KPI :")
        col_obj,col_description_kpi = st.columns([5.4,4.2], vertical_alignment="top")
        with col_obj:
            with st.container(border=True):
                st.markdown(
                """
                <div style="background-color: #ADD8E6; ">
                🎯 L'objectif de cette présentation consiste à distinguer deux types de territoires : ceux qui gagnent des seniors (secteur attractif)
                de ceux qui y restent pour leur vie (secteur d'ancrage).<br>
                
                Cette information est importante pour l'organisation et la gestion du territoire, la production de services des 
                populations ciblées (protection sociale, attentes économiques, spécificités culturelles..).<br> 
                <br>
                ➡️ Les territoires avec beaucoup de seniors indiquent potentiellement : <br>
                <b>-</b> Des transferts d’épargne et d’immobilier <br>
                <b>-</b> Une activation future de contrats d’assurance-vie <br>
                <br>
                ➡️ Les territoires avec beaucoup d'exogènes séniors soulignent : <br>
                <b>-</b> La présence d'infrastructure medicales, d'hepad <br>
                <b>-</b> Une meilleure qualité de vie <br>
                <br>
                ➡️ Les territoires avec beaucoup d'originaires séniors soulignent : <br>
                <b>-</b> Un fort attachement à son territoire, une identité très marquée qui peut 
                s'appuyer sur des habitudes culturelles ou de consommation <br>
                <br>
                ➡️ La temporalité des decès permet d'identifier : <br>
                <b>-</b> Les spécificités des trajectoires de vie 
                <br>
                </div>                
                """,
                unsafe_allow_html=True,
            )
                
        with col_description_kpi:
            with st.container(border=True):
                
                st.write(":material/expand_circle_down: Taux d'ancrage de fin de vie")
                st.markdown(
                    """
                    <div style="background-color: #ADD8E6; ">
                    📌 Le taux d'ancrage de fin de vie (TAFV) mesure la capacité d'un secteur à garder,
                        au moment du décès, des personnes qui y sont nées. \n 
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
                            <b>-</b> TAFV < 0.5 le secteur est très attractif en fin de vie pour les exogènes. Cela peut 
                            refléter la présence d'hôpitaux, d'EHPAD ou de zones de retraite résidentielle.<br>
                            <b>-</b> TAFV > 0.5 les décès sont majoritairement locaux (fort ancrage territorial).
                            Cela correspond à une faible mobilité residentielle soulignant une forte identité culturelle.<br>
                        </div>              
                        """,
                        unsafe_allow_html=True,
                )
                st.write(":material/expand_circle_down: Indice de mobilité différentielle")
                st.markdown(
                    """
                    <div style="background-color: #ADD8E6; ">
                    📌 L’indice de mobilité différentielle (IMD) permet de répondre à cette question :
                    Ce territoire est-il plus ou moins mobile que la moyenne nationale ?\n     
                    <b></b>\n                    
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
                        <b>-</b> IMD < 0 Ce territoire accueille ou est associé à des populations dont 
                        les trajectoires de vie sont significativement plus mobiles que la moyenne nationale.
                        Cela correspond à des territoires de circulation.<br>
                        <b>-</b> IMD = 0 Le secteur a une mobilité identique à la moyenne du pays. <br>
                        <b>-</b> IMD > 0 le secteur a une mobilité plus faible que la moyenne nationale.
                        Cela peut refléter des territoires d'ancrage. <br>
                    </div>                
                    """,
                        unsafe_allow_html=True,
                    )
                with st.container(border=True):
                    nb_deces_hors_france_presentation =f"{nb_deces_hors_france:,}".replace(",", " ").replace(".0", " ")
                    proportion_hf = round((nb_deces_hors_france)*100/ nb_total_selection,0)    
                    st.write(f"""
                            :material/info: Les décès à l'étranger sont exclus de cette analyse :
                            - Nombre     : {nb_deces_hors_france_presentation}
                            - Proportion : {proportion_hf} %
                            """)

with tabKpi:
    if origine_secteur == "origine_nationale":
        sous_titre_indicateur_personne = "Portrait moyen du défunt en France"
        sous_titre_indicateur_secteur = "Indicateurs nationaux"
    else:
        sous_titre_indicateur_personne = "Portrait moyen du défunt sur ce secteur"
        sous_titre_indicateur_secteur = "Indicateurs sectoriels"


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
        ) = st.columns([1.4, 1.0, 2.6, 2.9, 2.9, 1, 1.3])

        (
            age_moyen,
            serie_sex,
            serie_prenom,
            serie_lieu_naissance,
            serie_lieu_deces,
            origine_dominante,
            distance_moy,
        ) = recherche_dominant_sur_secteur(df_fnl_m, nom_secteur, origine_secteur)

        col_age.metric("Âge moy.", f"{age_moyen} ans", border=True)
        # Affichage des icônes SVG dans la colonne `col_sex`
        with col_sex:
            sex = "homme"  

            if serie_sex[0] == "H":
                st.image(image_path_men, width=160 )  # Affichage de l'icône homme 
            else:
                st.image(image_path_women, width=160)  # Affichage de l'icône femme

        col_pren.metric("Prénom dominant", serie_prenom[0], border=True )
        col_lieu_nai.metric(
            "Secteur de naissance dominant", serie_lieu_naissance[0],
            border=True
        )
        col_lieu_dec.metric("Secteur de décès dominant", serie_lieu_deces[0], border=True)
        col_origine.metric("Originaire", origine_dominante, border=True)
        col_dist.metric("Distance moy.*", distance_moy, border=True)

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

        ce_graph_proportion_age = graph_score_age(df_fnl_m, nom_secteur, origine_secteur)
        
        fig_pourcentage = ce_graph_proportion_age.render_graph_pourcentage_age(les_proportions_age,
        st.proportion_nat_age)

        ce_graph_proportion_EXO_age = graph_score_age(df_fnl_m, nom_secteur, origine_secteur)
        
        fig_pourcentage_EXO = ce_graph_proportion_EXO_age.render_graph_pourcentage_age(les_proportions_EXO_age,
        st.proportion_EXO_nat_age,"exogene")            

        with st.container(border=True):
            st.subheader("Pourcentage des défunts par classes d'âge")
            with st.container(border=True):
                col_img,col_graphe = st.columns([0.6,4.2], vertical_alignment="center")
                with col_img:
                    st.image(image_path_pourcentage, width=260 ) 
                
                with col_graphe:
                    st.plotly_chart(
                    fig_pourcentage, width='stretch', key="Graphe_score_pourcentage"
                    )
            
            with st.container(border=True):
                col_img,col_graphe = st.columns([0.6,4.2], vertical_alignment="center")
                with col_img:
                    st.image(image_path_pourcentage_EXO, width=260 ) 
                
                with col_graphe:
                    st.plotly_chart(
                    fig_pourcentage_EXO, width='stretch', key="Graphe_score_pourcentage_exo"
                    )

with tabGraphAnalyse:                                
    # RESTITUTION DES GRAPHES
    # instanciation faite précedemment
    fig_score, message_score, df_score = ce_graph_TAFV.render_graph_score(
        page=st.session_state.page
    )
    # Instancie la classe
    ce_graph_TAFV_age = graph_score_age(df_fnl_m, nom_secteur, origine_secteur,)

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
        st.session_state.moyenne_nationale.loc[0], #  
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
        True, page=st.session_state.page, indicateur="mobilite_secteur"
    )

    fig_score_IMD_age_inertie = ce_graph_IMD_age.render_graph_score_age_IMD(
        False, page=st.session_state.page, indicateur="mobilite_secteur"
    )

    with st.container(border=True):
                
        col_ind_tafv, col_ind_imd, col_message = st.columns(
            [0.5, 0.5,5.0])  #
        
        col_ind_tafv.metric("TAFV " + secteur_selectionne(niveau_hierarchique).upper()[:3]+".",
                            ind_tafv,border=True )  
        col_ind_imd.metric("IMD", ind_imd,border=True )

        with col_message:
            le_message = quel_est_le_message_info(ind_tafv,ind_imd,ce_graph_IMD)
            st.info(le_message)
            if origine_secteur !=CST_ORIGINE_VILLE:
                st.write(f"""
                            :material/info: Les graphes TAFV et IMD se ressemblent ; les sous secteurs occupent des
                            rangs similaires mais il y a quelques différences. Elles révèlent des sous secteurs comportant 
                            des différences notables : 
                            soit une mobilité inter sectorielle très forte (métropole, villégiature..), 
                            soit une mobilité intra sectorielle élevée (enclave, ile).
                        """)
            
        with st.container(border=True):
            # Préparation de l'alignement des graphes 
            col_TAFV, col_separateur, col_IMD = st.columns([4.9, 0.6, 4.7])
            with col_TAFV:

                with st.container(border=True):

                    # Tabs ds Streamlit
                    if not origine_secteur == CST_ORIGINE_VILLE:#"origine_ville":
                        # Ouvre un pop-up
                        with st.popover("ℹ️ À propos de ce graphique"):
                            st.markdown(
                                """
                                <div style="background-color: #ADD8E6;
                                    padding:12px;
                                    border-radius:8px;
                                    border-left:4px solid #1f77b4; ">
                                Le taux d'ancrage découpe le graphe en 2 zones : <br>
                                <b>-</b> Zone à forte présence d'exogènes dans ce secteur <br>
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
                                    <b>-</b> Zone à forte mobilité par rapport à la moyenne nationale <br>
                                    <b>-</b> Zone à forte inertie par rapport à la moyenne nationale <br>
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

            col_age_TAFV, col_separateur, col_age_IMD = st.columns(
                [4.9, 0.6, 4.7]
            )

            with col_age_TAFV:
                with st.container(border=True):
                    # Ouvre un pop-up
                    with st.popover("ℹ️ À propos de ces graphiques"):
                        if not origine_secteur == CST_ORIGINE_VILLE :#"origine_ville":
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
                                <b>-</b> La meilleure classe d'age et le meilleur secteur sont mis en surbrillance.
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
                    if not origine_secteur == CST_ORIGINE_VILLE:#"origine_ville":
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
                                        Ces deux visualisations présentent le top 5 des meilleurs IMD pour la mobilité et l'inertie
                                        des territoires.<br>
                                        <b>-</b> Les 5 meilleures cellules apparaissent avec un cadre noir. 
                                        Leur rang est spécifié dans l'encadré.<br>
                                        <b>-</b> La meilleure classe d'age et le meilleur secteur sont mis en surbrillance.
                                        </div>
                                        """,
                                        unsafe_allow_html=True,
                                    )
                    if not origine_secteur == CST_ORIGINE_VILLE:#"origine_ville":
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
                    if not origine_secteur == CST_ORIGINE_VILLE:#"origine_ville":
                        with tab12I_: #
                            st.plotly_chart(
                                fig_score_IMD_age_inertie,
                                width="stretch",
                                key="Graphe_age_IMD_i",
                            )
