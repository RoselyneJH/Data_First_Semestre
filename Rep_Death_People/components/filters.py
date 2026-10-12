import pandas as pd
import streamlit as st


def render_filters(
    df_grp: pd.DataFrame,
    df: pd.DataFrame,
):

    st.sidebar.header("Filtres")

    # -------------------------
    # Région
    # -------------------------

    regions = (
        ["Toutes les régions"]
        + sorted(
            df_grp["nom_region_deces"]
            .unique()
            .tolist()
        )
    )

    region_selected = st.sidebar.selectbox(
        "Région :",
        regions,
    )

    if region_selected == "Toutes les régions":

        df_region = df_grp.copy()
        df_rgn = df.copy()

    else:

        df_region = df_grp[
            df_grp["nom_region_deces"]
            == region_selected
        ]

        df_rgn = df[
            df["nom_region_deces"]
            == region_selected
        ]

    # -------------------------
    # Département
    # -------------------------

    departements = (
        ["Tous les départements"]
        + sorted(
            df_region["nom_departement_deces"]
            .unique()
            .tolist()
        )
    )

    departement_selected = st.sidebar.selectbox(
        "Département :",
        departements,
    )

    if departement_selected == "Tous les départements":

        df_dept = df_region.copy()
        df_dpt = df_rgn.copy()

    else:

        df_dept = df_region[
            df_region["nom_departement_deces"]
            == departement_selected
        ]

        df_dpt = df_rgn[
            df_rgn["nom_departement_deces"]
            == departement_selected
        ]

    # -------------------------
    # Ville
    # -------------------------

    cette_agglomeration = ""
    agglomeration = False
    est_ville_standard = True

    with st.sidebar:

        villes = (
            ["Toutes les villes"]
            + sorted(
                df_dept["ville_deces"]
                .unique()
                .tolist()
            )
        )

        ville_selected = st.selectbox(
            "Ville / Arrondissement :",
            villes,
            disabled=(
                departement_selected
                == "Tous les départements"
            ),
        )

        if (
            ville_selected.startswith("PARIS")
            or ville_selected.startswith("MARSEILLE")
            or ville_selected.startswith("LYON")
        ):

            est_ville_standard = False

            cette_agglomeration = (
                ville_selected.split()[0]
            )

            le_libelle_agg = (
                "Agglomération de "
                + cette_agglomeration
            )

        if (
            not est_ville_standard
            and ville_selected != "Toutes les villes"
        ):

            agglomeration = st.checkbox(
                le_libelle_agg
            )

    # -------------------------
    # Filtrage Ville
    # -------------------------

    if ville_selected == "Toutes les villes":

        df_final_ = df_dept.copy()
        df_fnl_ = df_dpt.copy()

    elif agglomeration:

        df_final_ = df_dept[
            df_dept["ville_deces"]
            .str.contains(cette_agglomeration)
        ]

        df_fnl_ = df_dpt[
            df_dpt["ville_deces"]
            .str.contains(cette_agglomeration)
        ]

    else:

        df_final_ = df_dept[
            df_dept["ville_deces"]
            == ville_selected
        ]

        df_fnl_ = df_dpt[
            df_dpt["ville_deces"]
            == ville_selected
        ]

    # -------------------------
    # Genre
    # -------------------------

    st.sidebar.write("-------------------------")

    choix_genre = st.sidebar.radio(
        "Genre :",
        ["Tout", "H", "F"],
        horizontal=True,
        disabled=(
            departement_selected
            != "Tous les départements"
            or region_selected
            != "Toutes les régions"
        ),
    )

    if choix_genre == "H":

        df_final_f = df_final_.query(
            "sex == '1'"
        )

        df_fnl_f = df_fnl_.query(
            "sex == '1'"
        )

    elif choix_genre == "F":

        df_final_f = df_final_.query(
            "sex == '2'"
        )

        df_fnl_f = df_fnl_.query(
            "sex == '2'"
        )

    else:

        df_final_f = df_final_.copy()
        df_fnl_f = df_fnl_.copy()

    # -------------------------
    # Âge
    # -------------------------

    start, end = st.sidebar.slider(
        "Âge :",
        0,
        105,
        (20, 85),
        disabled=(
            departement_selected
            != "Tous les départements"
            or region_selected
            != "Toutes les régions"
        ),
    )

    df_final = df_final_f[
        (df_final_f["age"] >= start)
        & (df_final_f["age"] <= end)
    ]

    df_fnl = df_fnl_f[
        (df_fnl_f["age"] >= start)
        & (df_fnl_f["age"] <= end)
    ]

    return {
        "df_final": df_final,
        "df_fnl": df_fnl,
        "region_selected": region_selected,
        "departement_selected": departement_selected,
        "ville_selected": ville_selected,
        "choix_genre": choix_genre,
        "start": start,
        "end": end,
        "agglomeration": agglomeration,
        "cette_agglomeration": cette_agglomeration,
    }