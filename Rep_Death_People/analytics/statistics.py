from typing import Tuple

import numpy as np
import pandas as pd
import polars as pl


def moyenne_ecart_type_national(
    df_fnl: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
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
        .select(
            [
                ((pl.col("distance") + 1).log())
                .mean()
                .alias("moy_distance"),

                ((pl.col("distance") + 1).log())
                .std()
                .alias("std_distance"),

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
        .group_by("classe_age")
        .agg(
            [
                ((pl.col("distance") + 1).log())
                .mean()
                .alias("moy_distance"),

                ((pl.col("distance") + 1).log())
                .std()
                .alias("std_distance"),
            ]
        )
        .collect()
    )

    return (
        df_polars.to_pandas(),
        df_polars_age.to_pandas(),
    )


def recherche_dominant_sur_secteur(
    df_fnl_m: pd.DataFrame,
    ce_secteur: str,
    cette_origine_secteur: str,
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

    age_moyen = str(
        round(df_fnl_m["age"].mean(), 0)
    ).replace(".0", "")

    serie_sex = "H"

    if df_fnl_m["sex"].mode()[0] == "2":
        serie_sex = "F"

        serie_prenom = (
            df_fnl_m[
                df_fnl_m["sex"] == "2"
            ]["prenom"].mode()
        )

    else:
        serie_prenom = (
            df_fnl_m[
                df_fnl_m["sex"] == "1"
            ]["prenom"].mode()
        )

    ce_secteur_naissance = ce_secteur.replace(
        "deces",
        "naissance",
    )

    serie_lieu_naissance = (
        df_fnl_m[ce_secteur_naissance].mode()
    )

    serie_lieu_deces = (
        df_fnl_m[ce_secteur].mode()
    )

    nb_originaire = (
        df_fnl_m[
            df_fnl_m[cette_origine_secteur] == "O"
        ][cette_origine_secteur]
        .count()
    )

    nb_non_originaire = (
        df_fnl_m[
            df_fnl_m[cette_origine_secteur] == "N"
        ][cette_origine_secteur]
        .count()
    )

    origine_dominante = "N"

    if nb_originaire >= nb_non_originaire:
        origine_dominante = "O"

    distance_moy = str(
        round(
            df_fnl_m[
                df_fnl_m[cette_origine_secteur]
                == origine_dominante
            ]["distance"].mean(),
            0,
        )
    ).replace(".0", "")

    distance_moy += " km"

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
    df_fnl_e: pd.DataFrame,
    ce_secteur: str,
    cette_origine_secteur: str,
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
    df_fnl_m = df_fnl_e.copy()

    ordre = [
        "0-1",
        "1-20",
        "20-35",
        "35-50",
        "50-65",
        "65-90",
        "90+",
    ]

    les_proportions = (
        df_fnl_m["classe_age"]
        .value_counts(normalize=True)
        .mul(100)
        .round(3)
        .reset_index(name="pourcentage")
    )

    les_proportions["classe_age"] = (
        les_proportions["classe_age"]
        .cat.reorder_categories(
            ordre,
            ordered=True,
        )
    )

    les_proportions = les_proportions.sort_values(
        "classe_age"
    )

    nb_originaire = (
        df_fnl_m[
            df_fnl_m[cette_origine_secteur] == "O"
        ][cette_origine_secteur]
        .count()
    )

    nb_exogene = (
        df_fnl_m[
            df_fnl_m[cette_origine_secteur] == "N"
        ][cette_origine_secteur]
        .count()
    )

    pct_originaire = str(
        round(
            100 * nb_originaire / len(df_fnl_m),
            2,
        )
    )

    pct_exogene = str(
        round(
            100 * nb_exogene / len(df_fnl_m),
            2,
        )
    )

    ind_tafv = str(
        round(
            nb_originaire
            / (nb_originaire + nb_exogene),
            1,
        )
    )

    distance_med_originaire = (
        str(
            df_fnl_m[
                df_fnl_m[cette_origine_secteur] == "O"
            ]["distance"].median()
        ).replace(".0", "")
        + " Km"
    )

    distance_med_exogene = (
        str(
            df_fnl_m[
                df_fnl_m[cette_origine_secteur] == "N"
            ]["distance"].median()
        ).replace(".0", "")
        + " Km"
    )

    age_moy_originaire = str(
        round(
            df_fnl_m[
                df_fnl_m[cette_origine_secteur] == "O"
            ]["age"].mean(),
            0,
        )
    ).replace(".0", "")

    age_moy_exogene = str(
        round(
            df_fnl_m[
                df_fnl_m[cette_origine_secteur] == "N"
            ]["age"].mean(),
            0,
        )
    ).replace(".0", "")

    df_fnl_m["distance_av_som"] = (
        1 + df_fnl_m["distance"]
    )

    df_fnl_m["distance_lng"] = np.log(
        df_fnl_m["distance_av_som"]
    )

    distance_log_sum_prp = (
        df_fnl_m["distance_lng"].sum()
        / len(df_fnl_m)
    )

    return (
        pct_originaire,
        pct_exogene,
        ind_tafv,
        distance_log_sum_prp,
        age_moy_originaire,
        age_moy_exogene,
        distance_med_originaire,
        distance_med_exogene,
        les_proportions,
    )