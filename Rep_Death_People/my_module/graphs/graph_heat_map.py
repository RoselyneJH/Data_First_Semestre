import plotly.express as px
import pandas as pd

def render_graph_heat_map_origine(
    df_heat_map_brut: pd.DataFrame,
    nom_secteur: str,
    origine_secteur: str,
    list_ordonnee: list,
    type_trt: str = "O",
):
    ordre_cls_age = ["0-1", "1-20", "20-35", "35-50", "50-65", "65-90", "90+"]

    # Constitution du dataframe afin de recuperer les secteurs selectionnés
    df_secteurs = pd.DataFrame({nom_secteur: list_ordonnee})

    #  je selectionne uniquement le bon type_trt et je fais le merge
    df_heat_map_filtre = df_secteurs.merge(
        df_heat_map_brut[df_heat_map_brut[origine_secteur] == type_trt],
        on=nom_secteur,
        how="left",
    )
    # recupération du max
    zmax = df_heat_map_filtre["nb_deces"].max()

    pivot_S = df_heat_map_filtre.pivot(
        index=nom_secteur,
        columns="classe_age",
        values="nb_deces",
    ).fillna(0)
    
    # réindexerles colonnes afin de respecter l'ordre
    pivot_S = pivot_S.reindex(columns=ordre_cls_age)

    if type_trt == "O":
        le_titre = "Originaire"
        fig = px.imshow(
            pivot_S,
            # title="Originaire",
            color_continuous_scale=[
                [0.0, "steelblue"],  # valeur basse  steelblue
                [1.0, "darkorange"],
            ],
            aspect="auto",
            zmin=0,
            zmax=zmax,
        )
    else:
        le_titre = "Exogène"
        fig = px.imshow(
            pivot_S,
            # title="Originaire",
            color_continuous_scale=[
                [0.0, "darkorange"],  # valeur basse  steelblue
                [1.0, "steelblue"],
            ],
            aspect="auto",
            zmin=0,
            zmax=zmax,
        )

    # attribution du titre de color_continuous_scale :
    fig.update_coloraxes(colorbar_title=le_titre)

    fig.update_layout(
        title="Mortalité par secteur et classe d’âge",  # "Mortalité et originaires [total]",
        yaxis=dict(
            title="Secteurs",
            categoryarray=list_ordonnee,
            categoryorder="array",
            # categoryarray=labels,
            autorange="reversed",
        ),
        xaxis=dict(
            title="Classe d'âge",
            # side="left",
            showgrid=True,
        ),
        plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
        paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
        height=650,
        width=650,
    )
    # inclinaison des libelles
    fig.update_xaxes(tickangle=45)

    return fig
