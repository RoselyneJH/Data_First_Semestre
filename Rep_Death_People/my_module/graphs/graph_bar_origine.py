import plotly.express as px
import pandas as pd

# import streamlit as st

import plotly.graph_objects as go


def render_graph_bar_origine(
    df_bar: pd.DataFrame, nom_secteur: str, origine_secteur: str
):

    fig = px.bar(
        df_bar,
        y=df_bar[nom_secteur],
        x="nb_deces",
        color=origine_secteur,
        barmode="group",
        title="Proportions par secteur",
        color_discrete_map={"N": "steelblue", "O": "darkorange"},
        orientation="h",
        category_orders={"origine": ["N", "O"]},
    )

    # Mettre le fond transparent
    fig.update_layout(
        title="Mortalité et origine géographique des personnes ",
        xaxis=dict(title="Mortalié"),
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
        paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent
        # (fond du “papier” autour du tracé)
        height=650,
        width=450,
    )
    # l’axe vertical est inversé, donc le premier élément du DataFrame se retrouve en haut,
    # et le dernier en bas ; il faut donc inverser :
    fig.update_yaxes(autorange="reversed")

    # Recuperation des secteurs de l'axe des ordonnées
    # Un tuple est immuable, donc on ne peut pas supprimer des éléments en place.
    # Alors, recupération des clés pour conserver l'ORDRE
    m_ordre = fig.layout.yaxis.categoryarray  # c'est un tuple  (1, 2, 2, 3, 1, 4)

    # Récupérer sans doublon le contenu du tuple en conservant son ordre :
    tuple_sans_doublons = tuple(dict.fromkeys(m_ordre))

    # Je transforme mon tuple en list :
    list_ordonnee_secteur_sans_dbl = list(tuple_sans_doublons)

    return fig, list_ordonnee_secteur_sans_dbl
