import plotly.express as px
import pandas as pd

# def render_graph_heat_map(df_heat_map_brut: pd.DataFrame, nom_secteur: str, origine_secteur: str,
#                           list_ordonnee: list ):
# 
#     df_secteurs = pd.DataFrame({nom_secteur: list_ordonnee})
# 
#     df_heat_map_filtre = df_secteurs.merge(
#         df_heat_map_brut,
#         on=nom_secteur,
#         how="left"
#     )
#     #print("list_ordonnee----------> ",list_ordonnee)
#     #print("Resultat du merge -----> ",df_heat_map_filtre.info())
# 
#     fig = px.density_heatmap(
#         df_heat_map_filtre,
#         x="classe_age",
#         y=nom_secteur,
#         z="nb_deces",
#         histfunc="sum",
#         color_continuous_scale=[
#         [0.0, "steelblue"],   # valeur basse  steelblue
#         [1.0, "darkorange"],   # valeur haute
#        # [0.0, "#2166ac"],  # bleu
#        # [1.0, "#fdae61"]   # orange doux
#         ],
#         title=origine_secteur, #"Mortalité "
#    )
#     # attribution du titre de color_continuous_scale :
#     fig.update_coloraxes(
#     colorbar_title="Originaire"
#     )

#     fig.update_layout(
#        title="Mortalité par secteur et classe d’âge",  # "Mortalité et originaires [total]",
#         yaxis=dict(title="Secteurs",
#                     categoryarray = list_ordonnee,
#                     categoryorder="array",
#                     #categoryarray=labels,
#                     autorange="reversed",
#                     ),
#         xaxis=dict(title="Classe d'âge", 
#                    #side="left", 
#                    showgrid=True,
#                    ),
#         plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
#         paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
#         height=650,
#         width=450,
#     )

#     return fig

def render_graph_heat_map_origine(df_heat_map_brut: pd.DataFrame, 
                        nom_secteur: str, origine_secteur: str,
                        list_ordonnee: list,
                         type_trt:str = 'O'):
    
    # Constitution du dataframe afin de recuperer les secteurs selectionnés
    df_secteurs = pd.DataFrame({nom_secteur: list_ordonnee})

    #  je selectionne uniquement le bon type_trt et je fais le merge
    df_heat_map_filtre = df_secteurs.merge(
        df_heat_map_brut[df_heat_map_brut[origine_secteur] == type_trt],
        on=nom_secteur,
        how="left"
    )
    # recupération du max
    zmax = df_heat_map_filtre["nb_deces"].max()


    pivot_S = (
        df_heat_map_filtre
        .pivot(
            index=nom_secteur,
            columns="classe_age",
            values= "nb_deces",
        )
        .fillna(0)
    )

    

    if type_trt =='O':
        le_titre = "Originaire"
        fig = px.imshow(
            pivot_S,
            #title="Originaire",
            color_continuous_scale=[
            [0.0, "steelblue"],   # valeur basse  steelblue
            [1.0, "darkorange"]],
            aspect="auto",
            zmin=0,
            zmax=zmax,
        )
    else:
        le_titre = "Exogène"
        fig = px.imshow(
            pivot_S,
            #title="Originaire",
            color_continuous_scale=[
            [0.0, "darkorange"],   # valeur basse  steelblue
            [1.0, "steelblue"]],
            aspect="auto",
            zmin=0,
            zmax=zmax,
        )

    # attribution du titre de color_continuous_scale :
    fig.update_coloraxes(
    colorbar_title=le_titre
    )

    fig.update_layout(
        title="Mortalité par secteur et classe d’âge",  # "Mortalité et originaires [total]",
        yaxis=dict(title="Secteurs",
                    categoryarray = list_ordonnee,
                    categoryorder="array",
                    #categoryarray=labels,
                    autorange="reversed",
                    ),
        xaxis=dict(title="Classe d'âge", 
                   #side="left", 
                   showgrid=True,
                   ),
        plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
        paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
        height=650,
        width=450,
    )
    # inclinaison des libelles
    #fig.update_xaxes(tickangle=45)

    return fig
