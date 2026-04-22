import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import polars as pl
from typing import Tuple

# Acceder à la classe de filtrage des données
# from Cls_graphe_score_pour_viz  import ClsScorePourViz

from my_module.graphs.Cls_graphe_score_pour_viz  import ClsScorePourViz


def render_graph_score(df_fnl: pd.DataFrame,
                            nom_secteur:str,
                            origine_secteur:str,
                            visualisation_secteur_sans_deces_originaire:bool=True) -> Tuple[go.Figure(), pd.DataFrame, str] :
    '''
    Initialise le traitement du graph

        Args:
            df_fnl        : dataframe
            nom_secteur   : nom du secteur à traiter
            cette_origine : origine 
        
        Return:
            fig : une figure, graphe
            df  : dataframe
    '''
    height_val = 580
    # ---  Recupération de mes données via la classe ---
    class_filtrage = ClsScorePourViz(df_fnl, nom_secteur, origine_secteur)
    df_score,distance_origine, nb_origine, distance_non_origine, nb_non_origine = class_filtrage.score_secteur()

    texte_sur_secteur_sans_deces_originaire=""
    
    # graphe :
    # Alors secteur différent d'une ville ou bien departement = region (df_score=1)
    # vue region, departement, sinon graphe suivant :
    if len(df_score)>=1 and not (origine_secteur == "origine_ville"): 
            fig = go.Figure()
            if class_filtrage.nb_secteur_sans_deces_originaire >0:
                ce_nom_secteur = nom_secteur.replace("deces","").replace("_","").replace("nom","")
                texte_sur_secteur_sans_deces_originaire = "⚠️ Il y a "+str(class_filtrage.nb_secteur_sans_deces_originaire)
                texte_sur_secteur_sans_deces_originaire = texte_sur_secteur_sans_deces_originaire+" "+ce_nom_secteur +"(s)"
                texte_sur_secteur_sans_deces_originaire = texte_sur_secteur_sans_deces_originaire +" sans décès d'originaire. "
                
            #if class_filtrage.nb_secteur_sans_deces_originaire == 0: #visualisation_secteur_sans_deces_originaire:
            #    df_score_ = df_score[df_score[nb_origine]>0].sort_values(nom_secteur, ascending = False)
            #else:
            df_score_ = df_score.sort_values(nom_secteur, ascending = False) 

            if len(df_score_)>0:
                fig.add_scatter(
                    y=df_score_[nom_secteur],
                    x=df_score_["TAFV"],
                    text=df_score_[nom_secteur],     # colonne à afficher
                    hoverinfo="text+x+y",           # ce qui apparaît
                    mode="markers",
                    marker=dict(
                        colorscale="Viridis",        # affiche le type de couleur de la colorbar
                        colorbar=dict(# personnalise la colorbar
                            title="Décès des exogènes",#
                            tickmode="array",   # ← IMPORTANT
                        ),
                        size=df_score_[nb_non_origine], #df_score[nb_non_origine], # affiche la taille en fonction de cette valeur
                        color=df_score_[nb_non_origine],#+df_score[nb_non_origine] , # affiche la couleur en fonction de cette valeur
                        showscale=True,              # affiche la colorbar
                        sizemode="area", # defini la zone d'utilisation homogenéité des marqueurs par defaut cette valeur
                        sizeref=2.*max(df_score_[nb_non_origine])/(40.**2),
                        opacity=0.6,
                    ),
                )           

                x_fin = 0.80 # 
                fig.add_annotation(
                x=x_fin,         
                y=-0.55,    # sur l’axe des abscisses
                xref="x",
                yref="y",
                showarrow=False,
                text="Originaires",
                font=dict(size=12, color="blue"),
                )
                
                x_deb = 0.15 #
                fig.add_annotation(
                x=x_deb,              
                y=-0.55,    # sur l’axe des abscisses
                xref="x",
                yref="y",
                showarrow=False,
                text="Exogènes",
                font=dict(size=12, color="blue"),
                )
                # Annotation pour identifier la tendance de cette zone
                fig.add_annotation(
                    x=0,                        # 
                    y=1.05,                        
                    xref="paper",
                    yref="paper", # paper =coordonées relative à la feuille
                    showarrow=False,
                    text=class_filtrage.etat_global_de_ces_secteurs,
                    font=dict(size=12, color="blue"),
                )
                
                fig.add_vline(
                        x=0.3,
                        line_dash="dash",
                        line_color="white"
                )
                fig.add_vline(
                        x=0.6,
                        line_dash="dash",
                        line_color="white"
                )

                fig.update_layout(
                    xaxis=dict(rangemode="tozero"),  # 0 est maintenant garanti   
                    title= "Répartition du TAFV par zone géographique",#"Secteurs à fort ancrage territorial ou à forte attractivité ",
                    showlegend=False, # desactive la legende pour les 2 add_scatter
                    xaxis_title="TAFV",
                    yaxis_title="Secteur",
                    plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
                    paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
                    height=height_val,
                    margin=dict(t=80, b=50, l=50, r=50), # permet d'avoir même hauteur de graphe
                    title_x=0.2, # centre le titre du graphique
                )
                        
                return fig, texte_sur_secteur_sans_deces_originaire ,df_score
            else:
                fig.add_scatter(
                    y=df_score[nom_secteur],
                    x=df_score["TAFV"],
                    text=df_score[nom_secteur],     # colonne à afficher
                    hoverinfo="text+x+y",           # ce qui apparaît
                    mode="markers",
                    marker=dict(
                        colorscale="Viridis",        # affiche le type de couleur de la colorbar
                        colorbar=dict(# personnalise la colorbar
                            title="Décès des exogènes",#
                            tickmode="array",   # ← IMPORTANT
                        ),
                        size=df_score[nb_non_origine], #df_score[nb_non_origine], # affiche la taille en fonction de cette valeur
                        color=df_score[nb_non_origine],#+df_score[nb_non_origine] , # affiche la couleur en fonction de cette valeur
                        showscale=True,              # affiche la colorbar
                        sizemode="area", # defini la zone d'utilisation homogenéité des marqueurs par defaut cette valeur
                        sizeref=2.*max(df_score[nb_non_origine])/(40.**2),
                        opacity=0.6,
                    ),
                )           

                x_fin = 0.80 # 
                fig.add_annotation(
                x=x_fin,         
                y=-0.55,    # sur l’axe des abscisses
                xref="x",
                yref="y",
                showarrow=False,
                text="Originaires",
                font=dict(size=12, color="blue"),
                )
                
                x_deb = 0.15 #
                fig.add_annotation(
                x=x_deb,              
                y=-0.55,    # sur l’axe des abscisses
                xref="x",
                yref="y",
                showarrow=False,
                text="Exogènes",
                font=dict(size=12, color="blue"),
                )
                # Annotation pour identifier la tendance de cette zone
                fig.add_annotation(
                    x=0,                        # 
                    y=1.05,                        
                    xref="paper",
                    yref="paper", # paper =coordonées relative à la feuille
                    showarrow=False,
                    text=class_filtrage.etat_global_de_ces_secteurs,
                    font=dict(size=14, color="blue"),
                )
                
                fig.add_vline(
                        x=0.3,
                        line_dash="dash",
                        line_color="white"
                )
                fig.add_vline(
                        x=0.6,
                        line_dash="dash",
                        line_color="white"
                )

                fig.update_layout(
                    xaxis=dict(rangemode="tozero"),  # 0 est maintenant garanti   
                    title="Secteurs à fort ancrage territorial ou à forte attractivité ",
                    showlegend=False, # desactive la legende pour les 2 add_scatter
                    xaxis_title="TAFV",
                    yaxis_title="Secteur",
                    plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
                    paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
                    height=height_val,
                    margin=dict(t=80, b=50, l=50, r=50), # permet d'avoir même hauteur de graphe
                    title_x=0.2, # centre le titre du graphique
                )
                        
                return fig, texte_sur_secteur_sans_deces_originaire,df_score
                
    else:        
        df_plot = class_filtrage.preparation_treemap(df_fnl,df_fnl.iloc[0,15])
        # 7️⃣ Treemap
        fig = px.treemap(
            df_plot,
            path=["statut", "origine"],
            values="valeur",
            color="statut",
            color_discrete_map={
                "Originaire": "#A9E24E",
                "Exogene": "#81B0E3",
            }
        )
        fig.update_traces(textinfo="label+percent entry")

        fig.update_layout(
            title=f"D'ou viennent les défunts du secteur {df_fnl.iloc[0,15]} ? <br>Quelles sont les proportions ? <br>",
            paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
            height=height_val,
            width=400,
            )
        return fig, "", df_fnl  
