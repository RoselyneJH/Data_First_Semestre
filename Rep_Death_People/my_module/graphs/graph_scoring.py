import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import polars as pl

def score_secteur(df:pd.DataFrame, ce_ss_secteur:str, cette_origine_:str)->pd.DataFrame:
    '''
        Args    : Dataframe à transformer
                  sous_secteur à filtrer
                  origine du sous secteur
        Return  : Dataframe transformé
        Process : Comptabilité liée par sous-secteurs sur deces exogènes,
                  deces originaires, distance selon age
                  Utilisation de Polars pour performance
    '''
    # Petite manip liée à un décallage des niveaux de secteur
    le_string = ce_ss_secteur    
    cette_origine_1 ="origine_"+le_string.replace("_deces","")
    cette_origine =cette_origine_1.replace("_nom","")
    
    # item qui permet de préparer le filtre/groupby : paramétrage
    nb_non_origine = "nb_non_origine_"+cette_origine.replace("origine_", "")
    nb_origine = "nb_origine_"+cette_origine.replace("origine_", "")
    
    distance_non_origine = "distance_non_origine_"+cette_origine.replace("origine_", "")
    distance_origine = "distance_origine_"+cette_origine.replace("origine_", "")
    
        
    # liste des secteurs descendant afin de préparer le filtre 
    liste_secteur = ['pays_naissance', 'nom_region_deces', 'nom_departement_deces','ville_deces']
    # initialisation de la liste
    liste_a_traiter = []
    position= -1
    # processus de creation du groupby/filtre sectoriel  
    for ind, item in enumerate(liste_secteur):
        if position<0:
            liste_a_traiter.append(item)
        if item == ce_ss_secteur and ind>0:
            position = ind
    
    # transforme pandas en polar 
    mon_pl = pl.DataFrame(df)
    
    # FILTRAGE/GROUPBY
    pl_cumul_secteur = (
        mon_pl.lazy()
        .filter(pl.col("pays_naissance") == "FRANCE")
        .group_by(liste_a_traiter)
        .agg([
            # Nombre de non-originaires
            (pl.col(cette_origine) == "N").sum().alias("item_nb_non_origine"),
            # Nombre d'originaires
            (pl.col(cette_origine) == "O").sum().alias("item_nb_origine"),
            # Distance cumulée non-originaires
            (pl.col("distance").filter(pl.col(cette_origine) == "N"))
            .sum()
            .alias("item_distance_non_origine"),
            # Distance cumulée originaires
            (pl.col("distance").filter(pl.col(cette_origine) == "O"))
            .sum()
            .alias("item_distance_origine"),
            (pl.col("distance").filter(pl.col(cette_origine) == "N"))
            .median().alias("med_distance_non_ori"),
            (pl.col("distance").filter(pl.col(cette_origine) == "O"))
            .median().alias("med_distance_ori"), 
        ])
        
        # Taux d'attractivité de fin de vie TAFV
        .with_columns([
            (pl.col("item_nb_non_origine")/
              (pl.col("item_nb_origine")+pl.col("item_nb_non_origine") )
            ).round(3).alias("TAFV") # Taux d'attractivité de fin de vie
        ])
        # Indice de Mobilité différentielle IMD
        .with_columns([
            pl.when(
            (pl.col("med_distance_ori") == 0) | pl.col("med_distance_ori").is_null()
            ).then(
                None
            ).otherwise(
            ((pl.col("med_distance_non_ori"))/
             (pl.col("med_distance_ori")) )
            ).round(2).alias("IMD") #  
        ])
        # Indice de mobilité différentielle normalisé 
        .with_columns([
            pl.when(pl.col("IMD").is_null()).then(1).otherwise(
            ((pl.col("IMD")-pl.col("IMD").min())/
             (pl.col("IMD").max()-pl.col("IMD").min())
            )).alias("IMD_nor") #  
        ])
        .with_columns([
            ((pl.col("item_nb_non_origine") - pl.col("item_nb_non_origine").min())/
            (pl.col("item_nb_non_origine").max() - pl.col("item_nb_non_origine").min())
            ).alias("non_orig_norm"),
            ((pl.col("item_nb_origine") - pl.col("item_nb_origine").min()) /
            (pl.col("item_nb_origine").max() - pl.col("item_nb_origine").min())
            ).alias("orig_norm"),
            ((pl.col("item_distance_non_origine") - pl.col("item_distance_non_origine").min()) /
            (pl.col("item_distance_non_origine").max() - pl.col("item_distance_non_origine").min())
            ).alias("distance_non_orig_norm"),            
        ])
        .with_columns([
            ((0.6 * pl.col("orig_norm") ) + (0.2 * pl.col("non_orig_norm")) # Formule du scoring
              +(0.1 * (1 - pl.col("distance_non_orig_norm"))))
              .alias("score"),
            ])
        .collect()
        )  

    # transforme polar en pandas)
    df_cumul_secteur = pl_cumul_secteur.to_pandas()
    
    # renommer les colonnes correctement 
    df_cumul_secteur.rename(columns={'item_nb_non_origine': nb_non_origine,
                                     'item_nb_origine': nb_origine,
                                     'item_distance_non_origine': distance_non_origine,
                                     'item_distance_origine': distance_origine, 
                                     },inplace =True) 
       
    return df_cumul_secteur,distance_origine,nb_origine, distance_non_origine, nb_non_origine

def render_graph_score(df_fnl: pd.DataFrame,
                            nom_secteur:str,
                            origine_secteur:str)-> go.Figure():

    # Permet d'identifier la vue à présenter en fonction du choix utilisateur
    view_secteur= "N"
    if nom_secteur == "nom_departement_deces" or nom_secteur == "nom_region_deces":
        view_secteur= "O"        
    # Acces à la fonction de decoupage sectorielle
    df_score,distance_origine,nb_origine, distance_non_origine, nb_non_origine = score_secteur (df_fnl,
                                                                        nom_secteur,
                                                                        origine_secteur)
    # graphe :
    if len(df_score)>1: # Alors secteur différent d'une ville 
        if view_secteur=='N':   # Secteur = departement ou region     
            fig_score = px.scatter(
                df_score,
                x=distance_non_origine,
                y=nb_non_origine,
                color="score", 
                size="score", 
                hover_name=nom_secteur,
                color_continuous_scale=[
                    [0.0, "darkorange"],  # valeur basse  steelblue
                    [1.0, "steelblue"],
                ], #"Viridis",
                title="Scoring des villes", 
                labels={
                    distance_non_origine: "Distance parcourue par les originaires",
                    nb_non_origine: "Effectif des originaires",
                }
            )
            fig_score.update_layout(
            plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
            paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
            height=450,
            width=450,
            )
            fig_score.update_coloraxes(
                colorbar=dict(
                    title="Ancrage",
                    tickvals=[0, 0.25, 0.5, 0.75, 1],
                    ticktext=[
                        "Très faible",
                        "Faible",
                        "Moyenne",
                        "Bonne",
                        "Très élevée",
                    ]
                )
            )
            return fig_score, df_score
        else:
            # Je dois prendre en compte le cas ou la 3ieme DIM =0 (cad df_score ne comporte pas d'originaire
            # sur ce secteur donc la taille pose pb dans le visuel du graphe)
            # Je sépare les 2 courbes et j'attribue de façon artificielle une taille à la valeur nulle :
            fig_IMD = go.Figure()
            df_score_sup = df_score[df_score[nb_origine]>0]
            val_artificiel= 0.3*max(df_score_sup[nb_origine])/(40.**2)

            fig_IMD.add_scatter(
                y=df_score_sup["TAFV"],
                x=df_score_sup["IMD_nor"],
                text=df_score_sup[nom_secteur],     # colonne à afficher
                hoverinfo="text+x+y",   # ce qui apparaît
                mode="markers",
                marker=dict(
                    colorscale="Viridis",        # affiche le type de couleur de la colorbar
                    colorbar=dict(# personnalise la colorbar
                        title="Locaux (nb)",
                        #tickvals=[0, 0.25, 0.5, 0.75, 1],
                        tickmode="array",   # ← IMPORTANT
                        #ticktext=[
                        #    "Très faible",
                        #    "Faible",
                        #    "Moyenne",
                        #    "Bonne",
                        #    "Très élevée"
                        #]
                    ),
                    size=df_score_sup[nb_origine], # affiche la taille en fonction de cette valeur
                    color=df_score_sup[nb_origine] , # affiche la couleur en fonction de cette valeur
                    showscale=True,              # affiche la colorbar
                    sizemode="area", # defini la zone d'utilisation homogenéité des marqueurs par defaut cette valeur
                    sizeref=2.*max(df_score_sup[nb_origine])/(40.**2),
                    opacity=0.6,
                ),
            )

            df_score_inf = df_score[df_score[nb_origine]==0]
            fig_IMD.add_scatter(
                y=df_score_inf["TAFV"],
                x=df_score_inf["IMD_nor"],
                #name="",
                mode="markers",
                marker=dict(
                    colorscale="Viridis",
                    size=val_artificiel, # taille artificielle pour "voir" l'étoile
                    color=df_score_inf[nb_origine] ,
                    symbol="star",
                    sizemode="area",
                    sizeref=val_artificiel,
                    opacity=0.6,
                ),
            )
            
            fig_IMD.update_layout(
            title="Indice de mobilité différentielle vs Taux d'attractivité",
            showlegend=False, # desactive la legende pour les 2 add_scatter
            xaxis_title="Taux d'attractivité ",
            yaxis_title="Indice de mobilité différentielle",
            plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
            paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
            height=450,
            width=450,
            )
                       
            return fig_IMD, df_score
    else:
        data = {
            "Decès": [nb_non_origine, nb_origine],
            "Valeur": [df_score.iloc[0,4], df_score.iloc[0,5]]
        }
        df = pd.DataFrame(data)
        fig = px.pie(
            df,
            names="Decès",
            values="Valeur",
            title="Répartition des décès",
            labels={
                "Decès": "Type de population",
                "Valeur": "Nombre de personnes"
                    }
        )
        fig.update_layout(
            #plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
            paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
            height=450,
            width=450,
            )
        return fig,df_score
