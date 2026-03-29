import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import polars as pl
from typing import Tuple

CST_TITRE_ORIGINAIRE = "Ancrage territorial en fin de vie et classes d’âge"
CST_TITRE_EXOGENE = "Attractivité territoriale en fin de vie et classes d’âge"

# Acceder à la classe de filtrage des données
from my_module.graphs.Cls_graphe_score_pour_viz  import ClsScorePourViz

def identifier_item_de_la_list(ma_list:list):
    '''
        identifier l'item de la list proposé en argument de cette fonction
        Args :
            ma_list  : list
        return :
            l'item 
    '''
    mon_item = ""  
    if len(ma_list) == 1:
        mon_item = str(ma_list[0])
    else:    
        mon_item = str(ma_list[0]) # je n'ai pas de supériorité identifié

    return mon_item


def render_graph_score_age(df_fnl: pd.DataFrame,
                            nom_secteur:str,
                            origine_secteur:str,
                            meilleurs_secteurs_originaires:bool=True,
                            visualisation_secteur_sans_deces_originaire:bool=True) -> Tuple[go.Figure(), pd.DataFrame]:
    '''
    Initialise le traitement du graph

        Args:
            df_fnl        : dataframe
            nom_secteur   : nom du secteur à traiter
            cette_origine : origine 
            seuil         : pour fixer un seuil de scoring
        
        Return:
            fig : une figure, graphe
            df  : dataframe
    '''   
    # ---  Recupération de mes données via la classe ---
    class_filtrage = ClsScorePourViz(df_fnl ,nom_secteur,origine_secteur)
    df_score_,distance_origine,nb_origine, distance_non_origine, nb_non_origine = class_filtrage.score_secteur(True)
    
    vision_ville = False
    if origine_secteur =='origine_ville':
        vision_ville = True

    if not vision_ville:
        if visualisation_secteur_sans_deces_originaire: # variable à mettre en place en amont
            if class_filtrage.nb_secteur_sans_deces_originaire == 0 :
            # Alors on peut visualiser les secteurs car présence d'originaire
                df_score = df_score_[df_score_[nb_origine]>0]         
            else:    # secteurs sans originaires
                df_score = df_score_.copy()

        ordre_cls_age = ["0-1", "1-20", "20-35", "35-50", "50-65", "65-90", "90+"]
        # Je veux eviter de faire apparaitre des classes qui ne sont pas presentes dans 
        # le dataframe alors je recrée l'odre mais uniquement avec les classes 
        # presentes :
        classes_presentes = df_score['classe_age'].unique()
        ordre_filtre = [c for c in ordre_cls_age if c in classes_presentes]
        
        # preparation au imshow
        df_heatmap = (
            df_score
            .pivot(index=nom_secteur, columns='classe_age', values='TAFV')
        )
        # réindexer les colonnes afin de respecter l'ordre
        df_heatmap = df_heatmap.reindex(columns=ordre_filtre)     

        # connaitre le nombre max de cellule dans mon heatmap :
        nb_cells = df_heatmap.size
        # ne pas encadrer toutes les cellules du heatmap sinon pas pertinent alors :
        top_n = min(5, nb_cells - 2)
        # Calcul de la somme pour chaque classe :
        mean_col = df_heatmap.sum(axis=0)  #df_heatmap.mean(axis=0)    

        # Calcul de la somme pour chaque secteur :
        mean_row = df_heatmap.mean(axis=1)  #df_heatmap.mean(axis=1)

        # Liste des Secteurs
        sectors = df_heatmap.index.tolist()

        if meilleurs_secteurs_originaires:
            # Top n des cellules
            top_cells = df_heatmap.stack().nlargest(top_n)
            classes_extreme = mean_col[mean_col == mean_col.max()].index.tolist()
            secteurs_extreme = mean_row[mean_row == mean_row.max()].index.tolist()
            le_titre =CST_TITRE_ORIGINAIRE
        else: # meilleur secteur exogène
            top_cells = df_heatmap.stack().nsmallest(top_n)
            classes_extreme = mean_col[mean_col == mean_col.min()].index.tolist()
            secteurs_extreme = mean_row[mean_row == mean_row.min()].index.tolist()
            le_titre =CST_TITRE_EXOGENE    

        ma_classe_extreme = identifier_item_de_la_list(classes_extreme)
        mon_secteur_extreme = identifier_item_de_la_list(secteurs_extreme)
            
        fig = px.imshow(df_heatmap,
                        title=le_titre, #"Classe d'âge des exogènes et des originaires ",
                        aspect="auto",
                        color_continuous_scale="Viridis",#"RdBu_r",
                        labels=dict(color="TAFV"),
                        zmin=0,
                        zmax=1,
        )
        # colorbar
        fig.update_coloraxes(
        colorbar=dict(
            tickvals=[0, 0.5,  1],
            ticktext=[
                "Exogène",
                "Neutre",
                "Originaire" ]
            )
        )
        # les couleurs 
        fig.update_layout(        
            xaxis_title="Classe d'âge",
            yaxis_title="Secteur",
            plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
            paper_bgcolor="#ADD8E6",
            height=750,
            margin=dict(t=80, b=50, l=50, r=50), # permet d'avoir même hauteur de graphe
            title_x=0.2, # centre le titre du graphique
        )
        
        # permet d'afficher le rang des cellules
        for i, ((secteur, classe), value) in enumerate(top_cells.items(), start=1):

            row_index = df_heatmap.index.get_loc(secteur)
            col_index = df_heatmap.columns.get_loc(classe)

            fig.add_annotation(
                x=col_index,
                y=row_index,
                text=f"{i}",
                showarrow=False,
                font=dict(size=14, color="black")
            )
        # encadre les cellules 
        for (secteur, classe), value in top_cells.items():

            row_index = df_heatmap.index.get_loc(secteur)
            col_index = df_heatmap.columns.get_loc(classe)

            fig.add_shape(
                type="rect",
                x0=col_index - 0.5,
                x1=col_index + 0.5,
                y0=row_index - 0.5,
                y1=row_index + 0.5,
                line=dict(color="black", width=3)
            )
        # Performance des classes et secteurs 
        couleur = "#AEA222" if meilleurs_secteurs_originaires else "purple" # 1 #FBFFCD #696D44 #696E3B
        # #938C4E
        # Meilleur classe :
        fig.update_xaxes(
            tickvals=ordre_filtre,
            ticktext=[
                f"<b style='color:{couleur}'>★{age}</b>" if age == ma_classe_extreme else age
                for age in ordre_filtre
            ]
        )
        # Meilleur secteur    
        fig.update_yaxes(
            tickvals=sectors,
            ticktext=[
                f"<b style='color:{couleur}'>★{s}</b>" if s == mon_secteur_extreme else s
                for s in sectors
            ]
        )
        return fig, df_heatmap
    else:        
        la_ville= df_fnl.iloc[0,15]
        liste_dep = class_filtrage.preparation_treemap(df_fnl,la_ville)
        
        df_top = liste_dep.copy()   
        # Ajout d'un top pour identifier les  departements topés et identifiés
        df_top['top_dep']= 'O'
        df_top.loc[df_top['origine']=='Autres','top_dep']='N'

        # creation du dataframe master sur lequel on effectue le graphe
        df_master = df_fnl[df_fnl['ville_deces']==la_ville]

        if not  meilleurs_secteurs_originaires:            
            # tri des departements
            df_top_ordre = df_top.query("origine!='Autres' & origine!=@la_ville").sort_values('valeur', 
                                        ascending=False)['origine'].to_list()           
            secteur_naissance = 'nom_departement_naissance'  
        else:
            # tri des departements pour originaires
            df_top_ordre = df_top.query("origine==@la_ville").sort_values('valeur', ascending=False)['origine'].to_list()
            secteur_naissance = 'ville_naissance' 

        df_merge = df_master.merge( df_top,left_on=secteur_naissance, 
                                right_on='origine', how='left')
        df_merge['top_dep'] = df_merge['top_dep'].fillna('N')
        df_merge_st_top = df_merge.query("top_dep =='O' ").groupby([secteur_naissance,'classe_age'],
                                                            as_index= False,
                                                            observed=True).agg(nb=('idligne','count'))
            
        fig = px.bar(df_merge_st_top, x=secteur_naissance, y='nb',  color="classe_age",#animation_frame='event_dt', 
            category_orders={
            "classe_age": ["0-1", "1-20", "20-35", "35-50", "50-65", "65-90", "90+" ],
            secteur_naissance: df_top_ordre},
            color_discrete_sequence=px.colors.qualitative.Pastel,
            barmode='group', title='your title')
        
        if df_fnl[df_fnl['origine_ville']=="O"]['origine_ville'].shape[0]==0 and meilleurs_secteurs_originaires==True:
            # Annotation pour notifier l'abscence d'originaire            
            fig.add_annotation(
                x=0.5,                        # 
                y=0.5,                        
                xref="paper",
                yref="paper", # paper =coordonées relative à la feuille
                showarrow=False,
                text="Pas d'originaire décédé pour cette ville",
                font=dict(size=19, color="blue"),
            )

        fig.update_layout(
            title=f"Origine et âges des défunts à {df_fnl.iloc[0,15]}",
            xaxis = {"title" : "Secteur de naissance"},
            yaxis = {"title" : "Nombre de décès"},
            paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
            height=750,
            width=500,
            )
        return fig, df_merge_st_top 
