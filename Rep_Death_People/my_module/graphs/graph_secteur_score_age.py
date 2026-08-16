import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import polars as pl
from typing import Tuple

CST_TITRE_ORIGINAIRE = "Ancrage territorial et classes d’âge"
CST_TITRE_EXOGENE = "Attractivité territoriale et classes d’âge"
CST_TITRE_MOBILE = "Mobilité territoriale et classes d’âge"
CST_TITRE_INERTIE = "Inertie territoriale et classes d’âge"


# Acceder à la classe de filtrage des données
from my_module.graphs.Cls_graphe_score_pour_viz import ClsScorePourViz


class ClsGraphScoreAge:

    def __init__(
        self,
        df_fnl: pd.DataFrame,
        nom_secteur: str,
        origine_secteur: str,
        le_df_ecart_type_moy_age: pd.DataFrame = None,
        distance_dep_inf: float = 2.0,
        distance_dep_sup: float = 30.0,
        distance_nat_sup: float = 17235.0,
    ):
        """
        Initialise le graphe pour une meilleur pagination

        Args:
            df_fnl        : dataframe
            nom_secteur   : nom du secteur à traiter
            cette_origine : origine
            le_df_ecart_type_moy_age : le dataframe des ecart-types et moyenne selon classe d'age

        """
        self.df_fnl = df_fnl
        self.nom_secteur = nom_secteur
        self.origine_secteur = origine_secteur
        self.le_df_ecart_type_moy_age = le_df_ecart_type_moy_age
        # Comme le dataframe est ecourté la position de cette colonne
        # change :
        self.pos_col_ville_deces =self.df_fnl.columns.get_loc("ville_deces")

        self.distance_nat_inf = 0
        self.distance_dep_inf = distance_dep_inf  # 200
        self.distance_dep_sup = distance_dep_sup  # 600
        self.distance_nat_sup = distance_nat_sup  # 2000

        self.class_filtrage = ClsScorePourViz(
            self.df_fnl,
            self.nom_secteur,
            self.origine_secteur,
            le_df_ecart_type_moy_age=self.le_df_ecart_type_moy_age,
        )
        self.df_score, self.nb_non_origine = self.class_filtrage.score_secteur(True)
        self.pages = self.class_filtrage.pages

    @property
    def nombre_de_page(self):
        return self.pages

    def identifier_item_de_la_list(self, ma_list: list):
        """
        identifier l'item de la list proposé en argument de cette fonction
        Args :
            ma_list  : list
        return :
            l'item
        """
        mon_item = ""
        if len(ma_list) == 1:
            mon_item = str(ma_list[0])
        else:
            mon_item = str(ma_list[0])  # je n'ai pas de supériorité identifié

        return mon_item

    def render_graph_score_age(
        self, secteurs_originaires: bool = True, page: int = 0
    ) -> Tuple[go.Figure(), pd.DataFrame]:
        """
        Initialise le traitement du graph

            Args:
                page à lire
                secteurs_originaires : affiche les originaires ou les exogènes

            Return:
                fig : une figure, graphe
                df  : dataframe
        """
        height_val = 580

        vision_ville = False
        if self.origine_secteur == "origine_ville":
            vision_ville = True

        if not vision_ville:

            df_score_ = self.class_filtrage.liste_des_df_secteur[page]
            df_score = df_score_.sort_values(self.nom_secteur, ascending=False).copy()

            ordre_cls_age = ["0-1", "1-20", "20-35", "35-50", "50-65", "65-90", "90+"]
            # Je veux eviter de faire apparaitre des classes qui ne sont pas presentes dans
            # le dataframe alors je recrée l'odre mais uniquement avec les classes
            # presentes :
            classes_presentes = df_score["classe_age"].unique()
            ordre_filtre = [c for c in ordre_cls_age if c in classes_presentes]

            # preparation au imshow
            df_heatmap = df_score.pivot(
                index=self.nom_secteur, columns="classe_age", values="TAFV"
            )
            # réindexer les colonnes afin de respecter l'ordre
            df_heatmap = df_heatmap.reindex(columns=ordre_filtre)

            # connaitre le nombre max de cellule dans mon heatmap :
            nb_cells = df_heatmap.size
            # ne pas encadrer toutes les cellules du heatmap sinon pas pertinent alors :
            top_n = min(5, nb_cells - 2)
            # Calcul de la somme pour chaque classe :
            mean_col = df_heatmap.sum(axis=0)  # df_heatmap.mean(axis=0)

            # Calcul de la somme pour chaque secteur :
            mean_row = df_heatmap.mean(axis=1)  # df_heatmap.mean(axis=1)

            # Liste des Secteurs
            sectors = df_heatmap.index.tolist()

            if secteurs_originaires:
                # Top n des cellules
                top_cells = df_heatmap.stack().nlargest(top_n)
                classes_extreme = mean_col[mean_col == mean_col.max()].index.tolist()
                secteurs_extreme = mean_row[mean_row == mean_row.max()].index.tolist()
                le_titre = CST_TITRE_ORIGINAIRE
            else:  # secteur exogène
                top_cells = df_heatmap.stack().nsmallest(top_n)
                classes_extreme = mean_col[mean_col == mean_col.min()].index.tolist()
                secteurs_extreme = mean_row[mean_row == mean_row.min()].index.tolist()
                le_titre = CST_TITRE_EXOGENE

            ma_classe_extreme = self.identifier_item_de_la_list(classes_extreme)
            mon_secteur_extreme = self.identifier_item_de_la_list(secteurs_extreme)

            fig = px.imshow(
                df_heatmap,
                title=le_titre,  # "Classe d'âge des exogènes et des originaires ",
                aspect="auto",
                color_continuous_scale="Viridis",  # "RdBu_r",
                labels=dict(color="TAFV"),
                zmin=0,
                zmax=1,
            )
            # colorbar
            fig.update_coloraxes(
                colorbar=dict(
                    tickvals=[0, 0.5, 1], ticktext=["Exogène", "Neutre", "Originaire"]
                )
            )
            # les couleurs
            fig.update_layout(
                xaxis_title="Classe d'âge",
                yaxis_title="Secteur",
                plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
                paper_bgcolor="#ADD8E6",
                height=height_val,
                margin=dict(
                    t=80, b=50, l=50, r=50
                ),  # permet d'avoir même hauteur de graphe
                title_x=0.3,  # centre le titre du graphique
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
                    font=dict(size=14, color="white"),
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
                    line=dict(color="black", width=3),
                )
            # Performance des classes et secteurs
            couleur = (
                "#AEA222" if secteurs_originaires else "purple"
            )  # 1 #FBFFCD #696D44 #696E3B
            # #938C4E
            # Meilleur classe :
            fig.update_xaxes(
                tickvals=ordre_filtre,
                ticktext=[
                    (
                        f"<b style='color:{couleur}'>★{age}</b>"
                        if age == ma_classe_extreme
                        else age
                    )
                    for age in ordre_filtre
                ],
            )
            # Meilleur secteur
            fig.update_yaxes(
                tickvals=sectors,
                ticktext=[
                    (
                        f"<b style='color:{couleur}'>★{s}</b>"
                        if s == mon_secteur_extreme
                        else s
                    )
                    for s in sectors
                ],
            )
            return fig  # , self.class_filtrage.liste_des_df_secteur[page]
        else:
            palette_originaire = [
                "#FFFDE7",
                "#FFF59D",
                "#FFEE58",
                "#DCE775",
                "#C0CA33",
                "#9CCC65",
                "#7CB342",
                "#A9E24E",
            ]
            palette_exogene = [
                "#C8A2D6",  # violet pastel
                "#F29E9E",  # rouge pastel
                "#F2C57C",  # jaune chaud doux
                "#9AD0A5",  # vert pastel
                "#6FC3B2",  # vert-bleu doux
                "#A1CBEF",  # bleu très clair
                "#81B0E3",  # bleu de base
                "#5A8FD8",  # bleu un peu plus soutenu
            ]

            la_ville = self.df_fnl.iloc[0, self.pos_col_ville_deces]
            liste_dep = self.class_filtrage.preparation_treemap(self.df_fnl, la_ville)

            df_top = liste_dep.copy()
            # Ajout d'un top pour identifier les  departements topés et identifiés
            df_top["top_dep"] = "O"
            df_top.loc[df_top["origine"] == "Autres", "top_dep"] = "N"
            
            # creation du dataframe master sur lequel on effectue le graphe
            df_cette_ville = self.df_fnl[self.df_fnl["ville_deces"] == la_ville]

            if not secteurs_originaires:
                # tri des departements
                df_top_ordre = (
                    df_top.query("origine!='Autres' & origine!=@la_ville")
                    .sort_values("valeur", ascending=False)["origine"]
                    .to_list()
                )
                secteur_naissance = "nom_departement_naissance"
                palette = palette_exogene
                
                df_merge = df_cette_ville.merge(
                df_top, left_on=secteur_naissance, right_on="origine", how="left"
                )
            else:
                # tri des departements pour originaires
                df_top_ordre = (
                    df_top.query("origine==@la_ville")
                    .sort_values("valeur", ascending=False)["origine"]
                    .to_list()
                )
                # est une agglomeration ? ,df_merge_st_top,"secteur_naissance"
                if la_ville =='PARIS' or la_ville =='LYON' or la_ville =='MARSEILLE':
                    secteur_naissance="nom_departement_naissance"
                else:
                    secteur_naissance = "ville_naissance"
                
                palette = palette_originaire
                
                df_merge = df_cette_ville.merge(
                df_top.query("origine==@la_ville"), left_on=secteur_naissance, right_on="origine", how="left"
                )


            df_merge["top_dep"] = df_merge["top_dep"].fillna("N")
            df_merge_st_top = (
                df_merge.query("top_dep =='O' ")
                .groupby(
                    [secteur_naissance, "classe_age"], as_index=False, observed=True
                )
                .agg(nb=("idligne", "count"))
            )
            
            fig = px.bar(
                df_merge_st_top,
                x=secteur_naissance,
                y="nb",
                color="classe_age",
                category_orders={
                    "classe_age": [
                        "0-1",
                        "1-20",
                        "20-35",
                        "35-50",
                        "50-65",
                        "65-90",
                        "90+",
                    ],
                    secteur_naissance: df_top_ordre,
                },
                color_discrete_sequence=palette,  # 
                barmode="group",
                title="your title",
            )
            
            if (
                self.df_fnl[self.df_fnl["origine_ville"] == "O"]["origine_ville"].shape[
                    0
                ]
                == 0
                and secteurs_originaires == True
            ):
                # Annotation pour notifier l'abscence d'originaire
                fig.add_annotation(
                    x=0.5,  #
                    y=0.5,
                    xref="paper",
                    yref="paper",  # paper =coordonées relative à la feuille
                    showarrow=False,
                    text="Pas d'originaire décédé pour cette ville",
                    font=dict(size=19, color="blue"),
                )
            # preparation du titre du graphe
            if secteurs_originaires:
                le_titre = f"Ages des défunts à {self.df_fnl.iloc[0,self.pos_col_ville_deces]}"
            else:
                le_titre = (
                    f"Origine et âges des défunts exogènes à {self.df_fnl.iloc[0,self.pos_col_ville_deces]}"
                )

            fig.update_layout(
                title=le_titre,
                xaxis={"title": "Secteur de naissance"},
                yaxis={"title": "Nombre de décès"},
                paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
                height=height_val,
                width=500,
            )
            return fig  # , df_score_ #df_merge_st_top

    def render_graph_score_age_IMD(
        self, secteurs_mobiles: bool = True, page: int = 0, indicateur: str = "IMD"
    ) -> Tuple[go.Figure(), pd.DataFrame]:
        """
        Initialise le traitement du graph

            Args:
                page à lire
                secteurs_mobiles               : affiche les mobiles ou inertie
                choix de l'indicateur à visualiser : ici IMD

            Return:
                fig : une figure, graphe
                df  : dataframe
        """
        height_val = 580

        vision_ville = False
        if self.origine_secteur == "origine_ville":
            vision_ville = True

        if not vision_ville:

            df_score_ = self.class_filtrage.liste_des_df_secteur[page]
            df_score = df_score_.sort_values(self.nom_secteur, ascending=False).copy()

            ordre_cls_age = ["0-1", "1-20", "20-35", "35-50", "50-65", "65-90", "90+"]
            # Je veux eviter de faire apparaitre des classes qui ne sont pas presentes dans
            # le dataframe alors je recrée l'odre mais uniquement avec les classes
            # presentes :
            classes_presentes = df_score["classe_age"].unique()
            ordre_filtre = [c for c in ordre_cls_age if c in classes_presentes]

            # preparation au imshow
            df_heatmap = df_score.pivot(
                index=self.nom_secteur, columns="classe_age", values=indicateur
            )
            # réindexer les colonnes afin de respecter l'ordre
            df_heatmap = df_heatmap.reindex(columns=ordre_filtre)

            # connaitre le nombre max de cellule dans mon heatmap :
            nb_cells = df_heatmap.size
            # ne pas encadrer toutes les cellules du heatmap sinon pas pertinent alors :
            top_n = min(5, nb_cells - 2)
            # Calcul de la somme pour chaque classe :
            mean_col = df_heatmap.sum(axis=0)  # df_heatmap.mean(axis=0)

            # Calcul de la somme pour chaque secteur :
            mean_row = df_heatmap.mean(axis=1)  # df_heatmap.mean(axis=1)

            # Liste des Secteurs
            sectors = df_heatmap.index.tolist()

            if secteurs_mobiles:
                # Top n des cellules
                top_cells = df_heatmap.stack().nlargest(top_n)
                classes_extreme = mean_col[mean_col == mean_col.max()].index.tolist()
                secteurs_extreme = mean_row[mean_row == mean_row.max()].index.tolist()
                le_titre = CST_TITRE_INERTIE
            else:  # secteur inertie
                top_cells = df_heatmap.stack().nsmallest(top_n)
                classes_extreme = mean_col[mean_col == mean_col.min()].index.tolist()
                secteurs_extreme = mean_row[mean_row == mean_row.min()].index.tolist()
                le_titre = CST_TITRE_MOBILE

            ma_classe_extreme = self.identifier_item_de_la_list(classes_extreme)
            mon_secteur_extreme = self.identifier_item_de_la_list(secteurs_extreme)

            fig = px.imshow(
                df_heatmap,
                title=le_titre, #"IMD et Classe d'âge ",
                aspect="auto",
                color_continuous_scale="Viridis",  # "RdBu_r",
                labels=dict(color="IMD"),
                zmin=-10,
                zmax=8,
                
            )
            # colorbar
            fig.update_coloraxes(
                colorbar=dict(
                    tickvals=[-9, 0, 5], ticktext=["Mobilité", "Neutre", "Inertie"]
                )
            )
            # les couleurs
            fig.update_layout(
                xaxis_title="Classe d'âge",
                yaxis_title="Secteur",
                plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
                paper_bgcolor="#ADD8E6",
                height=height_val,
                margin=dict(
                    t=80, b=50, l=50, r=50
                ),  # permet d'avoir même hauteur de graphe
                title_x=0.3,  # centre le titre du graphique
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
                    font=dict(size=14, color="white"),
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
                    line=dict(color="black", width=3),
                )
            # Performance des classes et secteurs
            couleur = (
                "#AEA222" if secteurs_mobiles else "purple"
            )  #
            # 
            # Meilleur classe :
            fig.update_xaxes(
                tickvals=ordre_filtre,
                ticktext=[
                    (
                        f"<b style='color:{couleur}'>★{age}</b>"
                        if age == ma_classe_extreme
                        else age
                    )
                    for age in ordre_filtre
                ],
            )
            # Meilleur secteur
            fig.update_yaxes(
                tickvals=sectors,
                ticktext=[
                    (
                        f"<b style='color:{couleur}'>★{s}</b>"
                        if s == mon_secteur_extreme
                        else s
                    )
                    for s in sectors
                ],
            )
            return fig
        else:
            palette_originaire = [
                "#FFFDE7",
                "#FFF59D",
                "#FFEE58",
                "#DCE775",
                "#C0CA33",
                "#9CCC65",
                "#7CB342",
                "#A9E24E",
            ]
            palette_exogene = [
                "#C8A2D6",  # violet pastel
                "#F29E9E",  # rouge pastel
                "#F2C57C",  # jaune chaud doux
                "#9AD0A5",  # vert pastel
                "#6FC3B2",  # vert-bleu doux
                "#A1CBEF",  # bleu très clair
                "#81B0E3",  # bleu de base
                "#5A8FD8",  # bleu un peu plus soutenu
            ]

            la_ville = self.df_fnl.iloc[0, self.pos_col_ville_deces]
            liste_dep = self.class_filtrage.preparation_treemap(self.df_fnl, la_ville)

            df_top = liste_dep.copy()
            # Ajout d'un top pour identifier les  departements topés et identifiés
            df_top["top_dep"] = "O"
            df_top.loc[df_top["origine"] == "Autres", "top_dep"] = "N"

            # creation du dataframe master sur lequel on effectue le graphe
            df_cette_ville = self.df_fnl[self.df_fnl["ville_deces"] == la_ville]

            labels = ["Urbaine", "Départementale", "Régionale", "Nationale","Internationale"]
            # creation de la colonne mobility
            df_cette_ville["mobility"] = "Internationale"
            df_cette_ville.loc[
                (df_cette_ville["origine_ville"] == "O"), ["mobility"]
            ] = ["Urbaine"]
            df_cette_ville.loc[
                (df_cette_ville["origine_departement"] == "O")
                & (df_cette_ville["origine_ville"] == "N"),
                ["mobility"],
            ] = ["Départementale"]
            df_cette_ville.loc[
                (df_cette_ville["origine_region"] == "O")
                & (df_cette_ville["origine_departement"] == "N")
                & (df_cette_ville["origine_ville"] == "N"),
                ["mobility"],
            ] = ["Régionale"]
            df_cette_ville.loc[
                (df_cette_ville["origine_nationale"] == "O")
                & (df_cette_ville["origine_region"] == "N")
                & (df_cette_ville["origine_departement"] == "N")
                & (df_cette_ville["origine_ville"] == "N"),
                ["mobility"],
            ] = ["Nationale"]

            df_m = df_cette_ville.groupby(
                ["classe_age", "mobility"], as_index=False, observed=True
            ).agg(nb_deces=("idligne", "count"))

            fig = go.Figure()
            # 
            fig.add_scatter(
                y=df_m["mobility"],
                x=df_m["classe_age"],
                text=df_m["classe_age"],  # colonne à afficher
                hoverinfo="text+x+y",  # ce qui apparaît
                mode="markers",
                marker=dict(
                    colorscale="Viridis",  # affiche le type de couleur de la colorbar
                    colorbar=dict(  # personnalise la colorbar
                        title="Décès",  #
                        tickmode="array",  # ← IMPORTANT
                    ),
                    size=df_m[
                        "nb_deces"
                    ],  # affiche la taille en fonction de cette valeur
                    color=df_m[
                        "nb_deces"
                    ],  # affiche la couleur en fonction de cette valeur
                    showscale=True,  # affiche la colorbar
                    sizemode="area",  # defini la zone d'utilisation homogenéité des marqueurs par defaut cette valeur
                    opacity=0.6,
                ),
            )

            fig.update_layout(
                yaxis=dict(
                    categoryorder="array",
                    categoryarray=labels,  # essayons d'ordonner nos categories sur l'ordonnées
                )
            )

            if (
                self.df_fnl[self.df_fnl["origine_ville"] == "O"]["origine_ville"].shape[
                    0
                ]
                == 0
                and secteurs_mobiles == True
            ):
                # Annotation pour notifier l'abscence d'originaire
                fig.add_annotation(
                    x=0.5,  #
                    y=0.5,
                    xref="paper",
                    yref="paper",  # paper =coordonées relative à la feuille
                    showarrow=False,
                    text="Pas d'originaire décédé pour cette ville",
                    font=dict(size=19, color="blue"),
                )
            # preparation du titre du graphe
            le_titre = f"Quelle est la mobilité sectorielle des défunts à {self.df_fnl.iloc[0,self.pos_col_ville_deces]}"

            fig.update_layout(
                title=le_titre,
                xaxis={"title": "Classe âge"},
                yaxis={"title": "Mobilité"},
                paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
                height=height_val,
                width=500,
            )
            return fig
