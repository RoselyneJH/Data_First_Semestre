import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
# Gestion des couleurs
import plotly.colors as pc

def render_graph_bar_month(df_bar_month: pd.DataFrame, origine_secteur: str ):
    # Ordre des mois 
    ordre_mois = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    # Séparation des données par origine
    df_O = df_bar_month[df_bar_month[origine_secteur] == "O"]
    df_N = df_bar_month[df_bar_month[origine_secteur] == "N"]

    # Création de la figure
    fig = go.Figure()

    somme = df_O["nb_deces"].sum() + df_N["nb_deces"].sum()
    le_max= df_O["nb_deces"].max() + df_N["nb_deces"].max()

    fig.add_bar(
        x=df_O["month_deces"],
        y=df_O["nb_deces"],
        name="Origine O",
        marker_color='darkorange',
    )

    fig.add_bar(
        x=df_N["month_deces"],
        y=df_N["nb_deces"],
        name="Origine N",
        marker_color='steelblue',
    )

    fig.update_xaxes(
        categoryorder="array",
        categoryarray=ordre_mois,
    )

    # Annotation avec variable
    fig.add_annotation(
        x=1,
        y=le_max*1.1,
        text=f"Total décès : {somme}",
        showarrow=False,
        #bgcolor="lightyellow",
    )
    
    fig.update_layout(
        title="Quelles sont les mois ou la mortalité est importante ?",  #
        plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
        paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
        height=600,
        width=400,
        barmode="stack", #  Empilement explicite
        xaxis_title="Mois",
        yaxis_title="Nombre de décès", 
        # barmode="stack",
    )
    
    return fig
    
def render_graph_bar_class_age_month(df_bar_month_: pd.DataFrame, origine_secteur: str ):
    # Ordre des mois 
    ordre_mois = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    # Palette des couleurs : 
    # https://www.geeksforgeeks.org/python/python-plotly-how-to-set-up-a-color-palette/
    colors = pc.qualitative.Pastel #Prism #Vivid
    
    ordre_cls_age = ["0-1", "1-20", "20-35", "35-50", "50-65", "65-90", "90+"]

    # Je respecte le regroupement de mes categories
    df_bar_month = df_bar_month_.groupby(["classe_age","month_deces"],
                                    observed=True, as_index=False).agg({'nb_deces':'sum'})
    
    # Je trie correctement 
    df_bar_month["premier_numero"] = df_bar_month["classe_age"].str.extract(r"(\d+)").astype(int)
    ordre_cls_age_un_car =sorted(list(set(df_bar_month["premier_numero"])),reverse = False)

    df_bar_month["classe_age_1"] = pd.Categorical(df_bar_month["premier_numero"], 
                                                  ordre_cls_age_un_car, 
                                                  ordered=True)

    df_bar_month["month_deces_1"] = pd.Categorical(df_bar_month["month_deces"], 
                                                   ordre_mois, 
                                                   ordered=True)

    df_bar_month = df_bar_month.sort_values(["classe_age_1", 
                                    "month_deces_1"]).drop(columns=["classe_age_1", "month_deces_1"])
    
    # Création de la figure
    fig = go.Figure()
    somme=0;le_max = 0
    
    for i, cls_age in enumerate(ordre_cls_age):
        if df_bar_month[df_bar_month["classe_age"]==cls_age]['nb_deces'].count()>0:
            df_age = df_bar_month[df_bar_month["classe_age"]==cls_age]
            somme= somme + df_age['nb_deces'].sum()
            le_max = le_max + df_age['nb_deces'].max()
            
            # Reindexation sur tous les mois
            df_age = (
                df_age
                .assign(month_deces=df_age["month_deces"].astype(str).str.strip())
                .query("month_deces in @ordre_mois")
                .set_index("month_deces")
                .reindex(ordre_mois)
                .fillna({"nb_deces": 0})
                .reset_index()
            )

            fig.add_bar(
                x=df_age["month_deces"],
                y=df_age['nb_deces'],
                name=cls_age,
                marker_color=colors[i % len(colors)], # gestion des couleurs en modulo
            )
    # ordonner axe des abscisses
    fig.update_xaxes(
        categoryorder="array",
        categoryarray=ordre_mois,
    )
 
    
    # Annotation avec variable
    fig.add_annotation(
        x=1,
        y=le_max*(1.1),
        text=f"Total décès : {somme}",
        showarrow=False,
        #bgcolor="lightyellow",
    )
   
    fig.update_layout(
        title="Quelle est la mortalité mensuelle selon les classes d'âge ?",  #
        plot_bgcolor="#ADD8E6",  # zone de tracé transparente (fond de la zone de tracé)
        paper_bgcolor="#ADD8E6",  # fond autour du tracé transparent (fond du “papier” autour du tracé)
        height=600,
        width=400,
        barmode="stack", #  Empilement explicite  stack
        xaxis_title="Mois",
        yaxis_title="Nombre de décès", 
        hovermode="x unified",
    )
    
    return fig