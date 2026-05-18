import polars as pl
import pandas as pd
from typing import Tuple

##############################################################################
#                      CLASSE POUR SCORING DATAVIZ
##############################################################################

SECTEUR_EXOGENE = "Exogènes majoritaires sur ces zones"
SECTEUR_ORIGINAIRE = "Originaires majoritaires sur ces zones"
SECTEUR_NEUTRE = "Equilibre Originaires - Exogènes sur ces zones"
PAGE_SIZE = 20 # nombre de secteur affiché dans le graphe

class ClsScorePourViz:    

    def __init__(self, df:pd.DataFrame, ce_ss_secteur:str, cette_origine:str):
        """
        Initialise un filtre des personnes decedées pour restitution des scores

        Args:
            df            : dataframe
            ce_ss_secteur : le sous secteur
            cette_origine : origine des deces
        
        """
        self.df = df
        self.ce_ss_secteur = ce_ss_secteur
        self.cette_origine = cette_origine

        self.df_cumul_secteur = pd.DataFrame()
        self.nb_origine = ""

        self._nb_secteur_sans_deces_originaire = 0

        self.liste_departement_naissance_treemap=list()
        # une liste de dataframe
        self.liste_df_cumul_secteur=[]
        # nombre de pages
        self.pages = 0           

    def score_secteur(self, filtrer_age:bool = False)-> Tuple[pd.DataFrame, str, str, str, str] :
        '''
            Args    : Dataframe à transformer
                    sous_secteur à filtrer
                    origine du sous secteur
            Return  : Dataframe transformé
            Process : Comptabilité liée par sous-secteurs sur deces exogènes,
                    deces originaires, distance selon age
                    Utilisation de Polars pour performance
        '''
        #profondeur_resultat = 100 # pas sure de pertinence !
        # Petite manip liée à un décallage des niveaux de secteur
        le_string = self.ce_ss_secteur    
        cette_origine_1 ="origine_"+le_string.replace("_deces","")
        cette_origine =cette_origine_1.replace("_nom","")
        
        # item qui permet de préparer le filtre/groupby : paramétrage
        nb_non_origine = "nb_non_origine_"+self.cette_origine.replace("origine_", "")
        nb_origine = "nb_origine_"+cette_origine.replace("origine_", "")
        
        distance_non_origine = "distance_non_origine_"+self.cette_origine.replace("origine_", "")
        distance_origine = "distance_origine_"+self.cette_origine.replace("origine_", "")    
            
        # liste des secteurs descendant afin de préparer le filtre 
        liste_secteur = ['pays_naissance', 'nom_region_deces', 'nom_departement_deces','ville_deces']
        # initialisation de la liste
        liste_a_traiter = []
        position= -1
        # processus de creation du groupby/filtre sectoriel  
        for ind, item in enumerate(liste_secteur):
            if position<0:
                liste_a_traiter.append(item)
            if item == self.ce_ss_secteur and ind>0:
                position = ind

        la_liste_sans_filtre_age = liste_a_traiter.copy()

        if filtrer_age:
            # ajout du dernier critère si souhaité : age,  
            liste_a_traiter.append("classe_age")  

        # transforme pandas en polar 
        mon_pl = pl.DataFrame(self.df)
        
        # FILTRAGE/GROUPBY
        pl_cumul_secteur = (
            mon_pl.lazy()
            .filter(pl.col("pays_naissance").is_in(["FRANCE"] ))  # !
            .group_by(liste_a_traiter)
            .agg([
                pl.col("idligne").count().alias("deces"),
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
                (pl.col("item_nb_origine")/
                (pl.col("item_nb_origine")+pl.col("item_nb_non_origine") )
                ).round(3).alias("TAFV") # Taux d'attractivité de fin de vie
            ])
            
            .collect()
            )  
        # En ajoutant la clonne age à ntre dataframe, on perd l'ordre des secteurs,
        # d'ou l'interet de ce regroupement => conserver l'ordre
        pl_cumul_secteur_restreint = (
            mon_pl.lazy()
            .filter(pl.col("pays_naissance").is_in(["FRANCE"] )) 
            .group_by(la_liste_sans_filtre_age)
            .agg(
                #(pl.col(cette_origine) == "N").sum().alias("item_nb_non_origine")
                (pl.col("idligne").count().alias("deces"))
                ) #,

            .with_columns(
                #pl.col("item_nb_non_origine")
                pl.col("deces")
                .rank(descending=True)
                #.over(la_liste_sans_filtre_age)
                .alias("rang_deces").cast(pl.Int64)
            )
            .collect()
        )

        df = pl_cumul_secteur.join(pl_cumul_secteur_restreint, on=la_liste_sans_filtre_age, how="inner")
        
        # Je supprime cette colonne resultant du merge :
        df = df.drop("deces_right")

        # transforme polar en pandas)
        df_cumul_secteur_ = df.to_pandas()
        
        # renommer les colonnes correctement 
        df_cumul_secteur_.rename(columns={'item_nb_non_origine': nb_non_origine,
                                        'item_nb_origine': nb_origine,
                                        'item_distance_non_origine': distance_non_origine,
                                        'item_distance_origine': distance_origine, 
                                        },inplace =True)        
        
        df_cumul_secteur = df_cumul_secteur_.sort_values("deces", ascending=False)          
        
        # Nombre de page à visualiser pour parcourir l'ensemble des données
        reste = 1 if (df_cumul_secteur_['rang_deces'].max() % PAGE_SIZE) > 0 else 0 # opérateur ternaire 
        # C'est le script que j'aurais du mettre en place mais je préfère plafonner :
        #df_cumul_secteur_["rang_deces"].max()//PAGE_SIZE + reste # <= c'est le alcul du nombre de pages

        # identifier le nombre de page max
        page_max = df_cumul_secteur_["rang_deces"].max()//PAGE_SIZE + reste
        
        # Mais je dois plafonner le nmbre de pages car j'ai des pbs de performance à 3 oiu moins selon cas
        self.pages = page_max if page_max <= 4 else 4
        
        # Chargement de la liste des dataframes  pour simuler les differentes pages      
        p=1
        for i in range(1,self.pages+1):                                
            self.liste_df_cumul_secteur.append(df_cumul_secteur_[(df_cumul_secteur_['rang_deces'] >= p) & (df_cumul_secteur_['rang_deces'] < p + PAGE_SIZE )])          
            p = (i*PAGE_SIZE)+1
                
        self.df_cumul_secteur = df_cumul_secteur
        self.nb_origine = nb_origine
        # recupère le nombre de secteur sans deces originaire
        self._nb_secteur_sans_deces_originaire = self.df_cumul_secteur[self.df_cumul_secteur[self.nb_origine]==0].shape[0]
         
        return df_cumul_secteur, "deces" #nb_non_origine

    def identification_top_treemap(self, df:pd.DataFrame, la_ville:str, top_n:int=5):
        '''
        Permet de constituer le top pour le treemap
            Args :  
                    le dataframe à considérer
                    la ville sur laquelle le treemap est effectuée
                    le nombre de profondeur de niveau realisé
            Return : -> pl.Dataframe
                    renvoie un dataframe Top
        '''
        liste_a_traiter=['nom_departement_naissance']
        niveau = "nom_departement_naissance" 

        mon_pl = pl.DataFrame(df)

        nb_orig =  (mon_pl.lazy()
                    .filter(pl.col("ville_deces")==la_ville)  
                    .group_by(liste_a_traiter)
                    .agg([                       
                        (pl.col("origine_ville") == "O").sum().alias("nb"),
                    ]).collect()
        )
        nb_orig = nb_orig["nb"].sum()

        df_non_orig =  (mon_pl.lazy()
                            .filter(pl.col("ville_deces")==la_ville)   
                            .group_by(liste_a_traiter)
                            .agg([                       
                                (pl.col("origine_ville") == "N").sum().alias("nb"),
                            ]).collect()
        )

        top = (
            df_non_orig
            .group_by(niveau)
            .agg(
                pl.col("nb").max().alias("somme")
            )
            .sort("somme", descending=True)
            .head(top_n)
        )
        
        return top,nb_orig,df_non_orig
    
    def preparation_treemap(self, df:pd.DataFrame,la_ville:str, top_n:int=5):

        '''
        Permet de constituer le dataframe pour le treemap et de transmettre 
        en attribut dataframe du le top_n des departements de naissance  
            Args :  
                    le dataframe à considérer
                    la ville sur laquelle le treemap est effectuée
                    le nombre de profondeur de niveau realisé
            Return : -> pd.Dataframe
                    renvoie un dataframe pour le treemap
        '''         
        niveau = "nom_departement_naissance"    
       
        mon_pl = pl.DataFrame(df)

        top,nb_orig,df_non_orig =  self.identification_top_treemap(df,la_ville)
        # Recupérer la liste des départements de naissance trouvés
        self.liste_departement_naissance_treemap = top["nom_departement_naissance"].to_list()
        
        # 4️⃣ Calcul "Autres"
        total_non_orig = df_non_orig.select(pl.col("nb").sum().alias("max_count")).item() #df_non_orig.height
        autres = total_non_orig - top["somme"].sum()
        # 5️⃣ Construction du dataframe final (Polars)

        # Gestion des colonnes qi vont être difficile à caster
        nb_orig = int(nb_orig) if nb_orig is not None else 0
        autres = int(autres) if autres is not None else 0

        df_treemap = pl.concat([
            pl.DataFrame({
                "statut": ["Originaire"],
                "origine": [la_ville],
                "valeur": [nb_orig]
            }).with_columns(pl.col("valeur")),

            top.select([
                pl.lit("Exogene").alias("statut"),
                pl.col(niveau).alias("origine"),
                pl.col("somme").cast(pl.Int64).alias("valeur")
            ]),

            pl.DataFrame({
                "statut": ["Exogene"],
                "origine": ["Autres"],
                "valeur": [autres]
            }).with_columns(pl.col("valeur"))
        ])

        # 6️⃣ Conversion vers pandas pour Plotly
        df_plot = df_treemap.to_pandas()

        return df_plot


    @property
    def etat_global_de_ces_secteurs(self):
        '''
            Args    : Le df
    
            Return  : le type de secteurs selectionnés : exogène ou originaire ?            
        '''
        nb_valeur_sup =self.df_cumul_secteur[self.df_cumul_secteur['TAFV']>0.59]['TAFV'].shape[0]
        nb_valeur_inf =self.df_cumul_secteur[self.df_cumul_secteur['TAFV']<0.31]['TAFV'].shape[0]
        nb_valeur_neutre =self.df_cumul_secteur[(self.df_cumul_secteur['TAFV']>0.30) & (self.df_cumul_secteur['TAFV']<0.60)]['TAFV'].shape[0]

        if (nb_valeur_neutre> nb_valeur_sup and nb_valeur_neutre> nb_valeur_inf ):
            return SECTEUR_NEUTRE
        else:
            if nb_valeur_inf==nb_valeur_sup:
                return SECTEUR_NEUTRE
            else:
                if nb_valeur_sup>nb_valeur_inf:
                    return SECTEUR_ORIGINAIRE
                else:
                    return SECTEUR_EXOGENE                
    
    @property
    def nb_secteur_sans_deces_originaire(self):
        '''
            Args    : Le df
      
            Return  : combien de secteurs sans originaire ?            
        '''    
       
        return self._nb_secteur_sans_deces_originaire
    
    @property
    def liste_des_df_secteur(self):
        return self.liste_df_cumul_secteur

    @property
    def start(self, la_page:int=1):
        '''
            Args    : Par défaut la première page
      
            Return  : Debut de la page            
        '''  
        self.start = la_page * PAGE_SIZE
        return self.start
    
    @property
    def start(self):
        '''
            Args    : None
      
            Return  : Fin de la page            
        ''' 
        self.end = self.start + PAGE_SIZE
        return self.end
        

