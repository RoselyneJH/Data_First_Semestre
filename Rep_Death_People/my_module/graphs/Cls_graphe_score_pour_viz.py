import polars as pl
import pandas as pd
from typing import Tuple

##############################################################################
#                      CLASSE POUR SCORING DATAVIZ
##############################################################################

SECTEUR_EXOGENE = "Exogènes majoritaires sur ces zones"
SECTEUR_ORIGINAIRE = "Originaires majoritaires sur ces zones"
SECTEUR_NEUTRE = "Equilibre Originaires - Exogènes sur ces zones"

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
        profondeur_resultat = 100
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
        
        if filtrer_age:
            # ajout du dernier critère si souhaité : age,  
            liste_a_traiter.append("classe_age")

        # transforme pandas en polar 
        mon_pl = pl.DataFrame(self.df)
        
        # FILTRAGE/GROUPBY
        pl_cumul_secteur = (
            mon_pl.lazy()
            .filter(pl.col("pays_naissance").is_in(["FRANCE"] )) 
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
                (pl.col("item_nb_origine")/
                (pl.col("item_nb_origine")+pl.col("item_nb_non_origine") )
                ).round(3).alias("TAFV") # Taux d'attractivité de fin de vie
            ])
            # Indice de Mobilité différentielle IMD
            .with_columns([
                pl.when(
                (pl.col("item_distance_origine") == 0) | pl.col("item_distance_origine").is_null()
                ).then(
                    0
                ).otherwise(
                ((pl.col("item_distance_origine"))/
                (pl.col("item_distance_non_origine")+pl.col("item_distance_origine")) )
                ).round(2).alias("IMD") #  
            ])
            # Indice de mobilité différentielle normalisé 
            .with_columns([
                pl.when(pl.col("IMD").is_null()).then(None).otherwise(
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
        df_cumul_secteur_ = pl_cumul_secteur.to_pandas()
        
        # renommer les colonnes correctement 
        df_cumul_secteur_.rename(columns={'item_nb_non_origine': nb_non_origine,
                                        'item_nb_origine': nb_origine,
                                        'item_distance_non_origine': distance_non_origine,
                                        'item_distance_origine': distance_origine, 
                                        },inplace =True)
        
        df_cumul_secteur = df_cumul_secteur_.sort_values(nb_non_origine, ascending=False).head(profondeur_resultat)
                
        self.df_cumul_secteur = df_cumul_secteur
        self.nb_origine = nb_origine

        self._nb_secteur_sans_deces_originaire = self.df_cumul_secteur[self.df_cumul_secteur[self.nb_origine]==0].shape[0]
         
        return df_cumul_secteur,distance_origine,nb_origine, distance_non_origine, nb_non_origine

    def identification_top_treemap(self, df:pd.DataFrame,la_ville:str, top_n:int=5):
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
                    .filter(pl.col("ville_deces")==la_ville)  #.is_in( [ville] )) 
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
        #liste_a_traiter=['nom_departement_naissance']        

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
    
    
        

