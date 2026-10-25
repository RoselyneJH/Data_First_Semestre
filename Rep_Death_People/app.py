#####################################################################################
#                                                                                   #
#                               A P P L I C A T I O N                               #
#                                 S T R E A M L I T                                 #
#                                                                                   #
#####################################################################################

import streamlit as st

from components.session import initialize_session
from components.styles import apply_styles


st.set_page_config(
    layout="wide",
    page_title="Dynamiques territoriales",
)

# Le titre
st.title("Trajectoires de vie : dynamiques et attractivités territoriales en 2024")
st.header("Insights pour sociétés de service et politiques publiques")
#st.subheader("Trajectoires de vie des défunts en France en 2024")

# -------------------------------------------------------------------------------------
st.write("Auteur : R.Jean / Source : https://www.insee.fr/fr/statistiques")
# -------------------------------------------------------------------------------------

st.set_page_config(
    page_title="Dynamiques et attractivités des territoires",
    page_icon="📊",
    layout="wide",
)

page_analyse = st.Page(
    "pages/1_📊_Analyse_territoriale.py",
    title="Analyse territoriale",
    icon="📊",
    default=True,
)

pg = st.navigation(
    {
        "Analyse": [
            page_analyse,            
        ],
    }
)

pg.run()