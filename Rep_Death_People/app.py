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

# Le titre :
st.title(":material/travel_explore: Trajectoires de vie en France")

st.header("Insights pour sociétés de service et politiques publiques")
 
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
 
# -------------------------------------------------------------------------------------
st.write(":material/people: R.Jean - www.linkedin.com/in/roselyne-jean/ / :material/dataset_linked: www.insee.fr/fr/statistiques")
