import streamlit as st


def initialize_session():

    defaults = {
        "page": 0,
        "filters": None,
        "ecart_type_national": None,
        "moyenne_nationale": None,
        "distance_departement_min": None,
        "distance_departement_max": None,
        "df_ecart_type_moy_age": None,
        "proportion_nat_age": None,
    }

    for key, value in defaults.items():

        if key not in st.session_state:
            st.session_state[key] = value