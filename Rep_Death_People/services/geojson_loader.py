import requests
import streamlit as st


@st.cache_data
def load_geojsons():
    """Charge les GeoJSON des régions et départements.""" 

    url_regions = (
    "https://raw.githubusercontent.com/"
    "gregoiredavid/france-geojson/master/regions-version-simplifiee.geojson"
    )

    # "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/regions.geojson" # avant
    # https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/regions-version-simplifiee.geojson

    url_departements = (
        "https://raw.githubusercontent.com/"
        "gregoiredavid/france-geojson/master/departements-version-simplifiee.geojson"
    )

    response_regions = requests.get(url_regions, timeout=30)
    response_regions.raise_for_status()
    geojson_regions = response_regions.json()

    response_departements = requests.get(url_departements, timeout=30)
    response_departements.raise_for_status()
    geojson_departements = response_departements.json()

    return geojson_regions, geojson_departements