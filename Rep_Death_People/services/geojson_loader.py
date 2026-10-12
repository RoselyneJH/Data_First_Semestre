import requests
import streamlit as st


@st.cache_data
def load_geojsons():
    """Charge les GeoJSON des régions et départements."""

    geojson_regions = requests.get(
        "https://france-geojson.gregoiredavid.fr/repo/regions.geojson"
    ).json()

    geojson_departements = requests.get(
        "https://france-geojson.gregoiredavid.fr/repo/departements.geojson"
    ).json()

    return geojson_regions, geojson_departements