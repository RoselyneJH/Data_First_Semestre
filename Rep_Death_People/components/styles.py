import streamlit as st


def apply_styles():
    
    # --- Fond d'écran ---
    st.markdown(
        """
        <style>
        /*  fond d'écran */
        .stApp {
            background-color: #ADD8E6; /* bleu ciel */
        }
        
        /* Changer le fond de la sidebar */
        [data-testid="stSidebar"] {
            background-color: #9ec9d7;
       
        }
        /* Changer la couleur du bouton du Tab */

         button[data-baseweb="tab"] {
            color: #444;
        }

        button[data-baseweb="tab"][aria-selected="true"] {
            color: white;
            background-color: #9ec9d7;
            border-radius: 6px 6px 0 0;
        }
        /* Label du metric */
        [data-testid="stMetricLabel"] {
            font-size: 18px;        
            font-family: 'Arial', sans-serif;
            color: #555555;
        }

        /* Valeur principale */
        [data-testid="stMetricValue"] {
            font-size: 18px;        
            color: #1f77b4;
        }
        /* Delta */
        [data-testid="stMetricDelta"] {
            font-size: 16px;
            font-family: 'Courier New', monospace;
        }
        
        /* Valeurs du slider (20, 85) */
            .stSlider * {
                color: #00090f !important;
        }
        /* Taille des icone dite Material*/
        span[style*="Material Symbols Rounded"] {
            font-size: 26px !important;  
            vertical-align: -0.25em !important;             
        }

        </style>
        """,
        unsafe_allow_html=True,
    )