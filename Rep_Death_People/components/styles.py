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
        
        /* ============================================================
            MATERIAL SYMBOLS - STREAMLIT 1.53.0
            Gestion des tailles par composant
        ============================================================ */

        /* 1. TITRES */

        /* st.title */
        h1 span[style*="Material Symbols Rounded"] {
            font-size: 1.10em !important;
            vertical-align: -0.12em !important;
        }

        /* st.header */
        h2 span[style*="Material Symbols Rounded"] {
            font-size: 1.35em !important;
            vertical-align: -0.15em !important;
        }

        /* st.subheader */
        h3 span[style*="Material Symbols Rounded"] {
            font-size: 1em !important;
            vertical-align: -0.12em !important;
        }


        /* 2. TEXTE COURANT */

        /* st.write, st.markdown */
        [data-testid="stMarkdownContainer"] 
        span[style*="Material Symbols Rounded"] {
            font-size: 1em !important;
            vertical-align: -0.15em !important;
        }


        /* 3. RADIO BUTTONS */

        /* Radio : icône du libellé principal */
        .stRadio [data-testid="stWidgetLabel"] span[style*="Material Symbols Rounded"] {
            font-size: 1.15em !important;
            vertical-align: -0.15em !important;
        }

       

        /* Icônes dans les options des boutons radio */
        .stRadio label span[style*="Material Symbols Rounded"] {
            font-size: 1.35em !important;
            vertical-align: -0.18em !important;
        }

        /* 4. SLIDER */

        /* Slider : icône du libellé */
        .stSlider [data-testid="stWidgetLabel"] span[style*="Material Symbols Rounded"] {
            font-size: 1.15em !important;
            vertical-align: -0.15em !important;
        }


        /* 5. SELECTBOX */

        /* Selectbox : icône du libellé */
        .stSelectbox [data-testid="stWidgetLabel"] span[style*="Material Symbols Rounded"] {
            font-size: 1.35em !important;
            vertical-align: -0.15em !important;
        }

        /* Icônes dans la valeur sélectionnée */
        .stSelectbox [data-baseweb="select"]
        span[style*="Material Symbols Rounded"] {
            font-size: 0.95em !important;
            vertical-align: -0.12em !important;
        }


        /* 6. CAPTION */

        /* st.caption */
        [data-testid="stCaptionContainer"]
        span[style*="Material Symbols Rounded"] {
            font-size: 0.95em !important;
            vertical-align: -0.12em !important;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )