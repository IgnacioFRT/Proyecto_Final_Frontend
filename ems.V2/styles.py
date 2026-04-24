import streamlit as st

def load_global_styles():
    st.markdown("""
    <style>

        footer {visibility: hidden;}
        .stAppDeployButton {display: none;}
        [data-testid="stToolbarActions"] {display: none;}

        .block-container {
            padding-top: 2rem !important;
            padding-bottom: 2rem !important;
        }

        /* ===== SIDEBAR OSCURO ===== */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f2027, #203a43);
        }

        /* Texto blanco en sidebar */
        [data-testid="stSidebar"] * {
            color: white !important;
        }

        /* ===== TITULOS ===== */
        .main-title {
            font-size: 2.3rem;
            font-weight: 700;
            color: #1f2d3d;
            text-align: center;
            margin-bottom: 0.2rem;
        }

        .sub-title {
            text-align: center;
            color: #6b7a89;
            font-size: 1rem;
            margin-bottom: 1.5rem;
        }

        .section-title {
            font-size: 1.25rem;
            font-weight: 600;
            color: #22313f;
            margin-top: 0.8rem;
            margin-bottom: 0.8rem;
        }

        /* ===== KPI CARDS ===== */
        .kpi-card {
            background: #f8fafc;
            border: 1px solid #dce3ea;
            border-radius: 14px;
            padding: 18px 14px;
            text-align: center;
            box-shadow: 0 2px 6px rgba(0,0,0,0.04);
            min-height: 150px;
        }

        .kpi-title {
            font-size: 0.85rem;
            font-weight: 700;
            color: #6b7a89;
            text-transform: uppercase;
            margin-bottom: 0.4rem;
        }

        .kpi-value {
            font-size: 1.6rem;
            font-weight: 800;
            color: #1f2d3d;
            line-height: 1.2;
        }

        .kpi-sub {
            font-size: 0.9rem;
            color: #7f8c8d;
            margin-top: 0.4rem;
        }

        /* ===== KPI CHICOS ===== */
        .kpi-card-indicator {
            background: #ffffff;
            border: 1px solid #dce3ea;
            border-radius: 12px;
            padding: 14px 10px;
            text-align: center;
            box-shadow: 0 1px 4px rgba(0,0,0,0.04);
            min-height: 105px;
        }

        .kpi-title-indicator {
            font-size: 0.8rem;
            font-weight: 700;
            color: #6b7a89;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
        }

        .kpi-value-indicator {
            font-size: 1.4rem;
            font-weight: 800;
            color: #1f2d3d;
            line-height: 1.2;
        }

        .kpi-sub-indicator {
            font-size: 0.75rem;
            color: #8b97a3;
            margin-top: 0.35rem;
        }

        /* ===== BOTONES SIDEBAR ===== */
        .stButton > button {
            width: 100%;
            border-radius: 8px;
            height: 42px;
            font-weight: 500;
            background-color: #2c5282;
            color: white;
            border: 1px solid #2c5282;
            transition: 0.2s;
        }

        .stButton > button:hover {
            background-color: #2c5282;
            border-color: #4c6ef5;
        }

    </style>
    """, unsafe_allow_html=True)
