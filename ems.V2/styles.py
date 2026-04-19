import streamlit as st

def load_global_styles():
    st.markdown("""
    <style>
        footer {visibility: hidden;}
        .stAppDeployButton {display: none;}
        [data-testid="stToolbarActions"] {display: none;}

        /* subir el contenido global */
        .block-container {
            padding-top: 1rem !important;
            padding-bottom: 2rem !important;
        }

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

        .kpi-card {
            background: #f8fafc;
            border: 1px solid #dce3ea;
            border-radius: 14px;
            padding: 18px 14px;
            text-align: center;
            box-shadow: 0 2px 6px rgba(0,0,0,0.04);
            min-height: 145px;
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

        .realtime-title {
            font-size: 2rem;
            font-weight: 700;
            color: #1f2d3d;
            text-align: center;
            margin-top: -10px;
            margin-bottom: 0.2rem;
        }

        .realtime-subtitle {
            text-align: center;
            color: #6b7a89;
            font-size: 0.95rem;
            margin-bottom: 1.2rem;
        }
    </style>
    """, unsafe_allow_html=True)
