import streamlit as st


def nav_button(label: str, key: str) -> None:
    active = st.session_state.section == label

    if active:
        st.markdown(
            f"""
            <style>
            div[data-testid="stButton"] button[kind="secondary"][title="{label}"] {{
                background-color: #ff4b4b !important;
                color: white !important;
                border: 1px solid #ff4b4b !important;
                font-weight: 700 !important;
            }}
            </style>
            """,
            unsafe_allow_html=True
        )

    if st.button(label, key=key, use_container_width=True):
        st.session_state.section = label
        st.rerun()


def render_sidebar() -> str:
    if "section" not in st.session_state:
        st.session_state.section = "Inicio"

    with st.sidebar:
        st.title("EMS UTN - v2")

        st.markdown("### Monitoreo")
        nav_button("Inicio", "btn_inicio")
        nav_button("Tiempo Real", "btn_tiempo_real")

        st.markdown("### Análisis energético")
        nav_button("Resumen Histórico", "btn_resumen")
        nav_button("Perfil Dinámico", "btn_perfil")
        nav_button("Detección de Anomalías", "btn_anomalias")

        st.markdown("### Impacto y gestión")
        nav_button("Calidad QoS", "btn_qos")
        nav_button("Huella de Carbono", "btn_huella")
        nav_button("Impacto Climático", "btn_clima")

        st.markdown("---")
        st.caption("Versión nueva del dashboard")

    return st.session_state.section
