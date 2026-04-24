import streamlit as st


def nav_button(label: str, key: str) -> None:
    active = st.session_state.section == label

    if active:
        st.markdown(
            f"""
            <div style="
                width:100%;
                padding: 10px 12px;
                margin-bottom: 8px;
                border-radius: 8px;
                background-color: #e7f0ff;
                color: #0d47a1;
                font-weight: 700;
                text-align: center;
                border-left: 1px solid #b6d4fe;
            ">
                {label}
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
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
