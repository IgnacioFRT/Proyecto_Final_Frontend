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
        import base64
        from pathlib import Path

        logo_path = Path(__file__).parent / "assets" / "utn_logo.png"

        with open(logo_path, "rb") as img_file:
            logo_b64 = base64.b64encode(img_file.read()).decode()

        st.markdown(
            f"""
            <div style="
                background-color: #1e2a38;
                border-radius: 10px;
                padding: 15px 10px;
                text-align: center;
                margin-bottom: 20px;
            ">
                <img src="data:image/png;base64,{logo_b64}" width="160">
            </div>
            """,
            unsafe_allow_html=True
        )

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

        st.markdown(
    """
    <div style="
        background: linear-gradient(135deg, #1e3a5f, #2c5282);
        color: white;
        padding: 12px;
        border-radius: 10px;
        text-align: center;
        font-weight: 600;
        margin-top: 10px;
    ">
        Departamento Ingeniería Electrónica
    </div>
    """,
    unsafe_allow_html=True
)

    return st.session_state.section
