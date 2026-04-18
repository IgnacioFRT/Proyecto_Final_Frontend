import streamlit as st
from styles import load_global_styles
from config import APP_TITLE, APP_SUBTITLE

st.set_page_config(
    page_title="EMS - PAC3200 UTN v2",
    layout="wide",
    initial_sidebar_state="expanded"
)

load_global_styles()

with st.sidebar:
    st.title("EMS UTN - v2")
    section = st.radio(
        "Navegación",
        ["Inicio"]
    )
    st.markdown("---")
    st.caption("Versión nueva del dashboard")

if section == "Inicio":
    st.markdown(f'<div class="main-title">⚡ {APP_TITLE}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sub-title">{APP_SUBTITLE}</div>', unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown("""
        <div class="kpi-card">
            <div class="kpi-title">Estado</div>
            <div class="kpi-value" style="color:#27ae60;">En línea</div>
            <div class="kpi-sub">Sistema operativo</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown("""
        <div class="kpi-card">
            <div class="kpi-title">Última sincronización</div>
            <div class="kpi-value">--:--</div>
            <div class="kpi-sub">Hora Argentina</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown("""
        <div class="kpi-card">
            <div class="kpi-title">Energía</div>
            <div class="kpi-value">0.0 kWh</div>
            <div class="kpi-sub">Acumulada</div>
        </div>
        """, unsafe_allow_html=True)

    with c4:
        st.markdown("""
        <div class="kpi-card">
            <div class="kpi-title">Disponibilidad</div>
            <div class="kpi-value">100%</div>
            <div class="kpi-sub">Base inicial</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("### Resumen del sistema")

    a, b = st.columns(2)

    with a:
        st.info(
            "**Hardware de adquisición**\n\n"
            "- Siemens PAC3200\n"
            "- Medición trifásica\n"
            "- Variables eléctricas"
        )

    with b:
        st.info(
            "**Software y visualización**\n\n"
            "- Streamlit\n"
            "- Plotly\n"
            "- InfluxDB"
        )

    st.markdown("### Estado del proyecto")
    st.success("Nueva versión del frontend desplegada correctamente.")
