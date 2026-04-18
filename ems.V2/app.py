import streamlit as st
from styles import load_global_styles
from config import APP_TITLE, APP_SUBTITLE
from services.influx_service import get_latest_data

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

    try:
        data, latest_time = get_latest_data()

        estado = "En línea" if latest_time else "Sin datos"
        estado_color = "#27ae60" if latest_time else "#e74c3c"
        hora_txt = latest_time.strftime("%d/%m/%Y %H:%M") if latest_time else "--:--"

        vmed = data.get("Vmed", 0)
        freq = data.get("freq", 0)
        temp = data.get("temp", 0)

    except Exception as e:
        data = {}
        estado = "Error"
        estado_color = "#e74c3c"
        hora_txt = "--:--"
        vmed = 0
        freq = 0
        temp = 0
        st.error(f"No se pudieron cargar los datos: {e}")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Estado</div>
            <div class="kpi-value" style="color:{estado_color};">{estado}</div>
            <div class="kpi-sub">Estado actual del sistema</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Última sincronización</div>
            <div class="kpi-value" style="font-size:1.3rem;">{hora_txt}</div>
            <div class="kpi-sub">Hora Argentina</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Tensión media</div>
            <div class="kpi-value">{vmed:.1f} V</div>
            <div class="kpi-sub">Promedio actual</div>
        </div>
        """, unsafe_allow_html=True)

    with c4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Frecuencia</div>
            <div class="kpi-value">{freq:.2f} Hz</div>
            <div class="kpi-sub">Red eléctrica</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("### Resumen del sistema")

    a, b = st.columns(2)

    with a:
        st.info(
            "**Hardware de adquisición**\n\n"
            "- Siemens PAC3200\n"
            "- Medición trifásica\n"
            "- Variables eléctricas en tiempo real"
        )

    with b:
        st.info(
            "**Software y visualización**\n\n"
            "- Streamlit\n"
            "- Plotly\n"
            "- InfluxDB"
        )

    st.markdown("### Variables actuales")
    x1, x2, x3 = st.columns(3)
    x1.metric("Temperatura", f"{temp:.1f} °C")
    x2.metric("Frecuencia", f"{freq:.2f} Hz")
    x3.metric("Tensión media", f"{vmed:.1f} V")
