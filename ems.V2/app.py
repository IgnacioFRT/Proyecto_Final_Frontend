import streamlit as st
import base64
from pathlib import Path
from styles import load_global_styles
from config import APP_TITLE, APP_SUBTITLE
from services.influx_service import get_latest_data, get_raw_data_count
from views.realtime import render_realtime
from views.historico import render_historico
from streamlit_autorefresh import st_autorefresh

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
        ["Inicio", "Tiempo Real", "Resumen Histórico"]
    )
    st.markdown("---")
    st.caption("Versión nueva del dashboard")


def render_status_banner(status_text, status_type="success"):
    if status_type == "success":
        st.success(f"✅ {status_text}")
    elif status_type == "warning":
        st.warning(f"⚠️ {status_text}")
    else:
        st.error(f"❌ {status_text}")

def render_home():
    st_autorefresh(interval=60000, key="refresh_home")

    st.markdown(f'<div class="main-title">⚡ {APP_TITLE}</div>', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 3, 1])
    with col2:
        logo_path = Path(__file__).parent / "assets" / "logo_utn_frt.jpeg"
        st.markdown(
            f"""
            <div style="text-align: center;">
                <img src="data:image/jpeg;base64,{base64.b64encode(open(logo_path, "rb").read()).decode()}" width="300">
            </div>
            """,
            unsafe_allow_html=True
    )
    st.markdown(f'<div class="sub-title">{APP_SUBTITLE}</div>', unsafe_allow_html=True)
    st.caption("Monitoreo energético y calidad de suministro en tiempo real para la UTN FRT")

    try:
        data, latest_time = get_latest_data()
        raw_count = get_raw_data_count()

        estado = "En línea" if latest_time else "Sin datos"
        estado_color = "#27ae60" if latest_time else "#e74c3c"
        fecha_txt = latest_time.strftime("%d/%m/%Y") if latest_time else "--/--/----"
        hora_txt = latest_time.strftime("%H:%M") if latest_time else "--:--"

        if latest_time:
            render_status_banner("Sistema operativo. Se están recibiendo datos del PAC3200.", "success")
        else:
            render_status_banner("No se encontraron datos recientes.", "warning")

    except Exception as e:
        estado = "Error"
        estado_color = "#e74c3c"
        hora_txt = "--:--"
        raw_count = 0
        render_status_banner(f"No se pudieron cargar los datos: {e}", "error")

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Estado del sistema</div>
            <div class="kpi-value" style="color:{estado_color};">{estado}</div>
            <div class="kpi-sub">Estado actual del monitoreo</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Última sincronización</div>
            <div class="kpi-value">
                {fecha_txt}<br>
                {hora_txt}
            </div>
            <div class="kpi-sub">Hora local Argentina</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Datos crudos recibidos</div>
            <div class="kpi-value">{raw_count:,}</div>
            <div class="kpi-sub">Registros históricos en InfluxDB</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("## Descripción del sistema")

    b1, b2 = st.columns(2)

    with b1:
        st.info(
            "**Hardware de adquisición**\n\n"
            "- Siemens PAC3200\n"
            "- Medición trifásica\n"
            "- Variables eléctricas en tiempo real\n"
            "- Supervisión de calidad de energía"
        )

    with b2:
        st.info(
            "**Software y visualización**\n\n"
            "- Streamlit\n"
            "- InfluxDB\n"
            "- Dashboard técnico para monitoreo\n"
            "- Base para análisis de eficiencia energética"
        )

if section == "Inicio":
    render_home()
elif section == "Tiempo Real":
    render_realtime()
elif section == "Resumen Histórico":
    render_historico()
