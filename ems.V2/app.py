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


def render_status_banner(status_text, status_type="success"):
    if status_type == "success":
        st.success(f"✅ {status_text}")
    elif status_type == "warning":
        st.warning(f"⚠️ {status_text}")
    else:
        st.error(f"❌ {status_text}")


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
        imed = data.get("Imed", 0)
        fp1 = data.get("FP1", 0)
        fp2 = data.get("FP2", 0)
        fp3 = data.get("FP3", 0)
        fp_prom = (fp1 + fp2 + fp3) / 3 if any([fp1, fp2, fp3]) else 0

        if latest_time:
            render_status_banner("Sistema operativo. Se están recibiendo datos del PAC3200.", "success")
        else:
            render_status_banner("No se encontraron datos recientes.", "warning")

    except Exception as e:
        estado = "Error"
        estado_color = "#e74c3c"
        hora_txt = "--:--"
        vmed = 0
        freq = 0
        temp = 0
        imed = 0
        fp_prom = 0
        render_status_banner(f"No se pudieron cargar los datos: {e}", "error")

    c1, c2, c3, c4 = st.columns(4)

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
            <div class="kpi-value" style="font-size:1.2rem;">{hora_txt}</div>
            <div class="kpi-sub">Hora local Argentina</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Tensión media</div>
            <div class="kpi-value">{vmed:.1f} V</div>
            <div class="kpi-sub">Promedio trifásico actual</div>
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

    st.markdown("## Resumen operativo")

    a1, a2, a3 = st.columns(3)

    with a1:
        st.metric("Temperatura", f"{temp:.1f} °C")
    with a2:
        st.metric("Corriente media", f"{imed:.2f} A")
    with a3:
        st.metric("FP promedio", f"{fp_prom:.2f}")

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

    st.markdown("## Lectura rápida del sistema")

    col_left, col_right = st.columns(2)

    with col_left:
        if 49 <= freq <= 51:
            st.success("Frecuencia dentro de rango normal.")
        else:
            st.warning("Frecuencia fuera del rango esperado.")

        if 200 <= vmed <= 250:
            st.success("Tensión media en rango aceptable.")
        else:
            st.warning("Tensión media fuera de rango.")

    with col_right:
        if fp_prom >= 0.85:
            st.success("Factor de potencia promedio aceptable.")
        else:
            st.warning("Factor de potencia promedio bajo.")

        if temp <= 35:
            st.success("Temperatura ambiente sin anomalías visibles.")
        else:
            st.warning("Temperatura elevada. Conviene revisar impacto térmico.")
