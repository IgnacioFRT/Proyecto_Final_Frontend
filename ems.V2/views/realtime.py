import streamlit as st
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from services.influx_service import get_latest_data



def create_currents_bar(il1, il2, il3):
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=["L1", "L2", "L3"],
        y=[il1, il2, il3],
        text=[f"{il1:.2f} A", f"{il2:.2f} A", f"{il3:.2f} A"],
        textposition="auto",
        marker=dict(color=colors)
    ))

    fig.update_layout(
        title="Corriente por fase",
        template="plotly_white",
        height=380,
        margin=dict(l=20, r=20, t=50, b=20),
        yaxis_title="Corriente (A)"
    )
    return fig

def create_voltage_bar(v1, v2, v3):
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]  # MISMA PALETA

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=["UL1N", "UL2N", "UL3N"],
        y=[v1, v2, v3],
        text=[f"{v1:.1f} V", f"{v2:.1f} V", f"{v3:.1f} V"],
        textposition="auto",
        marker=dict(color=colors)
    ))

    fig.update_layout(
        title="Tensión por fase",
        template="plotly_white",
        height=380,
        margin=dict(l=20, r=20, t=50, b=20),
        yaxis_title="Tensión (V)"
    )
    return fig


def kpi_card(title, value, subtitle=""):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">{title}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{subtitle}</div>
    </div>
    """, unsafe_allow_html=True)


def indicator_card(title, value, subtitle=""):
    st.markdown(f"""
    <div class="kpi-card-indicator">
        <div class="kpi-title-indicator">{title}</div>
        <div class="kpi-value-indicator">{value}</div>
        <div class="kpi-sub-indicator">{subtitle}</div>
    </div>
    """, unsafe_allow_html=True)


def render_realtime():
    st_autorefresh(interval=30000, key="refresh_realtime")

    try:
        data, latest_time = get_latest_data()

        freq = data.get("freq", 0)
        temp = data.get("temp", 0)
        hum = data.get("hum", 0)
        wind = data.get("wind", 0)

        vmed = data.get("Vmed", 0)
        imed = data.get("Imed", 0)

        il1 = data.get("IL1", 0)
        il2 = data.get("IL2", 0)
        il3 = data.get("IL3", 0)

        ul1n = data.get("UL1N", 0)
        ul2n = data.get("UL2N", 0)
        ul3n = data.get("UL3N", 0)

        fp1 = data.get("FP1", 0)
        fp2 = data.get("FP2", 0)
        fp3 = data.get("FP3", 0)

        thdv1 = data.get("THDv1", 0)
        thdv2 = data.get("THDv2", 0)
        thdv3 = data.get("THDv3", 0)

        thdi1 = data.get("THDi1", 0)
        thdi2 = data.get("THDi2", 0)
        thdi3 = data.get("THDi3", 0)

        hora_txt = latest_time.strftime("%d/%m/%Y %H:%M:%S") if latest_time else "--:--:--"
        st.success(f"Último dato recibido: {hora_txt}")

    except Exception as e:
        st.error(f"Error cargando datos en tiempo real: {e}")
        return

    st.markdown("### Variables eléctricas principales")
    c1, c2, c3 = st.columns(3)
    with c1:
        kpi_card("Frecuencia", f"{freq:.2f} Hz", "Red eléctrica")
    with c2:
        kpi_card("Tensión media", f"{vmed:.1f} V", "Promedio actual")
    with c3:
        kpi_card("Corriente media", f"{imed:.2f} A", "Promedio actual")

    st.markdown("### Variables ambientales")
    a1, a2, a3 = st.columns(3)
    with a1:
        kpi_card("Temperatura", f"{temp:.1f} °C", "Ambiente")
    with a2:
        kpi_card("Humedad", f"{hum:.1f} %", "Ambiente")
    with a3:
        kpi_card("Velocidad de viento", f"{wind:.1f} km/h", "Entorno")

    st.markdown("### Variables por fase")
    left, right = st.columns(2)

    with left:
        st.plotly_chart(create_currents_bar(il1, il2, il3), use_container_width=True)

    with right:
        st.plotly_chart(create_voltage_bar(ul1n, ul2n, ul3n), use_container_width=True)

    st.markdown("### Indicadores eléctricos")
    tab1, tab2, tab3 = st.tabs(["Factor de Potencia", "THD Tensión", "THD Corriente"])

    with tab1:
        a, b, c = st.columns(3)
        with a:
            indicator_card("FP1", f"{fp1:.2f}", "Factor de potencia")
        with b:
            indicator_card("FP2", f"{fp2:.2f}", "Factor de potencia")
        with c:
            indicator_card("FP3", f"{fp3:.2f}", "Factor de potencia")

    with tab2:
        a, b, c = st.columns(3)
        with a:
            indicator_card("THDv1", f"{thdv1:.1f} %", "THD tensión")
        with b:
            indicator_card("THDv2", f"{thdv2:.1f} %", "THD tensión")
        with c:
            indicator_card("THDv3", f"{thdv3:.1f} %", "THD tensión")

    with tab3:
        a, b, c = st.columns(3)
        with a:
            indicator_card("THDi1", f"{thdi1:.1f} %", "THD corriente")
        with b:
            indicator_card("THDi2", f"{thdi2:.1f} %", "THD corriente")
        with c:
            indicator_card("THDi3", f"{thdi3:.1f} %", "THD corriente")
