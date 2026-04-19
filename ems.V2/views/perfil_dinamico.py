import streamlit as st
import plotly.graph_objects as go
from services.influx_service import get_latest_data


def render_perfil_dinamico():
    st.markdown("## Perfil dinámico")
    st.caption("Vista de comportamiento dinámico del sistema eléctrico")

    try:
        data, latest_time = get_latest_data()

        freq = data.get("freq", 0)
        vmed = data.get("Vmed", 0)
        imed = data.get("Imed", 0)
        temp = data.get("temp", 0)

        hora_txt = latest_time.strftime("%d/%m/%Y %H:%M:%S") if latest_time else "--:--:--"

        st.success(f"Último dato recibido: {hora_txt}")

    except Exception as e:
        st.error(f"Error cargando perfil dinámico: {e}")
        return

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Frecuencia</div>
            <div class="kpi-value">{freq:.2f} Hz</div>
            <div class="kpi-sub">Estado instantáneo</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Tensión media</div>
            <div class="kpi-value">{vmed:.1f} V</div>
            <div class="kpi-sub">Promedio actual</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Corriente media</div>
            <div class="kpi-value">{imed:.2f} A</div>
            <div class="kpi-sub">Promedio actual</div>
        </div>
        """, unsafe_allow_html=True)

    with c4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Temperatura</div>
            <div class="kpi-value">{temp:.1f} °C</div>
            <div class="kpi-sub">Ambiente</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("### Vista preliminar")

    fig = go.Figure()
    fig.add_trace(go.Indicator(
        mode="gauge+number",
        value=freq,
        title={"text": "Frecuencia instantánea"},
        gauge={
            "axis": {"range": [45, 55]},
            "bar": {"color": "#1f77b4"},
            "steps": [
                {"range": [45, 49], "color": "#f8d7da"},
                {"range": [49, 51], "color": "#d4edda"},
                {"range": [51, 55], "color": "#fff3cd"},
            ],
        },
    ))

    fig.update_layout(height=350, margin=dict(l=20, r=20, t=60, b=20))
    st.plotly_chart(fig, use_container_width=True)

    st.info("Esta ventana puede evolucionar luego a perfil dinámico de carga, variación temporal y comportamiento por fase.")
