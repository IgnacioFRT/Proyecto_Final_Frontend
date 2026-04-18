import streamlit as st
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from services.influx_service import get_latest_data


def create_currents_bar(il1, il2, il3):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=["L1", "L2", "L3"],
        y=[il1, il2, il3],
        text=[f"{il1:.2f} A", f"{il2:.2f} A", f"{il3:.2f} A"],
        textposition="auto",
        marker_color=["#1f77b4", "#ff7f0e", "#2ca02c"]
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
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=["UL1N", "UL2N", "UL3N"],
        y=[v1, v2, v3],
        text=[f"{v1:.1f} V", f"{v2:.1f} V", f"{v3:.1f} V"],
        textposition="auto",
        marker_color=["#2E86C1", "#5DADE2", "#85C1E9"]
    ))

    fig.update_layout(
        title="Tensión por fase",
        template="plotly_white",
        height=380,
        margin=dict(l=20, r=20, t=50, b=20),
        yaxis_title="Tensión (V)"
    )
    return fig


def render_realtime():
    st_autorefresh(interval=30000, key="refresh_realtime")

    st.markdown("## Tiempo Real")
    st.caption("Monitoreo instantáneo de variables eléctricas y ambientales")

    try:
        data, latest_time = get_latest_data()

        freq = data.get("freq", 0)
        temp = data.get("temp", 0)
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

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Frecuencia", f"{freq:.2f} Hz")
    c2.metric("Tensión media", f"{vmed:.1f} V")
    c3.metric("Corriente media", f"{imed:.2f} A")
    c4.metric("Temperatura", f"{temp:.1f} °C")

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
        a.metric("FP1", f"{fp1:.2f}")
        b.metric("FP2", f"{fp2:.2f}")
        c.metric("FP3", f"{fp3:.2f}")

    with tab2:
        a, b, c = st.columns(3)
        a.metric("THDv1", f"{thdv1:.1f} %")
        b.metric("THDv2", f"{thdv2:.1f} %")
        c.metric("THDv3", f"{thdv3:.1f} %")

    with tab3:
        a, b, c = st.columns(3)
        a.metric("THDi1", f"{thdi1:.1f} %")
        b.metric("THDi2", f"{thdi2:.1f} %")
        c.metric("THDi3", f"{thdi3:.1f} %")

    st.markdown("### Lectura técnica rápida")

    l1, l2 = st.columns(2)

    with l1:
        if 49 <= freq <= 51:
            st.success("Frecuencia estable.")
        else:
            st.warning("Frecuencia fuera de rango.")

        if min(ul1n, ul2n, ul3n) >= 200 and max(ul1n, ul2n, ul3n) <= 250:
            st.success("Tensiones dentro del rango esperado.")
        else:
            st.warning("Hay tensiones fuera de rango.")

    with l2:
        if min(fp1, fp2, fp3) >= 0.85:
            st.success("Factor de potencia aceptable en las tres fases.")
        else:
            st.warning("Alguna fase presenta factor de potencia bajo.")

        if max(thdv1, thdv2, thdv3) <= 8:
            st.success("THD de tensión dentro de valores razonables.")
        else:
            st.warning("THD de tensión elevado.")
