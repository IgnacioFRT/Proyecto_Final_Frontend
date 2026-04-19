import streamlit as st
import plotly.graph_objects as go

from services.influx_service import get_latest_data


def render_calidad_qos():
    try:
        data, latest_time = get_latest_data()

        freq = float(data.get("freq", 0))
        vmed = float(data.get("Vmed", 0))
        fp1 = float(data.get("FP1", 0))
        fp2 = float(data.get("FP2", 0))
        fp3 = float(data.get("FP3", 0))

        thdv1 = float(data.get("THDv1", 0))
        thdv2 = float(data.get("THDv2", 0))
        thdv3 = float(data.get("THDv3", 0))

        thdi1 = float(data.get("THDi1", 0))
        thdi2 = float(data.get("THDi2", 0))
        thdi3 = float(data.get("THDi3", 0))

        fp_prom = (fp1 + fp2 + fp3) / 3 if any([fp1, fp2, fp3]) else 0

        # Estado general simple
        if 49 <= freq <= 51 and 210 <= vmed <= 240:
            estado = "Aceptable"
            estado_color = "#27ae60"
        elif 48 <= freq <= 52 and 200 <= vmed <= 250:
            estado = "Advertencia"
            estado_color = "#f39c12"
        else:
            estado = "Crítico"
            estado_color = "#e74c3c"

        hora_txt = latest_time.strftime("%d/%m/%Y %H:%M") if latest_time else "--:--"

    except Exception as e:
        st.error(f"Error cargando calidad QoS: {e}")
        return

    st.success(f"Último dato recibido: {hora_txt}")

    # ===== KPI SUPERIOR =====
    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Frecuencia</div>
            <div class="kpi-value">{freq:.2f} Hz</div>
            <div class="kpi-sub">Red eléctrica</div>
        </div>
        """, unsafe_allow_html=True)

    with k2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Tensión media</div>
            <div class="kpi-value">{vmed:.1f} V</div>
            <div class="kpi-sub">Promedio actual</div>
        </div>
        """, unsafe_allow_html=True)

    with k3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">FP promedio</div>
            <div class="kpi-value">{fp_prom:.2f}</div>
            <div class="kpi-sub">Factor de potencia</div>
        </div>
        """, unsafe_allow_html=True)

    with k4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Estado de calidad</div>
            <div class="kpi-value" style="color:{estado_color};">{estado}</div>
            <div class="kpi-sub">Evaluación general</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)

    # ===== THD TENSIÓN / THD CORRIENTE =====
    c1, c2 = st.columns(2)

    colores_fase = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    with c1:
        st.markdown("#### THD de tensión por fase")

        fig_thdv = go.Figure()
        fig_thdv.add_trace(go.Bar(
            x=["L1", "L2", "L3"],
            y=[thdv1, thdv2, thdv3],
            marker_color=colores_fase,
            text=[f"{thdv1:.2f}%", f"{thdv2:.2f}%", f"{thdv3:.2f}%"],
            textposition="auto"
        ))

        fig_thdv.update_layout(
            height=360,
            template="plotly_white",
            margin=dict(t=30, b=20, l=20, r=20),
            yaxis_title="%"
        )
        st.plotly_chart(fig_thdv, use_container_width=True)

    with c2:
        st.markdown("#### THD de corriente por fase")

        fig_thdi = go.Figure()
        fig_thdi.add_trace(go.Bar(
            x=["L1", "L2", "L3"],
            y=[thdi1, thdi2, thdi3],
            marker_color=colores_fase,
            text=[f"{thdi1:.2f}%", f"{thdi2:.2f}%", f"{thdi3:.2f}%"],
            textposition="auto"
        ))

        fig_thdi.update_layout(
            height=360,
            template="plotly_white",
            margin=dict(t=30, b=20, l=20, r=20),
            yaxis_title="%"
        )
        st.plotly_chart(fig_thdi, use_container_width=True)

    st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)

    # ===== RESUMEN INFERIOR =====
    r1, r2, r3 = st.columns(3)

    with r1:
        texto_freq = "Dentro de rango" if 49 <= freq <= 51 else "Fuera de rango"
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Frecuencia</div>
            <div class="kpi-value">{texto_freq}</div>
            <div class="kpi-sub">{freq:.2f} Hz medidos</div>
        </div>
        """, unsafe_allow_html=True)

    with r2:
        mayor_thdv = max(thdv1, thdv2, thdv3)
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Máximo THD tensión</div>
            <div class="kpi-value">{mayor_thdv:.2f}%</div>
            <div class="kpi-sub">Peor fase medida</div>
        </div>
        """, unsafe_allow_html=True)

    with r3:
        mayor_thdi = max(thdi1, thdi2, thdi3)
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Máximo THD corriente</div>
            <div class="kpi-value">{mayor_thdi:.2f}%</div>
            <div class="kpi-sub">Peor fase medida</div>
        </div>
        """, unsafe_allow_html=True)
