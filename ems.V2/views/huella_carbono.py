import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from views.historico import get_historical_data


def render_huella_carbono():
    try:
        with st.spinner("Calculando métricas de emisiones y tendencias... ⏳"):
            df = get_historical_data().copy()

            if df.empty:
                st.warning("No se encontraron datos históricos para la huella de carbono.")
                return

            # ===== 1. PARÁMETROS GLOBALES =====
            FACTOR_EMISION = 0.5  # kg CO2 / kWh

            total_kwh_real = float(df["EA_imp_T1_kwh"].max() - df["EA_imp_T1_kwh"].min())
            total_co2_kg = total_kwh_real * FACTOR_EMISION

            arboles_equivalentes = total_co2_kg / 22
            km_auto_equivalente = total_co2_kg / 0.12

            # ===== 2. DATOS DIARIOS =====
            df_diario = pd.DataFrame()
            df_diario["EA_max"] = df.resample("D")["EA_imp_T1_kwh"].max()
            df_diario["consumo_kWh"] = df_diario["EA_max"].diff().clip(lower=0).fillna(0)
            df_diario["emisiones_diarias"] = df_diario["consumo_kWh"] * FACTOR_EMISION
            df_diario["emisiones_acumuladas"] = df_diario["emisiones_diarias"].cumsum()
            df_diario = df_diario[df_diario["consumo_kWh"] >= 0].copy()

            # ===== 3. DATOS MENSUALES =====
            df_mensual = df_diario.resample("MS")["emisiones_diarias"].sum()
            df_mensual = df_mensual[df_mensual > 0]

            meses_nombres = df_mensual.index.strftime("%b %Y").str.capitalize()

            # ===== 4. MÉTRICAS EXTRA =====
            pico_diario_co2 = float(df_diario["emisiones_diarias"].max()) if not df_diario.empty else 0.0
            fecha_pico = (
                df_diario["emisiones_diarias"].idxmax().strftime("%d/%m/%Y")
                if not df_diario.empty else "--/--/----"
            )

        # ===== FILA 1: KPIs =====
        k1, k2, k3 = st.columns(3)

        with k1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Huella de carbono total</div>
                <div class="kpi-value">{total_co2_kg:,.1f} kg CO₂</div>
                <div class="kpi-sub">Factor: {FACTOR_EMISION:.2f} kg/kWh</div>
            </div>
            """, unsafe_allow_html=True)

        with k2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Equivalencia forestal</div>
                <div class="kpi-value">{arboles_equivalentes:,.0f}</div>
                <div class="kpi-sub">Árboles equivalentes / año</div>
            </div>
            """, unsafe_allow_html=True)

        with k3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Equivalencia vehicular</div>
                <div class="kpi-value">{km_auto_equivalente:,.0f} km</div>
                <div class="kpi-sub">Vehículo a combustión</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='margin-top: 18px;'></div>", unsafe_allow_html=True)

        # ===== FILA 2: TENDENCIA MENSUAL =====
        st.markdown("#### Tendencia mensual de la huella de carbono")

        fig_mensual = go.Figure()
        fig_mensual.add_trace(go.Bar(
            x=meses_nombres,
            y=df_mensual.values,
            marker_color="#27ae60",
            text=[f"{v:,.1f} kg" for v in df_mensual.values],
            textposition="auto",
            hovertemplate="<b>%{x}</b><br>Emisiones: <b>%{y:,.2f} kg CO₂</b><extra></extra>"
        ))

        fig_mensual.update_layout(
            height=360,
            margin=dict(t=30, b=20, l=20, r=20),
            template="plotly_white",
            yaxis=dict(title="kg CO₂", gridcolor="#e5e8e8"),
            xaxis=dict(gridcolor="#e5e8e8")
        )
        st.plotly_chart(fig_mensual, use_container_width=True)

        st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)

        # ===== FILA 3: DIARIO + ACUMULADO =====
        st.markdown("#### Huella de carbono en el tiempo")

        fig_tiempo = go.Figure()

        fig_tiempo.add_trace(go.Bar(
            x=df_diario.index,
            y=df_diario["emisiones_diarias"],
            name="Emisión diaria",
            marker_color="#66bb6a",
            hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Emisión diaria: <b>%{y:,.2f} kg CO₂</b><extra></extra>"
        ))

        fig_tiempo.add_trace(go.Scatter(
            x=df_diario.index,
            y=df_diario["emisiones_acumuladas"],
            mode="lines",
            name="Huella acumulada",
            line=dict(color="#1f77b4", width=3),
            fill="tozeroy",
            fillcolor="rgba(31,119,180,0.18)",
            hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Acumulado: <b>%{y:,.2f} kg CO₂</b><extra></extra>"
        ))

        fig_tiempo.update_layout(
            height=420,
            margin=dict(t=30, b=20, l=20, r=20),
            template="plotly_white",
            hovermode="x unified",
            yaxis=dict(title="kg CO₂", gridcolor="#e5e8e8"),
            xaxis=dict(gridcolor="#e5e8e8"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_tiempo, use_container_width=True)

        st.markdown("<div style='margin-top: 18px;'></div>", unsafe_allow_html=True)

        # ===== FILA 4: KPIs INFERIORES =====
        b1, b2 = st.columns(2)

        with b1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Pico diario de emisión</div>
                <div class="kpi-value">{pico_diario_co2:,.1f} kg CO₂</div>
                <div class="kpi-sub">Máximo diario detectado</div>
            </div>
            """, unsafe_allow_html=True)

        with b2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Fecha de mayor impacto</div>
                <div class="kpi-value">{fecha_pico}</div>
                <div class="kpi-sub">Máxima emisión diaria</div>
            </div>
            """, unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Error al generar la ventana de huella de carbono: {e}")
