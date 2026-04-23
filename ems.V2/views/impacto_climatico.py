import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from views.historico import get_historical_data


def render_impacto_climatico():
    try:
        with st.spinner("Analizando variables climáticas... ⏳"):
            df = get_historical_data().copy()

            if df.empty:
                st.warning("No hay datos históricos para analizar el impacto climático.")
                return

            if "EA_imp_T1_kwh" not in df.columns or "temp" not in df.columns:
                st.warning("Faltan variables necesarias: EA_imp_T1_kwh y/o temp.")
                return

            # =========================================================
            # 1. PREPARACIÓN DE DATOS
            # =========================================================
            df_diario = pd.DataFrame(index=df.resample("D").last().index)

            # Energía máxima diaria y consumo diario
            df_diario["EA_max"] = df.resample("D")["EA_imp_T1_kwh"].max()
            df_diario["consumo_diario_kWh"] = (
                df_diario["EA_max"].diff().clip(lower=0).fillna(0)
            )

            # Temperatura promedio diaria
            df_diario["temp_promedio"] = df.resample("D")["temp"].mean()

            # Limpieza de temperatura absurda
            df_diario.loc[
                (df_diario["temp_promedio"] < 0) | (df_diario["temp_promedio"] > 50),
                "temp_promedio"
            ] = np.nan

            # Calendario simple
            dias_semana_es = {
                0: "Lunes",
                1: "Martes",
                2: "Miércoles",
                3: "Jueves",
                4: "Viernes",
                5: "Sábado",
                6: "Domingo",
            }

            df_diario["nombre_dia"] = df_diario.index.dayofweek.map(dias_semana_es)
            df_diario["es_habil"] = df_diario.index.dayofweek < 5

            # Dataframe limpio general
            df_limpio = df_diario.copy()

            # Para regresión: solo hábiles y fuera del receso
            df_temp = df_limpio[df_limpio["es_habil"]].dropna(
                subset=["temp_promedio", "consumo_diario_kWh"]
            )
            df_habiles = df_temp[~df_temp.index.month.isin([1, 2])].copy()

            # =========================================================
            # 2. KPIs
            # =========================================================
            total_kwh = df_limpio["consumo_diario_kWh"].sum()
            temp_media = df_limpio["temp_promedio"].mean()

            if len(df_habiles) > 1:
                x_reg = df_habiles["temp_promedio"].values
                y_reg = df_habiles["consumo_diario_kWh"].values

                coeffs = np.polyfit(x_reg, y_reg, 1)
                pendiente = float(coeffs[0])
                intercepto = float(coeffs[1])

                y_pred = np.poly1d(coeffs)(x_reg)
                y_bar = np.mean(y_reg)
                ss_res = np.sum((y_reg - y_pred) ** 2)
                ss_tot = np.sum((y_reg - y_bar) ** 2)
                r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0.0
            else:
                pendiente = 0.0
                intercepto = 0.0
                r_squared = 0.0

            dia_max = df_limpio["consumo_diario_kWh"].idxmax() if not df_limpio.empty else None
            valor_max = df_limpio["consumo_diario_kWh"].max() if not df_limpio.empty else 0.0

            st.markdown("### Impacto climático")

            k1, k2, k3 = st.columns(3)

            with k1:
                st.markdown(
                    f"""
                    <div class="kpi-card">
                        <div class="kpi-title">Consumo acumulado analizado</div>
                        <div class="kpi-value">{total_kwh:,.1f} kWh</div>
                        <div class="kpi-sub">Período histórico</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            with k2:
                st.markdown(
                    f"""
                    <div class="kpi-card">
                        <div class="kpi-title">Temperatura promedio</div>
                        <div class="kpi-value">{temp_media:.1f} °C</div>
                        <div class="kpi-sub">Promedio diario</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            with k3:
                st.markdown(
                    f"""
                    <div class="kpi-card">
                        <div class="kpi-title">Termosensibilidad</div>
                        <div class="kpi-value">{pendiente:.2f}</div>
                        <div class="kpi-sub">kWh por cada °C adicional</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

            if dia_max is not None:
                st.info(
                    f"El análisis muestra una pendiente térmica de **{pendiente:.2f} kWh/°C** "
                    f"con un ajuste **R² = {r_squared:.2f}**. "
                    f"El mayor consumo diario detectado fue **{valor_max:.1f} kWh** el **{dia_max.strftime('%d/%m/%Y')}**."
                )

            # =========================================================
            # 3. GRÁFICO 1: EVOLUCIÓN HISTÓRICA CON DOBLE EJE
            # =========================================================
            st.markdown("#### Evolución histórica: consumo vs temperatura")

            fig_dual = make_subplots(specs=[[{"secondary_y": True}]])

            fig_dual.add_trace(
                go.Bar(
                    x=df_limpio.index,
                    y=df_limpio["consumo_diario_kWh"],
                    name="Consumo diario",
                    marker_color="#2ca02c",
                    customdata=df_limpio["nombre_dia"],
                    hovertemplate=(
                        "<b>%{x|%d/%m/%Y} (%{customdata})</b>"
                        "<br>Consumo: <b>%{y:.2f} kWh</b><extra></extra>"
                    ),
                ),
                secondary_y=False,
            )

            fig_dual.add_trace(
                go.Scatter(
                    x=df_limpio.index,
                    y=df_limpio["temp_promedio"],
                    name="Temperatura promedio",
                    mode="lines+markers",
                    line=dict(color="#ff7f0e", width=2),
                    marker=dict(size=5),
                    customdata=df_limpio["nombre_dia"],
                    hovertemplate=(
                        "<b>%{x|%d/%m/%Y} (%{customdata})</b>"
                        "<br>Temperatura: <b>%{y:.1f} °C</b><extra></extra>"
                    ),
                ),
                secondary_y=True,
            )

            fig_dual.update_layout(
                template="plotly_white",
                hovermode="x unified",
                height=360,
                font=dict(color="black"),
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                ),
                margin=dict(l=20, r=20, t=20, b=20),
            )
            fig_dual.update_yaxes(
                title_text="Consumo diario (kWh)",
                color="#2ca02c",
                secondary_y=False,
                gridcolor="#e5e8e8",
            )
            fig_dual.update_yaxes(
                title_text="Temperatura (°C)",
                color="#ff7f0e",
                secondary_y=True,
                showgrid=False,
            )
            fig_dual.update_xaxes(gridcolor="#e5e8e8")

            st.plotly_chart(fig_dual, use_container_width=True)

            st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

            # =========================================================
            # 4. GRÁFICO 2: DISPERSIÓN + TENDENCIA
            # =========================================================
            st.markdown("#### Termosensibilidad del edificio")

            col_disp, col_info = st.columns([2.4, 1])

            with col_disp:
                fig_scatter = go.Figure()

                fig_scatter.add_trace(
                    go.Scatter(
                        x=df_habiles["temp_promedio"],
                        y=df_habiles["consumo_diario_kWh"],
                        mode="markers",
                        name="Días hábiles",
                        marker=dict(
                            size=9,
                            color="rgba(0, 102, 204, 0.40)",
                            line=dict(width=1, color="rgba(0, 51, 102, 1)"),
                        ),
                        customdata=np.stack(
                            (
                                df_habiles.index.strftime("%d/%m/%Y"),
                                df_habiles["nombre_dia"],
                            ),
                            axis=-1,
                        ) if len(df_habiles) > 0 else None,
                        hovertemplate=(
                            "<b>%{customdata[0]} (%{customdata[1]})</b>"
                            "<br>Temperatura: <b>%{x:.1f} °C</b>"
                            "<br>Consumo: <b>%{y:.1f} kWh</b><extra></extra>"
                        ),
                    )
                )

                if len(df_habiles) > 1:
                    x = df_habiles["temp_promedio"]
                    y = df_habiles["consumo_diario_kWh"]
                    coeffs = np.polyfit(x, y, 1)
                    p = np.poly1d(coeffs)
                    x_trend = np.linspace(x.min(), x.max(), 100)

                    fig_scatter.add_trace(
                        go.Scatter(
                            x=x_trend,
                            y=p(x_trend),
                            mode="lines",
                            name="Tendencia lineal",
                            line=dict(color="#e74c3c", width=3),
                            hoverinfo="skip",
                        )
                    )

                fig_scatter.update_layout(
                    title=dict(
                        text=f"Dispersión y tendencia lineal hábil (R² = {r_squared:.2f})",
                        x=0.5,
                        xanchor="center",
                        font=dict(size=14, color="black"),
                    ),
                    template="plotly_white",
                    height=320,
                    margin=dict(l=10, r=20, t=40, b=10),
                    font=dict(color="black"),
                    xaxis=dict(
                        title="Temperatura promedio diaria (°C)",
                        gridcolor="#e5e8e8",
                        zeroline=False,
                    ),
                    yaxis=dict(
                        title="Consumo diario (kWh)",
                        gridcolor="#e5e8e8",
                        zeroline=False,
                    ),
                    legend=dict(
                        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                    ),
                )

                st.plotly_chart(fig_scatter, use_container_width=True)

            with col_info:
                st.info("¿Cómo leer este análisis?")
                st.write(
                    "Cada punto representa un día hábil con temperatura y consumo válidos."
                )
                st.write(
                    "La línea roja muestra la tendencia matemática entre temperatura y consumo."
                )
                st.write(
                    "Si la pendiente es positiva y marcada, el edificio es termosensible: "
                    "a mayor temperatura, mayor demanda energética."
                )
                st.write(
                    "Excluir enero y febrero ayuda a no contaminar la pendiente con el receso académico."
                )

            st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

            # =========================================================
            # 5. GRÁFICO 3: PROMEDIO POR RANGO TÉRMICO
            # =========================================================
            st.markdown("#### Consumo promedio según rango térmico")

            df_bins = df_habiles.copy()

            if len(df_bins) > 0:
                bins = [0, 10, 15, 20, 25, 30, 35, 50]
                labels = ["0-10", "10-15", "15-20", "20-25", "25-30", "30-35", "35+"]
                df_bins["rango_temp"] = pd.cut(
                    df_bins["temp_promedio"], bins=bins, labels=labels, include_lowest=True
                )

                resumen_bins = (
                    df_bins.groupby("rango_temp")["consumo_diario_kWh"]
                    .mean()
                    .reindex(labels)
                )

                fig_bins = go.Figure()
                fig_bins.add_trace(
                    go.Bar(
                        x=resumen_bins.index.astype(str),
                        y=resumen_bins.values,
                        marker_color="#16a085",
                        text=[f"{v:.1f}" if pd.notna(v) else "" for v in resumen_bins.values],
                        textposition="outside",
                        hovertemplate=(
                            "Rango térmico: <b>%{x} °C</b>"
                            "<br>Consumo promedio: <b>%{y:.2f} kWh</b><extra></extra>"
                        ),
                    )
                )

                fig_bins.update_layout(
                    template="plotly_white",
                    height=300,
                    margin=dict(l=20, r=20, t=20, b=20),
                    xaxis=dict(title="Rango de temperatura (°C)", gridcolor="#e5e8e8"),
                    yaxis=dict(title="Consumo promedio (kWh)", gridcolor="#e5e8e8"),
                    showlegend=False,
                )

                st.plotly_chart(fig_bins, use_container_width=True)
            else:
                st.info("No hay datos hábiles suficientes para construir rangos térmicos.")

    except Exception as e:
        st.error(f"Error al generar el análisis climático: {e}")
