import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from views.historico import get_historical_data


def render_deteccion_anomalias():
    try:
        with st.spinner("Analizando anomalías del consumo... ⏳"):
            df = get_historical_data().copy()

            if df.empty:
                st.warning("No hay datos históricos para detectar anomalías.")
                return

            if "EA_imp_T1_kwh" not in df.columns:
                st.warning("No se encontró la variable EA_imp_T1_kwh en los datos históricos.")
                return

            # =========================================================
            # 1. PREPARACIÓN DE DATOS
            # =========================================================
            df_work = pd.DataFrame(index=df.index)
            df_work["EA_imp_T1_kwh"] = df["EA_imp_T1_kwh"]

            # Consumo incremental
            df_work["consumo_kwh"] = df_work["EA_imp_T1_kwh"].diff().clip(lower=0)
            df_work = df_work.dropna()

            if df_work.empty:
                st.warning("No hay suficientes datos para construir la serie de consumo.")
                return

            # Features temporales
            df_work["hora"] = df_work.index.hour
            df_work["dia_semana"] = df_work.index.dayofweek
            df_work["mes"] = df_work.index.month

            # Nombres para mostrar
            dias_map = {
                0: "Lunes", 1: "Martes", 2: "Miércoles",
                3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo"
            }
            meses_map = {
                1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
                5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
                9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
            }

            df_work["nombre_dia"] = df_work["dia_semana"].map(dias_map)
            df_work["nombre_mes"] = df_work.index.month.map(meses_map) + " " + df_work.index.year.astype(str)

        # =========================================================
        # 2. CONFIGURACIÓN
        # =========================================================
        st.markdown("### Configuración del análisis")

        meses_disponibles = df_work["nombre_mes"].drop_duplicates().tolist()

        c1, c2, c3 = st.columns(3)

        with c1:
            mes_ref = st.selectbox("Mes de referencia", meses_disponibles, index=0)

        with c2:
            opciones_eval = ["Todo el histórico"] + meses_disponibles
            mes_eval = st.selectbox("Período a evaluar", opciones_eval, index=0)

        with c3:
            contaminacion = st.slider(
                "Sensibilidad del detector",
                min_value=0.005,
                max_value=0.10,
                value=0.02,
                step=0.005
            )

        # =========================================================
        # 3. ENTRENAMIENTO / EVALUACIÓN
        # =========================================================
        df_train = df_work[df_work["nombre_mes"] == mes_ref].copy()

        if df_train.empty:
            st.warning("No hay datos suficientes en el mes de referencia.")
            return

        if mes_eval == "Todo el histórico":
            df_eval = df_work.copy()
        else:
            df_eval = df_work[df_work["nombre_mes"] == mes_eval].copy()

        if df_eval.empty:
            st.warning("No hay datos suficientes en el período evaluado.")
            return

        features = ["consumo_kwh", "hora", "dia_semana"]

        scaler = StandardScaler()
        X_train = scaler.fit_transform(df_train[features])
        X_eval = scaler.transform(df_eval[features])

        model = IsolationForest(
            contamination=contaminacion,
            random_state=42
        )
        model.fit(X_train)

        df_eval["anomalia_flag"] = model.predict(X_eval)
        df_eval["score"] = model.decision_function(X_eval)
        df_eval["es_anomalia"] = df_eval["anomalia_flag"] == -1

        anomalias = df_eval[df_eval["es_anomalia"]].copy()
        normales = df_eval[~df_eval["es_anomalia"]].copy()

        # =========================================================
        # 4. KPIs
        # =========================================================
        total_registros = len(df_eval)
        total_anomalias = len(anomalias)
        porcentaje_anomalias = (total_anomalias / total_registros * 100) if total_registros > 0 else 0

        if not anomalias.empty:
            idx_max = anomalias["consumo_kwh"].idxmax()
            max_anomalia = anomalias.loc[idx_max, "consumo_kwh"]
            fecha_max_anomalia = idx_max.strftime("%d/%m/%Y %H:%M")
        else:
            max_anomalia = 0
            fecha_max_anomalia = "--"

        hora_mas_conflictiva = (
            int(anomalias.index.hour.value_counts().idxmax())
            if not anomalias.empty else None
        )

        k1, k2, k3 = st.columns(3)

        with k1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Registros evaluados</div>
                <div class="kpi-value">{total_registros:,}</div>
                <div class="kpi-sub">Período analizado</div>
            </div>
            """, unsafe_allow_html=True)

        with k2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Anomalías detectadas</div>
                <div class="kpi-value">{total_anomalias}</div>
                <div class="kpi-sub">{porcentaje_anomalias:.2f}% del total</div>
            </div>
            """, unsafe_allow_html=True)

        with k3:
            subt = f"Hora crítica: {hora_mas_conflictiva:02d}:00" if hora_mas_conflictiva is not None else "Sin anomalías"
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Mayor anomalía detectada</div>
                <div class="kpi-value">{max_anomalia:.2f} kWh</div>
                <div class="kpi-sub">{subt}</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='margin-top: 18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 5. SERIE TEMPORAL CON ANOMALÍAS
        # =========================================================
        st.markdown("#### Serie auditada con anomalías")

        fig_serie = go.Figure()

        fig_serie.add_trace(go.Scatter(
            x=normales.index,
            y=normales["consumo_kwh"],
            mode="markers",
            name="Normal",
            marker=dict(color="#66bb6a", size=4, opacity=0.55)
        ))

        if not anomalias.empty:
            fig_serie.add_trace(go.Scatter(
                x=anomalias.index,
                y=anomalias["consumo_kwh"],
                mode="markers",
                name="Anomalía",
                marker=dict(color="#e74c3c", size=7, symbol="x")
            ))

        fig_serie.update_layout(
            height=420,
            template="plotly_white",
            margin=dict(t=20, b=20, l=20, r=20),
            yaxis=dict(title="Consumo incremental (kWh)", gridcolor="#e5e8e8"),
            xaxis=dict(title="Fecha", gridcolor="#e5e8e8"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        st.plotly_chart(fig_serie, use_container_width=True)

        st.markdown("<div style='margin-top: 18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 6. PERFIL IDEAL VS HORAS CONFLICTIVAS
        # =========================================================
        st.markdown("#### Perfil ideal vs concentración horaria de anomalías")

        col_g1, col_g2 = st.columns(2)

        with col_g1:
            perfil = (
                df_train.groupby("hora")["consumo_kwh"]
                .agg(["mean", "std"])
                .reset_index()
                .fillna(0)
            )
            perfil["upper"] = perfil["mean"] + 2 * perfil["std"]
            perfil["lower"] = (perfil["mean"] - 2 * perfil["std"]).clip(lower=0)

            fig_perfil = go.Figure()

            fig_perfil.add_trace(go.Scatter(
                x=perfil["hora"],
                y=perfil["mean"],
                mode="lines",
                name="Perfil ideal",
                line=dict(color="#1f77b4", width=3)
            ))

            fig_perfil.add_trace(go.Scatter(
                x=perfil["hora"],
                y=perfil["upper"],
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip"
            ))

            fig_perfil.add_trace(go.Scatter(
                x=perfil["hora"],
                y=perfil["lower"],
                mode="lines",
                fill="tonexty",
                fillcolor="rgba(31,119,180,0.15)",
                line=dict(width=0),
                name="Banda esperada",
                hoverinfo="skip"
            ))

            fig_perfil.update_layout(
                height=360,
                template="plotly_white",
                margin=dict(t=20, b=20, l=20, r=20),
                xaxis=dict(title="Hora del día", tickmode="linear", dtick=1, gridcolor="#e5e8e8"),
                yaxis=dict(title="Consumo incremental (kWh)", gridcolor="#e5e8e8"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )

            st.plotly_chart(fig_perfil, use_container_width=True)

        with col_g2:
            if not anomalias.empty:
                anom_hora = (
                    anomalias.groupby(anomalias.index.hour)
                    .size()
                    .reindex(range(24), fill_value=0)
                )

                fig_horas = go.Figure()
                fig_horas.add_trace(go.Bar(
                    x=anom_hora.index,
                    y=anom_hora.values,
                    marker_color="#ff7f0e"
                ))

                fig_horas.update_layout(
                    height=360,
                    template="plotly_white",
                    margin=dict(t=20, b=20, l=20, r=20),
                    xaxis=dict(title="Hora del día", tickmode="linear", dtick=1, gridcolor="#e5e8e8"),
                    yaxis=dict(title="Cantidad de anomalías", gridcolor="#e5e8e8"),
                    showlegend=False
                )

                st.plotly_chart(fig_horas, use_container_width=True)
            else:
                st.success("No se detectaron anomalías en el período seleccionado.")

        st.markdown("<div style='margin-top: 18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 7. TABLA
        # =========================================================
        st.markdown("#### Detalle de anomalías detectadas")

        if not anomalias.empty:
            tabla = anomalias.copy()
            tabla["Fecha y Hora"] = tabla.index.strftime("%d/%m/%Y %H:%M")
            tabla["Score"] = tabla["score"].round(4)
            tabla["Consumo incremental (kWh)"] = tabla["consumo_kwh"].round(3)

            tabla_mostrar = tabla[[
                "Fecha y Hora",
                "nombre_dia",
                "nombre_mes",
                "hora",
                "Consumo incremental (kWh)",
                "Score"
            ]].rename(columns={
                "nombre_dia": "Día",
                "nombre_mes": "Mes",
                "hora": "Hora"
            })

            st.dataframe(
                tabla_mostrar.sort_values("Consumo incremental (kWh)", ascending=False),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No se registraron anomalías para la configuración actual.")

    except Exception as e:
        st.error(f"Error al renderizar la detección de anomalías: {e}")
