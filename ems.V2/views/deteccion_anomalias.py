import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.preprocessing import StandardScaler

from views.historico import get_historical_data


st.warning("VERSION KW ACTIVA - SOLO ML")


def clasificar_anomalia_ml(row: pd.Series) -> str:
    """
    Clasificación simple basada en:
    - signo del error
    - horario
    - fin de semana
    """
    if row["dia_semana"] >= 5 and abs(row["error"]) > 0:
        return "Fin de semana atípico"

    if row["hora"] < 6 or row["hora"] >= 22:
        if row["error"] > 0:
            return "Demanda fuera de horario"
        return "Baja fuera de horario"

    if row["error"] > 0:
        return "Pico de demanda"

    if row["error"] < 0:
        return "Demanda anormalmente baja"

    return "Desvío general"


def render_deteccion_anomalias():
    try:
        with st.spinner("Analizando anomalías de demanda... ⏳"):
            df = get_historical_data().copy()

            if df.empty:
                st.warning("No hay datos históricos para detectar anomalías.")
                return

            if "EA_imp_T1_kwh" not in df.columns:
                st.warning("No se encontró EA_imp_T1_kwh en los datos históricos.")
                return

            # =========================================================
            # 1. PREPARACIÓN BASE EN kW
            # =========================================================
            df_work = pd.DataFrame(index=pd.to_datetime(df.index))
            df_work["EA_imp_T1_kwh"] = df["EA_imp_T1_kwh"].values

            df_work["energia_kwh"] = df_work["EA_imp_T1_kwh"].diff().clip(lower=0)

            df_work["delta_horas"] = (
                df_work.index.to_series().diff().dt.total_seconds() / 3600
            )

            df_work["potencia_kw"] = df_work["energia_kwh"] / df_work["delta_horas"]

            df_work = df_work.replace([np.inf, -np.inf], np.nan)
            df_work = df_work.dropna(subset=["potencia_kw"])

            if df_work.empty:
                st.warning("No hay suficientes datos para construir la serie de potencia.")
                return

            # Variables temporales base
            df_work["hora"] = df_work.index.hour
            df_work["dia_semana"] = df_work.index.dayofweek
            df_work["mes_num"] = df_work.index.month
            df_work["es_fin_semana"] = (df_work["dia_semana"] >= 5).astype(int)

            # Variables cíclicas
            df_work["hora_sin"] = np.sin(2 * np.pi * df_work["hora"] / 24)
            df_work["hora_cos"] = np.cos(2 * np.pi * df_work["hora"] / 24)
            df_work["dow_sin"] = np.sin(2 * np.pi * df_work["dia_semana"] / 7)
            df_work["dow_cos"] = np.cos(2 * np.pi * df_work["dia_semana"] / 7)

            # Lags y medias móviles
            df_work["lag_1"] = df_work["potencia_kw"].shift(1)
            df_work["lag_2"] = df_work["potencia_kw"].shift(2)
            df_work["lag_4"] = df_work["potencia_kw"].shift(4)
            df_work["lag_96"] = df_work["potencia_kw"].shift(96)

            df_work["mm_4"] = df_work["potencia_kw"].rolling(4, min_periods=1).mean()
            df_work["mm_12"] = df_work["potencia_kw"].rolling(12, min_periods=1).mean()
            df_work["mm_24"] = df_work["potencia_kw"].rolling(24, min_periods=1).mean()

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

            df_work = df_work.dropna()

            if df_work.empty:
                st.warning("No hay suficientes datos tras generar las variables del modelo.")
                return

        # =========================================================
        # 2. CONFIGURACIÓN
        # =========================================================
        st.markdown("### Configuración del análisis")

        meses_disponibles = df_work["nombre_mes"].drop_duplicates().tolist()

        idx_default = 0
        for i, m in enumerate(meses_disponibles):
            if "Noviembre" in m:
                idx_default = i
                break

        c1, c2, c3 = st.columns(3)

        with c1:
            mes_ref = st.selectbox("Mes de referencia", meses_disponibles, index=idx_default)

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
        # 3. DATOS DE ENTRENAMIENTO / EVALUACIÓN
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

        # =========================================================
        # 4. ISOLATION FOREST
        # =========================================================
        features_if = ["potencia_kw", "hora", "dia_semana", "es_fin_semana"]

        scaler = StandardScaler()
        X_train_if = scaler.fit_transform(df_train[features_if])
        X_eval_if = scaler.transform(df_eval[features_if])

        model_if = IsolationForest(
            contamination=contaminacion,
            random_state=42
        )
        model_if.fit(X_train_if)

        df_eval["anomalia_flag"] = model_if.predict(X_eval_if)
        df_eval["score_if"] = model_if.decision_function(X_eval_if)
        df_eval["es_anomalia_if"] = df_eval["anomalia_flag"] == -1

        # =========================================================
        # 5. RANDOM FOREST REGRESSOR
        # =========================================================
        features_reg = [
            "hora_sin", "hora_cos",
            "dow_sin", "dow_cos",
            "es_fin_semana",
            "mes_num",
            "lag_1", "lag_2", "lag_4", "lag_96",
            "mm_4", "mm_12", "mm_24"
        ]

        X_train_reg = df_train[features_reg]
        y_train_reg = df_train["potencia_kw"]

        X_eval_reg = df_eval[features_reg]

        modelo_reg = RandomForestRegressor(
            n_estimators=300,
            max_depth=12,
            min_samples_leaf=3,
            random_state=42,
            n_jobs=-1
        )
        modelo_reg.fit(X_train_reg, y_train_reg)

        df_eval["prediccion"] = modelo_reg.predict(X_eval_reg)
        df_eval["prediccion"] = df_eval["prediccion"].clip(lower=0)

        df_eval["error"] = df_eval["potencia_kw"] - df_eval["prediccion"]
        df_eval["error_abs"] = df_eval["error"].abs()
        umbral_error = df_eval["error"].std() * 2 if df_eval["error"].std() > 0 else 0.1
        df_eval["es_anomalia_reg"] = df_eval["error_abs"] > umbral_error

        # =========================================================
        # 6. ANOMALÍA FINAL = COMBINACIÓN ML
        # =========================================================
        df_eval["es_anomalia_final"] = df_eval["es_anomalia_if"] | df_eval["es_anomalia_reg"]

        anomalias_final = df_eval[df_eval["es_anomalia_final"]].copy()

        if not anomalias_final.empty:
            anomalias_final["tipo_anomalia"] = anomalias_final.apply(clasificar_anomalia_ml, axis=1)
        else:
            anomalias_final["tipo_anomalia"] = pd.Series(dtype="object")

        # =========================================================
        # 7. KPIs
        # =========================================================
        total_registros = len(df_eval)
        total_anomalias = len(anomalias_final)
        porcentaje_anomalias = (total_anomalias / total_registros * 100) if total_registros > 0 else 0
        error_medio = df_eval["error_abs"].mean()

        total_if = int(df_eval["es_anomalia_if"].sum())
        total_reg = int(df_eval["es_anomalia_reg"].sum())

        if not anomalias_final.empty:
            idx_max = anomalias_final["potencia_kw"].idxmax()
            max_anomalia = float(anomalias_final.loc[idx_max, "potencia_kw"])
            fecha_max_anomalia = pd.to_datetime(idx_max).strftime("%d/%m/%Y %H:%M")
            hora_mas_conflictiva = int(anomalias_final.index.hour.value_counts().idxmax())
            tipo_principal = anomalias_final["tipo_anomalia"].value_counts().idxmax()
        else:
            max_anomalia = 0.0
            fecha_max_anomalia = "--"
            hora_mas_conflictiva = None
            tipo_principal = "Sin anomalías"

        st.markdown("### Resultados de la auditoría")

        k1, k2, k3, k4, k5 = st.columns(5)

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
                <div class="kpi-title">Anomalías finales</div>
                <div class="kpi-value">{total_anomalias}</div>
                <div class="kpi-sub">{porcentaje_anomalias:.2f}% del total</div>
            </div>
            """, unsafe_allow_html=True)

        with k3:
            subt = f"Hora crítica: {hora_mas_conflictiva:02d}:00" if hora_mas_conflictiva is not None else "Sin anomalías"
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Mayor demanda anómala</div>
                <div class="kpi-value">{max_anomalia:.2f} kW</div>
                <div class="kpi-sub">{subt}</div>
            </div>
            """, unsafe_allow_html=True)

        with k4:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Tipo dominante</div>
                <div class="kpi-value">{tipo_principal}</div>
                <div class="kpi-sub">{fecha_max_anomalia}</div>
            </div>
            """, unsafe_allow_html=True)

        with k5:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Error medio RF</div>
                <div class="kpi-value">{error_medio:.2f} kW</div>
                <div class="kpi-sub">Demanda real vs predicción</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

        st.info(
            f"Isolation Forest detectó **{total_if}** eventos, Random Forest por error detectó **{total_reg}** "
            f"y la combinación final marcó **{total_anomalias}** anomalías en **{mes_eval}**."
        )

        # =========================================================
        # 8. SERIE AUDITADA - SOLO ML
        # =========================================================
        st.markdown("#### Serie auditada con anomalías")

        fig_serie = go.Figure()

        fig_serie.add_trace(go.Scatter(
            x=df_eval.index,
            y=df_eval["potencia_kw"],
            mode="lines",
            name="Demanda real",
            line=dict(color="#1f77b4", width=1),
            opacity=0.70
        ))

        fig_serie.add_trace(go.Scatter(
            x=df_eval.index,
            y=df_eval["prediccion"],
            mode="lines",
            name="Predicción (Random Forest)",
            line=dict(color="#2c3e50", width=2.5)
        ))

        anom_if_only = df_eval[df_eval["es_anomalia_if"] & ~df_eval["es_anomalia_reg"]]
        anom_reg_only = df_eval[df_eval["es_anomalia_reg"] & ~df_eval["es_anomalia_if"]]
        anom_both = df_eval[df_eval["es_anomalia_reg"] & df_eval["es_anomalia_if"]]

        if not anom_if_only.empty:
            fig_serie.add_trace(go.Scatter(
                x=anom_if_only.index,
                y=anom_if_only["potencia_kw"],
                mode="markers",
                name="Isolation Forest",
                marker=dict(color="#f39c12", size=7, symbol="diamond")
            ))

        if not anom_reg_only.empty:
            fig_serie.add_trace(go.Scatter(
                x=anom_reg_only.index,
                y=anom_reg_only["potencia_kw"],
                mode="markers",
                name="Random Forest (error)",
                marker=dict(color="#e74c3c", size=7, symbol="x")
            ))

        if not anom_both.empty:
            fig_serie.add_trace(go.Scatter(
                x=anom_both.index,
                y=anom_both["potencia_kw"],
                mode="markers",
                name="Coincidencia IF + RF",
                marker=dict(color="#8e44ad", size=9, symbol="star")
            ))

        fig_serie.update_layout(
            height=430,
            template="plotly_white",
            margin=dict(t=20, b=20, l=20, r=20),
            yaxis=dict(title="Demanda media (kW)", gridcolor="#e5e8e8"),
            xaxis=dict(title="Fecha", gridcolor="#e5e8e8"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        st.plotly_chart(fig_serie, use_container_width=True)

        # =========================================================
        # 8.1 ERROR DEL MODELO + UMBRAL
        # =========================================================
        st.markdown("#### Error del modelo y umbral de decisión")

        fig_error = go.Figure()

        fig_error.add_trace(go.Scatter(
            x=df_eval.index,
            y=df_eval["error_abs"],
            mode="lines",
            name="Error absoluto |Real - Predicción|",
            line=dict(color="#1f77b4", width=1.8)
        ))

        fig_error.add_trace(go.Scatter(
            x=df_eval.index,
            y=[umbral_error] * len(df_eval),
            mode="lines",
            name="Umbral de anomalía",
            line=dict(color="#e74c3c", width=2, dash="dash")
        ))

        if not anom_reg_only.empty:
            fig_error.add_trace(go.Scatter(
                x=anom_reg_only.index,
                y=anom_reg_only["error_abs"],
                mode="markers",
                name="RF supera umbral",
                marker=dict(color="#e74c3c", size=7, symbol="x")
            ))

        if not anom_both.empty:
            fig_error.add_trace(go.Scatter(
                x=anom_both.index,
                y=anom_both["error_abs"],
                mode="markers",
                name="IF + RF supera umbral",
                marker=dict(color="#8e44ad", size=9, symbol="star")
            ))

        fig_error.update_layout(
            height=280,
            template="plotly_white",
            margin=dict(t=20, b=20, l=20, r=20),
            yaxis=dict(title="Error absoluto (kW)", gridcolor="#e5e8e8"),
            xaxis=dict(title="Fecha", gridcolor="#e5e8e8"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        st.plotly_chart(fig_error, use_container_width=True)

        st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 9. MAPA DE CONCENTRACIÓN - SOLO ML
        # =========================================================
        st.markdown("#### Concentración de anomalías")

        col_g1, col_g2 = st.columns(2)

        with col_g1:
            if not anomalias_final.empty:
                heat = (
                    anomalias_final.assign(hora=anomalias_final.index.hour, dia=anomalias_final.index.dayofweek)
                    .groupby(["dia", "hora"])
                    .size()
                    .unstack(fill_value=0)
                    .reindex(index=range(7), columns=range(24), fill_value=0)
                )

                fig_heat = go.Figure(data=go.Heatmap(
                    z=heat.values,
                    x=[f"{h:02d}:00" for h in heat.columns],
                    y=["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"],
                    colorscale="YlOrRd",
                    hovertemplate="Día: %{y}<br>Hora: %{x}<br>Anomalías: %{z}<extra></extra>"
                ))

                fig_heat.update_layout(
                    height=360,
                    template="plotly_white",
                    margin=dict(t=20, b=20, l=20, r=20),
                    xaxis_title="Hora del día",
                    yaxis_title="Día"
                )

                st.plotly_chart(fig_heat, use_container_width=True)
            else:
                st.success("No hay anomalías para construir el mapa de calor.")

        with col_g2:
            if not anomalias_final.empty:
                tipo_count = anomalias_final["tipo_anomalia"].value_counts()

                fig_tipos = go.Figure()
                fig_tipos.add_trace(go.Bar(
                    x=tipo_count.index,
                    y=tipo_count.values
                ))

                fig_tipos.update_layout(
                    height=360,
                    template="plotly_white",
                    margin=dict(t=20, b=20, l=20, r=20),
                    xaxis_title="Tipo de anomalía",
                    yaxis_title="Cantidad"
                )

                st.plotly_chart(fig_tipos, use_container_width=True)
            else:
                st.info("No hay anomalías para clasificar.")

        st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 10. CONCENTRACIÓN HORARIA + 3D
        # =========================================================
        st.markdown("#### Concentración horaria y visualización 3D")

        col_h1, col_h2 = st.columns(2)

        with col_h1:
            if not anomalias_final.empty:
                anom_hora = (
                    anomalias_final.groupby(anomalias_final.index.hour)
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
                st.info("No hay anomalías para mostrar por hora.")

        with col_h2:
            df_3d = df_eval.copy()
            df_3d["x_hora"] = df_3d.index.hour
            df_3d["y_dia"] = df_3d.index.dayofweek

            normales_3d = df_3d[~df_3d["es_anomalia_final"]]
            anomalias_3d = df_3d[df_3d["es_anomalia_final"]]

            fig_3d = go.Figure()

            fig_3d.add_trace(go.Scatter3d(
                x=normales_3d["x_hora"],
                y=normales_3d["y_dia"],
                z=normales_3d["potencia_kw"],
                mode="markers",
                name="Rutina normal",
                marker=dict(size=2.5, color="#7FDBFF", opacity=0.45)
            ))

            if not anomalias_3d.empty:
                fig_3d.add_trace(go.Scatter3d(
                    x=anomalias_3d["x_hora"],
                    y=anomalias_3d["y_dia"],
                    z=anomalias_3d["potencia_kw"],
                    mode="markers",
                    name="Anomalías",
                    marker=dict(size=4, color="#ff4136", symbol="cross")
                ))

            fig_3d.update_layout(
                height=360,
                template="plotly_white",
                margin=dict(t=20, b=20, l=0, r=0),
                scene=dict(
                    xaxis_title="Hora",
                    yaxis_title="Día",
                    zaxis_title="kW",
                    yaxis=dict(
                        tickvals=list(range(7)),
                        ticktext=["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
                    )
                )
            )

            st.plotly_chart(fig_3d, use_container_width=True)

        st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 11. TABLA
        # =========================================================
        st.markdown("#### Detalle de anomalías detectadas")

        if not anomalias_final.empty:
            tabla = anomalias_final.copy()
            tabla["Fecha y Hora"] = tabla.index.strftime("%d/%m/%Y %H:%M")
            tabla["Demanda media (kW)"] = tabla["potencia_kw"].round(3)
            tabla["Predicción (kW)"] = tabla["prediccion"].round(3)
            tabla["Error (kW)"] = tabla["error"].round(3)
            tabla["Score IF"] = tabla["score_if"].round(4)
            tabla["Tipo"] = tabla["tipo_anomalia"]

            tabla["Día"] = tabla["dia_semana"].map({
                0: "Lunes", 1: "Martes", 2: "Miércoles",
                3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo"
            })

            tabla["Mes"] = tabla.index.month.map({
                1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
                5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
                9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
            }) + " " + tabla.index.year.astype(str)

            tabla["Detectado por"] = tabla.apply(
                lambda row: "IF + RF" if row["es_anomalia_if"] and row["es_anomalia_reg"]
                else ("IF" if row["es_anomalia_if"] else "RF"),
                axis=1
            )

            tabla_mostrar = tabla[[
                "Fecha y Hora",
                "Día",
                "Mes",
                "hora",
                "Tipo",
                "Detectado por",
                "Demanda media (kW)",
                "Predicción (kW)",
                "Error (kW)",
                "Score IF"
            ]].rename(columns={"hora": "Hora"})

            st.dataframe(
                tabla_mostrar.sort_values("Error (kW)", ascending=False),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No se registraron anomalías relevantes según los modelos.")

    except Exception as e:
        st.error(f"Error al renderizar la detección de anomalías: {e}")
