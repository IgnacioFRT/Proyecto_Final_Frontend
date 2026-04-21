import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from views.historico import get_historical_data


def clasificar_anomalia(row: pd.Series) -> str:
    if row["consumo_kwh"] > row["upper_ref"]:
        if row["dia_semana"] >= 5:
            return "Fin de semana atípico"
        if row["hora"] < 6 or row["hora"] >= 22:
            return "Fuera de horario"
        return "Pico de consumo"

    if row["consumo_kwh"] < row["lower_ref"]:
        return "Consumo anormalmente bajo"

    if row["dia_semana"] >= 5:
        return "Fin de semana atípico"

    return "Desvío general"


def render_deteccion_anomalias():
    try:
        with st.spinner("Analizando anomalías del consumo... ⏳"):
            df = get_historical_data().copy()

            if df.empty:
                st.warning("No hay datos históricos para detectar anomalías.")
                return

            if "EA_imp_T1_kwh" not in df.columns:
                st.warning("No se encontró EA_imp_T1_kwh en los datos históricos.")
                return

            # =========================================================
            # 1. PREPARACIÓN BASE
            # =========================================================
            df_work = pd.DataFrame(index=df.index)
            df_work["EA_imp_T1_kwh"] = df["EA_imp_T1_kwh"]

            df_work["consumo_kwh"] = df_work["EA_imp_T1_kwh"].diff().clip(lower=0)
            df_work = df_work.dropna()

            if df_work.empty:
                st.warning("No hay suficientes datos para construir la serie de consumo.")
                return

            df_work["hora"] = df_work.index.hour
            df_work["dia_semana"] = df_work.index.dayofweek
            df_work["mes"] = df_work.index.month

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

        # =========================================================
        # 4. PERFIL IDEAL DE REFERENCIA
        # =========================================================
        perfil = (
            df_train.groupby("hora")["consumo_kwh"]
            .agg(["mean", "std"])
            .reset_index()
            .fillna(0)
        )

        perfil["upper_ref"] = perfil["mean"] + 2 * perfil["std"]
        perfil["lower_ref"] = (perfil["mean"] - 2 * perfil["std"]).clip(lower=0)

        df_eval = df_eval.merge(
            perfil[["hora", "mean", "upper_ref", "lower_ref"]],
            on="hora",
            how="left"
        )
        df_eval = df_eval.set_index(df_eval.index)

        anomalias = df_eval[df_eval["es_anomalia"]].copy()
        normales = df_eval[~df_eval["es_anomalia"]].copy()

        if not anomalias.empty:
            anomalias["tipo_anomalia"] = anomalias.apply(clasificar_anomalia, axis=1)
        else:
            anomalias["tipo_anomalia"] = []

        # =========================================================
        # 5. KPIs
        # =========================================================
        total_registros = len(df_eval)
        total_anomalias = len(anomalias)
        porcentaje_anomalias = (total_anomalias / total_registros * 100) if total_registros > 0 else 0

        if not anomalias.empty:
            idx_max = anomalias["consumo_kwh"].idxmax()
            max_anomalia = float(anomalias.loc[idx_max, "consumo_kwh"])
            fecha_max_anomalia = idx_max.strftime("%d/%m/%Y %H:%M")
            hora_mas_conflictiva = int(anomalias.index.hour.value_counts().idxmax())
            tipo_principal = anomalias["tipo_anomalia"].value_counts().idxmax()
        else:
            max_anomalia = 0.0
            fecha_max_anomalia = "--"
            hora_mas_conflictiva = None
            tipo_principal = "Sin anomalías"

        st.markdown("### Resultados de la auditoría")

        k1, k2, k3, k4 = st.columns(4)

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
                <div class="kpi-title">Mayor anomalía</div>
                <div class="kpi-value">{max_anomalia:.2f} kWh</div>
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

        st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 6. INSIGHT AUTOMÁTICO
        # =========================================================
        if total_anomalias > 0:
            st.info(
                f"Se detectaron **{total_anomalias} anomalías** en **{mes_eval}**. "
                f"La hora con mayor concentración fue **{hora_mas_conflictiva:02d}:00** "
                f"y el tipo predominante fue **{tipo_principal}**."
            )
        else:
            st.success("No se detectaron anomalías con la sensibilidad seleccionada.")

        # =========================================================
        # 7. SERIE TEMPORAL
        # =========================================================
        st.markdown("#### Serie auditada con anomalías")

        color_map = {
            "Pico de consumo": "#e74c3c",
            "Consumo anormalmente bajo": "#8e44ad",
            "Fuera de horario": "#f39c12",
            "Fin de semana atípico": "#c0392b",
            "Desvío general": "#ff6b6b"
        }

        fig_serie = go.Figure()

        fig_serie.add_trace(go.Scatter(
            x=normales.index,
            y=normales["consumo_kwh"],
            mode="markers",
            name="Normal",
            marker=dict(color="#66bb6a", size=4, opacity=0.45)
        ))

        for tipo, color in color_map.items():
            sub = anomalias[anomalias["tipo_anomalia"] == tipo]
            if not sub.empty:
                fig_serie.add_trace(go.Scatter(
                    x=sub.index,
                    y=sub["consumo_kwh"],
                    mode="markers",
                    name=tipo,
                    marker=dict(color=color, size=7, symbol="x"),
                    hovertemplate="<b>%{x|%d/%m/%Y %H:%M}</b><br>Consumo: %{y:.2f} kWh<extra></extra>"
                ))

        fig_serie.update_layout(
            height=430,
            template="plotly_white",
            margin=dict(t=20, b=20, l=20, r=20),
            yaxis=dict(title="Consumo incremental (kWh)", gridcolor="#e5e8e8"),
            xaxis=dict(title="Fecha", gridcolor="#e5e8e8"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        st.plotly_chart(fig_serie, use_container_width=True)

        st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 8. PERFIL IDEAL + HEATMAP
        # =========================================================
        st.markdown("#### Perfil ideal y mapa de concentración")

        col_g1, col_g2 = st.columns(2)

        with col_g1:
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
                y=perfil["upper_ref"],
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip"
            ))

            fig_perfil.add_trace(go.Scatter(
                x=perfil["hora"],
                y=perfil["lower_ref"],
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
                heat = (
                    anomalias.assign(hora=anomalias.index.hour, dia=anomalias.index.dayofweek)
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

        st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 9. BARRAS POR HORA + 3D
        # =========================================================
        st.markdown("#### Concentración horaria y visualización 3D")

        col_h1, col_h2 = st.columns(2)

        with col_h1:
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
                st.info("No hay anomalías para mostrar por hora.")

        with col_h2:
            df_3d = df_eval.copy()
            df_3d["x_hora"] = df_3d.index.hour
            df_3d["y_dia"] = df_3d.index.dayofweek

            normales_3d = df_3d[~df_3d["es_anomalia"]]
            anomalias_3d = df_3d[df_3d["es_anomalia"]]

            fig_3d = go.Figure()

            fig_3d.add_trace(go.Scatter3d(
                x=normales_3d["x_hora"],
                y=normales_3d["y_dia"],
                z=normales_3d["consumo_kwh"],
                mode="markers",
                name="Rutina normal",
                marker=dict(size=2.5, color="#7FDBFF", opacity=0.45)
            ))

            if not anomalias_3d.empty:
                fig_3d.add_trace(go.Scatter3d(
                    x=anomalias_3d["x_hora"],
                    y=anomalias_3d["y_dia"],
                    z=anomalias_3d["consumo_kwh"],
                    mode="markers",
                    name="Outliers",
                    marker=dict(size=4, color="#ff4136", symbol="cross")
                ))

            fig_3d.update_layout(
                height=360,
                template="plotly_white",
                margin=dict(t=20, b=20, l=0, r=0),
                scene=dict(
                    xaxis_title="Hora",
                    yaxis_title="Día",
                    zaxis_title="kWh",
                    yaxis=dict(
                        tickvals=list(range(7)),
                        ticktext=["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
                    )
                )
            )

            st.plotly_chart(fig_3d, use_container_width=True)

        st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

        # =========================================================
        # 10. TABLA
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
                "tipo_anomalia",
                "Consumo incremental (kWh)",
                "Score"
            ]].rename(columns={
                "nombre_dia": "Día",
                "nombre_mes": "Mes",
                "hora": "Hora",
                "tipo_anomalia": "Tipo"
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
