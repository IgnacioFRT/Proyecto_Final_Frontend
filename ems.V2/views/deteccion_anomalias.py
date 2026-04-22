import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

from views.historico import get_historical_data


def clasificar_anomalia(row: pd.Series) -> str:
    if row["potencia_kw"] > row["upper_ref"]:
        if row["dia_semana"] >= 5:
            return "Fin de semana atípico"
        if row["hora"] < 6 or row["hora"] >= 22:
            return "Fuera de horario"
        return "Pico de demanda"

    if row["potencia_kw"] < row["lower_ref"]:
        return "Demanda anormalmente baja"

    if row["dia_semana"] >= 5:
        return "Fin de semana atípico"

    return "Desvío general"


def render_deteccion_anomalias():
    try:
        with st.spinner("Analizando anomalías del consumo... ⏳"):
            df = get_historical_data().copy()

            if df.empty:
                st.warning("No hay datos históricos.")
                return

            if "EA_imp_T1_kwh" not in df.columns:
                st.warning("No se encontró energía acumulada.")
                return

            # =========================================================
            # 1. CÁLCULO EN kW (CLAVE)
            # =========================================================
            df_work = pd.DataFrame(index=pd.to_datetime(df.index))
            df_work["energia_kwh"] = df["EA_imp_T1_kwh"]

            # energía incremental
            df_work["delta_kwh"] = df_work["energia_kwh"].diff().clip(lower=0)

            # intervalo en horas (robusto)
            delta_horas = df_work.index.to_series().diff().dt.total_seconds() / 3600
            df_work["delta_horas"] = delta_horas

            # potencia = energía / tiempo
            df_work["potencia_kw"] = df_work["delta_kwh"] / df_work["delta_horas"]

            df_work = df_work.replace([float("inf"), -float("inf")], None)
            df_work = df_work.dropna()

            if df_work.empty:
                st.warning("Datos insuficientes.")
                return

            df_work["hora"] = df_work.index.hour
            df_work["dia_semana"] = df_work.index.dayofweek

        # =========================================================
        # 2. CONFIGURACIÓN
        # =========================================================
        st.markdown("### Configuración del análisis")

        c1, c2 = st.columns(2)

        with c1:
            contaminacion = st.slider("Sensibilidad", 0.005, 0.1, 0.02, 0.005)

        with c2:
            ventana_media = st.slider("Ventana media móvil", 12, 96, 48)

        df_eval = df_work.copy()

        # =========================================================
        # 3. ISOLATION FOREST
        # =========================================================
        features = ["potencia_kw", "hora", "dia_semana"]

        scaler = StandardScaler()
        X = scaler.fit_transform(df_eval[features])

        model_if = IsolationForest(contamination=contaminacion, random_state=42)
        df_eval["anomalia_if"] = model_if.fit_predict(X)
        df_eval["es_anomalia_if"] = df_eval["anomalia_if"] == -1

        # =========================================================
        # 4. REGRESIÓN
        # =========================================================
        X_reg = df_eval[["hora", "dia_semana"]]
        y_reg = df_eval["potencia_kw"]

        modelo_reg = LinearRegression()
        modelo_reg.fit(X_reg, y_reg)

        df_eval["prediccion"] = modelo_reg.predict(X_reg)
        df_eval["error"] = df_eval["potencia_kw"] - df_eval["prediccion"]

        umbral = df_eval["error"].std() * 2
        df_eval["anomalia_reg"] = abs(df_eval["error"]) > umbral

        # =========================================================
        # 5. PERFIL IDEAL
        # =========================================================
        perfil = df_eval.groupby("hora")["potencia_kw"].agg(["mean", "std"]).reset_index()
        perfil["upper_ref"] = perfil["mean"] + 2 * perfil["std"]
        perfil["lower_ref"] = (perfil["mean"] - 2 * perfil["std"]).clip(lower=0)

        df_eval = df_eval.join(
            perfil.set_index("hora")[["upper_ref", "lower_ref"]],
            on="hora"
        )

        # =========================================================
        # 6. MEDIA MÓVIL
        # =========================================================
        df_eval["media_movil"] = df_eval["potencia_kw"].rolling(window=ventana_media, min_periods=1).mean()

        # =========================================================
        # 7. KPIs
        # =========================================================
        total = len(df_eval)
        anom = df_eval["anomalia_reg"].sum()
        error_medio = df_eval["error"].abs().mean()

        k1, k2, k3 = st.columns(3)

        k1.metric("Registros", total)
        k2.metric("Anomalías", anom)
        k3.metric("Error modelo", f"{error_medio:.2f} kW")

        # =========================================================
        # 8. GRÁFICO PRINCIPAL
        # =========================================================
        st.markdown("### Serie auditada (kW)")

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=df_eval.index,
            y=df_eval["potencia_kw"],
            mode="lines",
            name="Potencia real",
            line=dict(color="#1f77b4")
        ))

        fig.add_trace(go.Scatter(
            x=df_eval.index,
            y=df_eval["prediccion"],
            mode="lines",
            name="Modelo",
            line=dict(color="#2c3e50", width=2)
        ))

        fig.add_trace(go.Scatter(
            x=df_eval.index,
            y=df_eval["media_movil"],
            mode="lines",
            name="Media móvil",
            line=dict(color="black", dash="dash")
        ))

        fig.add_trace(go.Scatter(
            x=df_eval.index,
            y=df_eval["upper_ref"],
            mode="lines",
            line=dict(width=0),
            showlegend=False
        ))

        fig.add_trace(go.Scatter(
            x=df_eval.index,
            y=df_eval["lower_ref"],
            mode="lines",
            fill="tonexty",
            fillcolor="rgba(0,200,100,0.1)",
            name="Banda esperada",
            line=dict(width=0)
        ))

        # anomalías
        anomalias = df_eval[df_eval["anomalia_reg"]]

        fig.add_trace(go.Scatter(
            x=anomalias.index,
            y=anomalias["potencia_kw"],
            mode="markers",
            name="Anomalías",
            marker=dict(color="red", size=6)
        ))

        fig.update_layout(
            height=450,
            template="plotly_white",
            yaxis_title="Potencia (kW)",
            xaxis_title="Fecha"
        )

        st.plotly_chart(fig, use_container_width=True)

        # =========================================================
        # 9. HEATMAP
        # =========================================================
        st.markdown("### Mapa de anomalías")

        if not anomalias.empty:
            heat = (
                anomalias.assign(hora=anomalias.index.hour, dia=anomalias.index.dayofweek)
                .groupby(["dia", "hora"])
                .size()
                .unstack(fill_value=0)
            )

            fig_heat = go.Figure(data=go.Heatmap(
                z=heat.values,
                x=heat.columns,
                y=heat.index,
                colorscale="YlOrRd"
            ))

            st.plotly_chart(fig_heat, use_container_width=True)

        # =========================================================
        # 10. TABLA
        # =========================================================
        st.markdown("### Detalle")

        if not anomalias.empty:
            tabla = anomalias.copy()
            tabla["Fecha"] = tabla.index.strftime("%d/%m %H:%M")
            tabla["kW"] = tabla["potencia_kw"].round(2)
            tabla["Error"] = tabla["error"].round(2)

            st.dataframe(
                tabla[["Fecha", "kW", "Error"]],
                use_container_width=True
            )

    except Exception as e:
        st.error(f"Error: {e}")
