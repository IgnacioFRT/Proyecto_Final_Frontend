import streamlit as st
import plotly.graph_objects as go
import numpy as np


from views.historico import get_historical_data


def render_perfil_dinamico():
    try:
        with st.spinner("Procesando perfiles de carga interactivos... ⏳"):
            df = get_historical_data().copy()

            if df.empty:
                st.warning("No se encontraron datos históricos para perfil dinámico.")
                return

            df["incremento_kWh"] = df["EA_imp_T1_kwh"].diff().clip(lower=0).fillna(0)
            df["hora"] = df.index.hour

            dias_map = {
                0: "Lunes",
                1: "Martes",
                2: "Miércoles",
                3: "Jueves",
                4: "Viernes",
                5: "Sábado",
                6: "Domingo"
            }
            order_dias = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
            df["nombre_dia"] = df.index.dayofweek.map(dias_map)

        # ===== 1) PROMEDIO SEMANAL POR FASE =====
        df_diario_sem = df.resample("D").agg({
            "P1": "mean",
            "P2": "mean",
            "P3": "mean",
            "EA_imp_T1_kwh": "last"
        }).copy()

        df_diario_sem["P_total"] = df_diario_sem["P1"] + df_diario_sem["P2"] + df_diario_sem["P3"]
        diff_en_sem = df_diario_sem["EA_imp_T1_kwh"].diff().clip(lower=0).fillna(0)

        df_diario_sem["L1"] = np.where(
            df_diario_sem["P_total"] > 0,
            (df_diario_sem["P1"] / df_diario_sem["P_total"]) * diff_en_sem,
            0
        )
        df_diario_sem["L2"] = np.where(
            df_diario_sem["P_total"] > 0,
            (df_diario_sem["P2"] / df_diario_sem["P_total"]) * diff_en_sem,
            0
        )
        df_diario_sem["L3"] = np.where(
            df_diario_sem["P_total"] > 0,
            (df_diario_sem["P3"] / df_diario_sem["P_total"]) * diff_en_sem,
            0
        )

        df_diario_sem["nombre_dia"] = df_diario_sem.index.dayofweek.map(dias_map)
        df_semana_avg = df_diario_sem.groupby("nombre_dia")[["L1", "L2", "L3"]].mean().reindex(order_dias)
        df_semana_avg["Total"] = df_semana_avg.sum(axis=1)

        # ===== 2) PERFIL HORARIO PROMEDIO =====
        df_hora_avg = df.groupby("hora").agg({
            "P1": "mean",
            "P2": "mean",
            "P3": "mean",
            "incremento_kWh": "mean"
        }).copy()

        p_sum_h = df_hora_avg[["P1", "P2", "P3"]].sum(axis=1)

        for i in range(1, 4):
            df_hora_avg[f"L{i}_kWh"] = np.where(
                p_sum_h > 0,
                (df_hora_avg[f"P{i}"] / p_sum_h) * df_hora_avg["incremento_kWh"] * 4,
                0
            )

        df_hora_avg["Total"] = df_hora_avg[["L1_kWh", "L2_kWh", "L3_kWh"]].sum(axis=1)

        # ===== 3) MAPA DE CALOR =====
        df_heat = df.groupby(["nombre_dia", "hora"])["incremento_kWh"].mean().unstack().reindex(order_dias)

        # ===== FRONTEND =====
        st.markdown("### Perfil de carga dinámico")
        st.caption("Análisis de hábitos de consumo por semana, hora del día y mapa de calor")

        col_izq, col_espacio, col_der = st.columns([1.2, 0.08, 1.2])

        colores_fase = ["#1f77b4", "#ff7f0e", "#2ca02c"]

        with col_izq:
            st.markdown("#### Promedio diario por semana")

            fig_sem = go.Figure()
            for i, linea in enumerate(["L1", "L2", "L3"]):
                fig_sem.add_trace(go.Bar(
                    x=df_semana_avg.index,
                    y=df_semana_avg[linea],
                    name=f"Línea {i+1}",
                    marker_color=colores_fase[i]
                ))

            fig_sem.add_trace(go.Scatter(
                x=df_semana_avg.index,
                y=df_semana_avg["Total"],
                mode="text",
                text=df_semana_avg["Total"].apply(lambda x: f"<b>{x:.1f}</b>"),
                textposition="top center",
                showlegend=False
            ))

            fig_sem.update_layout(
                barmode="stack",
                height=430,
                template="plotly_white",
                margin=dict(t=20, b=110, l=40, r=20),
                updatemenus=[dict(
                    type="buttons",
                    direction="right",
                    active=0,
                    x=0.5,
                    y=-0.28,
                    xanchor="center",
                    buttons=[
                        dict(label="Ver Todo", method="update", args=[{"visible": [True, True, True, True]}]),
                        dict(label="Solo L1", method="update", args=[{"visible": [True, False, False, False]}]),
                        dict(label="Solo L2", method="update", args=[{"visible": [False, True, False, False]}]),
                        dict(label="Solo L3", method="update", args=[{"visible": [False, False, True, False]}]),
                    ]
                )]
            )
            st.plotly_chart(fig_sem, use_container_width=True)

            st.markdown("#### Perfil típico de 24 horas")

            fig_hora = go.Figure()
            horas_x = [f"{h:02d}:00" for h in range(24)]

            for i in range(1, 4):
                fig_hora.add_trace(go.Bar(
                    x=horas_x,
                    y=df_hora_avg[f"L{i}_kWh"],
                    name=f"Línea {i}",
                    marker_color=colores_fase[i-1]
                ))

            fig_hora.add_trace(go.Scatter(
                x=horas_x,
                y=df_hora_avg["Total"],
                mode="text",
                text=df_hora_avg["Total"].apply(lambda x: f"<b>{x:.1f}</b>"),
                textposition="top center",
                showlegend=False
            ))

            fig_hora.update_layout(
                barmode="stack",
                height=430,
                template="plotly_white",
                margin=dict(t=20, b=120, l=40, r=20),
                updatemenus=[dict(
                    type="buttons",
                    direction="right",
                    active=0,
                    x=0.5,
                    y=-0.30,
                    xanchor="center",
                    buttons=[
                        dict(label="Ver Todo", method="update", args=[{"visible": [True, True, True, True]}]),
                        dict(label="Solo L1", method="update", args=[{"visible": [True, False, False, False]}]),
                        dict(label="Solo L2", method="update", args=[{"visible": [False, True, False, False]}]),
                        dict(label="Solo L3", method="update", args=[{"visible": [False, False, True, False]}]),
                    ]
                )]
            )
            st.plotly_chart(fig_hora, use_container_width=True)

        with col_espacio:
            st.markdown(
                """
                <div style="border-left: 2px solid #e6e9ef; height: 900px; margin-left: 50%;"></div>
                """,
                unsafe_allow_html=True
            )

        with col_der:
            st.markdown("#### Mapa de calor de consumo (kWh)")

            fig_heat = go.Figure(data=go.Heatmap(
                z=df_heat.values,
                x=[f"{h:02d}:00" for h in range(24)],
                y=df_heat.index,
                colorscale="YlOrRd",
                hoverongaps=False,
                hovertemplate="Día: %{y}<br>Hora: %{x}<br>Consumo: <b>%{z:.2f} kWh</b><extra></extra>"
            ))

            fig_heat.update_layout(
                height=900,
                margin=dict(t=40, b=40, l=20, r=10),
                yaxis_autorange="reversed"
            )

            st.plotly_chart(fig_heat, use_container_width=True)
            st.info("💡 Las zonas más intensas indican horarios y días con mayor demanda promedio.")

    except Exception as e:
        st.error(f"Error al generar el perfil de carga dinámico: {e}")
