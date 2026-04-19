import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pandas.tseries.offsets import MonthEnd
from influxdb_client import InfluxDBClient
import pytz


@st.cache_data(ttl=60, show_spinner=False)
def get_qos_data():
    client = InfluxDBClient(
        url=st.secrets["INFLUX_URL"],
        token=st.secrets["INFLUX_TOKEN"],
        org=st.secrets["INFLUX_ORG"]
    )

    variables = ["UL1N", "UL2N", "UL3N"]
    filter_fields = " or ".join([f'r["_field"] == "{v}"' for v in variables])

    query = f'''
    from(bucket: "{st.secrets["INFLUX_BUCKET"]}")
      |> range(start: 0)
      |> filter(fn: (r) => r._measurement == "{st.secrets["MEASUREMENT"]}")
      |> filter(fn: (r) => r.deviceID == "{st.secrets["DEVICE_ID"]}")
      |> filter(fn: (r) => r.proyecto == "{st.secrets["PROYECTO"]}")
      |> filter(fn: (r) => {filter_fields})
      |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    '''

    df = client.query_api().query_data_frame(query)

    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True)

    if df.empty:
        return pd.DataFrame()

    tz = pytz.timezone(st.secrets["TZ_NAME"])
    df["_time"] = pd.to_datetime(df["_time"]).dt.tz_convert(tz)
    df = df.rename(columns={"_time": "time"})
    df = df.set_index("time")
    df = df.sort_index()

    cols = [c for c in ["UL1N", "UL2N", "UL3N"] if c in df.columns]
    df = df[cols].copy()

    # resample como en tu lógica original
    df = df.resample("15min").agg({
        "UL1N": "mean",
        "UL2N": "mean",
        "UL3N": "mean"
    })

    df = df.dropna(how="all")
    return df


def render_calidad_qos():
    try:
        with st.spinner("Evaluando disponibilidad y gaps de datos... ⏳"):
            df = get_qos_data()

            if df.empty:
                st.warning("No se encontraron datos históricos para análisis QoS.")
                return

            start = df.index.min()
            end = df.index.max()

            # ===== DISPONIBILIDAD GLOBAL =====
            esperados_global = len(pd.date_range(start, end, freq="15min"))
            reales_global = len(df)
            registrado_global = (reales_global / esperados_global) * 100 if esperados_global > 0 else 0
            no_registrado_global = 100 - registrado_global

            # ===== DISPONIBILIDAD MENSUAL =====
            df_reales_mes = df.resample("MS").size()
            meses_labels, porcentajes_mes, reales_lista, esperados_lista = [], [], [], []

            for mes_start, count in df_reales_mes.items():
                mes_end = mes_start + MonthEnd(1) + pd.Timedelta(hours=23, minutes=45)
                calc_start = max(mes_start, start.replace(second=0, microsecond=0))
                calc_end = min(mes_end, end.replace(second=0, microsecond=0))

                if calc_start <= calc_end:
                    esperados_m = len(pd.date_range(calc_start, calc_end, freq="15min"))
                    porc = (count / esperados_m) * 100 if esperados_m > 0 else 0

                    meses_labels.append(mes_start.strftime("%b %Y").capitalize())
                    porcentajes_mes.append(porc)
                    reales_lista.append(count)
                    esperados_lista.append(esperados_m)

            # ===== DETECCIÓN DE GAPS =====
            HORAS_FILTRO = 2

            df_grafico = df.copy()
            lista_cortes = []

            time_diff = df.index.to_series().diff()
            cortes_graves = df[time_diff >= pd.Timedelta(hours=HORAS_FILTRO)]

            nuevos_puntos_0v = []

            for idx, row in cortes_graves.iterrows():
                duracion = time_diff[idx]
                fin_corte = idx
                inicio_corte = idx - duracion

                horas, remainder = divmod(duracion.total_seconds(), 3600)
                minutos, _ = divmod(remainder, 60)

                lista_cortes.append({
                    "Inicio_dt": inicio_corte,
                    "Fecha y Hora": inicio_corte.strftime("%d/%m/%Y %H:%M"),
                    "Hora de Reconexión": fin_corte.strftime("%d/%m/%Y %H:%M"),
                    "Duración": f"{int(horas)}h {int(minutos)}m",
                    "Diagnóstico": "🔴 Falla de comunicación"
                })

                nuevos_puntos_0v.append({
                    "index": inicio_corte + pd.Timedelta(seconds=1),
                    "UL1N": 0,
                    "UL2N": 0,
                    "UL3N": 0
                })
                nuevos_puntos_0v.append({
                    "index": fin_corte - pd.Timedelta(seconds=1),
                    "UL1N": 0,
                    "UL2N": 0,
                    "UL3N": 0
                })

            df_cortes = pd.DataFrame(lista_cortes)

            if nuevos_puntos_0v:
                df_inyectado = pd.DataFrame(nuevos_puntos_0v).set_index("index")
                df_grafico = pd.concat([df_grafico, df_inyectado]).sort_index()

            # sin filtro por mes
            df_filtrado = df_grafico
            df_cortes_filtrado = df_cortes

        # ===== FILA 1 =====
        col_tendencia, col_torta = st.columns([1.5, 1])

        with col_tendencia:
            st.markdown("#### Tendencia de disponibilidad mensual")

            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=meses_labels,
                y=porcentajes_mes,
                mode="lines+markers+text",
                marker=dict(size=14, color="#1f77b4", line=dict(width=2, color="white")),
                line=dict(width=4, color="#1f77b4"),
                text=[f"{p:.1f}%" for p in porcentajes_mes],
                textposition="top center",
                customdata=list(zip(reales_lista, esperados_lista)),
                hovertemplate="<b>%{x}</b><br>Disponibilidad: <b>%{y:.2f}%</b><br>Registros: %{customdata[0]:,} / %{customdata[1]:,}<extra></extra>",
                textfont=dict(color="#333333")
            ))

            fig_trend.update_layout(
                height=420,
                margin=dict(t=40, b=40, l=40, r=20),
                font=dict(color="#333333"),
                yaxis=dict(
                    title="Disponibilidad (%)",
                    range=[max(0, min(porcentajes_mes) - 10) if porcentajes_mes else 0, 105],
                    gridcolor="#e5e8e8"
                ),
                xaxis=dict(gridcolor="#e5e8e8"),
                template="plotly_white"
            )
            st.plotly_chart(fig_trend, use_container_width=True)

        with col_torta:
            st.markdown("#### Resumen histórico global")

            fig_pie = go.Figure(data=[go.Pie(
                labels=["Datos registrados", "Gaps"],
                values=[registrado_global, no_registrado_global],
                marker_colors=["#66bb6a", "#ef5350"],
                pull=[0.05, 0],
                textinfo="percent+label",
                textposition="outside",
                textfont=dict(color="black")
            )])

            fig_pie.update_layout(
                height=380,
                margin=dict(t=40, b=20, l=20, r=20),
                showlegend=False,
                template="plotly_white"
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        st.divider()

        # ===== FILA 2 =====
        st.markdown("#### Análisis físico de caídas de tensión")

        col_grafico, col_tabla = st.columns([2, 1])

        with col_grafico:
            fig_tension = go.Figure()
            fig_tension.add_trace(go.Scatter(
                x=df_filtrado.index, y=df_filtrado["UL1N"],
                name="Línea 1", line=dict(color="#1f77b4", width=1)
            ))
            fig_tension.add_trace(go.Scatter(
                x=df_filtrado.index, y=df_filtrado["UL2N"],
                name="Línea 2", line=dict(color="#ff7f0e", width=1)
            ))
            fig_tension.add_trace(go.Scatter(
                x=df_filtrado.index, y=df_filtrado["UL3N"],
                name="Línea 3", line=dict(color="#2ca02c", width=1)
            ))

            fig_tension.update_layout(
                height=400,
                margin=dict(t=20, b=20, l=20, r=20),
                yaxis_title="Tensión (V)",
                template="plotly_dark",
                xaxis=dict(
                    rangeslider=dict(visible=True),
                    type="date"
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            st.plotly_chart(fig_tension, use_container_width=True)

        with col_tabla:
            st.markdown(f"**Registro de apagones (gaps ≥ {HORAS_FILTRO}h)**")
            if df_cortes_filtrado.empty:
                st.success("✅ No se registraron apagones reales en el período analizado.")
            else:
                df_mostrar = df_cortes_filtrado.drop(columns=["Inicio_dt"], errors="ignore")
                st.dataframe(df_mostrar, use_container_width=True, hide_index=True)

        st.markdown("<div style='margin-top: 18px;'></div>", unsafe_allow_html=True)

        # ===== FILA 3 =====
        max_gap_horas = 0.0
        if not df_cortes.empty:
            max_gap_horas = max(
                pd.to_timedelta(
                    df_cortes["Duración"].str.replace("h", " hours ").str.replace("m", " minutes"),
                    errors="coerce"
                ).dt.total_seconds().fillna(0) / 3600
            )

        k1, k2, k3 = st.columns(3)

        with k1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Disponibilidad global</div>
                <div class="kpi-value">{registrado_global:.1f}%</div>
                <div class="kpi-sub">Datos registrados</div>
            </div>
            """, unsafe_allow_html=True)

        with k2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Gaps detectados</div>
                <div class="kpi-value">{len(df_cortes)}</div>
                <div class="kpi-sub">Cortes mayores a {HORAS_FILTRO}h</div>
            </div>
            """, unsafe_allow_html=True)

        with k3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Gap máximo</div>
                <div class="kpi-value">{max_gap_horas:.1f} h</div>
                <div class="kpi-sub">Mayor interrupción detectada</div>
            </div>
            """, unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Error al generar el análisis de calidad QoS: {e}")
