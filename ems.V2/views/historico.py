import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from influxdb_client import InfluxDBClient
import pytz


def get_historical_data():
    client = InfluxDBClient(
        url=st.secrets["INFLUX_URL"],
        token=st.secrets["INFLUX_TOKEN"],
        org=st.secrets["INFLUX_ORG"]
    )

    query = f'''
    from(bucket: "{st.secrets["INFLUX_BUCKET"]}")
      |> range(start: -90d)
      |> filter(fn: (r) => r._measurement == "{st.secrets["MEASUREMENT"]}")
      |> filter(fn: (r) => r.deviceID == "{st.secrets["DEVICE_ID"]}")
      |> filter(fn: (r) => r.proyecto == "{st.secrets["PROYECTO"]}")
      |> filter(fn: (r) => r._field == "EA_imp_T1_kwh" or r._field == "temp")
      |> aggregateWindow(every: 1h, fn: last, createEmpty: false)
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    '''

    df = client.query_api().query_data_frame(query)

    if df.empty:
        return pd.DataFrame()

    tz = pytz.timezone(st.secrets["TZ_NAME"])
    df["_time"] = pd.to_datetime(df["_time"]).dt.tz_convert(tz)
    df = df.rename(columns={"_time": "time"})
    df = df.set_index("time")

    return df


def build_daily_consumption(df):
    df_daily = pd.DataFrame()
    df_daily["EA_max"] = df["EA_imp_T1_kwh"].resample("D").max()
    df_daily["consumo_diario_kWh"] = df_daily["EA_max"].diff().clip(lower=0).fillna(0)
    df_daily["temp_media"] = df["temp"].resample("D").mean()
    return df_daily


def build_monthly_consumption(df_daily):
    df_month = df_daily["consumo_diario_kWh"].resample("MS").sum()
    return df_month


def render_historico():
    st.markdown("## Resumen Histórico")
    st.caption("Vista resumida de energía, consumo diario y tendencia mensual")

    try:
        df = get_historical_data()

        if df.empty:
            st.warning("No se encontraron datos históricos.")
            return

        df_daily = build_daily_consumption(df)
        df_month = build_monthly_consumption(df_daily)

        energia_total = df["EA_imp_T1_kwh"].max() - df["EA_imp_T1_kwh"].min()
        consumo_promedio = df_daily["consumo_diario_kWh"].mean()
        temp_promedio = df_daily["temp_media"].mean()

    except Exception as e:
        st.error(f"Error cargando histórico: {e}")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Energía acumulada", f"{energia_total:,.1f} kWh")
    c2.metric("Consumo diario promedio", f"{consumo_promedio:,.1f} kWh/día")
    c3.metric("Temperatura media", f"{temp_promedio:,.1f} °C")

    st.markdown("### Consumo diario")

    fig_daily = go.Figure()
    fig_daily.add_trace(go.Bar(
        x=df_daily.index,
        y=df_daily["consumo_diario_kWh"],
        name="Consumo diario",
        hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Consumo: %{y:.2f} kWh<extra></extra>"
    ))
    fig_daily.update_layout(
        template="plotly_white",
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        yaxis_title="kWh"
    )
    st.plotly_chart(fig_daily, use_container_width=True)

    st.markdown("### Consumo mensual")

    fig_month = go.Figure()
    fig_month.add_trace(go.Bar(
        x=df_month.index.strftime("%b %Y"),
        y=df_month.values,
        name="Consumo mensual",
        hovertemplate="<b>%{x}</b><br>Consumo: %{y:.2f} kWh<extra></extra>"
    ))
    fig_month.update_layout(
        template="plotly_white",
        height=350,
        margin=dict(l=20, r=20, t=40, b=20),
        yaxis_title="kWh"
    )
    st.plotly_chart(fig_month, use_container_width=True)

    st.markdown("### Lectura rápida del comportamiento")

    left, right = st.columns(2)

    with left:
        max_day = df_daily["consumo_diario_kWh"].idxmax()
        max_value = df_daily["consumo_diario_kWh"].max()
        st.info(f"📌 Mayor consumo diario: **{max_value:.1f} kWh** el **{max_day.strftime('%d/%m/%Y')}**")

    with right:
        min_day = df_daily["consumo_diario_kWh"].idxmin()
        min_value = df_daily["consumo_diario_kWh"].min()
        st.info(f"📌 Menor consumo diario: **{min_value:.1f} kWh** el **{min_day.strftime('%d/%m/%Y')}**")
