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

    variables = ["EA_imp_T1_kwh", "temp", "P1", "P2", "P3"]
    filter_fields = " or ".join([f'r["_field"] == "{v}"' for v in variables])

    query = f'''
    from(bucket: "{st.secrets["INFLUX_BUCKET"]}")
      |> range(start: 0)
      |> filter(fn: (r) => r._measurement == "{st.secrets["MEASUREMENT"]}")
      |> filter(fn: (r) => r.deviceID == "{st.secrets["DEVICE_ID"]}")
      |> filter(fn: (r) => r.proyecto == "{st.secrets["PROYECTO"]}")
      |> filter(fn: (r) => {filter_fields})
      |> aggregateWindow(every: 1h, fn: last, createEmpty: false)
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

    # Nos quedamos solo con las columnas que importan si existen
    cols = [c for c in ["EA_imp_T1_kwh", "temp", "P1", "P2", "P3"] if c in df.columns]
    df = df[cols].copy()

    return df


def add_calendar_columns(df):
    feriados = pd.to_datetime([
        '2025-01-01', '2025-03-03', '2025-03-04', '2025-03-24', '2025-04-02',
        '2025-04-18', '2025-04-19', '2025-05-01', '2025-05-25', '2025-06-16',
        '2025-06-20', '2025-07-09', '2025-08-17', '2025-10-12', '2025-11-20',
        '2025-12-08', '2025-12-25',
        '2026-01-01', '2026-02-16', '2026-02-17', '2026-03-23', '2026-03-24',
        '2026-04-02', '2026-04-03', '2026-05-01', '2026-05-25', '2026-06-15',
        '2026-06-20', '2026-07-09', '2026-08-17', '2026-10-12', '2026-11-23',
        '2026-12-08', '2026-12-25'
    ]).date

    df = df.copy()
    df["fecha"] = df.index.date
    df["es_feriado"] = df["fecha"].isin(feriados)
    df["es_finde"] = df.index.weekday.isin([5, 6])
    df["es_habil"] = (~df["es_feriado"]) & (~df["es_finde"])

    def tipo_dia(row):
        if row["es_feriado"] or row.name.weekday() == 6:
            return "Domingo/Feriado"
        elif row.name.weekday() == 5:
            return "Sábado"
        else:
            return "Hábil"

    df["tipo_dia"] = df.apply(tipo_dia, axis=1)
    return df


def build_incremental_consumption(df):
    df = df.copy()
    df["incremental_consumption"] = df["EA_imp_T1_kwh"].diff().clip(lower=0).fillna(0)
    return df


def build_daily_data(df):
    df_daily = pd.DataFrame()
    df_daily["EA_max"] = df["EA_imp_T1_kwh"].resample("D").max()
    df_daily["consumo_diario_kWh"] = df_daily["EA_max"].diff().clip(lower=0).fillna(0)

    if "temp" in df.columns:
        df_daily["temp_media"] = df["temp"].resample("D").mean()
    else:
        df_daily["temp_media"] = 0

    dias_semana_es = {
        0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
        4: "Viernes", 5: "Sábado", 6: "Domingo"
    }
    df_daily["nombre_dia"] = df_daily.index.dayofweek.map(dias_semana_es)

    return df_daily


def classify_daily_categories(df_daily):
    df_daily = df_daily.copy()

    feriados = pd.to_datetime([
        '2025-01-01', '2025-03-03', '2025-03-04', '2025-03-24', '2025-04-02',
        '2025-04-18', '2025-04-19', '2025-05-01', '2025-05-25', '2025-06-16',
        '2025-06-20', '2025-07-09', '2025-08-17', '2025-10-12', '2025-11-20',
        '2025-12-08', '2025-12-25',
        '2026-01-01', '2026-02-16', '2026-02-17', '2026-03-23', '2026-03-24',
        '2026-04-02', '2026-04-03', '2026-05-01', '2026-05-25', '2026-06-15',
        '2026-06-20', '2026-07-09', '2026-08-17', '2026-10-12', '2026-11-23',
        '2026-12-08', '2026-12-25'
    ]).date

    def categorizar(fecha):
        if fecha.date() in feriados:
            return "Feriado"
        if fecha.weekday() == 6:
            return "Domingo"
        if fecha.weekday() == 5:
            return "Sábado"
        return "Día hábil"

    df_daily["categoria"] = df_daily.index.map(categorizar)
    return df_daily


def compute_energy_by_day_type(df):
    energia_total = df["EA_imp_T1_kwh"].max() - df["EA_imp_T1_kwh"].min()

    energia_habil_raw = df[df["tipo_dia"] == "Hábil"]["incremental_consumption"].sum()
    energia_sabado_raw = df[df["tipo_dia"] == "Sábado"]["incremental_consumption"].sum()
    energia_domfer_raw = df[df["tipo_dia"] == "Domingo/Feriado"]["incremental_consumption"].sum()

    total_raw = energia_habil_raw + energia_sabado_raw + energia_domfer_raw

    if total_raw > 0:
        factor = energia_total / total_raw
        energia_habil = energia_habil_raw * factor
        energia_sabado = energia_sabado_raw * factor
        energia_domfer = energia_domfer_raw * factor
    else:
        energia_habil = energia_sabado = energia_domfer = 0

    return energia_habil, energia_sabado, energia_domfer, energia_total


def compute_energy_by_phase(df, energia_total):
    p1_mean = df["P1"].mean() if "P1" in df.columns else 0
    p2_mean = df["P2"].mean() if "P2" in df.columns else 0
    p3_mean = df["P3"].mean() if "P3" in df.columns else 0

    p_total_mean = p1_mean + p2_mean + p3_mean

    if p_total_mean > 0:
        energia_p1 = (p1_mean / p_total_mean) * energia_total
        energia_p2 = (p2_mean / p_total_mean) * energia_total
        energia_p3 = (p3_mean / p_total_mean) * energia_total
    else:
        energia_p1 = energia_p2 = energia_p3 = 0

    return energia_p1, energia_p2, energia_p3


def compute_daily_phase_breakdown(df):
    df_phase = df.resample("D").agg({
        "P1": "mean",
        "P2": "mean",
        "P3": "mean",
        "EA_imp_T1_kwh": "last"
    }).copy()

    df_phase["P_total_medio"] = df_phase["P1"] + df_phase["P2"] + df_phase["P3"]
    df_phase["consumo_diario_total_kWh"] = df_phase["EA_imp_T1_kwh"].diff().clip(lower=0).fillna(0)

    df_phase["P1_kWh"] = 0.0
    df_phase["P2_kWh"] = 0.0
    df_phase["P3_kWh"] = 0.0

    mask = df_phase["P_total_medio"] > 0

    df_phase.loc[mask, "P1_kWh"] = (
        df_phase.loc[mask, "P1"] / df_phase.loc[mask, "P_total_medio"]
    ) * df_phase.loc[mask, "consumo_diario_total_kWh"]

    df_phase.loc[mask, "P2_kWh"] = (
        df_phase.loc[mask, "P2"] / df_phase.loc[mask, "P_total_medio"]
    ) * df_phase.loc[mask, "consumo_diario_total_kWh"]

    df_phase.loc[mask, "P3_kWh"] = (
        df_phase.loc[mask, "P3"] / df_phase.loc[mask, "P_total_medio"]
    ) * df_phase.loc[mask, "consumo_diario_total_kWh"]

    dias_semana_es = {
        0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
        4: "Viernes", 5: "Sábado", 6: "Domingo"
    }
    df_phase["nombre_dia"] = df_phase.index.dayofweek.map(dias_semana_es)

    return df_phase

def compute_monthly_consumption(df):
    df_month = pd.DataFrame()
    df_month["EA_max"] = df["EA_imp_T1_kwh"].resample("D").max()
    df_month["consumo_diario"] = df_month["EA_max"].diff().clip(lower=0).fillna(0)
    mensual = df_month["consumo_diario"].resample("MS").sum()
    mensual = mensual[mensual > 0]
    return mensual


def render_historico():
    st.markdown("## Resumen Histórico")
    st.caption("Histórico completo con tortas, evolución diaria y consumo mensual")

    try:
        df = get_historical_data()

        if df.empty:
            st.warning("No se encontraron datos históricos.")
            return

        df = add_calendar_columns(df)
        df = build_incremental_consumption(df)
        df_daily = build_daily_data(df)
        df_daily = classify_daily_categories(df_daily)

        energia_habil, energia_sabado, energia_domfer, energia_total = compute_energy_by_day_type(df)
        energia_p1, energia_p2, energia_p3 = compute_energy_by_phase(df, energia_total)
        df_month = compute_monthly_consumption(df)
        df_phase_daily = compute_daily_phase_breakdown(df)

        consumo_promedio = df_daily["consumo_diario_kWh"].mean()
        dias_con_datos = (df_daily["consumo_diario_kWh"] > 0).sum()
        dia_max = df_daily["consumo_diario_kWh"].idxmax()
        valor_max = df_daily["consumo_diario_kWh"].max()

    except Exception as e:
        st.error(f"Error cargando histórico: {e}")
        return

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Energía total", f"{energia_total:,.1f} kWh")
    k2.metric("Consumo diario promedio", f"{consumo_promedio:,.1f} kWh/día")
    k3.metric("Días con datos", f"{dias_con_datos}")
    k4.metric("Pico diario", f"{valor_max:,.1f} kWh")

    st.markdown("### Análisis principal")

    col_torta_dia, col_barras_dia = st.columns([1, 2])

    with col_torta_dia:
        fig_torta = go.Figure(data=[go.Pie(
            labels=["Días hábiles", "Sábados", "Domingos/Feriados"],
            values=[energia_habil, energia_sabado, energia_domfer],
            marker_colors=["#66bb6a", "#42a5f5", "#ef5350"],
            pull=[0.03, 0.03, 0.03],
            textinfo="percent+label",
            textposition="outside",
            hovertemplate="%{label}<br>%{value:,.1f} kWh<br>%{percent}<extra></extra>"
        )])

        fig_torta.update_layout(
            title="Consumo por tipo de día",
            height=430,
            showlegend=False,
            margin=dict(l=10, r=10, t=50, b=10),
            paper_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig_torta, use_container_width=True)

    with col_barras_dia:
        color_map = {
            "Día hábil": "#2ca02c",
            "Sábado": "#1f77b4",
            "Domingo": "#ff7f0e",
            "Feriado": "#d62728"
        }

        fig_barras = go.Figure()
        for categoria, color in color_map.items():
            df_temp = df_daily[df_daily["categoria"] == categoria]
            if not df_temp.empty:
                fig_barras.add_trace(go.Bar(
                    x=df_temp.index,
                    y=df_temp["consumo_diario_kWh"],
                    name=categoria,
                    marker_color=color,
                    customdata=df_temp[["nombre_dia", "categoria"]],
                    hovertemplate="<b>%{customdata[0]}</b>, %{x|%d/%m/%Y}<br>Consumo: %{y:.2f} kWh<br>Tipo: %{customdata[1]}<extra></extra>"
                ))

        fig_barras.update_layout(
            title="Evolución de consumo diario",
            height=430,
            template="plotly_white",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=20, r=20, t=50, b=20),
            yaxis_title="kWh"
        )
        st.plotly_chart(fig_barras, use_container_width=True)

    st.markdown("### Distribución eléctrica")

col_torta_fase, col_stack_fase = st.columns([1, 2])

with col_torta_fase:
    fig_fase = go.Figure(data=[go.Pie(
        labels=["Línea 1", "Línea 2", "Línea 3"],
        values=[energia_p1, energia_p2, energia_p3],
        marker_colors=["#1f77b4", "#ff7f0e", "#2ca02c"],
        pull=[0.03, 0.03, 0.03],
        textinfo="percent+label",
        textposition="outside",
        hovertemplate="%{label}<br>%{value:,.1f} kWh<br>%{percent}<extra></extra>"
    )])

    fig_fase.update_layout(
        title="Distribución estimada por fase",
        height=430,
        showlegend=False,
        margin=dict(l=10, r=10, t=50, b=10),
        paper_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig_fase, use_container_width=True)

with col_stack_fase:
    fig_stack = go.Figure()

    fig_stack.add_trace(go.Bar(
        x=df_phase_daily.index,
        y=df_phase_daily["P1_kWh"],
        name="Línea 1",
        marker_color="#1f77b4",
        customdata=df_phase_daily["nombre_dia"],
        hovertemplate="<b>%{customdata}</b>, %{x|%d/%m/%Y}<br>Línea 1: %{y:.2f} kWh<extra></extra>"
    ))

    fig_stack.add_trace(go.Bar(
        x=df_phase_daily.index,
        y=df_phase_daily["P2_kWh"],
        name="Línea 2",
        marker_color="#ff7f0e",
        customdata=df_phase_daily["nombre_dia"],
        hovertemplate="<b>%{customdata}</b>, %{x|%d/%m/%Y}<br>Línea 2: %{y:.2f} kWh<extra></extra>"
    ))

    fig_stack.add_trace(go.Bar(
        x=df_phase_daily.index,
        y=df_phase_daily["P3_kWh"],
        name="Línea 3",
        marker_color="#2ca02c",
        customdata=df_phase_daily["nombre_dia"],
        hovertemplate="<b>%{customdata}</b>, %{x|%d/%m/%Y}<br>Línea 3: %{y:.2f} kWh<extra></extra>"
    ))

    fig_stack.update_layout(
        title="Desglose diario por fase",
        barmode="stack",
        height=430,
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=20, r=20, t=50, b=20),
        yaxis_title="kWh",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig_stack, use_container_width=True)

st.markdown("### Consumo mensual")

meses_nombres = df_month.index.strftime("%b %Y").str.capitalize()

fig_month = go.Figure()
fig_month.add_trace(go.Bar(
    x=meses_nombres,
    y=df_month.values,
    marker_color="#2c3e50",
    text=[f"{val:,.0f} kWh" for val in df_month.values],
    textposition="auto",
    hovertemplate="<b>%{x}</b><br>Consumo: %{y:,.1f} kWh<extra></extra>"
))

fig_month.update_layout(
    title="Consumo mensual total",
    template="plotly_white",
    height=430,
    margin=dict(l=20, r=20, t=50, b=20),
    yaxis_title="kWh"
)
st.plotly_chart(fig_month, use_container_width=True)
    st.markdown("### Lectura rápida")

    i1, i2 = st.columns(2)
    with i1:
        st.info(f"📌 Mayor consumo diario: **{valor_max:.1f} kWh** el **{dia_max.strftime('%d/%m/%Y')}**")
    with i2:
        st.info(f"📌 Período analizado: **desde {df.index.min().strftime('%d/%m/%Y')} hasta {df.index.max().strftime('%d/%m/%Y')}**")
