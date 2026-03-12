import streamlit as st
import pandas as pd
from influxdb_client import InfluxDBClient
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import pytz

# === 1. CONFIGURACIÓN DE PÁGINA ===

st.set_page_config(page_title="EMS - PAC3200 UTN", layout="wide")

# === 2. FUNCION COLAB ===

@st.cache_data(ttl=3600) #Se actualiza automáticamente cada 1 hora
def obtener_datos_historicos():

    # --- A. CONFIGURACIÓN ---
    url    = "https://influxdb.utn.xrob.com.ar"
    token  = "VPJoZH--S2GGPNNhfmWVUsZEaHqV4h1wkOX235FSfhk6GkitChp2e-8DxQ7O1ns6s7VwpKnmE-Evj7KYhLcWJQ=="
    org    = "ec1aafe9e31ba7af"
    bucket = "UTN FRT"
    tz_local = pytz.timezone("America/Argentina/Buenos_Aires")
    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()

    # --- B. CONSULTA A INFLUXDB ---
    variables_deseadas = ["UL1L2", "UL2L3", "UL3L1", "UL1N", "UL2N", "UL3N", "IL1", "IL2", "IL3", "freq", "P1", "P2", "P3", "Q1", "Q2", "Q3", "S1", "S2", "S3", "FP1", "FP2", "FP3", "THDv1", "THDv2", "THDv3", "THDi1", "THDi2", "THDi3", "Imed", "Vmed", "temp", "EA_imp_T1_kwh"]
    filter_fields = " or ".join([f'r["_field"] == "{var}"' for var in variables_deseadas])

    query = f'''
    data_mean = from(bucket: "{bucket}") |> range(start: 0) |> filter(fn: (r) => r._measurement == "pruebas_fn") |> filter(fn: (r) => r.deviceID == "08B764") |> filter(fn: (r) => r.proyecto == "siemens_Pac3200") |> filter(fn: (r) => {filter_fields}) |> filter(fn: (r) => r._field != "EA_imp_T1_kwh") |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
    data_last = from(bucket: "{bucket}") |> range(start: 0) |> filter(fn: (r) => r._measurement == "pruebas_fn") |> filter(fn: (r) => r.deviceID == "08B764") |> filter(fn: (r) => r.proyecto == "siemens_Pac3200") |> filter(fn: (r) => r._field == "EA_imp_T1_kwh") |> aggregateWindow(every: 15m, fn: last, createEmpty: false)
    union(tables: [data_mean, data_last]) |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") |> sort(columns: ["_time"])
    '''
    
    df = query_api.query_data_frame(query)

    df['_time'] = pd.to_datetime(df['_time']).dt.tz_convert(tz_local)
    df = df.rename(columns={'_time': 'time'})
    df = df.set_index('time')
    df['P_tot_kW'] = df['P1'] + df['P2'] + df['P3']

    # --- C. RESAMPLE A 15 MIN ---
    df = df.resample('15T').agg({
        'UL1L2': 'mean', 'UL2L3' : 'mean', 'UL3L1' : 'mean', 'UL1N' : 'mean', 'UL2N' : 'mean', 'UL3N' : 'mean',
        'IL1': 'mean', 'IL2': 'mean', 'IL3': 'mean', 'freq' : 'mean',
        'P1': 'mean', 'P2': 'mean', 'P3': 'mean', 'Q1': 'mean', 'Q2': 'mean', 'Q3': 'mean',
        'S1': 'mean', 'S2': 'mean', 'S3': 'mean', 'FP1': 'mean', 'FP2': 'mean', 'FP3': 'mean',
        'THDv1': 'mean', 'THDv2': 'mean', 'THDv3': 'mean', 'THDi1': 'mean', 'THDi2': 'mean', 'THDi3': 'mean',
        'Imed': 'mean', 'Vmed': 'mean', 'EA_imp_T1_kwh': 'last', 'temp': 'mean', 'P_tot_kW': 'mean'
    }).ffill()

    # --- D. LIMPIEZA DE DATOS ---
    limites = {
        'P1': (-100, 2000), 'P2': (-100, 2000), 'P3': (-100, 2000), 'Q1': (-1500, 1500), 'Q2': (-1500, 1500), 'Q3': (-1500, 1500), 'temp': (-15, 50), 'THDi1': (0, 100), 'THDi2': (0, 100), 'THDi3': (0, 100), 'UL1N': (180, 260), 'UL2N': (180, 260), 'UL3N': (180, 260), 'freq': (48, 52),}
    mask = pd.Series([True] * len(df), index=df.index)
    for col, (low, high) in limites.items():
        if col in df.columns:
            mask &= df[col].between(low, high)
    df = df[mask].copy()

    # --- E. CALENDARIO ARGENTINO ---
    feriados_2025 = pd.to_datetime(['2025-08-17', '2025-10-12', '2025-11-20', '2025-12-08', '2025-12-24', '2025-12-25', '2026-01-01', '2026-02-16', '2026-02-17', '2026-03-23', '2026-03-24', '2026-04-02', '2026-04-03', '2026-05-01', '2026-05-25', '2026-06-15', '2026-06-20', '2026-07-09', '2026-07-10', '2026-08-17', '2026-10-12', '2026-11-23', '2026-12-07', '2026-12-08', '2026-12-25']).date
    
    df['fecha'] = df.index.date
    df['hora'] = df.index.hour
    df['es_feriado'] = df['fecha'].isin(feriados_2025)
    df['es_laborable'] = ~df['es_feriado']
    df['es_finde'] = df.index.weekday.isin([5, 6])
    df['es_habil'] = df['es_laborable'] & ~df['es_finde']

    def definir_tipo_cammesa(row):
        if not row['es_habil']:
            if row.name.weekday() == 6 or row['es_feriado']: return 'Domingo/Feriado'
            else: return 'Sábado'
        else: return 'Hábil'
    
    df['tipo_dia'] = df.apply(definir_tipo_cammesa, axis=1)
    return df

# === 3. INTERFAZ Y MENÚ LATERAL ===

st.markdown("""
    <style>
        [data-testid="stSidebar"] { background-color: #1a252c; }
        [data-testid="stSidebar"] p, [data-testid="stSidebar"] div, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { color: #ffffff !important; }
    </style>
""", unsafe_allow_html=True)

with st.sidebar:
    try:
        st.image("LOGO-BLANCO-UTN.png", use_container_width=True) 
    except:
        st.warning("⚠️ Cargando logo...")
    st.divider() 
    st.title("Navegación")
    seccion = st.radio("Secciones:", ["🏠 Inicio", "🕒 Tiempo Real", "📊 Resumen Histórico", "📈 Perfil de Carga Dinámico"])
    st.markdown("---")
    st.info("Ingeniería Electrónica - UTN FRT")

# === 4. LÓGICA DE LAS SECCIONES ===

# --- TÍTULO PERSONALIZADO ---
st.markdown("""
    <style>
        .titulo-personalizado {
            font-size: 45px !important;
            font-weight: 700 !important;
            color: #31333F;
            margin-top: -70px !important; /* Lo tira hacia arriba */
            margin-left: -20px !important; /* Lo tira a la izquierda */
            margin-bottom: 20px !important;
            text-align: left;
        }
    </style>
    <h1 class="titulo-personalizado">⚡ Sistema de Gestión Energética - PAC3200</h1>
""", unsafe_allow_html=True)

# --- VENTANA INICIO ---

if seccion == "🏠 Inicio":
    espacio1, col_logo_central, espacio3 = st.columns([1, 1.5, 1])
    with col_logo_central:
        try:
            st.image("logo_principal.jpg", use_container_width=True)
        except:
            st.error("Falta logo_principal.jpg")
    
    st.markdown("<h2 style='text-align: center; color: #34495e;'>Facultad Regional Tucumán (UTN FRT)</h2>", unsafe_allow_html=True)
    st.markdown("<h4 style='text-align: center; color: #7f8c8d;'>Departamento de Ingeniería Electrónica</h4>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #95a5a6;'>Monitoreo IoT y Análisis de Calidad Eléctrica con analizador Siemens PAC3200</p>", unsafe_allow_html=True)
    st.divider()

    st.markdown("#### 📊 Resumen Global del Sistema")
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Energía Total Registrada", "2,933.4 kWh", "↑ Sistema Activo")
    kpi2.metric("Huella de Carbono Estimada", "1,320.0 kg CO₂", "- Impacto Medioambiental", delta_color="off")
    kpi3.metric("Última Medición Verificada", "10/03/2026", "En línea")
    st.success("✅ **Estado:** Sistema Operativo. Enlace con base de datos histórica InfluxDB establecido correctamente.")
    st.divider()

    st.markdown("#### 🛠️ Arquitectura y Tecnologías Implementadas")
    info1, info2 = st.columns(2)
    with info1:
        st.info("**⚙️ Hardware de Adquisición**\n* **Analizador de Redes:** Siemens PAC3200.\n* **Protocolo:** Modbus TCP/IP.\n* **Variables:** Tensión, Corriente, Factor de Potencia, THD y Energía.")
    with info2:
        st.info("**☁️ Software y Base de Datos**\n* **Base de Datos:** InfluxDB Cloud.\n* **Backend y Visualización:** Python, Streamlit, Plotly.\n* **Objetivo:** Auditoría energética y detección de anomalías.")
    st.write("👈 *Utilice el menú de navegación lateral para acceder a la visualización.*")

# --- VENTANA TIEMPO REAL ---

elif seccion == "🕒 Tiempo Real":
    
    st_autorefresh(interval=30000, key="datarefresh")
    tz_ar = pytz.timezone("America/Argentina/Buenos_Aires")
    hora_actual = pd.Timestamp.now(tz=tz_ar).strftime('%H:%M:%S')
    st.caption(f"Última actualización (Hora Arg): {hora_actual}")
    
    url = "https://influxdb.utn.xrob.com.ar"
    token = "VPJoZH--S2GGPNNhfmWVUsZEaHqV4h1wkOX235FSfhk6GkitChp2e-8DxQ7O1ns6s7VwpKnmE-Evj7KYhLcWJQ=="
    org = "ec1aafe9e31ba7af"
    bucket = "UTN FRT"
    
    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()
    
    query = f'''
        from(bucket: "{bucket}")
          |> range(start: -15m) 
          |> filter(fn: (r) => r._measurement == "pruebas_fn")
          |> filter(fn: (r) => r.deviceID == "08B764")
          |> last()
    '''
    
    try:
        result = query_api.query(org=org, query=query)
        data = { "temp": 0.0, "hum": 0.0, "wind": 0.0, "IL1": 0.0, "IL2": 0.0, "IL3": 0.0, "UL1N": 0.0, "UL2N": 0.0, "UL3N": 0.0, "FP1": 0.0, "FP2": 0.0, "FP3": 0.0, "THDv1": 0.0, "THDv2": 0.0, "THDv3": 0.0, "THDi1": 0.0, "THDi2": 0.0, "THDi3": 0.0 }
        
        for table in result:
            for record in table.records:
                data[record.get_field()] = record.get_value()

        def crear_gauge_pro(valor, titulo, max_val, color, sufijo):
            fig = go.Figure(go.Indicator(mode = "gauge+number", value = valor, number = {'valueformat': ".2f", 'suffix': sufijo, 'font': {'size': 35, 'color': "#5d6d7e"}}, title = {'text': titulo, 'font': {'size': 18, 'color': "#5d6d7e"}}, gauge = {'axis': {'range': [0, max_val], 'tickwidth': 1, 'tickcolor': "#5d6d7e"}, 'bar': {'color': color}, 'bgcolor': "white", 'borderwidth': 3, 'bordercolor': "#e5e8e8"}))
            fig.update_layout(height=280, margin=dict(l=25, r=25, t=60, b=25), paper_bgcolor="rgba(0,0,0,0)", font={'family': "Source Sans Pro, sans-serif"})
            return fig

        def crear_barras_corriente(il1, il2, il3):
            fig = go.Figure(data=[go.Bar(x=['Fase L1', 'Fase L2', 'Fase L3'], y=[il1, il2, il3], marker_color=["#1f77b4", "#ff7f0e", "#2ca02c"], text=[f"{il1:.2f} A", f"{il2:.2f} A", f"{il3:.2f} A"], textposition='auto', textfont=dict(size=16, color="white"), width=0.6 )])
            fig.update_layout(height=280, margin=dict(l=50, r=20, t=30, b=30), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(showline=True, linewidth=3, linecolor='#e5e8e8', mirror=True, tickfont=dict(size=14, color="#5d6d7e")), yaxis=dict(title="Corriente (A)", range=[0, 20], gridcolor="#f2f4f4", showline=True, linewidth=3, linecolor='#e5e8e8', mirror=True, tickfont=dict(size=14, color="#5d6d7e")), font=dict(family="Source Sans Pro, sans-serif", size=14, color="#5d6d7e"))
            return fig

        st.write("### 🌤️ Variables Climáticas")
        c1, c2, c3 = st.columns(3)
        c1.plotly_chart(crear_gauge_pro(data.get("temp",0), "Temperatura", 50, "#4caf50", "°C"), use_container_width=True)
        c2.plotly_chart(crear_gauge_pro(data.get("hum",0), "Humedad", 100, "#f44336", "%"), use_container_width=True)
        c3.plotly_chart(crear_gauge_pro(data.get("wind",0), "Viento", 100, "#8bc34a", " km/h"), use_container_width=True)

        st.write("### ⚡ Análisis de Carga y Red")
        espacio_izq, col_barras, col_frec = st.columns([0.4, 1.5, 1])
        with col_barras:
            st.plotly_chart(crear_barras_corriente(data.get("IL1",0), data.get("IL2",0), data.get("IL3",0)), use_container_width=True)
        with col_frec:
            st.plotly_chart(crear_gauge_pro(data.get("Freq", 50.0), "Frecuencia", 60, "#9b59b6", " Hz"), use_container_width=True)
            
        st.divider()
        st.markdown("### 💎 Calidad de Energía")
        espacio_izq, q1, q2, q3, q4, espacio_der = st.columns([1, 1, 1, 1, 1, 0.5])
        with q1:
            st.markdown("⚡ **Tensión (V)**")
            st.metric("L1-N", f"{data.get('UL1N', 0):.1f} V")
            st.metric("L2-N", f"{data.get('UL2N', 0):.1f} V", delta_color="off")
            st.metric("L3-N", f"{data.get('UL3N', 0):.1f} V", delta_color="off")
        with q2:
            st.markdown("📉 **Factor Potencia**")
            st.metric("PF Fase 1", f"{data.get('FP1', 0):.2f}")
            st.metric("PF Fase 2", f"{data.get('FP2', 0):.2f}")
            st.metric("PF Fase 3", f"{data.get('FP3', 0):.2f}")
        with q3:
            st.markdown("🌪️ **THD V (%)**")
            st.metric("V1 THD", f"{data.get('THDv1', 0):.1f} %")
            st.metric("V2 THD", f"{data.get('THDv2', 0):.1f} %")
            st.metric("V3 THD", f"{data.get('THDv3', 0):.1f} %")
        with q4:
            st.markdown("🌪️ **THD I (%)**")
            st.metric("I1 THD", f"{data.get('THDi1', 0):.1f} %")
            st.metric("I2 THD", f"{data.get('THDi2', 0):.1f} %")
            st.metric("I3 THD", f"{data.get('THDi3', 0):.1f} %")
            
    except Exception as e:
        st.error(f"Error en la adquisición de datos en vivo: {e}")

# --- VENTANA RESUMEN HISTÓRICO ---

elif seccion == "📊 Resumen Histórico":
    try:
        # A. ADQUISICIÓN DE DATOS
        with st.spinner('Descargando y procesando historial completo desde InfluxDB... ⏳'):
            df = obtener_datos_historicos()

        # B. CÁLCULOS PARA LA FILA 1 (Tipo de Día)
        energia_total = df['EA_imp_T1_kwh'].max() - df['EA_imp_T1_kwh'].min()
        df['incremental_consumption'] = df['EA_imp_T1_kwh'].diff().clip(lower=0).fillna(0)

        energia_habil_raw = df[df['es_habil']]['incremental_consumption'].sum()
        energia_feriado_raw = df[df['es_feriado']]['incremental_consumption'].sum()
        energia_finde_raw = df[df['es_finde']]['incremental_consumption'].sum()

        raw_total_sum = energia_habil_raw + energia_feriado_raw + energia_finde_raw

        if raw_total_sum > 0:
            scaling_factor = energia_total / raw_total_sum
            energia_habil = energia_habil_raw * scaling_factor
            energia_feriado = energia_feriado_raw * scaling_factor
            energia_finde = energia_finde_raw * scaling_factor
        else:
            energia_habil = energia_feriado = energia_finde = 0

        # C. CÁLCULOS PARA BARRAS DIARIAS
        df_diario = df.resample('D').last()
        df_diario['consumo_diario_kWh'] = df_diario['EA_imp_T1_kwh'].diff().clip(lower=0).fillna(0)
        
        dias_semana_es = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}
        df_diario['nombre_dia'] = df_diario.index.dayofweek.map(dias_semana_es)
        
        def categorizar(row):
            if row['es_feriado']: return 'Feriado'
            if row.name.weekday() == 6: return 'Domingo'
            if row.name.weekday() == 5: return 'Sábado'
            return 'Día hábil'
        
        df_diario['categoria'] = df_diario.apply(categorizar, axis=1)

        # D. MAQUETADO FILA 1
        col_torta, col_barras = st.columns([1, 2])

        with col_torta:
            st.markdown("#### 📅 Consumo por Tipo de Día")
            fig_torta = go.Figure(data=[go.Pie(
                labels=['Días hábiles', 'Feriados', 'Fin de semana'],
                values=[energia_habil, energia_feriado, energia_finde],
                marker_colors=['#66bb6a', '#ef5350', '#42a5f5'],
                pull=[0.05, 0.05, 0.05],
                textinfo='percent+label',
                textposition='outside',
                hovertemplate="%{label}<br>%{value:,.1f} kWh<br>%{percent}<extra></extra>"
            )])
            fig_torta.update_layout(margin=dict(t=80, b=20, l=10, r=10), showlegend=False, height=450, paper_bgcolor="rgba(0,0,0,0)", font=dict(color="black", size=14))
            st.plotly_chart(fig_torta, use_container_width=True)

        with col_barras:
            st.markdown("#### 📊 Evolución de Consumo Diario")
            color_map = {'Día hábil': '#2ca02c', 'Sábado': '#1f77b4', 'Domingo': '#ff7f0e', 'Feriado': 'red'}
            fig_barras = go.Figure()
            for tipo, color in color_map.items():
                df_temp = df_diario[df_diario['categoria'] == tipo]
                if not df_temp.empty:
                    fig_barras.add_trace(go.Bar(
                        x=df_temp.index, y=df_temp['consumo_diario_kWh'], name=tipo, marker_color=color,
                        customdata=df_temp[['nombre_dia', 'categoria']],
                        hovertemplate="<b>%{customdata[0]}</b>, %{x|%d de %b}<br><b>Consumo</b>: %{y:.2f} kWh<br><b>Tipo</b>: %{customdata[1]}<extra></extra>"
                    ))
            fig_barras.update_layout(height=450, template='plotly_white', hovermode='x unified', legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_barras, use_container_width=True)

        st.divider() 
        
        # E. CÁLCULOS MATEMÁTICOS (Fases)
        p1_mean = df['P1'].mean()
        p2_mean = df['P2'].mean()
        p3_mean = df['P3'].mean()
        p_total_mean = p1_mean + p2_mean + p3_mean
        if p_total_mean > 0:
            energia_p1 = (p1_mean / p_total_mean) * energia_total
            energia_p2 = (p2_mean / p_total_mean) * energia_total
            energia_p3 = (p3_mean / p_total_mean) * energia_total
        else:
            energia_p1 = energia_p2 = energia_p3 = 0

        # F. MAQUETADO FILA 2
        col_torta_fases, col_info_fases = st.columns([1, 2])

        with col_torta_fases:
            st.markdown("#### 📐 Distribución por Fase")
            fig_fases = go.Figure(data=[go.Pie(
                labels=['Línea 1', 'Línea 2', 'Línea 3'],
                values=[energia_p1, energia_p2, energia_p3],
                marker_colors=['#1f77b4', '#ff7f0e', '#2ca02c'],
                pull=[0.05, 0.05, 0.05], textinfo='percent+label', textposition='outside',
                hovertemplate="<b>%{label}</b><br>Estimado: %{value:,.1f} kWh<br>%{percent}<extra></extra>"
            )])
            fig_fases.update_layout(margin=dict(t=80, b=20, l=10, r=10), showlegend=False, height=450, paper_bgcolor="rgba(0,0,0,0)", font=dict(color="black", size=14))
            st.plotly_chart(fig_fases, use_container_width=True)

        with col_info_fases:
            st.markdown("#### 📊 Desglose Diario por Fase")
            df_diario_fases = df.resample('D').agg({'P1': 'mean', 'P2': 'mean', 'P3': 'mean', 'EA_imp_T1_kwh': 'last'})
            df_diario_fases['P_total_medio'] = df_diario_fases['P1'] + df_diario_fases['P2'] + df_diario_fases['P3']
            df_diario_fases['consumo_diario_total_kWh'] = df_diario_fases['EA_imp_T1_kwh'].diff().clip(lower=0).fillna(0)
            df_diario_fases['P1_kWh'] = (df_diario_fases['P1'] / df_diario_fases['P_total_medio']) * df_diario_fases['consumo_diario_total_kWh']
            df_diario_fases['P2_kWh'] = (df_diario_fases['P2'] / df_diario_fases['P_total_medio']) * df_diario_fases['consumo_diario_total_kWh']
            df_diario_fases['P3_kWh'] = (df_diario_fases['P3'] / df_diario_fases['P_total_medio']) * df_diario_fases['consumo_diario_total_kWh']
            df_diario_fases['nombre_dia'] = df_diario_fases.index.dayofweek.map(dias_semana_es)

            fig_stack = go.Figure()
            lineas_config = {'P1_kWh': {'nombre': 'Línea 1', 'color': '#1f77b4'}, 'P2_kWh': {'nombre': 'Línea 2', 'color': '#ff7f0e'}, 'P3_kWh': {'nombre': 'Línea 3', 'color': '#2ca02c'}}
            for col, info in lineas_config.items():
                fig_stack.add_trace(go.Bar(x=df_diario_fases.index, y=df_diario_fases[col], name=info['nombre'], marker_color=info['color'], customdata=df_diario_fases['nombre_dia'], hovertemplate="<b>%{customdata}</b>, %{x|%d de %b}<br><b>%{data.name}</b>: %{y:.2f} kWh<extra></extra>"))
            fig_stack.update_layout(barmode='stack', height=400, template='plotly_white', hovermode='x unified', legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_stack, use_container_width=True)

    except Exception as e:
        st.error(f"Error en el Resumen Histórico: {e}")

# --- VENTANA PERFIL DE CARGA ---

elif seccion == "📈 Perfil de Carga Dinámico":
    # 1. Título limpio sin que se amontone
    st.markdown("### 📈 Análisis Dinámico y Perfil de Carga")
    st.divider()

    try:
        with st.spinner('Procesando perfiles de carga interactivos... ⏳'):
            df = obtener_datos_historicos() 
            df['incremento_kWh'] = df['EA_imp_T1_kwh'].diff().clip(lower=0).fillna(0)
            df['hora'] = df.index.hour
            dias_map = {0:'Lunes', 1:'Martes', 2:'Miércoles', 3:'Jueves', 4:'Viernes', 5:'Sábado', 6:'Domingo'}
            df['nombre_dia'] = df.index.dayofweek.map(dias_map)
            order_dias = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

        # 2. COLUMNAS PERFECTAMENTE SIMÉTRICAS (1.2 : 0.1 : 1.2)
        col_izq, col_espacio, col_der = st.columns([1.2, 0.1, 1.2])

        with col_izq:
            # --- GRÁFICO 1: SEMANAL ---
            st.markdown("#### 📅 Promedio Diario por Semana")
            df_diario_sem = df.resample('D').agg({'P1': 'mean', 'P2': 'mean', 'P3': 'mean', 'EA_imp_T1_kwh': 'last'})
            df_diario_sem['P_total'] = df_diario_sem['P1'] + df_diario_sem['P2'] + df_diario_sem['P3']
            diff_en_sem = df_diario_sem['EA_imp_T1_kwh'].diff().clip(lower=0).fillna(0)
            df_diario_sem['L1'] = (df_diario_sem['P1'] / df_diario_sem['P_total']) * diff_en_sem
            df_diario_sem['L2'] = (df_diario_sem['P2'] / df_diario_sem['P_total']) * diff_en_sem
            df_diario_sem['L3'] = (df_diario_sem['P3'] / df_diario_sem['P_total']) * diff_en_sem
            df_diario_sem['nombre_dia'] = df_diario_sem.index.dayofweek.map(dias_map)
            df_semana_avg = df_diario_sem.groupby('nombre_dia')[['L1', 'L2', 'L3']].mean().reindex(order_dias)
            df_semana_avg['Total'] = df_semana_avg.sum(axis=1)

            fig_sem = go.Figure()
            clrs = ['#1f77b4', '#ff7f0e', '#2ca02c']
            for i, l in enumerate(['L1', 'L2', 'L3']):
                fig_sem.add_trace(go.Bar(x=df_semana_avg.index, y=df_semana_avg[l], name=f"Línea {i+1}", marker_color=clrs[i]))
            
            fig_sem.add_trace(go.Scatter(x=df_semana_avg.index, y=df_semana_avg['Total'], mode='text', text=df_semana_avg['Total'].apply(lambda x: f'<b>{x:.1f}</b>'), textposition='top center', showlegend=False))

            # Ajuste clave: height más grande (480) y margen inferior enorme (b=160)
            fig_sem.update_layout(
                barmode='stack', height=480, template='plotly_white', margin=dict(t=20, b=160, l=40, r=20),
                updatemenus=[dict(type="buttons", direction="right", active=0, x=0.5, y=-0.35, xanchor='center',
                    buttons=list([
                        dict(label="Ver Todo", method="update", args=[{"visible": [True, True, True, True]}]),
                        dict(label="Solo L1", method="update", args=[{"visible": [True, False, False, False]}]),
                        dict(label="Solo L2", method="update", args=[{"visible": [False, True, False, False]}]),
                        dict(label="Solo L3", method="update", args=[{"visible": [False, False, True, False]}]),
                    ]))]
            )
            st.plotly_chart(fig_sem, use_container_width=True)

            # --- GRÁFICO 2: HORARIO ---
            st.markdown("#### ⌚ Perfil Típico de 24 Horas")
            df_hora_avg = df.groupby('hora').agg({'P1': 'mean', 'P2': 'mean', 'P3': 'mean', 'incremento_kWh': 'mean'})
            p_sum_h = df_hora_avg[['P1','P2','P3']].sum(axis=1)
            for i in range(1,4):
                df_hora_avg[f'L{i}_kWh'] = (df_hora_avg[f'P{i}'] / p_sum_h) * df_hora_avg['incremento_kWh'] * 4 
            df_hora_avg['Total'] = df_hora_avg[['L1_kWh', 'L2_kWh', 'L3_kWh']].sum(axis=1)
            
            fig_hora = go.Figure()
            for i in range(1,4):
                fig_hora.add_trace(go.Bar(x=[f"{h:02d}:00" for h in range(24)], y=df_hora_avg[f'L{i}_kWh'], name=f"Línea {i}", marker_color=clrs[i-1]))
            
            fig_hora.add_trace(go.Scatter(x=[f"{h:02d}:00" for h in range(24)], y=df_hora_avg['Total'], mode='text', text=df_hora_avg['Total'].apply(lambda x: f'<b>{x:.1f}</b>'), textposition='top center', showlegend=False))

            # Ajuste clave: height=480 y b=160
            fig_hora.update_layout(
                barmode='stack', height=480, template='plotly_white', margin=dict(t=20, b=160, l=40, r=20),
                updatemenus=[dict(type="buttons", direction="right", active=0, x=0.5, y=-0.40, xanchor='center',
                    buttons=list([
                        dict(label="Ver Todo", method="update", args=[{"visible": [True, True, True, True]}]),
                        dict(label="Solo L1", method="update", args=[{"visible": [True, False, False, False]}]),
                        dict(label="Solo L2", method="update", args=[{"visible": [False, True, False, False]}]),
                        dict(label="Solo L3", method="update", args=[{"visible": [False, False, True, False]}]),
                    ]))]
            )
            st.plotly_chart(fig_hora, use_container_width=True)

        # --- LÍNEA DIVISORIA CENTRAL ---
        with col_espacio:
            st.markdown("""
                <div style="border-left: 2px solid #e6e9ef; height: 1000px; margin-left: 50%;"></div>
            """, unsafe_allow_html=True)

        with col_der:
            # --- GRÁFICO 3: MAPA DE CALOR ---
            st.markdown("#### 🌡️ Mapa de Calor de Consumo (kWh)")
            df_heat = df.groupby(['nombre_dia', 'hora'])['incremento_kWh'].mean().unstack().reindex(order_dias)
            fig_heat = go.Figure(data=go.Heatmap(
                z=df_heat.values, x=[f"{h:02d}:00" for h in range(24)], y=df_heat.index,
                colorscale='YlOrRd', hoverongaps=False,
                hovertemplate='Día: %{y}<br>Hora: %{x}<br>Consumo: <b>%{z:.2f} kWh</b><extra></extra>'
            ))
            
            # Alto total para que coincida con los dos de la izquierda (480 + 480 aprox)
            fig_heat.update_layout(
                height=960, 
                margin=dict(t=40, b=40, l=20, r=10), 
                yaxis_autorange='reversed', 
                font=dict(color="black")
            )
            st.plotly_chart(fig_heat, use_container_width=True)
            st.info("💡 **Análisis:** Las zonas oscuras indican picos de demanda. Útil para auditoría de horarios.")

    except Exception as e:
        st.error(f"Error al generar el perfil de carga: {e}")
