import streamlit as st
import pandas as pd
from influxdb_client import InfluxDBClient
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import os
import pytz # Para la zona horaria


# 1. CONFIGURACIÓN DE PÁGINA

st.set_page_config(page_title="EMS - PAC3200 UTN", layout="wide")

# 2. ANALISIS REALIZADO EN COLAB

@st.cache_data(ttl=3600) # Se actualiza automáticamente cada 1 hora
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
    data_mean = from(bucket: "{bucket}")
      |> range(start: -60d)
      |> filter(fn: (r) => r._measurement == "pruebas_fn")
      |> filter(fn: (r) => r.deviceID == "08B764")
      |> filter(fn: (r) => r.proyecto == "siemens_Pac3200")
      |> filter(fn: (r) => {filter_fields})
      |> filter(fn: (r) => r._field != "EA_imp_T1_kwh")
      |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)

    data_last = from(bucket: "{bucket}")
      |> range(start: -60d)
      |> filter(fn: (r) => r._measurement == "pruebas_fn")
      |> filter(fn: (r) => r.deviceID == "08B764")
      |> filter(fn: (r) => r.proyecto == "siemens_Pac3200")
      |> filter(fn: (r) => r._field == "EA_imp_T1_kwh")
      |> aggregateWindow(every: 15m, fn: last, createEmpty: false)

    union(tables: [data_mean, data_last])
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    '''

    df = query_api.query_data_frame(query)

    df['_time'] = pd.to_datetime(df['_time']).dt.tz_convert(tz_local)
    df = df.rename(columns={'_time': 'time'})
    df = df.set_index('time')
    df['P_tot_kW'] = df['P1'] + df['P2'] + df['P3']

    # --- C. RESAMPLE A 15 MIN ---
    df = df.resample('15T').agg({
        'UL1L2': 'mean', 'UL2L3' : 'mean', 'UL3L1' : 'mean',
        'UL1N' : 'mean', 'UL2N' : 'mean', 'UL3N' : 'mean',
        'IL1': 'mean', 'IL2': 'mean', 'IL3': 'mean',
        'freq' : 'mean',
        'P1': 'mean', 'P2': 'mean', 'P3': 'mean',
        'Q1': 'mean', 'Q2': 'mean', 'Q3': 'mean',
        'S1': 'mean', 'S2': 'mean', 'S3': 'mean',
        'FP1': 'mean', 'FP2': 'mean', 'FP3': 'mean',
        'THDv1': 'mean', 'THDv2': 'mean', 'THDv3': 'mean',
        'THDi1': 'mean', 'THDi2': 'mean', 'THDi3': 'mean',
        'Imed': 'mean', 'Vmed': 'mean',
        'EA_imp_T1_kwh': 'last',
        'temp': 'mean', 'P_tot_kW': 'mean'
    }).ffill()

    # --- D. LIMPIEZA DE DATOS ---
    limites = {
        'P1': (-100, 2000), 'P2': (-100, 2000), 'P3': (-100, 2000),
        'Q1': (-1500, 1500), 'Q2': (-1500, 1500), 'Q3': (-1500, 1500),
        'temp': (-15, 50),
        'THDi1': (0, 100), 'THDi2': (0, 100), 'THDi3': (0, 100),
        'UL1N': (180, 260), 'UL2N': (180, 260), 'UL3N': (180, 260),
        'freq': (48, 52),
    }

    mask = pd.Series([True] * len(df), index=df.index)
    for col, (low, high) in limites.items():
        if col in df.columns:
            mask &= df[col].between(low, high)
    df = df[mask].copy()

    # --- E. CALENDARIO ARGENTINO ---
    feriados_2025 = pd.to_datetime([
        '2025-08-17', '2025-10-12', '2025-11-20', '2025-12-08', '2025-12-24', '2025-12-25',
        '2026-01-01', '2026-02-16', '2026-02-17', '2026-03-23', '2026-03-24', '2026-04-02', 
        '2026-04-03', '2026-05-01', '2026-05-25', '2026-06-15', '2026-06-20', '2026-07-09', 
        '2026-07-10', '2026-08-17', '2026-10-12', '2026-11-23', '2026-12-07', '2026-12-08', '2026-12-25'
    ]).date
    
    df['fecha'] = df.index.date
    df['hora'] = df.index.hour
    df['es_feriado'] = df['fecha'].isin(feriados_2025)
    df['es_laborable'] = ~df['es_feriado']
    df['es_finde'] = df.index.weekday.isin([5, 6])
    df['es_habil'] = df['es_laborable'] & ~df['es_finde']

    # Tipo de día para CAMMESA
    def definir_tipo_cammesa(row):
        if not row['es_habil']:
            if row.name.weekday() == 6 or row['es_feriado']: return 'Domingo/Feriado'
            else: return 'Sábado'
        else: return 'Hábil'
    
    df['tipo_dia'] = df.apply(definir_tipo_cammesa, axis=1)

    # Devolvemos el DataFrame listo y pulido
    return df

# 3. INTERFAZ Y MENÚ LATERAL

# --- ESTILOS CSS PERSONALIZADOS ---
st.markdown("""
    <style>
        /* Cambia el fondo de la barra lateral */
        [data-testid="stSidebar"] {
            background-color: #1a252c; /* Azul Marino Industrial */
        }
        /* Fuerza a que los textos del menú se vuelvan blancos para que se lean */
        [data-testid="stSidebar"] p, 
        [data-testid="stSidebar"] div, 
        [data-testid="stSidebar"] span, 
        [data-testid="stSidebar"] label {
            color: #ffffff !important;
        }
    </style>
""", unsafe_allow_html=True)

with st.sidebar:
    # use_container_width=True hace que se adapte perfecto a los bordes
    st.image("LOGO-BLANCO-UTN.png", use_container_width=True) 
    st.divider() # Una línea finita para separar el logo del menú queda muy prolija
    st.title("Navegación")
    seccion = st.radio("Secciones:", ["🏠 Inicio", "🕒 Tiempo Real", "📶 Calidad (QoS)", "📊 Consumo por Día"])
    st.markdown("---")
    st.info("Ingeniería Electrónica - UTN FRT")

# 4. LÓGICA DE LAS SECCIONES
if df is not None:
    st.title("⚡ Sistema de Gestión Energética - PAC3200")
    
    if seccion == "🏠 Inicio":
        
        # --- 1. ENCABEZADO INSTITUCIONAL CENTRADO ---
        # Usamos 3 columnas para atrapar el logo en el medio. 
        # [1, 1.5, 1] significa que la columna central es un poco más ancha.
        espacio1, col_logo_central, espacio3 = st.columns([1, 1.5, 1])
        
        with col_logo_central:
            try:
                st.image("logo_principal.jpg", use_container_width=True)
            except:
                st.error("Falta logo_utn.jpg")
        
        # Textos centrados usando HTML para darle formato de portada
        #st.markdown("<h1 style='text-align: center;'>⚡ Sistema de Gestión Energética - PAC3200</h1>", unsafe_allow_html=True)
        st.markdown("<h2 style='text-align: center; color: #34495e;'>Facultad Regional Tucumán (UTN FRT)</h2>", unsafe_allow_html=True)
        st.markdown("<h4 style='text-align: center; color: #7f8c8d;'>Departamento de Ingeniería Electrónica</h4>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #95a5a6;'>Monitoreo IoT y Análisis de Calidad Eléctrica con analizador Siemens PAC3200</p>", unsafe_allow_html=True)

        st.divider()

        # --- 2. MÉTRICAS GENERALES (Tus datos estáticos/históricos) ---
        st.markdown("#### 📊 Resumen Global del Sistema")
        
        kpi1, kpi2, kpi3 = st.columns(3)
        # Usamos delta para darle un toque visual verde indicando que está activo
        kpi1.metric("Energía Total Registrada", "2,933.4 kWh", "↑ Sistema Activo")
        kpi2.metric("Huella de Carbono Estimada", "1,320.0 kg CO₂", "- Impacto Medioambiental", delta_color="off")
        kpi3.metric("Última Medición Verificada", "10/03/2026", "En línea")

        st.success("✅ **Estado:** Sistema Operativo. Enlace con base de datos histórica InfluxDB establecido correctamente.")

        st.divider()

        # --- 3. ARQUITECTURA DEL PROYECTO (Ideal para la Tesis) ---
        st.markdown("#### 🛠️ Arquitectura y Tecnologías Implementadas")
        
        info1, info2 = st.columns(2)
        
        with info1:
            st.info("""
            **⚙️ Hardware de Adquisición**
            * **Analizador de Redes:** Siemens PAC3200.
            * **Protocolo de Comunicación:** Modbus TCP/IP.
            * **Variables Medidas:** Tensión, Corriente, Factor de Potencia, THD y Energía.
            """)
            
        with info2:
            st.info("""
            **☁️ Software y Base de Datos**
            * **Base de Datos (Time-Series):** InfluxDB Cloud.
            * **Backend y Visualización:** Python, Streamlit, Plotly.
            * **Objetivo:** Auditoría energética, detección de anomalías y visualización en tiempo real.
            """)

        # Mensaje de bienvenida final
        st.write("👈 *Utilice el menú de navegación lateral para acceder a la visualización en **Tiempo Real**, análisis de **Calidad (QoS)** o historial de **Consumo por Día**.*")

    elif seccion == "🕒 Tiempo Real":
        
        # 1. Configuración de Refresco y Hora
        st_autorefresh(interval=30000, key="datarefresh")
        tz_ar = pytz.timezone("America/Argentina/Buenos_Aires")
        hora_actual = pd.Timestamp.now(tz=tz_ar).strftime('%H:%M:%S')
        
        st.caption(f"Última actualización (Hora Arg): {hora_actual}")
        
        # 2. Conexión a InfluxDB
        url = "https://influxdb.utn.xrob.com.ar"
        token = "VPJoZH--S2GGPNNhfmWVUsZEaHqV4h1wkOX235FSfhk6GkitChp2e-8DxQ7O1ns6s7VwpKnmE-Evj7KYhLcWJQ=="
        org = "ec1aafe9e31ba7af"
        bucket = "UTN FRT"
        
        client = InfluxDBClient(url=url, token=token, org=org)
        query_api = client.query_api()
        
        # --- QUERY ---
        # Se eliminaron errores de comillas y se simplificó el filtro de campos
        query = f'''
            from(bucket: "{bucket}")
              |> range(start: -15m) 
              |> filter(fn: (r) => r._measurement == "pruebas_fn")
              |> filter(fn: (r) => r.deviceID == "08B764")
              |> last()
        '''
        
        try:
            result = query_api.query(org=org, query=query)
            
            # Inicializamos el diccionario con valores por defecto
            data = {
                "temp": 0.0, "hum": 0.0, "wind": 0.0,
                "IL1": 0.0, "IL2": 0.0, "IL3": 0.0,
                "UL1N": 0.0, "UL2N": 0.0, "UL3N": 0.0,
                "FP1": 0.0, "FP2": 0.0, "FP3": 0.0,
                "THDv1": 0.0, "THDv2": 0.0, "THDv3": 0.0,
                "THDi1": 0.0, "THDi2": 0.0, "THDi3": 0.0
            }
            
            # Llenamos el diccionario con lo que realmente venga de la base
            for table in result:
                for record in table.records:
                    data[record.get_field()] = record.get_value()

            # --- FUNCIÓN DE DISEÑO (Gauges) ---
            def crear_gauge_pro(valor, titulo, max_val, color, sufijo):
                fig = go.Figure(go.Indicator(
                    mode = "gauge+number", value = valor,
                    number = {'valueformat': ".2f", 'suffix': sufijo, 'font': {'size': 35, 'color': "#5d6d7e"}},
                    title = {'text': titulo, 'font': {'size': 18, 'color': "#5d6d7e"}},
                    gauge = {
                        'axis': {'range': [0, max_val], 'tickwidth': 1, 'tickcolor': "#5d6d7e"},
                        'bar': {'color': color},
                        'bgcolor': "white",
                        'borderwidth': 3, 'bordercolor': "#e5e8e8"
                    }
                ))
                fig.update_layout(height=280, margin=dict(l=25, r=25, t=60, b=25), paper_bgcolor="rgba(0,0,0,0)", font={'family': "Source Sans Pro, sans-serif"})
                return fig

            # --- FUNCIÓN: BARRAS COMPARATIVAS ENCERRADAS ---
            def crear_barras_corriente(il1, il2, il3):
                fig = go.Figure(data=[
                    go.Bar(
                        x=['Fase L1', 'Fase L2', 'Fase L3'],
                        y=[il1, il2, il3],
                        marker_color=["#1f77b4", "#ff7f0e", "#2ca02c"], # Azul, Naranja, Verde
                        text=[f"{il1:.2f} A", f"{il2:.2f} A", f"{il3:.2f} A"],
                        textposition='auto',
                        textfont=dict(size=16, color="white"),
                        width=0.6 # Controla el grosor de la barra para que no se vea desproporcionada
                    )
                ])
                fig.update_layout(
                    height=280,
                    margin=dict(l=50, r=20, t=30, b=30), # Ajustamos márgenes para que el borde se vea completo
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    # ENCERRANDO EL EJE X
                    xaxis=dict(
                        showline=True, linewidth=3, linecolor='#e5e8e8', mirror=True,
                        tickfont=dict(size=14, color="#5d6d7e")
                    ),
                    # ENCERRANDO EL EJE Y
                    yaxis=dict(
                        title="Corriente (A)", 
                        range=[0, 20], 
                        gridcolor="#f2f4f4", # Cuadrícula de fondo muy suave
                        showline=True, linewidth=3, linecolor='#e5e8e8', mirror=True,
                        tickfont=dict(size=14, color="#5d6d7e")
                    ),
                    font=dict(family="Source Sans Pro, sans-serif", size=14, color="#5d6d7e")
                )
                return fig

            # --- FILA 1: CLIMA ---
            st.write("### 🌤️ Variables Climáticas")
            c1, c2, c3 = st.columns(3)
            c1.plotly_chart(crear_gauge_pro(data.get("temp",0), "Temperatura", 50, "#4caf50", "°C"), use_container_width=True)
            c2.plotly_chart(crear_gauge_pro(data.get("hum",0), "Humedad", 100, "#f44336", "%"), use_container_width=True)
            c3.plotly_chart(crear_gauge_pro(data.get("wind",0), "Viento", 100, "#8bc34a", " km/h"), use_container_width=True)

           # --- FILA 2: CORRIENTES (Barras) + FRECUENCIA (Gauge) ---
            st.write("### ⚡ Análisis de Carga y Red")
            
            # Dividimos en 2 columnas: la izquierda más grande (barras) y la derecha más chica (frecuencia)
            espacio_izq, col_barras, col_frec = st.columns([0.4, 1.5, 1])
            
            with col_barras:
                st.plotly_chart(crear_barras_corriente(data.get("IL1",0), data.get("IL2",0), data.get("IL3",0)), use_container_width=True)
                
            with col_frec:
                # La frecuencia en Argentina es de 50 Hz. Rango visual de 45 a 55 para que el reloj se vea bien.
                st.plotly_chart(crear_gauge_pro(data.get("Freq", 50.0), "Frecuencia", 60, "#9b59b6", " Hz"), use_container_width=True)
                
            # --- FILA 3: MATRIZ DE CALIDAD ESTILO "DASHBOARD" ---
            st.divider()
            st.markdown("### 💎 Calidad de Energía")
            
            # Usamos 4 columnas igual que antes
            espacio_izq, q1, q2, q3, q4, espacio_der = st.columns([1, 1, 1, 1, 1, 0.5])
            
            with q1:
                st.markdown("⚡ **Tensión (V)**")
                # El componente metric es genial, pero vamos a agruparlos
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
            st.error(f"Error en la adquisición de datos: {e}")

    elif seccion == "📶 Calidad (QoS)":
        st.subheader("📶 Calidad de Servicio (QoS) y Gaps de Red")
        start, end = df.index.min(), df.index.max()
        esperados = len(pd.date_range(start, end, freq='15min'))
        reales = len(df)
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Registros Reales", f"{reales:,}")
        c2.metric("Esperados", f"{esperados:,}")
        c3.metric("Disponibilidad", f"{(reales/esperados)*100:.1f}%")

        fig_qos = go.Figure(data=[go.Pie(
            labels=['Registrados', 'Gaps'], values=[reales, max(0, esperados - reales)],
            marker_colors=['lightgreen', '#ef5350'], pull=[0.1, 0], textinfo='percent+label'
        )])
        fig_qos.update_layout(title_text='<b>INTEGRIDAD DE LA BASE DE DATOS</b>', title_x=0.5)
        st.plotly_chart(fig_qos, use_container_width=True)

    elif seccion == "📊 Consumo por Día":
    st.write("### 📊 Análisis de Consumo por Día y Fase")
    
    try:
        # Esto invoca la función global y muestra un cartelito mientras descarga
        with st.spinner('Descargando y procesando historial completo desde InfluxDB... ⏳'):
            df = obtener_datos_historicos()

        # ==========================================
        # MATEMÁTICA EXACTA DE TU COLAB
        # ==========================================
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

        # ==========================================
        # MAQUETADO: 1/3 TORTA | 2/3 BARRAS
        # ==========================================
        col_torta, col_barras = st.columns([1, 2])

        with col_torta:
            st.markdown("#### 📅 Consumo por Tipo de Día")
            
            if raw_total_sum == 0:
                st.warning("No se registró consumo de energía en el período.")
            else:
                # Tu configuración de Matplotlib pasada a Plotly
                labels = ['Días hábiles', 'Feriados', 'Fin de semana']
                sizes = [energia_habil, energia_feriado, energia_finde]
                colores = ['#66bb6a', '#ef5350', '#42a5f5']

                fig_torta = go.Figure(data=[go.Pie(
                    labels=labels,
                    values=sizes,
                    marker_colors=colores,
                    pull=[0.05, 0.05, 0.05], # Reemplaza el 'explode' de Colab (separa las porciones)
                    textinfo='percent+label',
                    hoverinfo='label+value+percent',
                    hovertemplate="%{label}<br>%{value:,.1f} kWh<br>%{percent}<extra></extra>" # Formato al pasar el mouse
                )])
                
                fig_torta.update_layout(
                    margin=dict(t=20, b=20, l=10, r=10),
                    showlegend=False,
                    height=350,
                    paper_bgcolor="rgba(0,0,0,0)"
                )
                st.plotly_chart(fig_torta, use_container_width=True)
                
                # Tu bloque de info convertido en un subtítulo limpio
                st.caption(f"**Total real registrado:** {energia_total:,.1f} kWh")

        with col_barras:
            st.markdown("#### 📊 Desglose de Consumo Diario")
            st.info("💡 ¡Espacio reservado! Acá inyectamos el código del gráfico de barras.")

    except Exception as e:
        st.error(f"Error procesando la base de datos: {e}")

else:
    st.error("❌ Archivo de datos no encontrado. Verifica el nombre exacto en GitHub.")
