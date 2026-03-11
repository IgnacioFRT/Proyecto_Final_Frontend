import streamlit as st
import pandas as pd
from influxdb_client import InfluxDBClient
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import os
import pytz # Para la zona horaria

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="EMS - PAC3200 UTN", layout="wide")

# 2. CARGA DE DATOS
@st.cache_data
def load_data():
    # AQUÍ ESTÁ LA CORRECCIÓN CON TU NOMBRE EXACTO
    archivo = "datos_energia_acumulada_liviano (6).xlsx"
    if not os.path.exists(archivo):
        return None
    df = pd.read_excel(archivo)
    df['fecha'] = pd.to_datetime(df['fecha'])
    df.set_index('fecha', inplace=True)
    return df

df = load_data()

# 3. INTERFAZ Y MENÚ LATERAL
with st.sidebar:
    st.image("https://www.frt.utn.edu.ar/imagenes/logo_utn.png", width=150)
    st.title("Navegación")
    seccion = st.radio("Secciones:", ["🏠 Inicio", "🕒 Tiempo Real", "📶 Calidad (QoS)", "📊 Consumo por Día"])
    st.markdown("---")
    st.info("Ingeniería Electrónica - UTN FRT")

# 4. LÓGICA DE LAS SECCIONES
if df is not None:
    st.title("⚡ Sistema de Gestión Energética - PAC3200")
    
    if seccion == "🏠 Inicio":
        st.subheader("Facultad Regional Tucumán")
        e_total = df['EA_imp_T1_kwh'].max() - df['EA_imp_T1_kwh'].min()
        c1, c2, c3 = st.columns(3)
        c1.metric("Energía Total Registrada", f"{e_total:,.1f} kWh")
        c2.metric("Huella de Carbono", f"{e_total * 0.45:,.1f} kg CO2")
        c3.metric("Última Medición", df.index.max().strftime('%d/%m/%Y'))
        st.success("Sistema Operativo y conectado a la base de datos histórica.")

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
        
        # --- QUERY CORREGIDA ---
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
                        'borderwidth': 1, 'bordercolor': "#e5e8e8"
                    }
                ))
                fig.update_layout(height=280, margin=dict(l=25, r=25, t=60, b=25), paper_bgcolor="rgba(0,0,0,0)", font={'family': "Source Sans Pro, sans-serif"})
                return fig

            # --- FILA 1: CLIMA ---
            st.write("### 🌤️ Variables Climáticas")
            c1, c2, c3 = st.columns(3)
            c1.plotly_chart(crear_gauge_pro(data.get("temp",0), "Temperatura", 50, "#4caf50", "°C"), use_container_width=True)
            c2.plotly_chart(crear_gauge_pro(data.get("hum",0), "Humedad", 100, "#f44336", "%"), use_container_width=True)
            c3.plotly_chart(crear_gauge_pro(data.get("wind",0), "Viento", 100, "#8bc34a", " km/h"), use_container_width=True)

           # --- FILA 2: CORRIENTES ---
            st.write("### ⚡ Corrientes por Fase")
            f1, f2, f3 = st.columns(3)
            f1.plotly_chart(crear_gauge_pro(data.get("IL1",0), "Corriente L1", 20, "#1f77b4", " A"), use_container_width=True)
            f2.plotly_chart(crear_gauge_pro(data.get("IL2",0), "Corriente L2", 20, "#ff7f0e", " A"), use_container_width=True)
            f3.plotly_chart(crear_gauge_pro(data.get("IL3",0), "Corriente L3", 20, "#2ca02c", " A"), use_container_width=True)

            # --- FILA 3: MATRIZ DE CALIDAD ESTILO "DASHBOARD" ---
            st.divider()
            st.markdown("### 💎 Calidad de Energía")
            
            # Usamos 4 columnas igual que antes
            q1, q2, q3, q4 = st.columns(4)
            
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
        st.subheader("📊 Distribución del Consumo Real")
        energia_total = df['EA_imp_T1_kwh'].max() - df['EA_imp_T1_kwh'].min()
        df['inc_cons'] = df['EA_imp_T1_kwh'].diff().clip(lower=0).fillna(0)
        
        # Agrupación por tipo de día
        e_habil_raw = df[df['tipo_dia'] == 'Día hábil']['inc_cons'].sum()
        e_feriado_raw = df[df['tipo_dia'] == 'Feriado']['inc_cons'].sum()
        e_finde_raw = df[df['tipo_dia'].isin(['Sábado', 'Domingo'])]['inc_cons'].sum()
        
        raw_sum = e_habil_raw + e_feriado_raw + e_finde_raw
        factor = energia_total / raw_sum if raw_sum > 0 else 0
        e_habil, e_feriado, e_finde = e_habil_raw * factor, e_feriado_raw * factor, e_finde_raw * factor

        fig_dias = go.Figure(data=[go.Pie(
            labels=['Días hábiles', 'Feriados', 'Fin de semana'], 
            values=[e_habil, e_feriado, e_finde], 
            marker_colors=['#66bb6a', '#ef5350', '#42a5f5'],
            pull=[0.1, 0.1, 0.1], textinfo='percent+label'
        )])
        fig_dias.update_layout(title_text='<b>CONSUMO REAL POR TIPO DE DÍA</b>', title_x=0.5)
        st.plotly_chart(fig_dias, use_container_width=True)

else:
    st.error("❌ Archivo de datos no encontrado. Verifica el nombre exacto en GitHub.")
