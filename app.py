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
        
        # --- CONFIGURACIÓN DE ACTUALIZACIÓN AUTOMÁTICA ---
        # Refresca la app cada 30 segundos (30000 milisegundos)
        count = st_autorefresh(interval=30000, key="datarefresh")

        # --- AJUSTE DE HORA ARGENTINA ---
        tz_ar = pytz.timezone("America/Argentina/Buenos_Aires")
        hora_actual = pd.Timestamp.now(tz=tz_ar).strftime('%H:%M:%S')
        
        st.subheader("⛅ Condiciones en Tiempo Real (Actualización Automática)")
        st.caption(f"Última actualización (Hora Arg): {hora_actual}")
        
        # 1. Credenciales y Conexión (Igual que antes)
        url = "https://influxdb.utn.xrob.com.ar"
        token = "VPJoZH--S2GGPNNhfmWVUsZEaHqV4h1wkOX235FSfhk6GkitChp2e-8DxQ7O1ns6s7VwpKnmE-Evj7KYhLcWJQ=="
        org = "ec1aafe9e31ba7af"
        bucket = "UTN FRT"
        
        client = InfluxDBClient(url=url, token=token, org=org)
        query_api = client.query_api()
        
        # 2. Consulta (Buscamos los últimos datos)
        query = f'''
            from(bucket: "{bucket}")
              |> range(start: -15m) 
              |> filter(fn: (r) => r._measurement == "pruebas_fn")
              |> filter(fn: (r) => r.deviceID == "08B764")
              |> filter(fn: (r) => r._field == "temp" or r._field == "hum" or r._field == "wind")
              |> last()
        '''
        
        try:
            result = query_api.query(org=org, query=query)
            val_temp, val_hum, val_wind = 0.0, 0.0, 0.0
            
            for table in result:
                for record in table.records:
                    campo = record.get_field()
                    if campo == "temp": val_temp = record.get_value()
                    elif campo == "hum": val_hum = record.get_value()
                    elif campo == "wind": val_wind = record.get_value()

            # 3. DIBUJAMOS LOS VELOCÍMETROS
            c1, c2, c3 = st.columns(3)
            
            # --- Reloj de Temperatura ---
            fig_t = go.Figure(go.Indicator(
                mode = "gauge+number", value = val_temp, 
                number = {'valueformat': ".2f"}, # <--- AQUÍ LOS DECIMALES
                title = {'text': "Temperatura (°C)"},
                gauge = {'axis': {'range': [0, 50]}, 'bar': {'color': "#4caf50"}}
            ))
            fig_t.update_layout(height=300, margin=dict(l=20, r=20, t=60, b=20))
            c1.plotly_chart(fig_t, use_container_width=True)

            # --- Reloj de Humedad ---
            fig_h = go.Figure(go.Indicator(
                mode = "gauge+number", value = val_hum,
                number = {'valueformat': ".2f"}, # <--- AQUÍ LOS DECIMALES
                title = {'text': "Humedad (%)"},
                gauge = {'axis': {'range': [0, 100]}, 'bar': {'color': "#f44336"}}
            ))
            fig_h.update_layout(height=300, margin=dict(l=20, r=20, t=60, b=20))
            c2.plotly_chart(fig_h, use_container_width=True)

            # --- Reloj de Viento ---
            fig_v = go.Figure(go.Indicator(
                mode = "gauge+number", value = val_wind,
                number = {'valueformat': ".2f"}, # <--- AQUÍ LOS DECIMALES
                title = {'text': "Viento (km/h)"},
                gauge = {'axis': {'range': [0, 100]}, 'bar': {'color': "#8bc34a"}}
            ))
            fig_v.update_layout(height=300, margin=dict(l=20, r=20, t=60, b=20))
            c3.plotly_chart(fig_v, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error en tiempo real: {e}")
        

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
