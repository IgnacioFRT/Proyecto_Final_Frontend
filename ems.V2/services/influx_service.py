from influxdb_client import InfluxDBClient
import streamlit as st
import pytz


def get_client():
    return InfluxDBClient(
        url=st.secrets["INFLUX_URL"],
        token=st.secrets["INFLUX_TOKEN"],
        org=st.secrets["INFLUX_ORG"]
    )


def get_latest_data():
    query = f'''
    from(bucket: "{st.secrets["INFLUX_BUCKET"]}")
      |> range(start: -15m)
      |> filter(fn: (r) => r._measurement == "{st.secrets["MEASUREMENT"]}")
      |> filter(fn: (r) => r.deviceID == "{st.secrets["DEVICE_ID"]}")
      |> filter(fn: (r) => r.proyecto == "{st.secrets["PROYECTO"]}")
      |> last()
    '''

    with get_client() as client:
        tables = client.query_api().query(org=st.secrets["INFLUX_ORG"], query=query)

    data = {}
    latest_time = None

    for table in tables:
        for record in table.records:
            data[record.get_field()] = record.get_value()
            t = record.get_time()
            if latest_time is None or t > latest_time:
                latest_time = t

    if latest_time is not None:
        tz = pytz.timezone(st.secrets["TZ_NAME"])
        latest_time = latest_time.astimezone(tz)

    return data, latest_time


def get_raw_data_count():
    query = f'''
    from(bucket: "{st.secrets["INFLUX_BUCKET"]}")
      |> range(start: 0)
      |> filter(fn: (r) => r._measurement == "{st.secrets["MEASUREMENT"]}")
      |> filter(fn: (r) => r.deviceID == "{st.secrets["DEVICE_ID"]}")
      |> filter(fn: (r) => r.proyecto == "{st.secrets["PROYECTO"]}")
      |> filter(fn: (r) => r._field == "freq")
      |> count()
    '''

    total_count = 0

    with get_client() as client:
        tables = client.query_api().query(org=st.secrets["INFLUX_ORG"], query=query)

    for table in tables:
        for record in table.records:
            total_count += record.get_value()

    return total_count
