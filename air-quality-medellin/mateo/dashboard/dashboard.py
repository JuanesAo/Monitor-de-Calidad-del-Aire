"""
Dashboard de Calidad del Aire - Valle de Aburra
================================================
Aplicacion Streamlit para visualizar datos de calidad del aire
del Valle de Aburra (Medellin, Bello, Envigado).

Uso:
    pip install streamlit pandas plotly
    streamlit run dashboard.py

Autor: Mateo Villada Higuita
Proyecto: Monitor de Calidad del Aire - Universidad EAFIT
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURACION DE PAGINA
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="Calidad del Aire - Valle de Aburra",
    page_icon="🌫️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Paleta de colores (mismo Ocean Gradient de las slides)
COLORS = {
    "primary":   "#065A82",
    "secondary": "#1C7293",
    "accent":    "#21295C",
    "green":     "#00A86B",
    "yellow":    "#F59E0B",
    "orange":    "#FF7E00",
    "red":       "#DC2626",
    "purple":    "#8F3F97",
}

# Colores por categoria AQI (estandar EPA)
AQI_COLORS = {
    "Buena":              "#00E400",
    "Moderada":           "#FFFF00",
    "Insalubre sensibles":"#FF7E00",
    "Insalubre":          "#FF0000",
    "Muy insalubre":      "#8F3F97",
    "Peligrosa":          "#7E0023",
}


# ─────────────────────────────────────────────
# CARGAR DATOS
# ─────────────────────────────────────────────

@st.cache_data
def load_data(csv_path):
    """Carga el CSV exportado de Athena y limpia tipos."""
    df = pd.read_csv(csv_path)

    # Convertir timestamp a datetime
    df["timestamp_parsed"] = pd.to_datetime(df["timestamp_parsed"])

    # Columnas derivadas utiles
    df["fecha"] = df["timestamp_parsed"].dt.date
    df["hora"]  = df["timestamp_parsed"].dt.hour

    return df


# ─────────────────────────────────────────────
# SIDEBAR - FILTROS
# ─────────────────────────────────────────────

st.sidebar.title("🌫️ Filtros")
st.sidebar.markdown("---")

# Cargar datos
CSV_PATH = st.sidebar.text_input(
    "Ruta del CSV",
    value="calidad_aire_ultimos_7_dias.csv",
    help="Ruta al archivo CSV exportado de Athena"
)

try:
    df = load_data(CSV_PATH)
except FileNotFoundError:
    st.error(f"No se encontro el archivo: {CSV_PATH}")
    st.info("Verifica que el archivo CSV este en la misma carpeta que dashboard.py")
    st.stop()

# Filtro de estaciones
estaciones_disponibles = sorted(df["station_name"].unique())
estaciones_seleccionadas = st.sidebar.multiselect(
    "Estaciones",
    options=estaciones_disponibles,
    default=estaciones_disponibles,
)

# Filtro de rango de fechas
fecha_min = df["timestamp_parsed"].min().date()
fecha_max = df["timestamp_parsed"].max().date()
rango_fechas = st.sidebar.date_input(
    "Rango de fechas",
    value=(fecha_min, fecha_max),
    min_value=fecha_min,
    max_value=fecha_max,
)

# Aplicar filtros
if len(rango_fechas) == 2:
    df_filt = df[
        (df["station_name"].isin(estaciones_seleccionadas))
        & (df["timestamp_parsed"].dt.date >= rango_fechas[0])
        & (df["timestamp_parsed"].dt.date <= rango_fechas[1])
    ]
else:
    df_filt = df[df["station_name"].isin(estaciones_seleccionadas)]

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Registros filtrados:** {len(df_filt):,}")
st.sidebar.markdown(f"**Periodo:** {fecha_min} a {fecha_max}")

st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    **Equipo:**
    - Nicolas Saldarriaga
    - Juan Esteban Alzate
    - Mateo Villada Higuita

    *Universidad EAFIT · 2026*
    """
)


# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────

st.title("🌫️ Monitor de Calidad del Aire — Valle de Aburrá")
st.markdown(
    "Datos en tiempo real procesados por el pipeline AWS · "
    f"**{len(df_filt):,} registros** del periodo seleccionado"
)
st.markdown("---")


# ─────────────────────────────────────────────
# PANEL 3 - KPIs (los ponemos arriba por jerarquia visual)
# ─────────────────────────────────────────────

if len(df_filt) > 0:
    col1, col2, col3, col4 = st.columns(4)

    aqi_max  = int(df_filt["aqi"].max())
    aqi_prom = round(df_filt["aqi"].mean(), 2)
    pm25_max = round(df_filt["pm25"].max(), 1)
    n_alertas = len(df_filt[df_filt["aqi"] >= 4])

    col1.metric(
        label="AQI Maximo Registrado",
        value=aqi_max,
        delta=f"escala 1-5",
        delta_color="off",
    )
    col2.metric(
        label="AQI Promedio",
        value=aqi_prom,
    )
    col3.metric(
        label="PM2.5 Maximo (µg/m³)",
        value=pm25_max,
    )
    col4.metric(
        label="Lecturas en zona peligrosa",
        value=f"{n_alertas:,}",
        delta=f"{round(100*n_alertas/len(df_filt), 1)}% del total",
        delta_color="inverse",
    )

st.markdown("---")


# ─────────────────────────────────────────────
# PANEL 1 - SERIE DE TIEMPO DEL AQI
# ─────────────────────────────────────────────

st.subheader("📈 Evolución del AQI en el tiempo")
st.caption("Linea por estacion · Umbral peligroso en AQI = 4")

if len(df_filt) > 0:
    # Agregar por hora para que no se sature
    df_serie = (
        df_filt.set_index("timestamp_parsed")
        .groupby([pd.Grouper(freq="1H"), "station_name"])["aqi"]
        .mean()
        .reset_index()
    )

    fig1 = px.line(
        df_serie,
        x="timestamp_parsed",
        y="aqi",
        color="station_name",
        labels={
            "timestamp_parsed": "Fecha y hora",
            "aqi":              "AQI promedio",
            "station_name":     "Estacion",
        },
        color_discrete_sequence=[COLORS["primary"], COLORS["secondary"], COLORS["red"]],
    )

    # Linea horizontal en umbral peligroso
    fig1.add_hline(
        y=4,
        line_dash="dash",
        line_color=COLORS["red"],
        annotation_text="Umbral peligroso",
        annotation_position="top right",
    )

    fig1.update_layout(
        height=400,
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=20, b=20),
    )
    fig1.update_yaxes(range=[0, 5.5], dtick=1)

    st.plotly_chart(fig1, use_container_width=True)
else:
    st.warning("No hay datos para los filtros seleccionados")


# ─────────────────────────────────────────────
# PANELES 2 y 4 EN COLUMNAS
# ─────────────────────────────────────────────

col_izq, col_der = st.columns([1, 1])

# ─────────────────────────────────────────────
# PANEL 2 - MAPA DE ESTACIONES
# ─────────────────────────────────────────────

with col_izq:
    st.subheader("🗺️ Mapa de estaciones")
    st.caption("Tamano segun PM2.5 promedio · Color segun AQI")

    if len(df_filt) > 0:
        df_mapa = (
            df_filt.groupby(["station_name", "latitude", "longitude"])
            .agg(
                aqi_prom=("aqi", "mean"),
                pm25_prom=("pm25", "mean"),
                lecturas=("aqi", "count"),
            )
            .reset_index()
        )

        fig2 = px.scatter_mapbox(
            df_mapa,
            lat="latitude",
            lon="longitude",
            size="pm25_prom",
            color="aqi_prom",
            hover_name="station_name",
            hover_data={
                "aqi_prom":  ":.2f",
                "pm25_prom": ":.1f",
                "lecturas":  True,
                "latitude":  False,
                "longitude": False,
            },
            color_continuous_scale=[
                [0,   COLORS["green"]],
                [0.5, COLORS["yellow"]],
                [0.8, COLORS["orange"]],
                [1,   COLORS["red"]],
            ],
            size_max=40,
            zoom=10,
            mapbox_style="open-street-map",
        )

        fig2.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=0, b=0),
        )

        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("Sin datos")


# ─────────────────────────────────────────────
# PANEL 4 - DISTRIBUCION DE CATEGORIAS AQI
# ─────────────────────────────────────────────

with col_der:
    st.subheader("📊 Distribución por categoría AQI")
    st.caption("Frecuencia de cada nivel de calidad del aire")

    if len(df_filt) > 0:
        df_cat = (
            df_filt.groupby("aqi_category")
            .size()
            .reset_index(name="lecturas")
            .sort_values("lecturas", ascending=True)
        )

        fig3 = px.bar(
            df_cat,
            y="aqi_category",
            x="lecturas",
            orientation="h",
            text="lecturas",
            color="aqi_category",
            color_discrete_map=AQI_COLORS,
            labels={
                "aqi_category": "Categoria",
                "lecturas":     "Numero de lecturas",
            },
        )

        fig3.update_traces(textposition="outside")
        fig3.update_layout(
            height=400,
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=20, b=20),
        )

        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.warning("Sin datos")


# ─────────────────────────────────────────────
# PANEL 5 - TABLA DE PEORES MOMENTOS
# ─────────────────────────────────────────────

st.markdown("---")
st.subheader("🔥 Top 10 lecturas con peor calidad del aire")
st.caption("Ordenadas de mayor a menor AQI, luego por PM2.5")

if len(df_filt) > 0:
    df_peores = (
        df_filt.nlargest(10, ["aqi", "pm25"])[
            ["station_name", "timestamp_parsed", "aqi", "aqi_category", "pm25", "pm10", "o3"]
        ]
        .rename(columns={
            "station_name":     "Estacion",
            "timestamp_parsed": "Fecha y hora",
            "aqi":              "AQI",
            "aqi_category":     "Categoria",
            "pm25":             "PM2.5",
            "pm10":             "PM10",
            "o3":               "O3",
        })
        .reset_index(drop=True)
    )

    st.dataframe(
        df_peores,
        use_container_width=True,
        hide_index=True,
    )


# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────

st.markdown("---")
st.markdown(
    f"""
    <div style='text-align: center; color: #64748B; padding: 20px;'>
        Pipeline AWS · Datos del SIATA via OpenWeather API · 
        Procesamiento con Glue + SparkML · Alertas via SNS<br>
        <small>Ultima actualizacion del dataset: {df['timestamp_parsed'].max().strftime('%Y-%m-%d %H:%M')}</small>
    </div>
    """,
    unsafe_allow_html=True,
)
