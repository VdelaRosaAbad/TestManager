#!/usr/bin/env python3
"""
Aplicación Streamlit para Análisis EDA de datos de empresa de aceros
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Configuración de la página
st.set_page_config(
    page_title="EDA - Empresa de Aceros",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuración
PROJECT_ID = "testmanager-470115"
DATASET = "dataset_acero"
TABLE = "datamartclean"

@st.cache_data(ttl=3600)
def load_data_from_bigquery():
    """Carga datos desde BigQuery con cache"""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        # Query para obtener muestra representativa de datos
        query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        ORDER BY RAND()
        LIMIT 100000
        """
        
        df = client.query(query).to_dataframe()
        return df
        
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return None

@st.cache_data
def load_summary_stats():
    """Carga estadísticas resumidas de toda la tabla"""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        query = f"""
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            COUNT(DISTINCT region) as unique_regions,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            AVG(quantity) as avg_quantity,
            AVG(unit_price) as avg_unit_price,
            AVG(total_amount) as avg_total_amount,
            SUM(total_amount) as total_revenue
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        """
        
        stats = client.query(query).to_dataframe()
        return stats
        
    except Exception as e:
        st.error(f"Error cargando estadísticas: {e}")
        return None

def main():
    st.title("🏭 Análisis EDA - Empresa de Aceros")
    st.markdown("### Análisis Exploratorio de Datos Comerciales")
    
    # Sidebar
    st.sidebar.header("Configuración")
    analysis_type = st.sidebar.selectbox(
        "Tipo de Análisis",
        ["Resumen General", "Análisis Temporal", "Análisis de Productos", "Análisis de Clientes", "Análisis Financiero"]
    )
    
    # Cargar datos
    with st.spinner("Cargando datos..."):
        df = load_data_from_bigquery()
        summary_stats = load_summary_stats()
    
    if df is None or summary_stats is None:
        st.error("No se pudieron cargar los datos. Verifica la conexión a BigQuery.")
        return
    
    # Mostrar resumen general
    if analysis_type == "Resumen General":
        show_general_summary(df, summary_stats)
    
    elif analysis_type == "Análisis Temporal":
        show_temporal_analysis(df)
    
    elif analysis_type == "Análisis de Productos":
        show_product_analysis(df)
    
    elif analysis_type == "Análisis de Clientes":
        show_customer_analysis(df)
    
    elif analysis_type == "Análisis Financiero":
        show_financial_analysis(df)

def show_general_summary(df, summary_stats):
    """Muestra resumen general de los datos"""
    st.header("📊 Resumen General de Datos")
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Transacciones", f"{summary_stats['total_transactions'].iloc[0]:,}")
    
    with col2:
        st.metric("Clientes Únicos", f"{summary_stats['unique_customers'].iloc[0]:,}")
    
    with col3:
        st.metric("Productos Únicos", f"{summary_stats['unique_products'].iloc[0]:,}")
    
    with col4:
        st.metric("Regiones", f"{summary_stats['unique_regions'].iloc[0]}")
    
    # Información de fechas
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Fecha Más Antigua", summary_stats['earliest_date'].iloc[0].strftime('%Y-%m-%d'))
    
    with col2:
        st.metric("Fecha Más Reciente", summary_stats['latest_date'].iloc[0].strftime('%Y-%m-%d'))
    
    # Métricas financieras
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Ingresos Totales", f"${summary_stats['total_revenue'].iloc[0]:,.2f}")
    
    with col2:
        st.metric("Precio Promedio", f"${summary_stats['avg_unit_price'].iloc[0]:.2f}")
    
    with col3:
        st.metric("Cantidad Promedio", f"{summary_stats['avg_quantity'].iloc[0]:.1f}")
    
    # Información de calidad de datos
    st.subheader("🔍 Calidad de Datos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Valores nulos por columna
        null_counts = df.isnull().sum()
        null_df = pd.DataFrame({
            'Columna': null_counts.index,
            'Valores Nulos': null_counts.values,
            'Porcentaje': (null_counts.values / len(df)) * 100
        }).sort_values('Valores Nulos', ascending=False)
        
        st.write("**Valores Nulos por Columna:**")
        st.dataframe(null_df, use_container_width=True)
    
    with col2:
        # Tipos de datos
        st.write("**Tipos de Datos:**")
        dtype_df = pd.DataFrame({
            'Columna': df.dtypes.index,
            'Tipo': df.dtypes.values
        })
        st.dataframe(dtype_df, use_container_width=True)
    
    # Distribución de regiones
    st.subheader("🌍 Distribución por Región")
    region_counts = df['region'].value_counts()
    
    fig = px.pie(
        values=region_counts.values,
        names=region_counts.index,
        title="Distribución de Transacciones por Región"
    )
    st.plotly_chart(fig, use_container_width=True)

def show_temporal_analysis(df):
    """Muestra análisis temporal de los datos"""
    st.header("📅 Análisis Temporal")
    
    # Convertir fecha si es necesario
    if 'date' in df.columns and df['date'].dtype == 'object':
        df['date'] = pd.to_datetime(df['date'])
    
    # Análisis por mes
    df['year_month'] = df['date'].dt.to_period('M')
    monthly_stats = df.groupby('year_month').agg({
        'total_amount': ['sum', 'mean', 'count'],
        'quantity': 'sum',
        'customer_id': 'nunique'
    }).round(2)
    
    monthly_stats.columns = ['Ingresos_Totales', 'Ingresos_Promedio', 'Transacciones', 'Cantidad_Total', 'Clientes_Unicos']
    monthly_stats = monthly_stats.reset_index()
    monthly_stats['year_month'] = monthly_stats['year_month'].astype(str)
    
    # Gráfico de evolución temporal
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Evolución de Ingresos', 'Evolución de Transacciones', 'Evolución de Cantidades', 'Evolución de Clientes'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Ingresos
    fig.add_trace(
        go.Scatter(x=monthly_stats['year_month'], y=monthly_stats['Ingresos_Totales'],
                  mode='lines+markers', name='Ingresos', line=dict(color='blue')),
        row=1, col=1
    )
    
    # Transacciones
    fig.add_trace(
        go.Scatter(x=monthly_stats['year_month'], y=monthly_stats['Transacciones'],
                  mode='lines+markers', name='Transacciones', line=dict(color='green')),
        row=1, col=2
    )
    
    # Cantidades
    fig.add_trace(
        go.Scatter(x=monthly_stats['year_month'], y=monthly_stats['Cantidad_Total'],
                  mode='lines+markers', name='Cantidades', line=dict(color='red')),
        row=2, col=1
    )
    
    # Clientes
    fig.add_trace(
        go.Scatter(x=monthly_stats['year_month'], y=monthly_stats['Clientes_Unicos'],
                  mode='lines+markers', name='Clientes', line=dict(color='orange')),
        row=2, col=2
    )
    
    fig.update_layout(height=600, title_text="Análisis Temporal de Métricas Clave")
    st.plotly_chart(fig, use_container_width=True)
    
    # Tabla de estadísticas mensuales
    st.subheader("📊 Estadísticas Mensuales")
    st.dataframe(monthly_stats, use_container_width=True)

def show_product_analysis(df):
    """Muestra análisis de productos"""
    st.header("📦 Análisis de Productos")
    
    # Análisis por categoría de producto
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribución por categoría
        category_stats = df.groupby('product_category').agg({
            'total_amount': 'sum',
            'quantity': 'sum',
            'transaction_id': 'count'
        }).round(2)
        
        category_stats.columns = ['Ingresos_Totales', 'Cantidad_Total', 'Transacciones']
        category_stats = category_stats.sort_values('Ingresos_Totales', ascending=False)
        
        fig = px.bar(
            category_stats,
            x=category_stats.index,
            y='Ingresos_Totales',
            title="Ingresos por Categoría de Producto",
            color='Ingresos_Totales',
            color_continuous_scale='viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top productos por ingresos
        product_stats = df.groupby('product_id').agg({
            'total_amount': 'sum',
            'quantity': 'sum',
            'transaction_id': 'count'
        }).round(2)
        
        product_stats.columns = ['Ingresos_Totales', 'Cantidad_Total', 'Transacciones']
        top_products = product_stats.sort_values('Ingresos_Totales', ascending=False).head(10)
        
        fig = px.bar(
            top_products,
            x='Ingresos_Totales',
            y=top_products.index,
            orientation='h',
            title="Top 10 Productos por Ingresos",
            color='Ingresos_Totales',
            color_continuous_scale='plasma'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Análisis de ciclo de vida de productos
    st.subheader("🔄 Análisis de Ciclo de Vida")
    
    lifecycle_stats = df.groupby('product_lifecycle').agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'transaction_id': 'count'
    }).round(2)
    
    lifecycle_stats.columns = ['Ingresos_Totales', 'Cantidad_Total', 'Transacciones']
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(
            values=lifecycle_stats['Ingresos_Totales'],
            names=lifecycle_stats.index,
            title="Distribución de Ingresos por Ciclo de Vida"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            lifecycle_stats,
            x=lifecycle_stats.index,
            y='Transacciones',
            title="Transacciones por Ciclo de Vida",
            color='Transacciones',
            color_continuous_scale='viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Tabla de estadísticas por categoría
    st.subheader("📋 Estadísticas por Categoría de Producto")
    st.dataframe(category_stats, use_container_width=True)

def show_customer_analysis(df):
    """Muestra análisis de clientes"""
    st.header("👥 Análisis de Clientes")
    
    # Análisis por segmento de cliente
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribución por segmento
        segment_stats = df.groupby('customer_segment').agg({
            'total_amount': 'sum',
            'quantity': 'sum',
            'transaction_id': 'count',
            'customer_id': 'nunique'
        }).round(2)
        
        segment_stats.columns = ['Ingresos_Totales', 'Cantidad_Total', 'Transacciones', 'Clientes_Unicos']
        segment_stats = segment_stats.sort_values('Ingresos_Totales', ascending=False)
        
        fig = px.bar(
            segment_stats,
            x=segment_stats.index,
            y='Ingresos_Totales',
            title="Ingresos por Segmento de Cliente",
            color='Ingresos_Totales',
            color_continuous_scale='viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top clientes por ingresos
        customer_stats = df.groupby('customer_id').agg({
            'total_amount': 'sum',
            'quantity': 'sum',
            'transaction_id': 'count'
        }).round(2)
        
        customer_stats.columns = ['Ingresos_Totales', 'Cantidad_Total', 'Transacciones']
        top_customers = customer_stats.sort_values('Ingresos_Totales', ascending=False).head(10)
        
        fig = px.bar(
            top_customers,
            x='Ingresos_Totales',
            y=top_customers.index,
            orientation='h',
            title="Top 10 Clientes por Ingresos",
            color='Ingresos_Totales',
            color_continuous_scale='plasma'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Análisis de métodos de pago
    st.subheader("💳 Análisis de Métodos de Pago")
    
    payment_stats = df.groupby('payment_method').agg({
        'total_amount': 'sum',
        'transaction_id': 'count'
    }).round(2)
    
    payment_stats.columns = ['Ingresos_Totales', 'Transacciones']
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(
            values=payment_stats['Ingresos_Totales'],
            names=payment_stats.index,
            title="Distribución de Ingresos por Método de Pago"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            payment_stats,
            x=payment_stats.index,
            y='Transacciones',
            title="Transacciones por Método de Pago",
            color='Transacciones',
            color_continuous_scale='viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Tabla de estadísticas por segmento
    st.subheader("📋 Estadísticas por Segmento de Cliente")
    st.dataframe(segment_stats, use_container_width=True)

def show_financial_analysis(df):
    """Muestra análisis financiero"""
    st.header("💰 Análisis Financiero")
    
    # Métricas financieras clave
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_revenue = df['total_amount'].sum()
        st.metric("Ingresos Totales", f"${total_revenue:,.2f}")
    
    with col2:
        avg_transaction = df['total_amount'].mean()
        st.metric("Ticket Promedio", f"${avg_transaction:.2f}")
    
    with col3:
        total_discount = df['discount_pct'].sum()
        st.metric("Descuentos Totales", f"{total_discount:.2f}%")
    
    with col4:
        total_tax = df['tax_amount'].sum()
        st.metric("Impuestos Totales", f"${total_tax:,.2f}")
    
    # Análisis de precios
    st.subheader("💵 Análisis de Precios")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribución de precios unitarios
        fig = px.histogram(
            df,
            x='unit_price',
            nbins=50,
            title="Distribución de Precios Unitarios",
            labels={'unit_price': 'Precio Unitario', 'count': 'Frecuencia'}
        )
        fig.update_layout(xaxis_range=[0, df['unit_price'].quantile(0.95)])
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Comparación de precios por región
        price_by_region = df.groupby('region')['unit_price'].mean().sort_values(ascending=False)
        
        fig = px.bar(
            x=price_by_region.index,
            y=price_by_region.values,
            title="Precio Promedio por Región",
            labels={'x': 'Región', 'y': 'Precio Promedio'},
            color=price_by_region.values,
            color_continuous_scale='viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Análisis de descuentos
    st.subheader("🎯 Análisis de Descuentos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribución de descuentos
        fig = px.histogram(
            df,
            x='discount_pct',
            nbins=30,
            title="Distribución de Porcentajes de Descuento",
            labels={'discount_pct': 'Porcentaje de Descuento', 'count': 'Frecuencia'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Descuentos por categoría de producto
        discount_by_category = df.groupby('product_category')['discount_pct'].mean().sort_values(ascending=False)
        
        fig = px.bar(
            x=discount_by_category.index,
            y=discount_by_category.values,
            title="Descuento Promedio por Categoría",
            labels={'x': 'Categoría', 'y': 'Descuento Promedio (%)'},
            color=discount_by_category.values,
            color_continuous_scale='plasma'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Análisis de rentabilidad por región
    st.subheader("📈 Rentabilidad por Región")
    
    profitability_by_region = df.groupby('region').agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'transaction_id': 'count'
    }).round(2)
    
    profitability_by_region['Ingresos_Totales'] = profitability_by_region['total_amount']
    profitability_by_region['Cantidad_Total'] = profitability_by_region['quantity']
    profitability_by_region['Transacciones'] = profitability_by_region['transaction_id']
    
    fig = px.scatter(
        profitability_by_region,
        x='Ingresos_Totales',
        y='Transacciones',
        size='Cantidad_Total',
        color=profitability_by_region.index,
        title="Rentabilidad por Región (Tamaño = Cantidad, Color = Región)",
        labels={'Ingresos_Totales': 'Ingresos Totales', 'Transacciones': 'Número de Transacciones'}
    )
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
