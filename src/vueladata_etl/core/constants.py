"""
Módulo Core: Constantes del Dominio Aeronáutico, Lakehouse y Bases de Datos
═══════════════════════════════════════════════════════════════════════════════
Centraliza reglas de negocio (RN-01, RN-08), esquemas PostgreSQL y tablas.
"""

import os

# ── Reglas de Negocio Aeronáuticas ───────────────────────────────────────────
# RN-01: Umbral internacional de puntualidad (On-Time Performance)
OTP_DELAY_THRESHOLD_MINUTES = 15.0

# RN-08: Tiempos mínimos de rotación de aeronaves en tierra (Turnaround)
TURNAROUND_MINUTES_NACIONAL = 45
TURNAROUND_MINUTES_INTERNACIONAL = 60

# ── Clasificación de Eventos Operacionales OOOI ──────────────────────────────
EVENT_OUT = "OUT"  # Salida de puerta
EVENT_OFF = "OFF"  # Despegue (ruedas fuera de pista)
EVENT_ON = "ON"    # Aterrizaje (ruedas en pista)
EVENT_IN = "IN"    # Llegada a puerta
VALID_OOOI_EVENTS = [EVENT_OUT, EVENT_OFF, EVENT_ON, EVENT_IN]

# Códigos de causas de retraso y cancelación
CAUSAS_VALIDAS = ["TECNICA", "CLIMA", "TRIPULACION", "OPERACIONAL", "SEGURIDAD"]

# ── Fuentes de Datos ─────────────────────────────────────────────────────────
FUENTE_F1_KAGGLE = "F1_KAGGLE"
FUENTE_F6_ROTACION = "F6_ROTACION"
FUENTE_RDS_OPERACIONAL = "RDS_OPERACIONAL"
FUENTE_WORKER_LAKE = "WORKER_LAKE"

# ── Esquemas y Tablas en PostgreSQL RDS ──────────────────────────────────────
SCHEMA_OPERACIONAL = "vueladata"
SCHEMA_AUDIT = "vueladata_audit"
SCHEMA_GOLD = "vueladata_gold"

TABLE_EXECUTION_LOG = "pipeline_execution_log"
TABLE_ERROR_QUARANTINE = "pipeline_error_quarantine"
TABLE_FACT_VUELO = "fact_vuelo_analitico"
TABLE_KPI_OTP = "puntualidad_aerolineas"
TABLE_KPI_PROPAGACION = "propagacion_flota"
TABLE_KPI_OTP_RUTAS = "puntualidad_rutas"
TABLE_KPI_CAUSAS = "causas_retraso"
TABLE_KPI_COMPENSACIONES = "costos_compensacion"
TABLE_KPI_CONEXIONES = "conexiones_hubs"
TABLE_KPI_CLIMA = "impacto_clima"

# ── Reglas de Negocio Adicionales (HU-31 / Dashboard Gold) ───────────────────
# Umbral de Compensación Económica Obligatoria (minutos de demora o cancelación)
COMPENSATION_THRESHOLD_DELAY_MINUTES = 180.0
COMPENSATION_BASE_COP_PER_PASSENGER = 200000.0  # ~$50 USD / COP estándar por pasajero
AVERAGE_PASSENGERS_PER_FLIGHT = 140

# Aeropuertos Hub Principales
HUBS_AEROPUERTOS = {
    "BOG": "Bogotá - El Dorado (Hub Principal)",
    "MDE": "Medellín - José María Córdova",
    "CLO": "Cali - Alfonso Bonilla Aragón",
    "ATL": "Atlanta Hartsfield-Jackson",
    "ORD": "Chicago O'Hare",
    "DFW": "Dallas Fort Worth",
    "DEN": "Denver International",
    "LAX": "Los Angeles International"
}

# ── Configuración de Conexión a Base de Datos ────────────────────────────────
def obtener_url_postgres() -> str:
    """Retorna la cadena de conexión estándar hacia PostgreSQL RDS."""
    host = os.environ.get("RDS_HOST", "vueladata-postgres-dev.ci9mcasuas3l.us-east-1.rds.amazonaws.com")
    port = os.environ.get("RDS_PORT", "5432")
    user = os.environ.get("RDS_USER", "postgres")
    password = os.environ.get("RDS_PASSWORD")
    if not password:
        raise RuntimeError("RDS_PASSWORD is required; inject it from SSM or the local environment")
    dbname = os.environ.get("RDS_DB", "vueladata")
    from urllib.parse import quote
    return f"postgresql+psycopg2://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}/{dbname}"
