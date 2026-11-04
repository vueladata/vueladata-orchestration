from vueladata_etl.core.schemas.f1_schemas import (
    SCHEMA_F1_FLIGHTS,
    SCHEMA_F1_AIRLINES,
    SCHEMA_F1_AIRPORTS
)
from vueladata_etl.core.schemas.f6_schemas import (
    SCHEMA_F6_ITINERARY,
    SCHEMA_F6_FLEET,
    SCHEMA_F6_ROUTES
)
from vueladata_etl.core.schemas.event_schemas import (
    SCHEMA_EVENTO_OOOI,
    SCHEMA_EVENTO_PAGO,
    SCHEMA_OBSERVACION_CLIMA,
    SCHEMA_POSICION_AERONAVE
)

__all__ = [
    "SCHEMA_F1_FLIGHTS",
    "SCHEMA_F1_AIRLINES",
    "SCHEMA_F1_AIRPORTS",
    "SCHEMA_F6_ITINERARY",
    "SCHEMA_F6_FLEET",
    "SCHEMA_F6_ROUTES",
    "SCHEMA_EVENTO_OOOI",
    "SCHEMA_EVENTO_PAGO",
    "SCHEMA_OBSERVACION_CLIMA",
    "SCHEMA_POSICION_AERONAVE"
]
