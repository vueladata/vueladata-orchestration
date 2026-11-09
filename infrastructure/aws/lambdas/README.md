# Ingesta F2/F3 por RabbitMQ

Fuente versionada de las dos Lambdas. Cada ZIP contiene `lambda_function.py`
(copiado de `opensky.py` u `openmeteo.py`) y `event_transport.py`; ambas funciones
usan una capa con `pika`. El directorio de trabajo externo `Lambdas-aws` conserva
copias de estos archivos, pero el código que se revisa en Git está aquí.

## Contrato y entrega

- Un mensaje JSON normalizado por posición/observación, con `event_type` y
  `event_id` estables. OpenSky usa ICAO24 y timestamp del proveedor, convierte
  m/s a km/h y no inventa un `id_aeronave`. Open-Meteo usa ATL, UTC y convierte
  visibilidad de metros a kilómetros. No se generan observaciones sintéticas
  cuando el proveedor devuelve errores o rate limits.
- El exchange durable fanout `vueladata.external` enlaza `q.rds_writer` y
  `q.lake_writer`. Cada publicación es persistente, obligatoria y confirmada
  por el broker. Un fallo de publicación se propaga; no se declara éxito por
  una entrega parcial. La semántica es al menos una vez, no una transacción
  distribuida entre S3 y PostgreSQL.
- Solo si falla la conexión AMQP y ECS confirma desired/running/pending=0 se
  guarda NDJSON en `bronze/events/{opensky|clima}/fallback_<hash>.json`, con tag
  `rabbitmq=pending`. Fallos de autenticación no activan el fallback.
- Con RabbitMQ disponible, cada invocación intenta republicar hasta cinco
  archivos pendientes por las dos colas y marca `rabbitmq=replayed` únicamente
  después de confirmar la publicación. No elimina los datos Bronze. El evento
  `{"replay_only": true}` permite drenar pendientes sin consultar el proveedor.
  Es necesario vigilar la antigüedad del backlog y repetir el replay si quedan
  pendientes; con el entorno apagado RDS no puede estar sincronizado todavía.

## Workers y despliegue coordinado

Los cambios correspondientes de `vueladata-api` son necesarios antes de encender
los consumidores. Worker RDS enruta por event_type, resuelve el ICAO24 con el
catálogo aeronave y el IATA con aeropuerto. Los eventos sin catálogo se transfieren
con confirmación a `q.rds_unresolved` para posterior corrección y replay, en lugar
de asignarlos a aeronave 1. Debe cargarse el catálogo real antes de esperar que
todas las posiciones observadas aparezcan en PostgreSQL.

Worker Lake escribe los nuevos eventos como NDJSON en `bronze/events/opensky/`
y `bronze/events/clima/`, los prefijos que consume Spark. Su fallback a disco es
opt-in (`LOCAL_LAKE_FALLBACK=true` solo para desarrollo); en ECS un fallo S3 deja
los mensajes sin ACK. S3 puede contener reentregas en lotes diferentes; Silver
deduplica por identificador del evento. Las claves RDS garantizan idempotencia
por posición y aeropuerto/fecha de observación.

## Configuración AWS

Se reutiliza la zona privada `vueladata.local` de la VPC. RabbitMQ se descubre
mediante un servicio Cloud Map DNS_HTTP, registros A y `serviceRegistries` de ECS;
Service Connect está desactivado para RabbitMQ. ECS registra las IP de sus tareas
cuando arrancan y las retira al detenerlas: con desired=0 no se espera una IP DNS.
Esto permite a Lambdas y servicios ECS utilizar `rabbitmq.vueladata.local:5672`.

Variables requeridas: `S3_BUCKET`, `RABBITMQ_HOST`, `RABBITMQ_PORT`,
`RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_QUEUE_DL`, `RABBITMQ_QUEUE_RDS`,
`ECS_CLUSTER`, `RABBITMQ_SERVICE`. Las credenciales se suministran desde SSM al
desplegar, sin guardarlas en el ZIP o Git. No hay contraseñas por defecto.
Ambas funciones usan las mismas dos subredes privadas y SG que Open-Meteo.

Permisos: acceso VPC/ENI, `ecs:DescribeServices` limitado al servicio RabbitMQ,
ListBucket limitado al prefijo propio y Get/PutObject/Get/PutObjectTagging en
ese prefijo. Timeout configurado en 60 segundos; HTTP/AMQP/SDK tienen límites
individuales. Las reglas EventBridge existentes se conservan.

Referencias: [service discovery y DNS de ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-discovery.html),
[publicación obligatoria con confirmaciones en Pika](https://pika.readthedocs.io/en/stable/examples/blocking_publish_mandatory.html).
