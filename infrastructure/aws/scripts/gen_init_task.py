import json
import os

with open('infrastructure/aws/rds/init_all_schemas.sql', 'r', encoding='utf-8') as f:
    sql = f.read()

py_script = """import os, sys
try:
    import psycopg2
except ImportError:
    import psycopg as psycopg2

db_url = os.environ.get('DATABASE_URL')
# Si el url tiene 'postgresql+psycopg://', reemplazar por 'postgresql://'
if db_url and db_url.startswith('postgresql+psycopg://'):
    db_url = db_url.replace('postgresql+psycopg://', 'postgresql://')

print('[*] Conectando a PostgreSQL RDS...')
conn = psycopg2.connect(db_url)
conn.autocommit = True
cur = conn.cursor()

sql_script = """ + repr(sql) + """
print('[*] Ejecutando DDL completo para airflow_db, vueladata_audit y vueladata_gold...')
cur.execute(sql_script)
print('[+] DDL ejecutado con exito total!')

cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('airflow_db', 'vueladata_audit', 'vueladata_gold', 'vueladata') ORDER BY schema_name;")
schemas = [r[0] for r in cur.fetchall()]
print('[+] Esquemas verificados en PostgreSQL RDS:', schemas)
conn.close()
print('[+] Proceso completado con exito.')
"""

data = {
    "cluster": "vueladata-cluster",
    "taskDefinition": "vueladata-worker-rds-task:3",
    "launchType": "FARGATE",
    "networkConfiguration": {
        "awsvpcConfiguration": {
            "subnets": json.loads(os.environ["ECS_SUBNET_IDS"]),
            "securityGroups": json.loads(os.environ["ECS_SECURITY_GROUP_IDS"]),
            "assignPublicIp": "DISABLED"
        }
    },
    "overrides": {
        "containerOverrides": [
            {
                "name": "worker-rds",
                "command": ["python", "-c", py_script]
            }
        ]
    }
}

with open('infrastructure/aws/ecs/resolved/run-init-db-via-rds-worker.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2)

print('JSON generado exitosamente')
