# ==============================================================================
# Dockerfile para vueladata/spark-etl
# Arquitectura limpia: Componente de procesamiento distribuido (Fargate Spot)
# ==============================================================================

FROM apache/spark:3.5.1-python3

USER root

# Descarga de conectores S3A Hadoop y AWS SDK usando python3 estándar para evitar fallos de conexión en espejos de apt
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN python3 - <<'EOF'
import os
import time
import urllib.request

h_ver = os.environ.get("HADOOP_AWS_VERSION", "3.3.4")
aws_ver = os.environ.get("AWS_SDK_VERSION", "1.12.262")

urls = [
    f"https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/{h_ver}/hadoop-aws-{h_ver}.jar",
    f"https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/{aws_ver}/aws-java-sdk-bundle-{aws_ver}.jar",
    "https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar"
]

for url in urls:
    dest = f"/opt/spark/jars/{url.split('/')[-1]}"
    print(f"Downloading {url} -> {dest}...")
    for attempt in range(3):
        try:
            urllib.request.urlretrieve(url, dest)
            print("Done.")
            break
        except Exception as e:
            if attempt == 2:
                raise
            print(f"Retrying {dest} due to {e}...")
            time.sleep(2)
EOF

WORKDIR /opt/spark/work-dir

# Instalar dependencias Python
COPY infrastructure/docker/spark/requirements.txt .
RUN python3 -m pip install --no-cache-dir --upgrade pip==25.0.1 && \
    python3 -m pip install --no-cache-dir -r requirements.txt && python3 -m pip check

# Copiar el paquete modular vueladata_etl
COPY src/ /opt/spark/work-dir/src/
COPY infrastructure/docker/spark/entrypoint.sh /opt/spark/work-dir/entrypoint.sh

RUN sed -i 's/\r$//' /opt/spark/work-dir/entrypoint.sh && \
    chmod +x /opt/spark/work-dir/entrypoint.sh

ENV PYTHONPATH="/opt/spark/work-dir/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
ENV PYSPARK_PYTHON=python3

USER spark

ENTRYPOINT ["/opt/spark/work-dir/entrypoint.sh"]
