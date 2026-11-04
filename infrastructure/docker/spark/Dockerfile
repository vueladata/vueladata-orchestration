# ==============================================================================
# Dockerfile para vueladata/spark-etl
# Arquitectura limpia: Componente de procesamiento distribuido (Fargate Spot)
# ==============================================================================

FROM apache/spark:3.5.1-python3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Descarga de conectores S3A Hadoop y AWS SDK
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN curl -sS -L -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sS -L -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -sS -L -o /opt/spark/jars/wildfly-openssl-1.1.3.Final.jar \
    https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar

WORKDIR /opt/spark/work-dir

# Instalar dependencias Python
COPY infrastructure/docker/spark/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el paquete modular vueladata_etl
COPY src/ /opt/spark/work-dir/src/
COPY infrastructure/docker/spark/entrypoint.sh /opt/spark/work-dir/entrypoint.sh

RUN chmod +x /opt/spark/work-dir/entrypoint.sh

ENV PYTHONPATH="/opt/spark/work-dir/src"

USER spark

ENTRYPOINT ["/opt/spark/work-dir/entrypoint.sh"]
