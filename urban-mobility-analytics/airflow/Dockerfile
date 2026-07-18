FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# baixado no build para o job PySpark não depender de internet em runtime
RUN mkdir -p /opt/spark-jars \
    && curl -fsSL -o /opt/spark-jars/postgresql-42.7.3.jar \
       https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
