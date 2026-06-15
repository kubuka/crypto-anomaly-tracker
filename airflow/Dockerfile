
FROM apache/airflow:2.8.1-python3.10

# Przełączamy się na chwilę na użytkownika root, żeby zainstalować pakiety systemowe
USER root

# Instalujemy Javę (OpenJDK 17), która jest  wymagana przez Sparka
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* 


ENV JAVA_HOME=/usr/lib/jvm/default-java



USER airflow


RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    dbt-duckdb 
