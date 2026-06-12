# 1. Bazujemy na oficjalnym obrazie Apache Airflow
FROM apache/airflow:2.8.1-python3.10

# 2. Przełączamy się na chwilę na użytkownika root, żeby zainstalować pakiety systemowe
USER root

# 3. Instalujemy Javę (OpenJDK 17), która jest absolutnie wymagana przez Sparka
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* 

# 4. Ustawiamy zmienną środowiskową JAVA_HOME, żeby Spark wiedział, gdzie szukać Javy
ENV JAVA_HOME=/usr/lib/jvm/default-java


# 5. Przełączamy się z powrotem na standardowego użytkownika airflow
USER airflow

# 6. Instalujemy potrzebne biblioteki Pythonowe (PySpark oraz dbt)
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    dbt-duckdb 
