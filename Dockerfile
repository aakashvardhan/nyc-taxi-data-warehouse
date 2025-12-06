FROM apache/airflow:2.10.1

USER root

# Install OpenJDK 17 (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (find Java installation automatically)
RUN echo "export JAVA_HOME=\$(readlink -f /usr/bin/java | sed 's:/bin/java::')" >> /etc/profile
ENV JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-arm64

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark \
    yfinance \
    apache-airflow-providers-snowflake \
    snowflake-connector-python \
    dbt-core==1.7.17 \
    dbt-snowflake==1.7.3
