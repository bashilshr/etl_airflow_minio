# Use the official Airflow image
FROM apache/airflow:2.9.2-python3.12

USER root

# Install system deps needed by Spark (Java + ps) and tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk \
        procps \
        curl \
        unzip \
        vim \
        nano \
        wget \
        python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so spark-submit finds Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install rclone
RUN curl https://rclone.org/install.sh | bash

# Create directories for configs
RUN mkdir -p /opt/airflow/.config/rclone && \
    mkdir -p /opt/airflow/credentials && \
    chown -R airflow: /opt/airflow

# Copy credentials
COPY airflow/credentials/service_account.json /opt/airflow/credentials/service_account.json
COPY airflow/rclone/rclone.conf /opt/airflow/.config/rclone/rclone.conf

USER airflow

# Install Python dependencies (no pip upgrade to avoid resolver conflicts)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
