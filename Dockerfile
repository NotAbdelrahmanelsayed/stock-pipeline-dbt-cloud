FROM apache/airflow:3.0.1

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN groupadd -g 1001 docker && \
    usermod -aG docker airflow
    
USER airflow