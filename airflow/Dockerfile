FROM apache/airflow:3.1.6-python3.12
COPY requirements.txt /requirements.txt
RUN pip install duckdb==1.2.2 && \
    pip install --no-cache-dir -r /requirements.txt