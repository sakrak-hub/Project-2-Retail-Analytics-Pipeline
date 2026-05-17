FROM apache/airflow:3.1.6-python3.12
COPY requirements.txt /requirements.txt
RUN pip install duckdb==1.4.1 && \
    pip install --no-cache-dir -r /requirements.txt