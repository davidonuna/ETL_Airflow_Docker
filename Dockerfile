FROM apache/airflow:2.7.3
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

# ENV AIRFLOW_HOME=/usr/local/airflow
# ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"
