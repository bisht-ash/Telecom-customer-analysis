FROM apache/airflow:2.5.3
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt