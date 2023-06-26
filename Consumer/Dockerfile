FROM openjdk:11

COPY --from=docker.io/bitnami/spark:3.2 / / 

COPY --from=python:3.6 / /


ADD consumer.py .

COPY .env .env

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

CMD [ "python3", "./consumer.py"]