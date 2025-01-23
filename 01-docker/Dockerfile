FROM python:3.13.1

RUN apt-get install wget
RUN pip install pandas sqlachemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]