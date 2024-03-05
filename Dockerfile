FROM python:3.8

WORKDIR /app

RUN pip install pandas

COPY main.py /app/
COPY data.py /app/

RUN pip install influxdb_client requests influxdb mdclogpy schedule

CMD ["python", "main.py"]
