import time
import pandas as pd
from influxdb import DataFrameClient
from configparser import ConfigParser
from mdclogpy import Logger
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from requests.exceptions import RequestException, ConnectionError
import json
import os
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WriteOptions

logger = Logger(name=__name__)


class DATABASE(object):
    
    def __init__(self, dbname='Timeseries', user='root', password='root', host="r4-influxdb.ricplt", port='8086', path='', ssl=False):
        self.data = None
        self.host = "http://10.233.31.193"
        self.port = '80'
        self.user = 'admin'
        self.password = '6mdlVbzj97u7oy89dOX41WPZh1wu7yjL'
        self.path = path
        self.ssl = ssl
        self.dbname = dbname
        self.client = None
        self.token = 'sF3jR78UFGg26JTp4tF2O14QWcO7n32g'
        self.address = 'http://10.233.31.193:80'
        self.org = 'influxdata'
        self.bucket = 'RIC-TEST-bucket'

    def connect(self):
        if self.client is not None:
            self.client.close()

        try:
            self.client = influxdb_client.InfluxDBClient(url=self.address, org=self.org, token=self.token)
            version = self.client.version()
            print("Conected to Influx Database, InfluxDB version : {}".format(version))
            return True

        except (RequestException, InfluxDBClientError, InfluxDBServerError, ConnectionError):
            print("Failed to establish a new connection with InflulxDB, Please check your url/hostname")
            time.sleep(120)

    def read_data(self, train=False, valid=False, limit=False):

        self.data = None
        query = 'from(bucket:"{}")'.format(self.bucket)
        query += ' |> range(start: -1d) '
        query += ' |> filter(fn: (r) => r["_measurement"] == "o-ran-pm") '
        query += ' |> filter(fn: (r) => r["_field"] == "PEE.AvgPower" or r["_field"] == "Time" or r["_field"] == "CellID") '
        query += ' |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") '

    
        result = self.query(query)
        self.data = result
        print(self.data)
        return result

    def query(self, query):
        try:
            query_api = self.client.query_api()
            result = query_api.query_data_frame(org=self.org, query=query)
        except (RequestException, InfluxDBClientError, InfluxDBServerError, ConnectionError) as e:
            print('Failed to connect to influxdb: {}'.format(e))
            result = False
        return result
