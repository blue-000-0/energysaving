import time
import pandas as pd
import schedule
from mdclogpy import Logger
from data import DATABASE
import requests

logger = Logger(name=__name__)

class ESrapp():
    def __init__(self):
        super().__init__()
        self.db = DATABASE()
        self.db.connect()
        self.threshold = 50
        self.consecutive_threshold = 2
        self.consecutive_counts = {}

    def entry(self):
        data = self.db.read_data()
        if data['PEE.AvgPower'] is not None and len(data['PEE.AvgPower']) > 0:
            self.check_and_perform_action(data)

    def check_and_perform_action(self, data):
        for cell_id, group_data in data.groupby('CellID'):
            avg_power_list = group_data['PEE.AvgPower'].tolist()
            
            if cell_id not in self.consecutive_counts:
                self.consecutive_counts[cell_id] = 0

            if all(avg_power < self.threshold for avg_power in avg_power_list):
                self.consecutive_counts[cell_id] += 1
            else:
                self.consecutive_counts[cell_id] = 0

            if self.consecutive_counts[cell_id] >= self.consecutive_threshold:
                low_power_indices = data[data['PEE.AvgPower'] < 50].index
                self.perform_action(low_power_indices, data)


    def perform_action(self, indices, data):
        for index in indices:

            api_endpoint = f"http://192.168.8.28:32441/O1/CM/ManagedElement=1193046,GnbCuCpFunction=1,NrCellCu={str(index)},CESManagementFunction={str(index)}"

            json_data = {
                "id": str(index),
                "objectInstance": f"ManagedElement=1193046,GnbCuCpFunction=1,NrCellCu={str(index)},CESManagementFunction={str(index)}",
                "attributes": {
                    "energySavingControl": "toBeEnergySaving",
                    "energySavingState": "isNotEnergySaving"
                }
            }

            headers = {"Content-Type": "application/json"}

            response = requests.put(api_endpoint, json=json_data, headers=headers)

            if response.status_code == 200:
                print(f"Successfully performed action for index {index}: {api_endpoint}")
            else:
                print(f"Failed to perform action for index {index}. Status code: {response.status_code}")
                print(response.text)

if __name__ == "__main__":
    rapp_instance = ESrapp()

    schedule.every(5).seconds.do(rapp_instance.entry)

    while True:
        schedule.run_pending()
        time.sleep(1)
