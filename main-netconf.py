import time
import pandas as pd
import schedule
from mdclogpy import Logger
from data import DATABASE
import requests
import requests
from ncclient import manager
from xml.etree import ElementTree as ET

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

    def convert_to_xml(index):
        root = ET.Element("config", xmlns="urn:ietf:params:xml:ns:netconf:base:1.0")
        managed_element = ET.SubElement(root, "ManagedElement", xmlns="urn:3gpp:sa5:_3gpp-common-managed-element")
        id_element = ET.SubElement(managed_element, "id")
        id_element.text = "1193046"
        gnb_ucp_function = ET.SubElement(managed_element, "GNBCUCPFunction", xmlns="urn:3gpp:sa5:_3gpp-nr-nrm-gnbcucpfunction")
        id_element = ET.SubElement(gnb_ucp_function, "id")
        id_element.text = "1"
        nr_cell_cu = ET.SubElement(gnb_ucp_function, "NRCellCU", xmlns="urn:3gpp:sa5:_3gpp-nr-nrm-nrcellcu")
        id_element = ET.SubElement(nr_cell_cu, "id")
        id_element.text = str(index)
        ces_management_function = ET.SubElement(nr_cell_cu, "CESManagementFunction", xmlns="urn:3gpp:sa5:_3gpp-nr-nrm-cesmanagementfunction")
        id_element = ET.SubElement(ces_management_function, "id")
        id_element.text = str(index)
        attributes = ET.SubElement(ces_management_function, "attributes")
        energy_saving_control = ET.SubElement(attributes, "energySavingControl")
        energy_saving_control.text = "toBeEnergySaving"
        energy_saving_state = ET.SubElement(attributes, "energySavingState")
        energy_saving_state.text = "isNotEnergySaving"
        return ET.tostring(root, encoding="unicode")

    def perform_action(self, indices, data):
        for index in indices:
            xml_data = convert_to_xml(index)
            with manager.connect(host="192.168.8.28", port=830, username="username", password="password", hostkey_verify=False) as m:
                try:
                    edit_response = m.edit_config(target="running", config=xml_data)
                    print(f"Successfully performed action for index {index}")
                except Exception as e:
                    print(f"Failed to perform action for index {index}: {str(e)}")

if __name__ == "__main__":
    rapp_instance = ESrapp()

    schedule.every(5).seconds.do(rapp_instance.entry)

    while True:
        schedule.run_pending()
        time.sleep(1)
