import requests
import yaml
import os
import pandas as pd
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
import json as json
from askdata_api_python_client.askdata import Agent
import uuid
from datetime import datetime

_LOG_FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] - %(asctime)s --> %(message)s"
g_logger = logging.getLogger()
logging.basicConfig(format=_LOG_FORMAT)
g_logger.setLevel(logging.INFO)

root_dir = os.path.abspath(os.path.dirname(__file__))
# retrieving base url
yaml_path = os.path.join(root_dir, '../askdata_api_python_client/askdata_config/base_url.yaml')
with open(yaml_path, 'r') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    url_list = yaml.load(file, Loader=yaml.FullLoader)



class Catalog:

    def __init__(self, Agent , empty=True):
        self.agentId = Agent.agentId
        self.workspaceId = Agent.workspaceId
        self.username = Agent.username
        self.language = Agent.language
        self.token = Agent.token
        self.env = Agent.env

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }

        if self.env == 'dev':
            self.base_url = url_list['BASE_URL_FEED_DEV']
        if self.env == 'qa':
            self.base_url = url_list['BASE_URL_FEED_QA']
        if self.env == 'prod':
            self.base_url = url_list['BASE_URL_FEED_PROD']

        if empty:
            flag = 'true'
        else:
            flag = 'false'

        authentication_url = self.base_url + '/' + self.workspaceId + '/discovery?emptyIncluded=' + flag
        r = requests.get(url=authentication_url, headers=self.headers)
        r.raise_for_status()
        self.catalogs = pd.DataFrame(r.json()['discovery'])

    def PushQuery(self,query,entryid,execute=False):

        data = {
            "type": "text",
            "payload": query,
            "title": query,
            "lang": self.language
            }

        if execute:
            flag_ex = 'true'
        else:
            flag_ex = 'false'

        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self.base_url + '/agents/' + self.agentId + '/discovery-entry/' + entryid + '/queries?execute=' + flag_ex
        r = s.post(url=authentication_url, headers=self.headers, json=data)
        r.raise_for_status()

        return r

    def DeleteQuery(self):
        pass