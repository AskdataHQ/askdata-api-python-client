import requests
import yaml
import os
import pandas as pd
import numpy as np
import logging
from askdata.insight import Insight
from askdata.channel import Channel
from askdata.catalog import Catalog
from askdata.dataset import Dataset
from askdata.security import SignUp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from askdata.askdata_client import Askdata

_LOG_FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] - %(asctime)s --> %(message)s"
g_logger = logging.getLogger()
logging.basicConfig(format=_LOG_FORMAT)
g_logger.setLevel(logging.INFO)

root_dir = os.path.abspath(os.path.dirname(__file__))
# retrieving base url
yaml_path = os.path.join(root_dir, '../askdata/askdata_config/base_url.yaml')
with open(yaml_path, 'r') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    url_list = yaml.load(file, Loader=yaml.FullLoader)

class Insight_Definition:


    def __init__(self, env:str, token, defintion):

        self.definition_id = defintion["id"]
        self.agent_id = defintion["agentId"]
        self.collection_id = defintion["collectionId"]
        self.name = defintion["name"]
        self.slug = defintion["slug"]
        self.icon = defintion["icon"]
        self.components = defintion["components"]

        self._token = token

        if env.lower() == 'dev':
            self.smart_insight_url = url_list['BASE_URL_INSIGHT_DEV']
        if env.lower() == 'qa':
            self.smart_insight_url = url_list['BASE_URL_INSIGHT_QA']
        if env.lower() == 'prod':
            self.smart_insight_url = url_list['BASE_URL_INSIGHT_PROD']


    def add_table(self, query="", columns=[]):

        body = {"type": "table", "position": (len(self.components)+1)}

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        query_url = self.smart_insight_url + '/definitions/' + self.definition_id + '/components/'

        r = s.post(url=query_url, json=body, headers=headers)
        print(r.json())

        self.components = r.json()["components"]

        if(query != "" and columns!=[]):
            self.edit_table(query, columns)


    def edit_table(self, query="", columns=[]):

        url = self.smart_insight_url + "/definitions/" + self.definition_id + "/components/"\
              + self.components[-1]["id"]

        body = {
            "id": self.components[-1]["id"],
            "type": "table",
            "name": "Table",
            "customName":False,
            "dependsOn":["q1"],
            "queryId": query,
            "columns": columns,
            "maxResults":50,
            "valid":True,
            "queryComponent":False
        }

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }

        r = s.put(url=url, json=body, headers=headers)

        self.components = r.json()["components"]