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
            self._base_url_askdata = url_list['BASE_URL_ASKDATA_DEV']
            self.smart_insight_url = url_list['BASE_URL_INSIGHT_DEV']
        if env.lower() == 'qa':
            self._base_url_askdata = url_list['BASE_URL_ASKDATA_QA']
            self.smart_insight_url = url_list['BASE_URL_INSIGHT_QA']
        if env.lower() == 'prod':
            self._base_url_askdata = url_list['BASE_URL_ASKDATA_PROD']
            self.smart_insight_url = url_list['BASE_URL_INSIGHT_PROD']


    def add_table(self, query="", columns=[]):

        position = (len(self.components))
        body = {"type": "table", "position": position}

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        query_url = self.smart_insight_url + '/definitions/' + self.definition_id + '/components/'
        logging.info("URL {}".format(query_url))
        r = s.post(url=query_url, json=body, headers=headers)
        r.raise_for_status()
        print(r.json())
        self.components = r.json()["components"]

        if(query != "" and columns!=[]):
            self.edit_table(self.components[position]["id"], query, columns)

        return self.components[position]["id"]


    def edit_table(self, table_id, query="", columns=[]):

        url = self.smart_insight_url + "/definitions/" + self.definition_id + "/tables/"+ table_id

        body = {
            "id": table_id,
            "type": "table",
            "name": "Table",
            "customName":False,
            "dependsOn": ["q1"],
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
        r.raise_for_status()
        self.components = r.json()["components"]


    def add_chart(self, type="", query="", params=[]):
        position = (len(self.components))
        body = {"type": "chart", "position": position}

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        query_url = self.smart_insight_url + '/definitions/' + self.definition_id + '/components/'
        logging.info("URL {}".format(query_url))
        r = s.post(url=query_url, json=body, headers=headers)
        r.raise_for_status()
        print(r.json())
        self.components = r.json()["components"]

        if (type != "" and query != ""):
            self.edit_chart(self.components[position]["id"], type, query, params)

        return self.components[position]["id"]


    def edit_chart(self, chart_id, type, query, params):

        url = self.smart_insight_url + "/definitions/" + self.definition_id + "/charts/" + chart_id

        body = {
            "chartType": type,
            "customName": False,
            "dependsOn": [],
            "id": chart_id,
            "name": "Fusionfood",
            "params": params,
            "queryComponent": False,
            "queryId": query,
            "type": "chart",
            "valid": True
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
        r.raise_for_status()
        self.components = r.json()["components"]


    def add_sql_query(self, query_sql, dataset_slug):

        position = (len(self.components))

        self.add_component("sql_query", position)

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartdataset/datasets/slug/' + self.agent_id + '/' + dataset_slug
        logging.info("AUTH URL {}".format(authentication_url))

        headers = {"Authorization": "Bearer" + " " + self._token}
        response = s.get(url=authentication_url, headers=headers)
        response.raise_for_status()
        r = response.json()
        dataset_id = r.json()["dataset"]["id"]

        url = self.smart_insight_url+"/definitions/"+self.definition_id+"/sql_queries/"+self.components[position]["id"]+"/sql"

        body = {
            "datasetId": dataset_id,
            "sql": query_sql
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
        r.raise_for_status()
        return self.components[position]["id"]


    def add_component(self, type, position):

        body = {"type": type, "position": position}

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        query_url = self.smart_insight_url + '/definitions/' + self.definition_id + '/components/'
        logging.info("URL {}".format(query_url))
        r = s.post(url=query_url, json=body, headers=headers)
        r.raise_for_status()
        print(r.json())
        self.components = r.json()["components"]

    def delete_component(self, component_id):
        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        query_url = self.smart_insight_url + '/definitions/' + self.definition_id + '/components/'+component_id
        logging.info("URL {}".format(query_url))
        r = s.delete(url=query_url, headers=headers)
        r.raise_for_status()

    def publish(self):

        url = self.smart_insight_url + "/definitions/" + self.definition_id + "/publish/"

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }

        r = s.post(url=url, headers=headers)
        r.raise_for_status()
