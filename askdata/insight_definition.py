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

        if (type != "" and query != "" and params!=[]):
            self.edit_chart(self.components[position]["id"], type, query, params)
        else:
            logging.info("One or more arguments between type, query and params are missing to update the chart")

        return self.components[position]["id"]


    def edit_chart(self, chart_id, type:str, query, params):

        url = self.smart_insight_url + "/definitions/" + self.definition_id + "/charts/" + chart_id

        body = {
            "chartType": type.upper(),
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


    def add_list(self):
        position = (len(self.components))
        list_id = self.add_component("list", position)
        return list_id

    def add_text(self, text):
        position = (len(self.components))
        text_id = self.add_component("text", position)
        self.edit_text(text_id, text)
        return text_id


    def edit_text(self, text_id , text, name="Text"):

        url = self.smart_insight_url+"/definitions/"+self.definition_id+"/texts/"+text_id

        body = {
            "customName": False,
            "dependsOn": [],
            "id": text_id,
            "name": name,
            "queryComponent": False,
            "text": "Ciao",
            "type": text,
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


    def add_html(self, code):
        position = (len(self.components))
        html_id = self.add_component("html", position)
        self.edit_html(html_id, code)
        return html_id


    def edit_html(self, html_id, code):

        url = self.smart_insight_url+"/definitions/"+self.definition_id+"/htmls/"+html_id+"/content"

        body = {
            "html": code
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


    def add_script(self, script):
        position = (len(self.components))
        script_id = self.add_component("script", position)
        self.edit_script(script_id, script)
        return script_id


    def edit_script(self, script_id, script):

        url = self.smart_insight_url+"/definitions/"+self.definition_id+"/scripts/"+script_id+"/content"

        body = {
            "script": script
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

    def add_map(self):
        position = (len(self.components))
        map_id = self.add_component("map", position)
        return map_id

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
        self.components = r.json()["components"]

        return self.components[position]["id"]


    def delete(self):

        url= self.smart_insight_url+"/definitions/"+self.definition_id
        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }

        r = s.delete(url=url, headers=headers)
        r.raise_for_status()

    '''def add_query_composer(self, dataset_slug, fields, conditions):

        #Get dataset_id
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

        #Get query composer element id

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        qc_url = self.smart_insight_url +"/composed_queries?datasetId=" + dataset_id
        logging.info("AUTH URL {}".format(authentication_url))

        headers = {"Authorization": "Bearer" + " " + self._token}
        response = s.get(url=qc_url, headers=headers)
        response.raise_for_status()
        resp = response.json()
        qc_id = resp["qc"]["id"]

        
        position = (len(self.components))
        body = {
            "position": position,
            "qc": qc_id,
            type: "query_composer"
        }
        url = self.smart_insight_url + "/definitions/"+self.definition_id+"/query_composers"
        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }

        logging.info("URL {}".format(url))
        r = s.post(url=url, json=body, headers=headers)
    '''

    def add_search_query(self, query):

        position = (len(self.components))
        query_id = self.add_component("nl_query", position)

        body_query = {"nl": query, "language": "en"}

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        query_url = self.smart_insight_url +'/definitions/'+ self.definition_id +'/nl_queries/'+ query_id + '/nl'

        logging.info("QUERY URL {}".format(query_url))
        r = s.put(url=query_url, json=body_query, headers=headers)

        self.components = r.json()["components"]

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
        return self.components[position]["id"]

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
