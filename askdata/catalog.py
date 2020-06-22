import requests
import yaml
import os
import pandas as pd
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from askdata.askdata_client import Agent
import numpy as np
import json as json
import uuid
from datetime import datetime

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



class Catalog:

    _language = None
    _agentId = None
    _domain = None

    def __init__(self, env ,token):

        self._headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + token
        }

        if env == 'dev':
            self._base_url_cat = url_list['BASE_URL_FEED_DEV']
            self._base_url_askdata = url_list['BASE_URL_ASKDATA_DEV']
        if env == 'qa':
            self._base_url_cat = url_list['BASE_URL_FEED_QA']
            self._base_url_askdata = url_list['BASE_URL_ASKDATA_QA']
        if env == 'prod':
            self._base_url_cat = url_list['BASE_URL_FEED_PROD']
            self._base_url_askdata = url_list['BASE_URL_ASKDATA_PROD']

    def create_catalog(self, name: str, icon="https://storage.googleapis.com/askdata/interactions/icoInteractionDiscovery.png" ):

        data = {"type": "CUSTOM",
                   "name": name,
                   "icon": icon,
                   "lang": self._language}

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartfeed/agents/' + self._agentId + '/discovery-entry'
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()

        logging.info('create catalog: {} -----> with Id {}'.format(str(r.json()['title']), str(r.json()['id'])))

        return r.json()['id']

    def load_catalogs(self, empty=True) -> pd.DataFrame:
        """
        Get all bookmarks of the agent

        :param empty: empty = True is for including all bookmarks (catalog) also empty
        :return: DataFrame
        """
        if empty:
            flag = 'true'
        else:
            flag = 'false'

        authentication_url = self._base_url_askdata + '/smartfeed/' + self._domain + '/discovery?emptyIncluded=' + flag
        r = requests.get(url=authentication_url, headers=self._headers)
        r.raise_for_status()
        df_catalogs = pd.DataFrame(r.json()['discovery'])

        return df_catalogs

    def create_query(self, query:str, entryid:str, execute=False)-> str:

        data = {
            "type": "text",
            "payload": query,
            "title": query,
            "lang": self._language
            }

        if execute:
            flag_ex = 'true'
        else:
            flag_ex = 'false'

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartfeed/agents/' + self._agentId + '/discovery-entry/' + entryid + '/queries?execute=' + flag_ex
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()

        logging.info('create query: "{}" in catalog: {}'.format(str(r.json()['payload']), entryid))

        return r.json()['id']

    def get_query_from_catalog(self,entryid: str) -> list:

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartfeed/agents/' + self._agentId + '/discovery-entry/' + entryid + '/queries'
        r = s.get(url=authentication_url, headers=self._headers)
        r.raise_for_status()

        return r.json()

    def delete_catalog(self,entryid):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartfeed/agents/' + self._agentId + '/discovery-entry/' + entryid
        r = s.delete(url=authentication_url, headers=self._headers)
        r.raise_for_status()

        logging.info('deleted catalog: {}'.format(entryid))
        return r

    def delete_query(self, entryid: str, queryid: str):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartfeed/agents/' + self._agentId + '/discovery-entry/' + entryid + '/queries/' + queryid
        r = s.delete(url=authentication_url, headers=self._headers)
        r.raise_for_status()

        logging.info('deleted query: {} from catalog: {}'.format(queryid, entryid))
        return r

    @staticmethod
    def delete_all_queries_catalog(agent: 'Agent', entry_id: str):
        queries_list = agent.get_query_from_catalog(entry_id)
        for query in queries_list:
            agent.delete_query(entryid=entry_id, queryid=query['id'])
        logging.info('All queries deleted from catalog with entry_id : {}'.format(entry_id))
