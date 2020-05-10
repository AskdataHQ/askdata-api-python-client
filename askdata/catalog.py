import requests
import yaml
import os
import pandas as pd
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
        if env == 'qa':
            self._base_url_cat = url_list['BASE_URL_FEED_QA']
        if env == 'prod':
            self._base_url_cat = url_list['BASE_URL_FEED_PROD']

    def load_catalogs(self, empty=True):

        if empty:
            flag = 'true'
        else:
            flag = 'false'
        # empty = True is for including all bookmarks (catalog) also empty
        authentication_url = self._base_url_cat + '/' + self._domain + '/discovery?emptyIncluded=' + flag
        r = requests.get(url=authentication_url, headers=self._headers)
        r.raise_for_status()
        df_catalogs = pd.DataFrame(r.json()['discovery'])

        return df_catalogs

    def create_query(self, query, entryid, execute=False):

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

        authentication_url = self._base_url_cat + '/agents/' + self._agentId + '/discovery-entry/' + entryid + '/queries?execute=' + flag_ex
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()

        return r

    # def DeleteQuery(self):
    #     pass