import requests
import yaml
import os
import pandas as pd
import numpy as np
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from datetime import datetime
#import askdata_api_python_client.askdata as askdata

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




class Dataset():

    '''
    Dataset Object
    '''

    _agentId = None
    _domain = None

    def __init__(self, env, token):

        self._headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " +token
        }

        if env == 'dev':
            self._base_url_dataset = url_list['BASE_URL_DATASET_DEV']
        if env == 'qa':
            self._base_url_dataset = url_list['BASE_URL_DATASET_QA']
        if env == 'prod':
            self._base_url_dataset = url_list['BASE_URL_DATASET_PROD']

    def GetDatasets(self):

        #to do test
        dataset_url = self._base_url_dataset + '/datasets?agentId=' + self._agentId
        response = requests.get(url=dataset_url, headers=self._headers)
        response.raise_for_status()
        r = response.json()
        r_df = pd.DataFrame(r)
        df_datasets = r_df.loc[:,['id', 'domain', 'type', 'code', 'name', 'description', 'createdBy', 'isActive', 'accessType', 'icon',
         'version', 'syncCount', 'visible', 'public', 'createdAt']]

        return df_datasets

    def ExecuteDatasetSync(self, dataset_id):

        dataset_url = self._base_url_dataset + '/datasets/' + dataset_id + '/sync'
        r = requests.post(url=dataset_url, headers=self._headers)
        r.raise_for_status()
        return r
