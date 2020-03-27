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

    def __init__(self, Agent):
        self.token = Agent.token
        self.env = Agent.env
        self.agentId = Agent.agentId

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }

        if self.env == 'dev':
            self.base_url = url_list['BASE_URL_DATASET_DEV']
        if self.env == 'qa':
            self.base_url = url_list['BASE_URL_DATASET_QA']
        if self.env == 'prod':
            self.base_url = url_list['BASE_URL_DATASET_PROD']

    def GetDatasets(self):

        #to do test
        dataset_url = self.base_url + '/datasets?agentId=' + self.agentId
        response = requests.get(url=dataset_url, headers=self.headers)
        response.raise_for_status()
        r = response.json()
        r_df = pd.DataFrame(r)
        df_datasets = r_df.loc[:,['id', 'domain', 'type', 'code', 'name', 'description', 'createdBy', 'isActive', 'accessType', 'icon',
         'version', 'syncCount', 'visible', 'public', 'createdAt']]

        return df_datasets

    def ExecuteDatasetSync(self, dataset_id):

        dataset_url = self.base_url + '/datasets/' + dataset_id + '/sync'
        r = requests.post(url=dataset_url, headers=self.headers)
        r.raise_for_status()
        return r
