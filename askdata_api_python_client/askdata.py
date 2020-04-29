import requests
import yaml
import os
import pandas as pd
import numpy as np
import logging
from askdata_api_python_client.insight import Insight
from askdata_api_python_client.channel import Channel
from askdata_api_python_client.catalog import Catalog
from askdata_api_python_client.dataset import Dataset
from askdata_api_python_client.security import SignUp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
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


class Askdata(SignUp):
    '''
    Authentication Object
    '''
    def __init__(self, username, password, domainlogin='askdata', env='prod'):
        self.username = username
        self.password = password
        self._domainlogin = domainlogin.upper()
        self._env = env

        data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password
        }
        # "domainlogin": self._domainlogin
        headers = {
            "Authorization": "Basic YXNrZGF0YS1zZGs6YXNrZGF0YS1zZGs=",
            "Content-Type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache,no-cache"
        }
        if self._env == 'dev':
            authentication_url = url_list['BASE_URL_AUTH_DEV'] + '/domain/' + self._domainlogin.lower() + '/oauth/token'
            authentication_url_userid = url_list['BASE_URL_AUTH_DEV'] + '/me'
        if self._env == 'qa':
            # authentication_url = url_list['BASE_URL_AUTH_QA']  + '/oauth/access_token'
            authentication_url = url_list['BASE_URL_AUTH_QA'] + '/domain/' + self._domainlogin.lower() + '/oauth/token'
            authentication_url_userid = url_list['BASE_URL_AUTH_QA'] + '/me'
        if self._env == 'prod':
            authentication_url = url_list['BASE_URL_AUTH_PROD'] + '/domain/' + self._domainlogin.lower() + '/oauth/token'
            authentication_url_userid = url_list['BASE_URL_AUTH_PROD'] + '/me'

        with requests.Session() as s:

            r1 = s.post(url=authentication_url, data=data, headers=headers)
            r1.raise_for_status()
            self._token = r1.json()['access_token']
            self.r1 = r1

            self._headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer" + " " + self._token
            }

            r_userid = s.get(url=authentication_url_userid, headers=self._headers)
            self.userid = r_userid.json()['id']

            if self._env == 'dev':
                self.base_url_security = url_list['BASE_URL_SECURITY_DEV']
                authentication_url = url_list['BASE_URL_AGENT_DEV']

            if self._env == 'qa':
                self.base_url_security = url_list['BASE_URL_SECURITY_QA']
                authentication_url = url_list['BASE_URL_AGENT_QA']

            if self._env == 'prod':

                self.base_url_security = url_list['BASE_URL_SECURITY_PROD']
                authentication_url = url_list['BASE_URL_AGENT_PROD']

            # request of all agents of the user/token
            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
            s.mount('https://', HTTPAdapter(max_retries=retries))
            response = s.get(url=authentication_url, headers=self._headers)
            response.raise_for_status()
        self.r2 = response.json()

        self.df_agents = pd.DataFrame(response.json())



    def signup_user(self, username, password, firstname='-', secondname='-', title='-'):
        response = super().signup_user(username, password, firstname, secondname, title)
        return response

    @property
    # ?
    def responce(self):
        return self.r2

    def __str__(self):
        return '{}'.format(self.df_agents)

# pensare di inserire quest classa in Askdata come metodo


class Agent(Insight, Channel, Catalog, Dataset):
    '''
    Agent Object
    '''

    def __init__(self,Askdata,name):

        self.username = Askdata.username
        self.password = Askdata.password
        self.userid = Askdata.userid
        self._domainlogin = Askdata._domainlogin
        self._env = Askdata._env
        self._token = Askdata._token
        self.df_agents = Askdata.df_agents

        self._headers = {
            "Content-Type": "application/json",
            #"Authorization": "Bearer" + " " + self._token
            "Authorization": "Bearer" + " " + self._token
        }

        try:
            agent = self.df_agents[self.df_agents['name'] == name]
            self._agentId = agent.iloc[0]['id']
            self._domain = agent.iloc[0]['domain']
            self._language = agent.iloc[0]['language']

        except Exception as ex:
            raise NameError('Agent name not exsist')

        # vedere come fuziona con **Kwargs
        # super(Insight, self).__init__(self._env, self._token)
        # super(Channel, self).__init__(self._env, self._token, self._agentId, self._domain)
        # super(Catalog, self).__init__(self._env, self._token, empty=True)
        Insight.__init__(self, self._env, self._token)
        Channel.__init__(self, self._env, self._token, self._agentId, self._domain)
        Catalog.__init__(self, self._env, self._token)
        Dataset.__init__(self, self._env, self._token)

    def __str__(self):
        return '{}'.format(self._agentId)

    def SwitchAgent(self):

        data = {
            "agent_id": self._agentId
        }

        if self._env == 'dev':
            self._base_url = url_list['BASE_URL_FEED_DEV']
        if self._env == 'qa':
            self._base_url = url_list['BASE_URL_FEED_QA']
        if self._env == 'prod':
            self._base_url = url_list['BASE_URL_FEED_PROD']

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))
        authentication_url = self._base_url + '/' + self._domain + '/agent/switch'
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()

        return r

    def ask(self, text, payload=''):

        data = {
            "text": text,
            "payload": payload
        }

        if self._env == 'dev':
            request_agent_url = url_list['BASE_URL_FEED_DEV'] + '/' + self._domain + '/agent/' + self._agentId + '/'
        if self._env == 'qa':
            request_agent_url = url_list['BASE_URL_FEED_QA'] + '/' + self._domain + '/agent/' + self._agentId + '/'
        if self._env == 'prod':
            request_agent_url = url_list['BASE_URL_FEED_PROD'] + '/' + self._domain + '/agent/' + self._agentId + '/'

        response = requests.post(url=request_agent_url, headers=self._headers, json=data)
        response.raise_for_status()
        r = response.json()
        # dataframe creation
        df = pd.DataFrame(np.array(r[0]['attachment']['body'][0]['details']['rows']), columns=r[0]['attachment']['body'][0]['details']['columns'])

        return df




