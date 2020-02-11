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


class Channel:

    def __init__(self, Agent):
        self.agentId = Agent.agentId
        self.workspaceId = Agent.workspaceId
        self.username = Agent.username
        self.language = Agent.language
        self.token = Agent.token
        self.env = Agent.env

        data = {
            "agent_id": self.agentId
        }

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

        authentication_url = self.base_url + '/' + self.workspaceId + '/agent/switch'
        r = requests.post(url=authentication_url, headers=self.headers, json=data)
        r.raise_for_status()
        self.r = r

    def GetChannels(self):

        authentication_url = self.base_url + '/channels/'+'?agentId='+self.agentId+'&page=0&limit=100000'
        r = requests.get(url=authentication_url, headers=self.headers)
        r.raise_for_status()
        df_channels = pd.DataFrame(r.json())

        return df_channels

    def CreateChannel(self, name, icon='https://s3.eu-central-1.amazonaws.com/innaas.smartfeed/icons/groupama/icone/channel/icon_channel_dm.png',
                      visibility='PRIVATE'):

        data = {
            "name": name,
            "icon": icon,
            "agentId": self.agentId,
            "visibility": visibility
        }

        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self.base_url + '/channels/'
        r = s.post(url=authentication_url, headers=self.headers, json=data)
        r.raise_for_status()
        return r

    def UpdateChannel(self,channel_id,visibility,
                      icon='https://s3.eu-central-1.amazonaws.com/innaas.smartfeed/icons/groupama/icone/channel/icon_channel_dm.png',
                      iconFlag = False):
        #visibility is PUBLIC or PRIVATE
        if iconFlag:
            data = {
                "icon": icon,
                "visibility": visibility
            }
        else:
            data = {
                "visibility": visibility
            }

        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self.base_url + '/channels/' + channel_id
        r = s.put(url=authentication_url, headers=self.headers,  json=data)
        r.raise_for_status()
        return r

    def DeleteChannel(self, channel_id):
        authentication_url = self.base_url + '/channels/' + channel_id
        r = requests.delete(url=authentication_url, headers=self.headers)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            if r.status_code != 500:
                raise ex
            logging.info(ex)
            logging.info('Channel already deleted or not exist')

        return r

    def GetUsers(self, channel_id):

        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self.base_url + '/channels/' + channel_id + '/users'
        r = s.get(url=authentication_url, headers=self.headers)

        r.raise_for_status()
        df_users = pd.DataFrame(r.json())
        return df_users

    def AddUser(self, channel_id, user_id, role="follower", mute="none"):

        data = {
            "userId": user_id,
            "role": role,
            "mute": mute
        }

        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self.base_url + '/channels/' + channel_id + '/users'
        r = s.post(url=authentication_url, headers=self.headers, json=data)
        r.raise_for_status()
        return r

    def DeleteUser(self, channel_id, user_id):
        authentication_url = self.base_url + '/channels/' + channel_id + '/users/' + user_id
        r = requests.delete(url=authentication_url, headers=self.headers)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            # usare libreria di logging
            if r.status_code != 409:
                raise ex
            logging.info(ex)
            logging.info('User already deleted or not exist')
        return r

    def UnMuteChannel(self, channel_id):

        authentication_url = self.base_url + '/channels/' + channel_id + '/unmute'
        r = requests.put(url=authentication_url, headers=self.headers)
        r.raise_for_status()
        return r

    def MuteChannel(self, channel_id):

        data = {

            "period": "DAYS_7"
        }
        authentication_url = self.base_url + '/channels/' + channel_id + '/mute'
        r = requests.put(url=authentication_url, headers=self.headers, json=data)
        r.raise_for_status()
        return r
