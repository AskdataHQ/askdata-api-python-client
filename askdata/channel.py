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


class Channel:

    def __init__(self, env, token, _agentId, _domain):

        data = {
            "agent_id": _agentId
        }

        self._headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + token
        }

        if env == 'dev':
            self._base_url_ch = url_list['BASE_URL_FEED_DEV']
        if env == 'qa':
            self._base_url_ch = url_list['BASE_URL_FEED_QA']
        if env == 'prod':
            self._base_url_ch = url_list['BASE_URL_FEED_PROD']

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))
        authentication_url = self._base_url_ch + '/' + self._domain + '/agent/switch'
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()
        self.r = r

    def load_channels(self):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_ch + '/channels/'+'?agentId='+self._agentId+'&page=0&limit=100000'
        r = s.get(url=authentication_url, headers=self._headers)
        r.raise_for_status()
        df_channels = pd.DataFrame(r.json())

        return df_channels

    def create_channel(self, name, icon='https://s3.eu-central-1.amazonaws.com/innaas.smartfeed/icons/groupama/icone/channel/icon_channel_dm.png',
                       visibility='PRIVATE'):

        data = {
            "name": name,
            "icon": icon,
            "agentId": self._agentId,
            "visibility": visibility
        }

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_ch + '/channels/'
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()
        return r.json()['id']

    def update_channel(self, channel_id, visibility,
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
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_ch + '/channels/' + channel_id
        r = s.put(url=authentication_url, headers=self._headers,  json=data)
        r.raise_for_status()
        return r

    def delete_channel(self, channel_id):
        authentication_url = self._base_url_ch + '/channels/' + channel_id
        r = requests.delete(url=authentication_url, headers=self._headers)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            if r.status_code != 500:
                raise ex
            logging.info(ex)
            logging.info('Channel already deleted or not exist')

        return r

    def load_users_fromch(self, channel_id):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_ch + '/channels/' + channel_id + '/users'
        r = s.get(url=authentication_url, headers=self._headers)

        r.raise_for_status()
        df_users = pd.DataFrame(r.json())
        return df_users

    def add_user_toch(self, channel_id, user_id, role="follower", mute="none"):

        data = {
            "userId": user_id,
            "role": role,
            "mute": mute
        }

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_ch + '/channels/' + channel_id + '/users'
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()
        return r

    def delete_user_fromch(self, channel_id, user_id):
        authentication_url = self._base_url_ch + '/channels/' + channel_id + '/users/' + user_id
        r = requests.delete(url=authentication_url, headers=self._headers)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            # usare libreria di logging
            if r.status_code != 409:
                raise ex
            logging.info(ex)
            logging.info('User already deleted or not exist')
        return r

    def un_mute_channel(self, channel_id):

        authentication_url = self._base_url_ch + '/channels/' + channel_id + '/unmute'
        r = requests.put(url=authentication_url, headers=self._headers)
        r.raise_for_status()
        return r

    def mute_channel(self, channel_id):

        data = {

            "period": "DAYS_7"
        }
        authentication_url = self._base_url_ch + '/channels/' + channel_id + '/mute'
        r = requests.put(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()
        return r
