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


# inglobata in Askdata come metodo
class SignUp:

    def __init__(self, askdata):

        self.username = askdata.username
        self._token = askdata._token
        self._env = askdata._env
        self._domainlogin = askdata._domainlogin.upper()

        self._headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }

        if self.env == 'dev':
            self.base_url_security = url_list['BASE_URL_SECURITY_DEV']
        if self.env == 'qa':
            self.base_url_security = url_list['BASE_URL_SECURITY_QA']
        if self.env == 'prod':
            self.base_url_security = url_list['BASE_URL_SECURITY_PROD']


    def signup_user(self, username, password, firstname='-', secondname='-', title='-'):

        data = {
            "username": username,
            "password": password,
            "firstName": firstname,
            "lastName": secondname,
            "mobile": "-",
            "title": title,
            "email": username
        }

        s = requests.Session()
        s.keep_alive = False
        #s.config['keep_alive'] = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))
        authentication_url = self.base_url_security + '/domain/' + self._domainlogin + '/usersignup'
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        logging.info("--- ------------ ----")
        logging.info("---- add user ---> {}  --- PENDING".format(username))

        return r.json()