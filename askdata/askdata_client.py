import requests
import yaml
import os
import pandas as pd
import numpy as np
import logging
import getpass
from askdata.insight import Insight
from askdata.channel import Channel
from askdata.catalog import Catalog
from askdata.dataset import Dataset
from askdata.insight_definition import Insight_Definition
from askdata.security import SignUp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from askdata.askdata_client import Askdata
import re
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



class Agent(Insight, Channel, Catalog, Dataset):
    '''
    Agent Object
    '''

    def __init__(self, askdata: 'Askdata', slug='', agent_name='', agent_id=''):

        self.username = askdata.username
        self.userid = askdata.userid
        self._domainlogin = askdata._domainlogin
        self._env = askdata._env
        self._token = askdata._token
        self.df_agents = askdata.agents_dataframe()

        self._headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }

        if self._env == 'dev':
            self.smart_insight_url = url_list['BASE_URL_INSIGHT_DEV']
        if self._env == 'qa':
            self.smart_insight_url = url_list['BASE_URL_INSIGHT_QA']
        if self._env == 'prod':
            self.smart_insight_url = url_list['BASE_URL_INSIGHT_PROD']

        try:
            if slug != '':
                agent = self.df_agents[self.df_agents['slug'] == slug.lower()]
            elif agent_id != '':
                agent = self.df_agents[self.df_agents['id'] == agent_id]
            else:
                agent = self.df_agents[self.df_agents['name'] == agent_name]

            self._agentId = agent.iloc[0]['id']
            self._domain = agent.iloc[0]['domain']
            self._language = agent.iloc[0]['language']
            self._agent_name = agent.iloc[0]['name']

        except Exception as ex:
            raise NameError('Agent slug/name/id not exsist or not insert')

        Insight.__init__(self, self._env, self._token)
        Channel.__init__(self, self._env, self._token, self._agentId, self._domain)
        Catalog.__init__(self, self._env, self._token)
        Dataset.__init__(self, self._env, self._token)

    def __str__(self):
        return '{}'.format(self._agentId)

    def switch_agent(self):

        data = {
            "agent_id": self._agentId
        }

        if self._env == 'dev':
            self._base_url = url_list['BASE_URL_FEED_DEV']
        if self._env == 'qa':
            self._base_url = url_list['BASE_URL_FEED_QA']
        if self._env == 'prod':
            self._base_url = url_list['BASE_URL_FEED_PROD']


        if self._env == 'dev':
            self._base_url_ch = url_list['BASE_URL_FEED_DEV']
        if self._env == 'qa':
            self._base_url_ch = url_list['BASE_URL_FEED_QA']
        if self._env == 'prod':
            self._base_url_ch = url_list['BASE_URL_FEED_PROD']

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
        #dataframe creation
        df = pd.DataFrame(np.array(r[0]['attachment']['body'][0]['details']['rows']), columns=r[0]['attachment']['body'][0]['details']['columns'])

        return df

    def ask_as_json(self, text, payload=''):

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

        return r

    def dataset(self, slug):
        """
        set in the agent object the properties of specific dataset

        :param slug: str, identification of the dataset
        :return: None
        """
        self._get_info_dataset_by_slug(slug)
        return self

    def update_dataset_name(self, dataset_slug, dataset_name):

        body = {"name": dataset_name}

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartdataset/datasets/'+dataset_slug+'/sdk'
        logging.info("AUTH URL {}".format(authentication_url))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        response = s.put(url=authentication_url, json=body, headers=headers)
        response.raise_for_status()

    def create_parquet_dataset(self, agent_slug, dataset_slug, file_path):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartbot/agents/'+agent_slug+'/datasets/'+dataset_slug+'/parquet'
        logging.info("AUTH URL {}".format(authentication_url))
        file = {'file': open(file_path, 'rb')}
        headers = {"Authorization": "Bearer" + " " + self._token}
        response = s.post(url=authentication_url, files=file, headers=headers)
        response.raise_for_status()
        r = response.json()


    def update_parquet_dataset(self, agent_slug, dataset_id, file_path, strategy):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartbot/agents/'+agent_slug+'/datasets/'+dataset_id+'/parquet?strategy='+strategy
        logging.info("AUTH URL {}".format(authentication_url))
        file = {'file': open(file_path, 'rb')}
        headers = {"Authorization": "Bearer" + " " + self._token}
        response = s.put(url=authentication_url, files=file, headers=headers)
        response.raise_for_status()
        r = response.json()


    def load_dataset(self, datasetSlug):

        dataset = self.get_dataset_by_slug(self._agentId, datasetSlug)

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartdataset/datasets/' + dataset["id"] +'/grid/data'
        logging.info("AUTH URL {}".format(authentication_url))

        headers = {"Authorization": "Bearer" + " " + self._token}
        response = s.get(url=authentication_url, headers=headers)
        response.raise_for_status()
        r = response.json()
        return pd.DataFrame(r['payload']['data'])


    def get_dataset_by_slug(self, agent_id, slug:str) -> Dataset:
        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartdataset/datasets/slug/'+ agent_id+'/'+ slug
        logging.info("AUTH URL {}".format(authentication_url))

        headers = {"Authorization": "Bearer" + " " + self._token}
        response = s.get(url=authentication_url, headers=headers)
        response.raise_for_status()
        r = response.json()
        if(r["found"]):
            return r["dataset"]
        else:
            return None


    def create_dataset(self, dataframe:pd.DataFrame, dataset_name:str, slug: str, settings: dict = None):

        body = {"label": dataset_name, "rows": dataframe.to_dict(orient="record")}

        settings_list = []
        if(settings != None):
            for key in settings.keys():
                settings[key]["column_name"] = key
                settings_list.append(settings[key])
            body["settings"] = settings_list


        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        url = self._base_url_askdata + '/smartbot/agents/'+self._agentId+'/datasets/'+slug+'/sdk'
        logging.info("AUTH URL {}".format(url))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        response = s.post(url=url, json=body, headers=headers)
        response.raise_for_status()

    def update_dataset(self, dataframe: pd.DataFrame, dataset_name: str, slug: str, settings: dict = None):

        body = {"label": dataset_name, "rows": dataframe.to_dict(orient="record")}

        settings_list = []
        if (settings != None):
            for key in settings.keys():
                settings[key]["column_name"] = key
                settings_list.append(settings[key])
            body["settings"] = settings_list

        s = requests.Session()

        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        url = self._base_url_askdata + '/smartbot/agents/' + self._agentId + '/datasets/' + slug + '/sdk'
        logging.info("AUTH URL {}".format(url))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        print(body)
        response = s.put(url=url, json=body, headers=headers)

        response.raise_for_status()

    '''La prima chiave è la stringa colonna del dataframe. Per ogni chiave ho dei setting:
    Name stringa
    Type(dimension o measure)
    IsDate booleano
    Synonyms: [] array di stringhe
    Date(pattern della data)
    Format
    DefaultAggreation(sum, avg..)

    {
        "column_name": {
            "Name": "column_name",
            "Type": "Dimension",
            "IsDate": False,
            "Synonyms": [],
            "Date",
            "Format": "",
            "DefaultAggreation": "sum"
        }
    }
'''


    def create_or_replace_dataset(self, dataframe:pd.DataFrame, dataset_name:str, slug:str, settings:dict = None):
        #Check if dataset exists
        dataset = self.get_dataset_by_slug(self._agentId, slug)

        if(dataset != None):
            #If it exists update it
            self.update_dataset(dataframe, dataset_name, slug, settings)
        else:
            # If not exists create a new one
            self.create_dataset(dataframe, dataset_name, slug, settings)



    def delete_dataset(self, slug='', dataset_id=''):

        if slug != '':
            self._get_info_dataset_by_slug(slug)
            self._delete_dataset(self._dataset_id)
            logging.info("---- dataset '{}' deleted ----- ".format(slug))
        elif dataset_id != '' and slug == '':
            self._delete_dataset(dataset_id)
            logging.info("---- dataset '{}' deleted ----- ".format(dataset_id))
        else:
            raise Exception('takes 2 positional arguments "slug, datset_id" but 0 were given')

    def create_datacard(self, channel: str, title: str,  search:str = "", slug: str = None):

        channel = self.get_channel(self._agentId, channel)
        if channel!=None:
            channel_id = channel["id"]
        else:
            channel_id = self.create_channel(channel)

        body = {
            "agentId": self._agentId,
            "channelId": channel_id,
            "name": title,
            "slug": slug
        }

        logging.info("Channel id {}".format(channel_id))

        if self._env == 'dev':
            smart_insight_url = url_list['BASE_URL_INSIGHT_DEV']
        if self._env == 'qa':
            smart_insight_url = url_list['BASE_URL_INSIGHT_QA']
        if self._env == 'prod':
            smart_insight_url = url_list['BASE_URL_INSIGHT_PROD']

        url = smart_insight_url+'/definitions'

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        logging.info("AUTH URL {}".format(url))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self._token
        }
        response = s.post(url=url, json=body, headers=headers)
        response.raise_for_status()
        definition = response.json()

        if(search!=""):
            body_query = {"nl": search, "language": "en"}

            s = requests.Session()
            s.keep_alive = False
            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
            s.mount('https://', HTTPAdapter(max_retries=retries))

            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer" + " " + self._token
            }
            query_url = smart_insight_url+'/definitions/'+definition["id"]+'/nl_queries/'+definition["components"][0]["id"]+'/nl'
            logging.info("QUERY URL {}".format(query_url))
            r = s.put(url=query_url, json=body_query, headers=headers)

        return Insight_Definition(self._env, self._token, definition)


    def get_datacard(self, slug):

        url = self.smart_insight_url+"definitions/agent/"+self._agentId+"/slug/"+slug

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        r = s.get(url=url, headers=self._headers)
        r.raise_for_status()

        definition = r.json()
        return Insight_Definition(self._env, self._token, definition)


    def create_channel(self, name, icon='https://storage.googleapis.com/askdata/smartfeed/icons/Channel@2x.png',
                       visibility='PRIVATE'):

        data = {
            "userId" : self.userid,
            "name": name,
            "icon": icon,
            "agentId": self._agentId,
            "visibility": visibility,
            "code": name,
            "autofollow": True
        }

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_ch + '/channels'
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()
        return r.json()['id']


    def get_channel(self, agent_id, channel_code):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        url = self._base_url_ch + '/channels?agentId='+agent_id+'&slug='+channel_code
        r = s.get(url=url, headers=self._headers)
        r.raise_for_status()
        if(r != None and r.json() != []):
            return r.json()[0]
        else:
            return None


    def get_dataset_slug_from_id(self, dataset_id:str)->str:
        """
        get dataset slug by the dataset id instantiated with slug
        :param dataset_id: str
        :return: slug: str
        """

        list_dataset = self.list_datasets()

        if list_dataset[list_dataset['id'] == dataset_id].empty:
            raise Exception('The dataset with id: {} not exist'.format(dataset_id))
        else:
            slug = list_dataset[list_dataset['id'] == dataset_id].loc[:, 'slug'].item()

        return slug


class Askdata(SignUp):
    '''
    Authentication Object
    '''

    def __init__(self, username='', password='', domainlogin='askdata', env='prod', token=''):

        with requests.Session() as s:

            self._token = token
            self._domainlogin = domainlogin.upper()
            self._env = env

            if self._env == 'dev':
                self.base_url_security = url_list['BASE_URL_SECURITY_DEV']

            if self._env == 'qa':
                self.base_url_security = url_list['BASE_URL_SECURITY_QA']

            if self._env == 'prod':
                self.base_url_security = url_list['BASE_URL_SECURITY_PROD']

            if token == '':

                if username == '':
                    # add control email like
                    username = input('Askdata Username: ')
                if password == '':
                    password = getpass.getpass(prompt='Askdata Password: ')

                self.username = username

                data = {
                    "grant_type": "password",
                    "username": self.username,
                    "password": password
                }

                headers = {
                    "Authorization": "Basic YXNrZGF0YS1zZGs6YXNrZGF0YS1zZGs=",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "cache-control": "no-cache,no-cache"
                }

                authentication_url = self.base_url_security + '/domain/' + self._domainlogin.lower() + '/oauth/token'

                # request token for the user
                r1 = s.post(url=authentication_url, data=data, headers=headers)
                r1.raise_for_status()
                self._token = r1.json()['access_token']
                self.r1 = r1

            authentication_url_userid = self.base_url_security + '/me'
            self._headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer" + " " + self._token
            }

            # request userId of the user
            r_userid = s.get(url=authentication_url_userid, headers=self._headers)
            r_userid.raise_for_status()
            self.userid = r_userid.json()['id']
            self.username = r_userid.json()['userName']

    def agent(self, slug="", agent_id="") -> 'Agent':
        #Agent.__init__(self, self, slug=slug)
        return Agent(self, slug=slug, agent_id=agent_id)

    def load_agents(self):

        if self._env == 'dev':
            authentication_url = url_list['BASE_URL_AGENT_DEV']

        if self._env == 'qa':
            authentication_url = url_list['BASE_URL_AGENT_QA']

        if self._env == 'prod':
            authentication_url = url_list['BASE_URL_AGENT_PROD']

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        # request of all agents of the user/token
        response = s.get(url=authentication_url, headers=self._headers)
        response.raise_for_status()

        return response.json()

    def agents_dataframe(self):
        return pd.DataFrame(self.load_agents())

    def signup_user(self, username, password, firstname='-', secondname='-', title='-'):
        response = super().signup_user(username, password, firstname, secondname, title)
        return response

    @property
    # ?
    def responce(self):
        return self.r2

    def create_agent(self, agent_name):

        data = {
            "name": agent_name,
            "language": "en"
        }

        if self._env == 'dev':
            self._base_url = url_list['BASE_URL_ASKDATA_DEV']
        if self._env == 'qa':
            self._base_url = url_list['BASE_URL_ASKDATA_QA']
        if self._env == 'prod':
            self._base_url = url_list['BASE_URL_ASKDATA_PROD']

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))
        authentication_url = self._base_url + '/smartbot/agents'
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()

        return r
