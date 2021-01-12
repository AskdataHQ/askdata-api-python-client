import requests
import yaml
import os
import pandas as pd
import numpy as np

root_dir = os.path.abspath(os.path.dirname(__file__))
# retrieving base url
yaml_path = os.path.join(root_dir, '../askdata_api_python_client/askdata_config/base_url.yaml')
with open(yaml_path, 'r') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    url_list = yaml.load(file, Loader=yaml.FullLoader)


class Askdata:
    '''
    Authentication Object
    '''
    def __init__(self, username, password, domain='askdata', env='prod'):
        self.username = username
        self.password = password
        self.domain = domain
        self.env = env

        data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password
        }
        # "domain": self.domain
        headers = {
            "Authorization": "Basic c21hcnRhZ2VudDpzbWFydGFnZW50",
            "Content-Type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache,no-cache"
        }
        if self.env == 'dev':
            authentication_url = url_list['BASE_URL_AUTH_DEV'] + '/domain/' + self.domain.lower() + '/oauth/token'
        if self.env == 'qa':
            # authentication_url = url_list['BASE_URL_AUTH_QA']  + '/oauth/access_token'
            authentication_url = url_list['BASE_URL_AUTH_QA'] + '/domain/' + self.domain.lower() + '/oauth/token'
        if self.env == 'prod':
            authentication_url = url_list['BASE_URL_AUTH_PROD'] + '/domain/' + self.domain.lower() + '/oauth/token'

        r1 = requests.post(url=authentication_url, data=data, headers=headers)
        r1.raise_for_status()
        self.token = r1.json()['access_token']
        self.r1 = r1
        #print('Status:' + str(r.status_code))

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }

        if self.env == 'dev':
            response = requests.get(url=url_list['BASE_URL_AGENT_DEV'], headers=headers)
            response.raise_for_status()
        if self.env == 'qa':
            response = requests.get(url=url_list['BASE_URL_AGENT_QA'], headers=headers)
            response.raise_for_status()
        if self.env == 'prod':
            response = requests.get(url=url_list['BASE_URL_AGENT_PROD'], headers=headers)
            response.raise_for_status()

        self.r2 = response.json()
        self.df_agents = pd.DataFrame(response.json())

    @property
    # moved the logic for returning area to a separate method
    def responce(self):
        return self.r2

    def __str__(self):
        return '{}'.format(self.df_agents)

# pensare di inserire quest classa in Askdata come metodo
class Agent(Askdata):

    '''
    Agent Object
    '''

    def __init__(self,askdata,name):
        self.username = askdata.username
        self.password = askdata.password
        self.domain = askdata.domain
        self.env = askdata.env
        self.token = askdata.token
        self.df_agents = askdata.df_agents

        try:
            agent = self.df_agents[self.df_agents['name'] == name]
            self.agentId = agent.iloc[0]['id']
            self.workspaceId = agent.iloc[0]['domain']
            self.language = agent.iloc[0]['language']

        except Exception as ex:
            raise NameError('Agent name not exsist')

    def __str__(self):
        return '{}'.format(self.agentId)




class Insight(Agent):

    '''
    Insight Object
    '''

    def __init__(self, agent):
        self.token = agent.token
        self.env = agent.env
        self.agentId = agent.agentId

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }

        if self.env == 'dev':
            self.base_url = url_list['BASE_URL_INSIGHT_DEV']
        if self.env == 'qa':
            self.base_url = url_list['BASE_URL_INSIGHT_QA']
        if self.env == 'prod':
            self.base_url = url_list['BASE_URL_INSIGHT_PROD']

    def GetRules(self):

        insight_url = self.base_url + '/' + 'rules' + '/' + '?agentId=' + self.agentId
        response = requests.get(url=insight_url, headers=self.headers)
        response.raise_for_status()
        r = response.json()
        df_rules = pd.DataFrame([dict(zip(['id', 'name', 'type', 'code', 'domain'], [d['id'], d['name'], d['type'], d['code'], d['domain']])) for d in r['data']])

        return df_rules

    def ExecuteRule(self, id_insight):

        insight_url = self.base_url + '/' + 'rules' + '/' + id_insight + '/produceAndSend'
        r = requests.post(url=insight_url, headers=self.headers)
        r.raise_for_status()

        return r

    def ExecuteRules(self, listid_insight):

        data = listid_insight

        insight_url = self.base_url + '/' + 'insight' + '/produceAndSendAsync'
        r = requests.post(url=insight_url, headers=self.headers, json=data)

        r.raise_for_status()
        #print('Success! Request is accepted, status: ' + str(r.status_code))
        return r


class Dataset(Agent):

    '''
    Dataset Object
    '''

    def __init__(self, agent):
        self.token = agent.token
        self.env = agent.env
        self.agentId = agent.agentId

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


class AskAgent(Agent):
    def __init__(self, agent):
        self.token = agent.token
        self.env = agent.env
        self.agentId = agent.agentId
        self.workspaceId = agent.workspaceId

    def RequestAgent(self, text, payload=''):

        data = {
            "text": text,
            "payload": payload
        }

        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }

        if self.env == 'dev':
            request_agent_url = url_list['BASE_URL_FEED_DEV'] + '/' + self.workspaceId + '/agent/' + self.agentId + '/'
        if self.env == 'qa':
            request_agent_url = url_list['BASE_URL_FEED_QA'] + '/' + self.workspaceId + '/agent/' + self.agentId + '/'
        if self.env == 'prod':
            request_agent_url = url_list['BASE_URL_FEED_PROD'] + '/' + self.workspaceId + '/agent/' + self.agentId + '/'

        response = requests.post(url=request_agent_url, headers=headers, json=data)
        response.raise_for_status()
        r = response.json()
        # dataframe creation
        df = pd.DataFrame(np.array(r[0]['attachment']['body'][0]['details']['rows']), columns=r[0]['attachment']['body'][0]['details']['columns'])

        return df

