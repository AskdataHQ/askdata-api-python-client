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
    def __init__(self, username, password, domain='Askdata', env='qa'):
        self.username = username
        self.password = password
        self.domain = domain
        self.env = env


class Agent(Askdata):

    '''
    Agent Object
    '''

    def __init__(self, askdata):
        self.username = askdata.username
        self.password = askdata.password
        self.domain = askdata.domain
        self.env = askdata.env

    def Login(self):
        data = {
            "username": self.username,
            "password": self.password,
            "domain": self.domain
        }

        headers = {
            "Content-Type": "application/json",
            "cache-control": "no-cache"
        }
        if self.env == 'dev':
            authentication_url = url_list['BASE_URL_AUTH_DEV'] + '/oauth/access_token'
            r = requests.post(url=authentication_url, json=data, headers=headers).json()
        if self.env == 'qa':
            authentication_url = url_list['BASE_URL_AUTH_QA']  + '/oauth/access_token'
            r = requests.post(url=authentication_url, json=data, headers=headers).json()
        if self.env == 'prod':
            authentication_url = url_list['BASE_URL_AUTH_PROD']  + '/oauth/access_token'
            r = requests.post(url=authentication_url, json=data, headers=headers).json()

        self.token = r['access_token']

    def GetAgents(self):

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }

        if self.env == 'dev':
            r = requests.get(url=url_list['BASE_URL_AGENT_DEV'], headers=headers).json()
        if self.env == 'qa':
            r = requests.get(url=url_list['BASE_URL_AGENT_QA'], headers=headers).json()
        if self.env == 'prod':
            r = requests.get(url=url_list['BASE_URL_AGENT_PROD'], headers=headers).json()

        self.dictAgents = pd.DataFrame([dict(zip(['name', 'code', 'id','domain'], [d['name'], d['code'], d['id'],d['domain']])) for d in r['result']])

        return self.dictAgents

    def GetAgent(self, _code):

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }
        if self.env == 'dev':
            r = requests.get(url=url_list['BASE_URL_AGENT_DEV'], headers=headers).json()
        if self.env == 'qa':
            r = requests.get(url=url_list['BASE_URL_AGENT_QA'], headers=headers).json()
        if self.env == 'prod':
            r = requests.get(url=url_list['BASE_URL_AGENT_PROD'], headers=headers).json()

        self.agentId = [d['id'] for d in r['result'] if d['code'] == _code][0]
        self.workspaceId = [d['domain'] for d in r['result'] if d['code'] == _code][0]

        return self.agentId



class Insight(Agent):

    '''
    Insight Object
    '''

    def __init__(self, agent):
        self.token = agent.token
        self.env = agent.env
        self.agentId = agent.agentId

    def GetRules(self):

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }
        
        if self.env == 'dev':
            insight_url = url_list['BASE_URL_INSIGHT_DEV'] + '/' + 'rules' + '/' + '?agentId=' + self.agentId +'&page=0&limit=5'
        if self.env == 'qa':
            insight_url = url_list['BASE_URL_INSIGHT_QA'] + '/' + 'rules' + '/' + '?agentId=' + self.agentId + '&page=0&limit=5'
        if self.env == 'prod':
            insight_url = url_list['BASE_URL_INSIGHT_PROD'] + '/' + 'rules' + '/' + '?agentId=' + self.agentId +'&page=0&limit=5'

        r = requests.get(url=insight_url, headers=headers).json()
        dictRules = [dict(zip(['id', 'name', 'type', 'code', 'domain'], [d['id'], d['name'], d['type'], d['code'], d['domain']])) for d in r['data']]

        return dictRules

    def ExecuteRule(self, id):
        
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }
        if self.env == 'dev':
            insight_url = url_list['BASE_URL_INSIGHT_DEV'] + '/' + 'rules' + '/' + id + '/produceAndSend'
        if self.env == 'qa':
            insight_url = url_list['BASE_URL_INSIGHT_QA'] + '/' + 'rules' + '/' + id + '/produceAndSend'
        if self.env == 'prod':
            insight_url = url_list['BASE_URL_INSIGHT_PROD'] + '/' + 'rules' + '/' + id + '/produceAndSend'

        r = requests.post(url=insight_url, headers=headers)

        if r.status_code == 202:
            print('Success!')
        else:
            print('Not Found.')

    def ExecuteRules(self, listId):
        data = listId
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }
        if self.env == 'dev':
            insight_url = url_list['BASE_URL_INSIGHT_DEV'] + '/' + 'insight' + '/produceAndSend'
        if self.env == 'qa':
            insight_url = url_list['BASE_URL_INSIGHT_QA'] + '/' + 'insight' + '/produceAndSend'
        if self.env == 'prod':
            insight_url = url_list['BASE_URL_INSIGHT_PROD'] + '/' + 'insight' + '/produceAndSend'

        r = requests.post(url=insight_url, headers=headers, json=data)

        if r.status_code == 202 or r.status_code == 200:
            print('Success!')
        else:
            print('Not Found.')


class Dataset(Agent):

    '''
    Dataset Object
    '''

    def __init__(self, agent):
        self.token = agent.token
        self.env = agent.env
        self.agentId = agent.agentId


    def GetDatasets(self):

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }

        if self.env == 'dev':
            dataset_url = url_list['BASE_URL_DATASET_DEV'] + '/' + self.agentId + '/datasets/list'
        if self.env == 'qa':
            dataset_url = url_list['BASE_URL_DATASET_QA'] + '/' + self.agentId + '/datasets/list'
        if self.env == 'prod':
            dataset_url = url_list['BASE_URL_DATASET_PROD'] + '/' + self.agentId + '/datasets/list'

        r = requests.get(url=dataset_url, headers=headers).json()

        dictRules = [
            dict(zip(['label', 'type', 'code'], [d['label'], d['type'], d['code']]))
            for d in r['payload']['data']]

        return dictRules

    def ExecuteDatasetSync(self, dataset_id):

        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }

        if self.env == 'dev':
            dataset_url = url_list['BASE_URL_DATASET_DEV'] + '/' + self.agentId + '/datasets/' + dataset_id + '/sync'
        if self.env == 'qa':
            dataset_url = url_list['BASE_URL_DATASET_QA'] + '/' + self.agentId + '/datasets/' + dataset_id + '/sync'
        if self.env == 'prod':
            dataset_url = url_list['BASE_URL_DATASET_PROD'] + '/' + self.agentId + '/datasets/' + dataset_id + '/sync'

        r = requests.post(url=dataset_url, headers=headers).json()


class AskAgent(Agent):
    def __init__(self, agent):
        self.token = agent.token
        self.env = agent.env
        self.agentId = agent.agentId
        self.workspaceId = agent.workspaceId

    def RequestAgent(self, text, payload = ''):

        data = {
            "text": text,
            "payload": payload
        }

        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }

        if self.env == 'dev':
            request_agent_url = url_list['BASE_URL_AUTH_DEV'] + '/' + self.workspaceId + '/agent/' + self.agentId
        if self.env == 'qa':
            request_agent_url = url_list['BASE_URL_AUTH_QA'] + '/' + self.workspaceId + '/agent/' + self.agentId
        if self.env == 'prod':
            request_agent_url = url_list['BASE_URL_AUTH_PROD'] + '/' + self.workspaceId + '/agent/' + self.agentId

        r = requests.post(url=request_agent_url, headers=headers, json = data).json()
        # dataframe creation
        df = pd.DataFrame(np.array(r[0]['attachment']['body'][0]['details']['rows']), columns = r[0]['attachment']['body'][0]['details']['columns'])

        return df

