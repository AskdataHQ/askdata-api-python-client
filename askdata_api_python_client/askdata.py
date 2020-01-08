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
            r = requests.post(url=authentication_url, data=data, headers=headers)
        if self.env == 'qa':
            # authentication_url = url_list['BASE_URL_AUTH_QA']  + '/oauth/access_token'
            authentication_url = url_list['BASE_URL_AUTH_QA'] + '/domain/' + self.domain.lower() + '/oauth/token'
            r = requests.post(url=authentication_url, data=data, headers=headers)
        if self.env == 'prod':
            authentication_url = url_list['BASE_URL_AUTH_PROD'] + '/domain/' + self.domain.lower() + '/oauth/token'
            r = requests.post(url=authentication_url, data=data, headers=headers)

        r.raise_for_status()
        self.token = r.json()['access_token']
        self.r = r
        #print('Status:' + str(r.status_code))

    def GetAgents(self):

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

        r = response.json()
        #self.dictAgents = pd.DataFrame([dict(zip(['name', 'code', 'id','domain'], [d['name'], d['code'], d['id'],d['domain']])) for d in r['result']])
        self.dictAgents = pd.DataFrame(
            [dict(zip(['name', 'id', 'domain'], [d['name'], d['id'], d['domain']])) for d in
             r['result']])

        return self.dictAgents


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
        # data = {
        #     "grant_type": "password",
        #     "username": self.username,
        #     "password": self.password
        # }
        # # "domain": self.domain
        # headers = {
        #     "Authorization" : "Basic c21hcnRhZ2VudDpzbWFydGFnZW50",
        #     "Content-Type": "application/x-www-form-urlencoded",
        #     "cache-control": "no-cache,no-cache"
        # }
        # if self.env == 'dev':
        #     authentication_url = url_list['BASE_URL_AUTH_DEV'] + '/domain/' + self.domain.lower() + '/oauth/token'
        #     r = requests.post(url=authentication_url, data=data, headers=headers)
        # if self.env == 'qa':
        #     #authentication_url = url_list['BASE_URL_AUTH_QA']  + '/oauth/access_token'
        #     authentication_url = url_list['BASE_URL_AUTH_QA'] + '/domain/' + self.domain.lower() + '/oauth/token'
        #     r = requests.post(url=authentication_url, data=data, headers=headers)
        # if self.env == 'prod':
        #     authentication_url = url_list['BASE_URL_AUTH_PROD'] + '/domain/' + self.domain.lower() + '/oauth/token'
        #     r = requests.post(url=authentication_url, data=data, headers=headers)
        #
        # r.raise_for_status()
        # self.token = r.json()['access_token']


    #def GetAgent(self, ):

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

        r = response.json()
        #print('Status:' + str(response.status_code))
        try:
            self.agentId = [d['id'] for d in r['result'] if d['name'] == name][0]
            self.workspaceId = [d['domain'] for d in r['result'] if d['name'] == name][0]
            self.language = [d['language'] for d in r['result'] if d['name'] == name][0]
            self.r = response
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

        response = requests.get(url=insight_url, headers=headers)
        response.raise_for_status()
        r = response.json()
        dfRules = pd.DataFrame([dict(zip(['id', 'name', 'type', 'code', 'domain'], [d['id'], d['name'], d['type'], d['code'], d['domain']])) for d in r['data']])

        return dfRules

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

        r.raise_for_status()
        #print('Success! Status:'+str(r.status_code))
        return r

    def ExecuteRules(self, listId):
        data = listId
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }
        if self.env == 'dev':
            insight_url = url_list['BASE_URL_INSIGHT_DEV'] + '/' + 'insight' + '/produceAndSendAsync'
        if self.env == 'qa':
            insight_url = url_list['BASE_URL_INSIGHT_QA'] + '/' + 'insight' + '/produceAndSendAsync'
        if self.env == 'prod':
            insight_url = url_list['BASE_URL_INSIGHT_PROD'] + '/' + 'insight' + '/produceAndSendAsync'

        r = requests.post(url=insight_url, headers=headers, json=data)

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

        response = requests.get(url=dataset_url, headers=headers)
        response.raise_for_status()
        r = response.json()
        df_datasets = pd.DataFrame([
            dict(zip(['label', 'type', 'code'], [d['label'], d['type'], d['code']]))
            for d in r['payload']['data']])

        return df_datasets

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

        response = requests.post(url=dataset_url, headers=headers)
        response.raise_for_status()
        r = response.json()
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

