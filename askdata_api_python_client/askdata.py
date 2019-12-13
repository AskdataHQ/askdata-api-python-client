import requests
import yaml
import os

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

        dictAgent = [dict(zip(['name', 'code', 'id','domain'], [d['name'], d['code'], d['id'],d['domain']])) for d in r['result']]

        return dictAgent

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
            insight_url = url_list['BASE_URL_INSIGHT_DEV'] + '?agentId=' + self.agentId +'&page=0&limit=5'
        if self.env == 'qa':
            insight_url = url_list['BASE_URL_INSIGHT_QA'] + '?agentId=' + self.agentId + '&page=0&limit=5'
        if self.env == 'prod':
            insight_url = url_list['BASE_URL_INSIGHT_PROD'] + '?agentId=' + self.agentId +'&page=0&limit=5'

        r = requests.get(url=insight_url, headers=headers).json()
        dictRules = [dict(zip(['id', 'name', 'type', 'code', 'domain'], [d['id'], d['name'], d['type'], d['code'], d['domain']])) for d in r['data']]

        return dictRules

    def ExecuteRule(self, id):
        
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }
        if self.env == 'dev':
            insight_url = url_list['BASE_URL_INSIGHT_DEV'] + '/' + id + '/produceAndSend'
        if self.env == 'qa':
            insight_url = url_list['BASE_URL_INSIGHT_QA'] + '/' + id + '/produceAndSend'
        if self.env == 'prod':
            insight_url = url_list['BASE_URL_INSIGHT_PROD'] + '/' + id + '/produceAndSend'

        r = requests.post(url=insight_url, headers=headers)

        if r.status_code == 202:
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


    def ExecuteDatasetSync(self, dataset_id):
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }
            
        url = sync_url + '/datasets/' + dataset_id + '/sync'
        requests.post(url=url, headers=headers)
        




