import requests
import yaml
import os
root_dir = os.path.abspath(os.path.dirname(__file__))

# retrieving base url
yaml_path = os.path.join(root_dir, '../askdata_api_python_client/config/base_url.yaml')
with open(yaml_path, 'r') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    url_list = yaml.load(file)


class Askdata:

    # def BASE_URL_INSIGHT =

    def __init__(self, username, password, domain='Askdata', env='qa'):
        self.username = username
        self.password = password
        self.domain = domain
        self.env = env


class Agent(Askdata):

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

        agent_list = [tuple([d['name'], d['code'], d['id']]) for d in r['result']]
        return agent_list

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

        id_agent = [d['id'] for d in r['result'] if d['code'] == _code]

        return id_agent



class Insight():
    
    def ExecuteInsights(self, _id):
        
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }
        if self.env == 'dev':
            insight_url = url_list['BASE_URL_INSIGHT_DEV'] + self.domain + '/' + _type + '/' + _code +'/produce'
        if self.env == 'qa':
            insight_url = url_list['BASE_URL_INSIGHT_QA'] + self.domain + '/' + _type + '/' + _code +'/produce'
        if self.env == 'prod':
            insight_url = url_list['BASE_URL_INSIGHT_PROD'] + self.domain + '/' + _type + '/' + _code + '/produce'

        r = requests.get(url=insight_url, headers=headers).json()

        
        #https://smartinsightsv2-qa.askdata.com/insight/GROUPAMA_QA/MONTHLY_DM/REQ_D20_3_LIST_SP/produce
     
    
#    def getAgentInsights():
       # ... list of Insights
        
#class Dataset():        
#    def ExecuteDatasetSync(self, dataset_id):
#         headers = {
#        "Content-Type": "application/json",
#        "Authorization": "Bearer" + " " + self.token
#        }
            
        #url = sync_url + '/datasets/' + dataset_id + '/sync'
        #requests.post(url=url, headers=headers)
        
        



