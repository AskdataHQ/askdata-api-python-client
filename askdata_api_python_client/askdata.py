import requests
import yaml
import os
import pandas as pd
import numpy as np
import logging
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
            "Authorization": "Basic YXNrZGF0YS1zZGs6YXNrZGF0YS1zZGs=",
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
    # ?
    def responce(self):
        return self.r2

    def __str__(self):
        return '{}'.format(self.df_agents)

# pensare di inserire quest classa in Askdata come metodo
class Agent:

    '''
    Agent Object
    '''

    def __init__(self,Askdata,name):
        self.username = Askdata.username
        self.password = Askdata.password
        self.domain = Askdata.domain
        self.env = Askdata.env
        self.token = Askdata.token
        self.df_agents = Askdata.df_agents

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }

        try:
            agent = self.df_agents[self.df_agents['name'] == name]
            self.agentId = agent.iloc[0]['id']
            self.workspaceId = agent.iloc[0]['domain']
            self.language = agent.iloc[0]['language']

        except Exception as ex:
            raise NameError('Agent name not exsist')

    def __str__(self):
        return '{}'.format(self.agentId)

    def AgentSwitch(self):

        data = {
            "agent_id": self.agentId
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
        return r


# class Insight(Agent):
#
#     '''
#     Insight Object
#     '''
#
#     def __init__(self, agent):
#         self.token = agent.token
#         self.env = agent.env
#         self.agentId = agent.agentId
#         self.workspaceId = agent.workspaceId
#         self.domain = agent.domain
#
#         self.headers = {
#             "Content-Type": "application/json",
#             "Authorization": "Bearer" + " " + self.token
#         }
#
#         if self.env == 'dev':
#             self.base_url = url_list['BASE_URL_INSIGHT_DEV']
#         if self.env == 'qa':
#             self.base_url = url_list['BASE_URL_INSIGHT_QA']
#         if self.env == 'prod':
#             self.base_url = url_list['BASE_URL_INSIGHT_PROD']
#
#     def GetRules(self):
#
#         insight_url = self.base_url + '/' + 'rules' + '/' + '?agentId=' + self.agentId + '&page=0&limit=100000'
#         response = requests.get(url=insight_url, headers=self.headers)
#         response.raise_for_status()
#         r = response.json()
#         #df_rules = pd.DataFrame([dict(zip(['id', 'name', 'type', 'code', 'domain'], [d['id'], d['name'], d['type'], d['code'], d['domain']])) for d in r['data']])
#         df_rules = pd.DataFrame(r['data'])
#
#         return df_rules
#
#     def ExecuteRule(self, id_insight):
#
#         insight_url = self.base_url + '/' + 'rules' + '/' + id_insight + '/produceAndSend'
#         r = requests.post(url=insight_url, headers=self.headers)
#         r.raise_for_status()
#
#         return r
#
#     def ExecuteRules(self, listid_insight):
#
#         data = listid_insight
#
#         insight_url = self.base_url + '/' + 'insight' + '/produceAndSendAsync'
#         r = requests.post(url=insight_url, headers=self.headers, json=data)
#
#         r.raise_for_status()
#         #print('Success! Request is accepted, status: ' + str(r.status_code))
#         return
#
#     def CreateRule(self, insight):
#         # insight is json with specific mandatory fileds ...
#         data = insight
#
#         s = requests.Session()
#         s.keep_alive = False
#         retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
#         s.mount('https://', HTTPAdapter(max_retries=retries))
#
#         authentication_url = self.base_url + '/rules'
#         r = s.post(url=authentication_url, headers=self.headers, json=data)
#         r.raise_for_status()
#         return r
#
#     def MigrationInsight(self, agent_source, insights_source):
#
#         insights_source.drop(columns=['createdAt', 'createdBy', 'id'], inplace=True)
#         insights_source.drop(insights_source[(insights_source["name"] == 'Sample rule') & (
#                     insights_source["process"] == 'viewAnalytics') & (insights_source["type"] == 'ANALYTICS')].index, inplace=True)
#
#         for row in insights_source.itertuples():
#
#             Ind = row.Index
#             for name_elem, elem in zip(row._fields, row):
#                 # Set a channel list on every document update
#                 if type(elem) == list:
#                     if(len(elem)>0):
#                         if type(elem[0]) == dict:
#                             replace_elem = []
#                             for d in elem:
#
#                                 replace_elem_ = {k: v if v is None else v.replace(agent_source.workspaceId, self.workspaceId)
#                                 if k != "query" else v.replace(agent_source.agentId.lower(), self.agentId.lower())
#                                                 for (k, v) in d.items()}
#                                 replace_elem.append(replace_elem_)
#
#                             insights_source.at[Ind, name_elem] = replace_elem
#                         else:
#                             replace_elem_ = [str(n).replace(agent_source.agentId, self.agentId) for n in elem]
#                             if replace_elem_.sort() == elem.sort():
#                                 replace_elem = [str(n).replace(agent_source.workspaceId, self.workspaceId) for n in replace_elem_]
#                             else:
#                                 replace_elem = replace_elem
#                             insights_source.loc[Ind, name_elem] = replace_elem
#
#                 if type(elem) == dict:
#
#                     replace_elem = {k: v.replace(agent_source.agentId.lower(), self.agentId.lower())
#                     if type(v)==str else v for (k, v) in elem.items()}
#
#                     # replace_elem = {k: v.replace(agent_source.workspaceId.lower(), self.workspaceId.lower())
#                     #     if type(v)==str else v for (k, v) in replace_elem_.items()}
#
#                     insights_source.at[Ind, name_elem] = replace_elem
#
#                 if type(elem) == str and type(elem) != int and type(elem) != bool:
#                     replace_elem_ = elem.replace(agent_source.agentId, self.agentId)
#                     if replace_elem_ == elem:
#                         replace_elem = replace_elem_.replace(agent_source.workspaceId, self.workspaceId)
#                     else:
#                         replace_elem = replace_elem_
#
#                     insights_source.loc[Ind,name_elem] = replace_elem
#                 else:
#                     pass
#
#         insight_records = insights_source.to_dict(orient='records')
#         for insight_record in insight_records:
#             try:
#                 self.CreateRule(insight_record)
#                 logging.info(
#                     f'migrate to agenteId {self.agentId} --> ruleId: {str(insight_record["domain"]) + "-" +str(insight_record["type"]) + "-" + str(insight_record["code"])}')
#                 logging.info(f'------ ------- --------------')
#             except:
#
#                 logging.info(f'--------- ruleId already exist ------------')
#                 logging.info(f'------ ------- --------------')
#                 insight_record['code'] = insight_record['code'] + '_' + 'D' + f'{datetime.strftime(datetime.now(),"%Y%m%d")}'
#
#                 self.CreateRule(insight_record)
#                 logging.info(f'migrate with new ruleId: {str(insight_record["domain"]) + "-" + str(insight_record["type"]) + "-" + str(insight_record["code"])}')
#         return insights_source
#
# class Dataset(Agent):
#
#     '''
#     Dataset Object
#     '''
#
#     def __init__(self, agent):
#         self.token = agent.token
#         self.env = agent.env
#         self.agentId = agent.agentId
#
#         self.headers = {
#             "Content-Type": "application/json",
#             "Authorization": "Bearer" + " " + self.token
#         }
#
#         if self.env == 'dev':
#             self.base_url = url_list['BASE_URL_DATASET_DEV']
#         if self.env == 'qa':
#             self.base_url = url_list['BASE_URL_DATASET_QA']
#         if self.env == 'prod':
#             self.base_url = url_list['BASE_URL_DATASET_PROD']
#
#     def GetDatasets(self):
#
#         #to do test
#         dataset_url = self.base_url + '/datasets?agentId=' + self.agentId
#         response = requests.get(url=dataset_url, headers=self.headers)
#         response.raise_for_status()
#         r = response.json()
#         r_df = pd.DataFrame(r)
#         df_datasets = r_df.loc[:,['id', 'domain', 'type', 'code', 'name', 'description', 'createdBy', 'isActive', 'accessType', 'icon',
#          'version', 'syncCount', 'visible', 'public', 'createdAt']]
#
#         return df_datasets
#
#     def ExecuteDatasetSync(self, dataset_id):
#
#         dataset_url = self.base_url + '/datasets/' + dataset_id + '/sync'
#         r = requests.post(url=dataset_url, headers=self.headers)
#         r.raise_for_status()
#         return r


class AskAgent(Agent):
    def __init__(self, Agent):
        self.token = Agent.token
        self.env = Agent.env
        self.agentId = Agent.agentId
        self.workspaceId = Agent.workspaceId

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

