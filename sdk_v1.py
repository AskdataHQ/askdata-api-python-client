
# coding: utf-8

# In[65]:


import requests

#TO DO:
# 1. insert something for environment
# 2. fixing BASE URL 

import requests

#TO DO:
# 1. insert something for environment
# 2. fixing BASE URL 

class Askdata:
    
    #def BASE_URL_INSIGHT = 
    
    # http://docs.python-requests.org/en/master/user/authentication/#new-forms-of-authentication
    def __init__(self, username, password, domain='Askdata'):
        self.username = username
        self.password = password
        self.domain = domain
        
        #''' default domain '''
        #''' default enviremnt '''
        

class Agent(Askdata):
    
    def Login():
        authentication_url = 'https://smartfeed-qa.askdata.com' + '/oauth/access_token'
        data = {
            "username": Askdata.username,
            "password": Askdata.password,
            "domain": Askdata.domain
        }

        headers = {
            "Content-Type": "application/json",
            "cache-control": "no-cache"
            }
        r = requests.post(url=authentication_url, json=data, headers=headers)
        r_json = r.json()
        Askdata.token = r_json['access_token']
        
    def GetAgents():
        
        url_get_agent = 'https://smartbot-qa.askdata.com/agent?page=0&size=300'
        
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + Askdata.token
        }
        
        r = requests.get(url=url_get_agent, headers=headers)
        r_json = r.json()
        agent_list = [tuple([d['name'],d['code'],d['id']]) for d in r_json['result']]
        return agent_list
        
    def GetAgent(_code):
        
        url_get_agent = 'https://smartbot-qa.askdata.com/agent?page=0&size=300'
        
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + Askdata.token
        }
        
        r = requests.get(url=url_get_agent, headers=headers)
        r_json = r.json()
        
        id_agent = [d['id'] for d in r_json['result'] if d['code'] == _code]
                
        return id_agent
    
    
    


class Insight():
    
    def ExecuteInsights(self, _id):
        
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }
        insight_url = 'https://smartinsightsv2-qa.askdata.com' + '/insight/' + self.domain + '/' + _type + '/' + _code +'/produce'
        
        r = requests.post(url=insight_url, headers=headers)
        
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
        
        


# In[66]:


username = 'g.demaio@askdata.com'
password = 'g.demaio'
#domain = 'DF426F64-7D7E-4573-8789-E2D6F08ACB7B'

Askdata = Askdata(username,password)


# In[67]:


#instatiate client
client = Agent.Login()


# In[68]:


# get list of Agents
lista = Agent.GetAgents()


# In[71]:


# get id agent
id_agent = Agent.GetAgent('AB_NYC_2019')


# In[72]:


id_agent


# In[45]:


# result of get 
lista

