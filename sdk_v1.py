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
        

    def Login(self):
        authentication_url = 'https://smartfeed-qa.askdata.com' + '/oauth/access_token'
        data = {
            "username": self.username,
            "password": self.password,
            "domain": self.domain
        }

        headers = {
            "Content-Type": "application/json",
            "cache-control": "no-cache"
            }
        r = requests.post(url=authentication_url, json=data, headers=headers)
        r_json = r.json()
        self.token = r_json['access_token']
        
    def GetListAgents(self):
        
        url_get_agent = 'https://smartbot-qa.askdata.com/agent?page=0&size=300'
        
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }
        
        r = requests.get(url=url_get_agent, headers=headers)
        r_json = r.json()
        
        self.r = r_json
        
    def GetAgent(self, _code):
        
        url_get_agent = 'https://smartbot-qa.askdata.com/agent?page=0&size=300'
        
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }
        
        r = requests.get(url=url_get_agent, headers=headers)
        r_json = r.json()
        
        
        self.r = r_json
        
        
        
        
        
    def InsightRun(self, _type, _code):
        
        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }
        insight_url = 'https://smartinsightsv2-qa.askdata.com' + '/insight/' + self.domain + '/' + _type + '/' + _code +'/produce'
        
        r = requests.post(url=insight_url, headers=headers)
        
        #https://smartinsightsv2-qa.askdata.com/insight/GROUPAMA_QA/MONTHLY_DM/REQ_D20_3_LIST_SP/produce
        
        
    def SynchronizeDataset(self, sync_url,dataset_id):
         headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer" + " " + self.token
        }
            
        #url = sync_url + '/datasets/' + dataset_id + '/sync'
        #requests.post(url=url, headers=headers)
        
          
            
            
    


# In[9]:


username = 'g.demaio@askdata.com'
password = 'g.demaio'
#domain = 'DF426F64-7D7E-4573-8789-E2D6F08ACB7B'


