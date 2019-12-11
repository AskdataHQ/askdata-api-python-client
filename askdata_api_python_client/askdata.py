import requests



class Askdata:

    # def BASE_URL_INSIGHT =

    def __init__(self, username, password, domain='Askdata', env='qa'):
        self.username = username
        self.password = password
        self.domain = domain
        self.env = env

        # ''' default domain '''
        # ''' default enviroment '''


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

        if self.env == 'qa':
            authentication_url = 'https://smartfeed-qa.askdata.com' + '/oauth/access_token'
            r = requests.post(url=authentication_url, json=data, headers=headers).json()

        self.token = r['access_token']

    def GetAgents(self):
        url_get_agent = 'https://smartbot-qa.askdata.com/agent?page=0&size=300'

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }

        r = requests.get(url=url_get_agent, headers=headers).json()
        agent_list = [tuple([d['name'], d['code'], d['id']]) for d in r['result']]
        return agent_list

    def GetAgent(self, _code):
        url_get_agent = 'https://smartbot-qa.askdata.com/agent?page=0&size=300'

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + self.token
        }

        r = requests.get(url=url_get_agent, headers=headers).json()

        id_agent = [d['id'] for d in r['result'] if d['code'] == _code]

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
        
        



