from askdata_api_python_client.askdata import Askdata, Agent
import askdata_api_python_client.catalog as catalog
import pandas as pd
from datetime import datetime

if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'askdata'
    env = 'qa'
    Askdata = Askdata(username, password, domain, env)
    # get list of Agents
    #df_GetAgents = Askdata.df_agents
    # get agent
    agent = Agent(Askdata, 'SDK_TESTER')

    df_cat = agent.load_catalogs()
    today = datetime.now().strftime('%Y%m%d')
    entry_id = list(df_cat[df_cat['title']=='CH_TEST_242814'].loc[:,'id'])[0]
    agent.create_query(f'pippo_{today}', entry_id, execute=False)
    print('ok')