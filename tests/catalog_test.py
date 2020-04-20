import askdata_api_python_client.askdata as askdata
import askdata_api_python_client.catalog as catalog
import pandas as pd
from datetime import datetime

if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'askdata'
    env = 'qa'
    Askdata = askdata.Askdata(username, password, domain, env)
    # get list of Agents
    #df_GetAgents = Askdata.df_agents
    # get agent
    agent = askdata.Agent(Askdata, 'SDK_TESTER')

    df_cat = agent.GetCatalogs()
    today = datetime.now().strftime('%Y%m%d')
    entry_id = list(df_cat[df_cat['title']=='CH_TEST_242814'].loc[:,'id'])[0]
    agent.PushQueryCt(f'pippo_{today}', entry_id, execute=False)
    print('ok')