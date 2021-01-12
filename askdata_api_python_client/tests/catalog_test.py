import askdata_api_python_client.askdata as askdata
import askdata_api_python_client.catalog as catalog
import pandas as pd

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

    cat = catalog.Catalog(agent)
    entry_id = list(cat.catalogs[cat.catalogs['title']=='CH_TEST'].loc[:,'id'])[0]
    cat.PushQuery('pippo', entry_id)
    print('ok')