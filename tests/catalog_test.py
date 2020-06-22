from askdata.askdata_client import Askdata, Agent
import askdata.catalog as catalog
import pandas as pd
from datetime import datetime
from askdata.catalog import Catalog

if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'askdata'
    env = 'prod'
    Askdata = Askdata(username, password, domain, env)
    # get list of Agents
    #df_GetAgents = Askdata.df_agents
    # get agent
    agent = Agent(Askdata, agent_name='SDK_TESTER')

    df_cat = agent.load_catalogs()
    today = datetime.now().strftime('%Y%m%d')
    entry_id_ = df_cat.id.values[1]
    entry_id = agent.create_catalog(name=today+'_catalog')
    list_query = agent.get_query_from_catalog(entry_id)
    id_query = agent.create_query(f'pippo_{today}', entry_id, execute=False)
    agent.delete_query(entry_id, id_query)
    agent.delete_catalog(entry_id)

    entry_id = agent.create_catalog(name=today + '_catalog_2')
    for i in range(0,8):
        id_query = agent.create_query(f'pippo_{today}_{i}', entry_id, execute=False)
    Catalog.delete_all_queries_catalog(agent, entry_id=entry_id)

    print('ok')