import askdata_api_python_client.askdata as askdata
import askdata_api_python_client.insight as ins
from datetime import datetime
import json
import random

if __name__ == '__main__':
    # agent a
    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'Askdata'
    env = 'qa'
    Askdata = askdata.Askdata(username, password, domain, env)
    today = datetime.now().strftime('%Y%m%d')
    # sigh up
    Sign = Askdata.signup_user(f'test{today}@askdata.com', f'test{today}')

    # get list of Agents
    df_GetAgents = Askdata.df_agents

    # get agent
    agent = askdata.Agent(Askdata, 'SDK_TESTER')

    switch = agent.SwitchAgent()

    # ------------------------------ send query NL to agent  -----------------------------------
    df = agent.RequestAgent('incassi per agenzia per canale')
    print(df.head(5))
