from askdata.askdata_client import Askdata, Agent
import askdata.insight as ins
from datetime import datetime
import json
import random

if __name__ == '__main__':
    # agent a
    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domainlogin = 'Askdata'
    env = 'dev'
    askdata = Askdata(username, password, domainlogin, env)
    #askdata = Askdata(token='17f1c212-b8e0-4ba8-91ba-337599ffb657',env='dev')
    #askdata = Askdata(env=env,domainlogin=domainlogin)

    # get list of Agents
    get_agents = askdata.load_agents()
    get_agents_df = askdata.agents_dataframe()
    today = datetime.now().strftime('%Y%m%d')
    # sigh up
    #sign = askdata.signup_user(f'test{today}@askdata.com', f'test{today}')

    # get agent
    agent = Agent(askdata, 'SDK_TEST')
    agent = Agent(askdata, agent_name='SDK_TESTER')
    agent = Agent(askdata, agent_id='2ed8390e-b542-4f62-82b8-c8f1b620ef15')
    #agent = Agent(askdata, agent_name='SDK_TEST')

    switch = agent.switch_agent()

    # ------------------------------ send query NL to agent  -----------------------------------
    df = agent.ask('incassi per agenzia per canale')
    print(df.head(5))
