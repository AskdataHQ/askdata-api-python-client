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
    env = 'prod'
    askdata = Askdata(username, password, domainlogin, env)
    #askdata = Askdata(env=env,domainlogin=domainlogin)

    # get list of Agents
    get_agents = askdata.load_agents()
    get_agents_df = askdata.agents_dataframe()
    today = datetime.now().strftime('%Y%m%d')
    # sigh up
    sign = askdata.signup_user(f'test{today}@askdata.com', f'test{today}')

    # get agent
    agent = Agent(askdata, 'SDK_TESTER')
    #agent = Agent(askdata, 'SDK_TEST')

    switch = agent.switch_agent()

    # ------------------------------ send query NL to agent  -----------------------------------
    df = agent.ask('incassi per agenzia per canale')
    print(df.head(5))
