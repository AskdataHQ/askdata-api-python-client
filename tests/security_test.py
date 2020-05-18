import askdata.askdata_client as askdata
import askdata.security as sec
import json
import random

if __name__ == '__main__':
    # agent a
    # username = 'g.demaio@askdata.com'
    # password = 'g.demaio'
    # domain = 'Askdata'
    # env = 'qa'
    username = 'flashfiber@askdata.com'
    password = 'flashfiber'
    domain = 'FLASHFIBER'
    env = 'qa'
    Askdata = askdata.Askdata(username, password, domain, env)
    # get list of Agents
    # df_GetAgents = Askdata.df_agents
    # get agent
    #agent = askdata.Agent(Askdata, agent_name='SDK_TESTER')
    agent = askdata.Agent(Askdata, agent_name='FlashFiber')
    security = sec.SignUp(Askdata)


    security.signup_user(username='paolo.impiglia@flashfiber.it', password='impiglia27!')
    #security.PushUser(username='g.demaio@askdata.com',password='g.demaio')
    print('ok')