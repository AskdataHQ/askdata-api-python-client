import askdata.askdata_client as askdata
import random
import askdata.channel as channel
import pandas as pd
from datetime import datetime

if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'Askdata'
    env = 'qa'
    Askdata = askdata.Askdata(username, password, domain, env)
    # get list of Agents
    #df_GetAgents = Askdata.df_agents
    # get agent
    agent = askdata.Agent(Askdata, agent_name='SDK_TESTER')

    list_channels = agent.load_channels()
    today = datetime.now().strftime('%Y%m%d')
    Name_ch = 'CH_TEST_{}'.format(today)
    create_channel_id = agent.create_channel(Name_ch)
    id_channel = list(list_channels[list_channels['name'] == 'CH_TEST']['id'])[0]
    list_user = agent.load_users_fromch(id_channel)
    new_user = 'b7da6a4e-f581-4019-9771-bf4853939d11'   #a.battaglia@askdata.com
    agent.add_user_toch(create_channel_id, new_user) #ab5a0b80-bc97-4864-b3d4-18ba059a3d23
    agent.update_channel(create_channel_id, 'PUBLIC', iconFlag=True)
    agent.un_mute_channel(create_channel_id)
    agent.mute_channel(create_channel_id)
    agent.delete_user_fromch(create_channel_id, new_user)
    agent.delete_channel(create_channel_id)
    print('ok')