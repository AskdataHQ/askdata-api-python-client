import askdata_api_python_client.askdata as askdata
import random
import askdata_api_python_client.channel as channel
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
    agent = askdata.Agent(Askdata, 'SDK_TESTER')

    list_channels = agent.GetChannels()
    today = datetime.now().strftime('%Y%m%d')
    Name_ch = f'CH_TEST_{today}'
    create_channel_id = agent.CreateChannel(Name_ch)
    id_channel = list(list_channels[list_channels['name'] == 'CH_TEST']['id'])[0]
    list_user = agent.GetUsersFromCh(id_channel)
    new_user = 'b7da6a4e-f581-4019-9771-bf4853939d11'   #a.battaglia@askdata.com
    agent.AddUserToCh(create_channel_id, new_user) #ab5a0b80-bc97-4864-b3d4-18ba059a3d23
    agent.UpdateChannel(create_channel_id, 'PUBLIC',iconFlag=True)
    agent.UnMuteChannel(create_channel_id)
    agent.MuteChannel(create_channel_id)
    agent.DeleteUserFromCh(create_channel_id, new_user)
    agent.DeleteChannel(create_channel_id)
    print('ok')