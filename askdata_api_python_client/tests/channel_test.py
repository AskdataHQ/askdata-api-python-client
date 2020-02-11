import askdata_api_python_client.askdata as askdata
import askdata_api_python_client.channel as channel
import pandas as pd

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

    ch = channel.Channel(agent)
    create_channel = ch.CreateChannel('CH_TEST')
    list_channels = ch.GetChannels()
    #id_channel = list(list_channels[list_channels['name'] == 'CH_TEST']['id'])[0]
    id_channel = '1098c339-08ed-4cb5-8f86-4f959a8606e0'
    #list_user = ch.GetUsers(id_channel2)
    #id_channel = 'e956afb0-dbb9-40b2-8600-df3b73500f8e'
    #new_user = 'b7da6a4e-f581-4019-9771-bf4853939d11'   #a.battaglia@askdata.com
    #ch.AddUser(id_channel, new_user) #ab5a0b80-bc97-4864-b3d4-18ba059a3d23
    #ch.DeleteUser(id_channel, new_user)
    #ch.UpdateChannel(id_channel, 'PRIVATE',iconFlag=True)
    ch.MuteChannel(id_channel)
    #ch.UnMuteChannel(id_channel)
    #ch.DeleteChannel(id_channel)
    #e956afb0-dbb9-40b2-8600-df3b73500f8e
    print('ok')