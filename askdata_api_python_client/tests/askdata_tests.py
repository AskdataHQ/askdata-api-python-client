import askdata_api_python_client.askdata as askdata

if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'


    Askdata = askdata.Askdata(username, password)

    # instatiate client
    client = askdata.Agent(Askdata)
    # login
    client.Login()
    # get list of Agents
    list_agents = client.GetAgents()
    print(list_agents)
    # get id agent
    id_agent = client.GetAgent('AB_NYC_2019')
    print (id_agent)


    lista_insight = askdata.Insight.GetRules()
    print(lista_insight)
    #insight.ExecuteRule(lista_insight[0])
    #insight.ExecuteRuleAsync(lista_insight[0])