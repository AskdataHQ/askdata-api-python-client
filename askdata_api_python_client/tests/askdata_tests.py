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
    dict_agents = client.GetAgents()
    print(dict_agents)
    # get id agent
    id_agent = client.GetAgent('SDK_TESTER')
    print (id_agent)

    dict_insight = askdata.Insight.GetRules(client)

    print(dict_insight)
    askdata.Insight.ExecuteRules(client, ['DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MONTHLY_DM-REQ_DIR_1_VAR_TOT_INC'])
    #askdata.Insight.ExecuteRule(client,dict_insight[0]['id'])

    dict_datasets = askdata.Dataset.GetDatasets(client)
    # sync by dataset ID
    client, dict_insight[0]['code']

    print(dict_datasets)

    #insight.ExecuteRuleAsync(lista_insight[0])