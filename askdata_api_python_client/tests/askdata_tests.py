import askdata_api_python_client.askdata as askdata

if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'Askdata'
    env = 'qa'
    Askdata = askdata.Askdata(username, password, domain, env)
    # get list of Agents
    df_GetAgents = Askdata.GetAgents()
    # get agent
    agent = askdata.Agent(Askdata,'SDK_TESTER')

    print(agent)

    df_insight = askdata.Insight.GetRules(agent)

    print(df_insight)
    card = askdata.Insight.ExecuteRules(agent, ['DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MONTHLY_DM-REQ_DIR_1_VAR_TOT_INC'])
    #askdata.Insight.ExecuteRule(agent,df_insight[0]['id'])

    dict_datasets = askdata.Dataset.GetDatasets(agent)

    print(dict_datasets)


    # sync by dataset ID
    resp_sync = askdata.Dataset.ExecuteDatasetSync(agent, dict_datasets['code'][0])


    df = askdata.AskAgent.RequestAgent(agent,'incassi per agenzia per canale')

    print(df.head(5))


