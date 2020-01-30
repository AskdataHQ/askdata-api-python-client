import askdata_api_python_client.askdata as askdata

if __name__ == '__main__':

    username = 'groupama@askdata.com'
    password = 'groupama'
    domain = 'Groupama_qa'
    env = 'qa'
    Askdata = askdata.Askdata(username, password, domain, env)
    # get list of Agents
    df_GetAgents = Askdata.GetAgents()
    # get agent
    #agent = askdata.Agent(Askdata,'SDK_TESTER')
    agent = askdata.Agent(Askdata, 'oKGroupama')
    print(agent)
    insight = askdata.Insight(agent)
    df_insight = insight.GetRules()

    list_insight = [ "GROUPAMA_QA-DAILY_DM-REQ_D11_LIST_AGZ_VAR_MED",
    "GROUPAMA_QA-DAILY_DM-REQ_D10_VAR_MED_AREA",
    "GROUPAMA_QA-DAILY_DM-REQ_D6_LIST_PORT",
    "GROUPAMA_QA-DAILY_DM-REQ_D5_TOP_CONV",
    "GROUPAMA_QA-DAILY_DM-REQ_D4_VAR_INC_PORT",
    "GROUPAMA_QA-DAILY_DM-REQ_D3_LIST_INCASSI"
    ]
    #card1 = insight.ExecuteRule('DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MONTHLY_DM-REQ_DIR_1_VAR_TOT_INC')
    card2 = insight.ExecuteRules(list_insight)
    #askdata.Insight.ExecuteRule(agent,df_insight[0]['id'])

    #dataset = askdata.Dataset(agent)
    #df_datasets = dataset.GetDatasets()

    #print(df_datasets)


    # sync by dataset ID

    #resp_sync = dataset.ExecuteDatasetSync('DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MYSQL-23232444-bc2c-4b90-93df-3500baa90151')
    #resp_sync = dataset.ExecuteDatasetSync(df_datasets['id'][0])


    #df = askdata.AskAgent.RequestAgent(agent,'incassi per agenzia per canale')

    #print(df.head(5))


