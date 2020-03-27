import askdata_api_python_client.askdata as askdata
import json
import random

if __name__ == '__main__':
    # agent a
    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'Askdata'
    env = 'qa'
    Askdata = askdata.Askdata(username, password, domain, env)
    # get list of Agents
    # df_GetAgents = Askdata.df_agents
    # get agent
    agent = askdata.Agent(Askdata, 'SDK_TESTER')

    # agent b

    username = 'groupama@askdata.com'
    password = 'groupama'
    domain = 'GROUPAMA'
    env = 'qa'
    Askdatab = askdata.Askdata(username, password, domain, env)
    agentb = askdata.Agent(Askdatab, 'oKGroupama')

    # -------   insight ----------------
    insight = askdata.Insight(agent)
    df_insight = insight.GetRules()

    insightb = askdata.Insight(agentb)
    df_insightb = insightb.GetRules()


    #  ---- test MigrationInsight method --------
    migration = insight.MigrationInsight(agentb,df_insightb)
    # -- Test CreateRule , change code and type or domain for creating different insghtid ----

    df_insight['code'] = f'TEST_CREATION{random.randint(1,500000)}'
    df_insight2 = df_insight.drop('id',axis=1)

    # --- - -----------------  convert into dictionary     -----------------------------------
    insight_record = df_insight.to_dict(orient='records')
    ins1 = insight_record[0]
    a = insight.CreateRule(ins1)

    # -------------------------produce and send insight    ---------------------------------

    list_insight = [ "GROUPAMA_QA-DAILY_DM-REQ_D11_LIST_AGZ_VAR_MED",
    "GROUPAMA_QA-DAILY_DM-REQ_D10_VAR_MED_AREA",
    "GROUPAMA_QA-DAILY_DM-REQ_D6_LIST_PORT",
    "GROUPAMA_QA-DAILY_DM-REQ_D5_TOP_CONV",
    "GROUPAMA_QA-DAILY_DM-REQ_D4_VAR_INC_PORT",
    "GROUPAMA_QA-DAILY_DM-REQ_D3_LIST_INCASSI"
    ]

    card1 = insight.ExecuteRule('DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MONTHLY_DM-REQ_DIR_1_VAR_TOT_INC')
    card2 = insight.ExecuteRules(list_insight)
    askdata.Insight.ExecuteRule(agent,df_insight[0]['id'])

    # --------------------------------   Dataset  -------------------------------------------

    dataset = askdata.Dataset(agent)
    df_datasets = dataset.GetDatasets()

    #print(df_datasets)


    # -------------------------------- sync by dataset ID -------------------------------

    resp_sync = dataset.ExecuteDatasetSync('DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MYSQL-23232444-bc2c-4b90-93df-3500baa90151')
    resp_sync = dataset.ExecuteDatasetSync(df_datasets['id'][0])

    # ------------------------------ send query NL to agent  -----------------------------------
    df = askdata.AskAgent.RequestAgent(agent,'incassi per agenzia per canale')

    print(df.head(5))


