import askdata_api_python_client.askdata as askdata
import askdata_api_python_client.insight as ins
import json
import random


if __name__ == '__main__':

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
    insight = ins.Insight(agent)
    df_insight = insight.GetRules()

    insightb = ins.Insight(agentb)
    df_insightb = insightb.GetRules()

    #  ---- test MigrationInsight method --------
    #migration = insight.MigrationInsight(agentb, df_insightb.loc[:3,:])
    # -- Test CreateRule , change code and type or domain for creating different insghtid ----

    df_insight['code'] = f'TEST_CREATION{random.randint(1,500000)}'
    df_insight2 = df_insight.drop('id', axis=1)

    # --- - -----------------  convert into dictionary     -----------------------------------
    insight_record = df_insight.to_dict(orient='records')
    ins1 = insight_record[0]
    a = insight.CreateRule(ins1)

    # -------------------------produce and send insight    ---------------------------------

    list_insight = ["DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MONTHLY_DM-REQ_DIR_1_VAR_TOT_INC"]

    card1 = insight.ExecuteRule('DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MONTHLY_DM-REQ_DIR_1_VAR_TOT_INC')
    card2 = insight.ExecuteRules(list_insight)

