import askdata.askdata_client as askdata
import askdata.insight as ins
import json
import random
from datetime import datetime


if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'Askdata'
    env = 'qa'
    Askdata = askdata.Askdata(username, password, domain, env)
    # get list of Agents
    # df_GetAgents = Askdata.df_agents
    # get agent
    agent = askdata.Agent(Askdata, agent_name='SDK_TESTER')

    # agent b

    username = 'groupama@askdata.com'
    password = 'groupama'
    domain = 'GROUPAMA'
    env = 'qa'
    Askdatab = askdata.Askdata(username, password, domain, env)
    agentb = askdata.Agent(Askdatab, agent_name='oKGroupama')

    # -------   insight ----------------
    df_insight = agent.load_rules()
    df_insightb = agentb.load_rules()

    #  ---- test MigrationInsight method --------
    migration = agent.migration_insight(agentb, df_insightb.loc[:5, :])
    # -- Test CreateRule , change code and type or domain for creating different insghtid ----
    today = datetime.now().strftime('%Y%m%d')
    df_insight['code'] = f'TEST_CREATION{today}'
    df_insight2 = df_insight.drop('id', axis=1)

    # --- - -----------------  convert into dictionary     -----------------------------------
    insight_record = df_insight.to_dict(orient='records')
    ins1 = insight_record[0]
    a = agent.create_rule(ins1)

    # -------------------------produce and send insight    ---------------------------------

    list_insight = ["DF426F64-7D7E-4573-8789-E2D6F08ACB7B-DAILY_DM-REQ_D1_VAR_TOT_INCASSI"]

    card1 = agent.execute_rule('DF426F64-7D7E-4573-8789-E2D6F08ACB7B-DAILY_DM-REQ_D1_VAR_TOT_INCASSI')
    card2 = agent.execute_rules(list_insight)


