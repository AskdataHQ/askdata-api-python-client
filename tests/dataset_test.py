import askdata_api_python_client.askdata as askdata
import askdata_api_python_client.dataset as dats
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

    # --------------------------------   Dataset  -------------------------------------------
    df_datasets = agent.GetDatasets()

    # print(df_datasets)

    # -------------------------------- sync by dataset ID -------------------------------

    resp_sync = agent.ExecuteDatasetSync(
        'DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MYSQL-23232444-bc2c-4b90-93df-3500baa90151')
    resp_sync = agent.ExecuteDatasetSync(df_datasets['id'][0])