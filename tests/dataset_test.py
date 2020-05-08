import askdata_api_python_client.askdata as askdata
import datetime
import pandas as pd
import askdata_api_python_client.dataset as dats
import json
import random

if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'Askdata'
    env = 'dev'
    Askdata = askdata.Askdata(username, password, domain, env)

    # get agent
    # agent = askdata.Agent(Askdata, 'SDK_TESTER')
    agent = askdata.Agent(Askdata, 'SDK_TEST')

    # --------------------------------   Dataset  -------------------------------------------
    df_datasets = agent.GetDatasets()

    # -------------------------------- Load by dataset ID -------------------------------
    #id_test = agent.GetIdDatasetByName('COPERTURA_BCK')
    #dataset_df = agent.LoadFromDataset(id_test[0])
    # agent.LoadFromDataset('DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MYSQL-95a9d1f1-8692-48a2-8b2b-f4a296e8fa27')

    # -------------------------------- sync by dataset ID -------------------------------
    # resp_sync = agent.ExecuteDatasetSync(
    #   'DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MYSQL-23232444-bc2c-4b90-93df-3500baa90151')
    # resp_sync = agent.ExecuteDatasetSync(df_datasets['id'][0])

    # ------ Create New dataset from existing table from your personal DB -------------
    df_test = pd.DataFrame([{"id": "pippo", "set": 123, "clm1": "ciao"}, {"id": "pippo1", "set": 423, "clm1": "ciao"},
                            {"id": "pippo2", "set": 1283, "clm1": "ciao"}])
    # ,indexclm=["set"]
    agent.SaveToDataset(frame=df_test, dataset_name=f'T_DF_{datetime.datetime.now().strftime("%Y%m%d")}',
                        indexclm=["set"])

    # ----- Create dataset mysql from  mysql connetion ---------------
    database_username = 'xxxx'
    database_password = 'xxxx'
    database_ip = 'xxxx'
    database_name = 'xxx'
    table_name = 'xxx'
    port = '3306'
    label = f'TEST_DATASET_{datetime.datetime.now().strftime("%Y%m%d")}'
    agent.CreateDatasetMySql(label, database_ip, port, database_name, database_username, database_password, table_name)