from askdata.askdata_client import Askdata,Agent
import datetime
import pandas as pd
import askdata.dataset as dats
import json
import random

if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'
    domain = 'Askdata'
    env = 'dev'
    Askdata = Askdata(username, password, domain, env)

    # get agent
    # agent = askdata.Agent(Askdata, 'SDK_TESTER')
    agent = Agent(Askdata, 'SDK_TEST')

    # --------------------------------   Dataset  -------------------------------------------
    df_datasets = agent.load_datasets()

    # -------------------------------- Load by dataset ID -------------------------------
    id_test = agent.get_id_dataset_by_name('COPERTURA_BCK')

    dataset_df = agent.load_dataset_to_df(id_test[0])
    # agent.LoadFromDataset('DF426F64-7D7E-4573-8789-E2D6F08ACB7B-MYSQL-95a9d1f1-8692-48a2-8b2b-f4a296e8fa27')

    # -------------------------------- sync by dataset ID -------------------------------
    resp_sync = agent.execute_dataset_sync(
       '0a82e596-b6e9-45d7-ba0e-ec75b974b752-MYSQL-17ff8411-5077-4add-b3d0-544f841a352d')
    #resp_sync = agent.ExecuteDatasetSync(df_datasets['id'][0])

    # ------ Create New dataset from existing table from your personal DB -------------
    df_test = pd.DataFrame([{"id": "pippo", "set": 123, "clm1": "ciao"}, {"id": "pippo1", "set": 423, "clm1": "ciao"},
                            {"id": "pippo2", "set": 1283, "clm1": "ciao"}])
    # ,indexclm=["set"]
    agent.save_to_dataset(frame=df_test, dataset_name='T_DF_{}'.format(datetime.datetime.now().strftime("%Y%m%d")),
                          indexclm=["set"])

    # ----- Create dataset mysql from  mysql connetion ---------------
    database_username = 'xxxx'
    database_password = 'xxxx'
    database_ip = 'xxxx'
    database_name = 'xxx'
    table_name = 'xxx'
    port = '3306'
    label = 'TEST_DATASET_{}'.format(datetime.datetime.now().strftime("%Y%m%d"))
    agent.create_dataset_byconn(label, database_ip, port, database_name, database_username, database_password,
                                table_name)