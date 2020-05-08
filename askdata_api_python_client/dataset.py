import requests
import time
import sqlalchemy
import yaml
import os
import pandas as pd
import numpy as np
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import DateTime
from sqlalchemy.schema import Index
from threading import Thread
import re
from datetime import datetime
#import askdata_api_python_client.askdata as askdata

_LOG_FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] - %(asctime)s --> %(message)s"
g_logger = logging.getLogger()
logging.basicConfig(format=_LOG_FORMAT)
g_logger.setLevel(logging.INFO)

root_dir = os.path.abspath(os.path.dirname(__file__))
# retrieving base url
yaml_path = os.path.join(root_dir, '../askdata_api_python_client/askdata_config/base_url.yaml')
with open(yaml_path, 'r') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    url_list = yaml.load(file, Loader=yaml.FullLoader)


class Dataset():

    '''
    Dataset Object
    '''

    _agentId = None
    _domain = None

    def __init__(self, env, token):

        self._headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " +token
        }

        if env == 'dev':
            self._base_url_dataset = url_list['BASE_URL_DATASET_DEV']
            self._base_url_askdata = url_list['BASE_URL_ASKDATA_DEV']
        if env == 'qa':
            self._base_url_dataset = url_list['BASE_URL_DATASET_QA']
            self._base_url_askdata = url_list['BASE_URL_ASKDATA_QA']
        if env == 'prod':
            self._base_url_dataset = url_list['BASE_URL_DATASET_PROD']
            self._base_url_askdata = url_list['BASE_URL_ASKDATA_PROD']

    def GetDatasets(self):


        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        dataset_url = self._base_url_dataset + '/datasets?agentId=' + self._agentId
        response = s.get(url=dataset_url, headers=self._headers)
        response.raise_for_status()
        r = response.json()
        r_df = pd.DataFrame(r)

        datasets_df = r_df.loc[:,['id', 'domain', 'type', 'code', 'name', 'description', 'createdBy', 'isActive', 'accessType', 'icon',
         'version', 'syncCount', 'visible', 'public', 'createdAt']]

        return datasets_df

    def GetIdDatasetByName(self,name_ds,exact=False):

        '''
        Get askdata dataset ids by name

        :param name_ds: String
        it's name searched
        :param exact: Boolean
        if param is true the method search the dataset id with exact match whereas if param is False
        it searches dataset ids that contain the name_ds
        :return: Array
                '''

        dataset_list = self.GetDatasets()

        if exact == False:
            dataset_select_name = dataset_list.name.str.contains(name_ds,flags=re.IGNORECASE,regex=True)
            dataset_choose = dataset_list[dataset_select_name]
        else:
            dataset_choose = dataset_list[dataset_list['name'] == name_ds]

        #return an array
        return dataset_choose.id.values

    def __GetDatasetConnection(self,datasetid):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        dataset_url = self._base_url_dataset + '/datasets?agentId=' + self._agentId
        response = requests.get(url=dataset_url, headers=self._headers)
        response.raise_for_status()
        r = response.json()
        connection_df = pd.DataFrame([row['connection'] for row in r if row['id'] == datasetid])
        id_createdby = [row['createdBy'] for row in r if row['id'] == datasetid][0]
        return connection_df.table_id.item(), connection_df.schema.item(), id_createdby

    def GetEntitiesDataset(self,datasetid, custent = True):

        df_datasets = self.GetDatasets()
        dataset_info = df_datasets[df_datasets['id'] == datasetid]
        with requests.Session() as s:

            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
            s.mount('https://', HTTPAdapter(max_retries=retries))

            authentication_url = f'{self._base_url_askdata}/smartbot/dataset/type/{dataset_info.type.item()}/id/{dataset_info.id.item()}/subset/{dataset_info.type.item()}?_page=0&_limit=1000'
            r = s.get(url=authentication_url, headers=self._headers)
            r.raise_for_status()

        entities_df = pd.DataFrame(r.json()['payload']['data'])
        entities_df_no_cust = entities_df[entities_df['custom'] == False].copy()
        index_nocust = entities_df_no_cust.index

        #schema_metadata_df = pd.DataFrame([row['schemaMetaData'] for row in r.json()['payload']['data']
        #                             if row['custom'] is False])

        columnsid = [row['columnId'] for row in entities_df.loc[index_nocust,'schemaMetaData']]
        entitie_code = entities_df.code.tolist()
        if custent == False:
            entitie_code = entities_df.loc[index_nocust,'code'].tolist()

        return entitie_code, columnsid

    def ExecuteDatasetSync(self, dataset_id):

        dataset_url = self._base_url_dataset + '/datasets/' + dataset_id + '/sync'
        r = requests.post(url=dataset_url, headers=self._headers)
        r.raise_for_status()
        return r

    def __Ask_DBengine(self,dataset_id,setting):
        # richiesta credenziali
        database_username = 'developer'
        database_password = 'password01!'
        database_ip = '104.155.56.139'
        database_name = 'sdk_test'
        database_port = '3306'

        # database_username = 'developer'
        # database_password = 'password01!'
        # database_ip = '104.199.46.97'
        # database_name = ''

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + f'/smartdataset/datasets/{dataset_id}/onetimecredentials'
        #dataset_url = 'https://smartsql-dev.askdata.com/custom/create'
        response = s.get(url=authentication_url, headers=self._headers)
        response.raise_for_status()
        r = response.json()

        host = setting['datasourceUrl'].split('/')[2].split(':')[0]
        port = setting['datasourceUrl'].split('/')[2].split(':')[1]

        database_engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.
                                          format(r['mysqlUsername'], r['mysqlPassword'],
                                                 host, port
                                                 , setting['schema']), pool_recycle=3600,
                                          pool_size=5)

        # database_engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.
        #                                            format(database_username, database_password,
        #                                            database_ip, database_port
        #                                            , database_name), pool_recycle=3600,
        #                                           pool_size=5)
        #
        # db_tablename = 'DF426F64_7D7E_4573_8789_E2D6F08ACB7B_SDK_TESTER_TEST_DF_TO_DAT'
        db_tablename = r['mysqlTable']

        return database_engine, db_tablename

    def __Ask_DelDBengine(self, dataset_id):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + f'/smartdataset/datasets/{dataset_id}/revokeonetimecredentials'
        # dataset_url = 'https://smartsql-dev.askdata.com/custom/create'
        response = s.delete(url=authentication_url, headers=self._headers)
        response.raise_for_status()
        logging.debug('---------------------------')
        logging.debug(f'-------delete mysqluser for dataset {dataset_id}------')


    def SaveToDataset(self,frame, dataset_name,add_indexdf = False, indexclm = [],if_exists='Replace'):

        # vedere upsert in mysql if_exists['replace','Append','upsert']
        '''
        Save the data frame in askdata dataset of the specific agent

        Parameters
        ----------
        frame : DataFrame
        Input dataframe+
        index: list of string

        name : string
        name of the dataset Askdata

        index: list
        the index is a list of column names of the data frame which are setting like indexes for increasing performance.
        Default empty list
        '''
        dataset_id, settingsDataset = self.__CreateDatasetDf(dataset_name)
        engine, db_tablename = self.__Ask_DBengine(dataset_id,settingsDataset)


        # with "with" we can close the connetion when we exit
        with engine.connect() as connection:

            # to check type of column of the Dataframa for creating a correct and performing table structure

            dtypeTable = dict()
            for clm in frame.select_dtypes(include=np.object).columns:
                maxLen = frame[clm].str.len().max()
                dtypeTable[clm] = VARCHAR(length=maxLen + 10)
            for clm in frame.select_dtypes(include=[np.datetime64]).columns:
                dtypeTable[clm] = DateTime()

            if not indexclm:
                frame.to_sql(con=connection, name=db_tablename, if_exists='replace', chunksize=1000, index=add_indexdf, index_label='INDEX_DF',
                             method='multi',dtype=dtypeTable)
            else:
                frame.to_sql(con=connection, name=db_tablename, if_exists='replace', chunksize=1000,
                             method='multi',index=add_indexdf, index_label='INDEX_DF', dtype=dtypeTable)

                # SQL Statement to create a secondary index
                for column_ind in indexclm:
                    sql_index = """CREATE INDEX index_{}_{} ON {}(`{}`);""".format(db_tablename, column_ind,
                                                                                   db_tablename, column_ind)
                    # Execute the sql - create index
                    connection.execute(sql_index)

                # Now list the indexes on the table
                sql_show_index = "show index from {}".format(db_tablename)
                indices_mysql = connection.execute(sql_show_index)
                for index_mysql in indices_mysql.fetchall():
                    logging.info('--- ----------- -----')
                    logging.info('--- add index: {}'.format(index_mysql[2]))




        logging.info('--- ----------- -----')
        logging.info(f'--- Save the Dataframe into Dataset {dataset_name}')


        #run discovery dataset
        self.__discovery_datset(dataset_id,settingsDataset)

        # delete mysql user
        self.__Ask_DelDBengine(dataset_id)



    def UpDateDataset(self, frame, tablename, if_exists='rename'):
        pass

    def LoadFromDataset(self,dataset_id):

        # to test (Francesco)

        '''
        read askdata dataset by datasetId and return data frame

        :param dataset_id: String
        id of dataset Askdata
        :return: DataFrame
        '''

        table_id, schema, id_createdby = self.__GetDatasetConnection(dataset_id)

        #check if userid (agent) is also the owner of the dataset
        if id_createdby != self.userid:
            raise Exception(f"the user {id_createdby} haven't privilege for this dataset")

        # non usare il columnId ma CODE del dataset
        entitie_code, columnsid = self.GetEntitiesDataset(dataset_id,custent = False)


        fields_query = ", ".join([str(n) for n in [f"`{n}`" for n in columnsid]])
        size = 1000
        authentication_url2 = f'{self._base_url_askdata}/smartdataset/v2/datasets/{dataset_id}/query'

        query_count = f"SELECT COUNT(`{columnsid[0]}`) FROM {schema}.{table_id} WHERE `{columnsid[0]}` is not NULL;"


        s_count = requests.Session()
        s_count.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s_count.mount('https://', HTTPAdapter(max_retries=retries))

        data_count = {"userId": self.userid,
                "query": query_count, "sqlParameters": {},
                "connectionId": None, "page": 0, "size": 1, "nativeType": False}

        r_count = s_count.post(url=authentication_url2, headers=self._headers, json=data_count)
        r_count.raise_for_status()
        count = int(r_count.json()['data'][0]['cells'][0]['value'])
        n_worker = int(count/1000)+1

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=200, pool_maxsize=200))

        def load_datatset_post(session,data,authentication_url2,indice):

            logging.debug(f'Thread_{indice} : starting update')

            r2 = session.post(url=authentication_url2, headers=self._headers, json=data)
            r2.raise_for_status()
            #dataset_temp = pd.DataFrame([[clm['value'] for clm in row['cells']] for row in r2.json()['data']],
            #                                 columns=columnsid)
            logging.debug(f'Thread_{indice}: finishing update')
            return r2.json()

        start = time.time()

        query = f"SELECT {fields_query} FROM {schema}.{table_id};"
        j = 0
        processes = []
        with ThreadPoolExecutor(max_workers=n_worker) as executor:
            for start_row in range(n_worker):
                data = {"userId": self.userid,
                        "query": query, "sqlParameters": {},
                        "connectionId": None, "page": start_row, "size": size, "nativeType": False}
                processes.append(executor.submit(load_datatset_post, s,data,authentication_url2,j))
                j+=1

        dataset_df = pd.DataFrame()
        i=0
        for task in as_completed(processes):
            dataset_temp =pd.DataFrame([[clm['value'] for clm in row['cells']] for row in task.result()['data']],columns=entitie_code)
            #dataset_df = dataset_df.append(task.result(), ignore_index=True, sort=False)
            dataset_df = dataset_df.append(dataset_temp, ignore_index=True, sort=False)
            logging.info(f'dataframe {i}')
            i+=1

        logging.debug(f'Time taken: {time.time() - start}')

        return dataset_df


    def DeleteDataset(self):
        pass

    def CreateDatasetMySql(self, label, host, port, schema, userconn, pswconn, tablename):

        data1 = {
            "type": "MYSQL"
        }

        data2 = {
            "label": label,
            "icon": "https://storage.googleapis.com/askdata/datasets/icons/icoDataMySQL.png",
            "settings": {
                "datasourceUrl": f"jdbc:mysql://{host}:{port}/{schema}",
                "host": host,
                "port": port,
                "schema": schema,
                "username": userconn,
                "password": pswconn,
                "table_id": tablename},
            "plan": "NONE",
            "authRequired": False
        }

        with requests.Session() as s:

            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
            s.mount('https://', HTTPAdapter(max_retries=retries))

            authentication_url1 = self._base_url_askdata + '/smartbot/agents/' + self._agentId + '/datasets'
            r1 = s.post(url=authentication_url1, headers=self._headers, json=data1)
            r1.raise_for_status()

            datasetId = r1.json()['id']

            authentication_url2 = self._base_url_askdata + '/smartbot/agents/' + self._agentId + '/datasets/' + datasetId
            r2 = s.put(url=authentication_url2, headers=self._headers, json=data2)
            r2.raise_for_status()

        logging.info('--- ----------- -----')
        logging.info(f'--- Create Dataset with id: {datasetId}')

        return datasetId

    def __CreateDatasetDf(self, label):

        data1 = {
            "domain" : self._domainlogin,
            "type" : "DATAFRAME",
            "settings" : {},
            "agent" : self._agentId
        }

        data2 = {
            "label": label,
            "icon": "https://storage.googleapis.com/askdata/datasets/icons/icoDataPandas.png"
        }

        with requests.Session() as s:
            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
            s.mount('https://', HTTPAdapter(max_retries=retries))

            authentication_url1 = self._base_url_dataset + '/datasets'
            r1 = s.post(url=authentication_url1, headers=self._headers, json=data1)
            r1.raise_for_status()

            datasetId = r1.json()['id']
            settingDataset = r1.json()['settings']

            authentication_url2 = self._base_url_askdata + f'/smartdataset/datasets/{datasetId}/settings'
            r2 = s.put(url=authentication_url2, headers=self._headers, json=data2)
            r2.raise_for_status()

        #self.ExecuteDatasetSync(datasetId)

        logging.debug('--- ----------- -----')
        logging.debug(f'--- Create Dataset with id: {datasetId}')

        return datasetId, settingDataset

    def __discovery_datset(self, dataset_id,settings):
        # è sbagliato il data rifarlo
        data = {"settings":settings}

        # data2 = {
        #     "label": label,
        #     "icon": "https://storage.googleapis.com/askdata/datasets/icons/icoDataMySQL.png",
        #     "settings": {
        #         "datasourceUrl": f"jdbc:mysql://{host}:{port}/{schema}",
        #         "host": host,
        #         "port": port,
        #         "schema": schema,
        #         "username": userconn,
        #         "password": pswconn,
        #         "table_id": tablename},
        #     "plan": "NONE",
        #     "authRequired": False
        # }

        with requests.Session() as s:

            authentication_url2 = self._base_url_askdata + '/smartbot/agents/' + self._agentId + '/datasets/' + dataset_id
            r2 = s.put(url=authentication_url2, headers=self._headers, json=data)
            r2.raise_for_status()

        logging.info('--- ----------- -----')
        logging.info(f'--- Create Dataset with id: {datasetId}')

        return dataset_id