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
#import askdata.askdata as askdata

_LOG_FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] - %(asctime)s --> %(message)s"
g_logger = logging.getLogger()
logging.basicConfig(format=_LOG_FORMAT)
g_logger.setLevel(logging.INFO)

root_dir = os.path.abspath(os.path.dirname(__file__))
# retrieving base url
yaml_path = os.path.join(root_dir, '../askdata/askdata_config/base_url.yaml')
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
            "Authorization": "Bearer" + " " + token
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

    def load_datasets(self):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        dataset_url = self._base_url_dataset + '/datasets?agentId=' + self._agentId
        response = s.get(url=dataset_url, headers=self._headers)
        response.raise_for_status()
        r = response.json()
        r_df = pd.DataFrame(r)

        datasets_df = r_df.loc[:, ['id', 'domain', 'type', 'code', 'name', 'description', 'createdBy', 'isActive',
                                   'accessType', 'icon', 'version', 'syncCount', 'visible', 'public', 'createdAt']]

        return datasets_df

    def get_id_dataset_by_name(self, name_ds, exact=False):

        '''
        Get askdata dataset ids by name

        :param name_ds: String
        it's name searched
        :param exact: Boolean
        if param is true the method search the dataset id with exact match whereas if param is False
        it searches dataset ids that contain the name_ds
        :return: Array
                '''

        dataset_list = self.load_datasets()

        if not exact:
            dataset_select_name = dataset_list.name.str.contains(name_ds, flags=re.IGNORECASE, regex=True)
            dataset_choose = dataset_list[dataset_select_name]
        else:
            dataset_choose = dataset_list[dataset_list['name'] == name_ds]

        #return an array
        return dataset_choose.id.values

    def __get_dataset_connection(self, datasetid):

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

    def load_entities_dataset(self, datasetid, select_custom = True):

        df_datasets = self.load_datasets()
        dataset_info = df_datasets[df_datasets['id'] == datasetid]
        with requests.Session() as s:

            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
            s.mount('https://', HTTPAdapter(max_retries=retries))

            authentication_url = '{}/smartbot/dataset/type/{}/id/{}/subset/{}?_page=0&_limit=1000'.format(
                self._base_url_askdata,dataset_info.type.item(),dataset_info.id.item(),dataset_info.type.item())

            r = s.get(url=authentication_url, headers=self._headers)
            r.raise_for_status()

        # get all entity
        entities_df = pd.DataFrame(r.json()['payload']['data'])

        # copy entity not custom
        entities_df_no_cust = entities_df[entities_df['custom'] == False].copy()
        index_nocust = entities_df_no_cust.index

        # select columnid only with custom = false
        columnsid = [row['columnId'] for row in entities_df.loc[index_nocust,'schemaMetaData']]
        entitie_code = entities_df.code.tolist()

        #select code of entities with custom = False
        if select_custom == False:
            entitie_code = entities_df.loc[index_nocust, 'code'].tolist()

        return entitie_code, columnsid

    def execute_dataset_sync(self, dataset_id):

        dataset_url = self._base_url_dataset + '/datasets/' + dataset_id + '/sync'
        r = requests.post(url=dataset_url, headers=self._headers)
        r.raise_for_status()
        return r

    def __ask_db_engine(self, dataset_id, setting):
        # request credential

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartdataset/datasets/' + dataset_id + '/onetimecredentials'

        response = s.get(url=authentication_url, headers=self._headers)
        response.raise_for_status()
        r = response.json()

        host = setting['datasourceUrl'].split('/')[2].split(':')[0]
        port = setting['datasourceUrl'].split('/')[2].split(':')[1]

        database_engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.
                                                   format(r['mysqlUsername'], r['mysqlPassword'], host, port,
                                                          setting['schema']), pool_recycle=3600, pool_size=5)

        db_tablename = r['mysqlTable']

        return database_engine, db_tablename

    def __ask_del_db_engine(self, dataset_id):

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartdataset/datasets/' + dataset_id + '/revokeonetimecredentials'
        # dataset_url = 'https://smartsql-dev.askdata.com/custom/create'
        response = s.delete(url=authentication_url, headers=self._headers)
        response.raise_for_status()
        logging.debug('---------------------------')
        logging.debug('-------delete mysqluser for dataset {}------'.format(dataset_id))


    def save_to_dataset(self, frame, dataset_name, add_indexdf = False, indexclm = [], if_exists='Replace'):

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
        dataset_id, settings_dataset = self.__create_dataset_df(dataset_name)
        engine, db_tablename = self.__ask_db_engine(dataset_id, settings_dataset)


        # with "with" we can close the connetion when we exit
        with engine.connect() as connection:

            # to check type of column of the Dataframa for creating a correct and performing table structure

            dtype_table = dict()
            for clm in frame.select_dtypes(include=np.object).columns:
                maxLen = frame[clm].str.len().max()
                dtype_table[clm] = VARCHAR(length=maxLen + 10)
            for clm in frame.select_dtypes(include=[np.datetime64]).columns:
                dtype_table[clm] = DateTime()

            if not indexclm:
                frame.to_sql(con=connection, name=db_tablename, if_exists='replace', chunksize=1000, index=add_indexdf,
                             index_label='INDEX_DF',
                             method='multi',dtype=dtype_table)
            else:
                frame.to_sql(con=connection, name=db_tablename, if_exists='replace', chunksize=1000,
                             method='multi',index=add_indexdf, index_label='INDEX_DF', dtype=dtype_table)

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
        logging.info('--- Save the Dataframe into Dataset {}'.format(dataset_name))


        #run discovery dataset
        #self.__discovery_datset(dataset_id,settingsDataset)
        # -----------------------------------
        self.execute_dataset_sync(dataset_id)

        # delete mysql user
        self.__ask_del_db_engine(dataset_id)



    # def update_dataset(self, frame, tablename, if_exists='rename'):
    # to do
    #     pass

    def load_dataset_to_df(self, dataset_id):

        '''
        read askdata dataset by datasetId and return data frame

        :param dataset_id: String
        id of dataset Askdata
        :return: DataFrame
        '''

        table_id, schema, id_createdby = self.__get_dataset_connection(dataset_id)

        #check if userid/username (agent) is also the owner of the
        if id_createdby not in (self.userid, self.username):
            raise Exception("the user {} haven't privilege for this dataset".format(id_createdby))

        # select entities columnId and CODE of the dataset
        entitie_code, columnsid = self.load_entities_dataset(dataset_id, select_custom=False)

        fields_query = ", ".join([str(n) for n in [f"`{n}`" for n in columnsid]])
        size = 1000
        authentication_url2 = '{}/smartdataset/v2/datasets/{}/query'.format(self._base_url_askdata, dataset_id)

        query_count = "SELECT COUNT(`{}`) FROM {}.{} WHERE `{}` is not NULL;".format(columnsid[0], schema, table_id,
                                                                                     columnsid[0])

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

            logging.debug('Thread_{} : starting update'.format(str(indice)))

            r2 = session.post(url=authentication_url2, headers=self._headers, json=data)
            r2.raise_for_status()
            logging.debug('Thread_{}: finishing update'.format(str(indice)))

            return r2.json()

        start = time.time()

        query = "SELECT {} FROM {}.{};".format(fields_query, schema, table_id)
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
            logging.debug('dataframe {}'.format(str(i)))
            i += 1

        logging.debug('Time taken: {}'.format(time.time() - start))
        logging.info('---------- -----------------------')
        logging.info('----Load Dataset to DataFrame ------')

        return dataset_df


    # def delete_dataset(self):
    #     pass

    def create_dataset_byconn(self, label, host, port, schema, userconn, pswconn, tablename,type="MYSQL"):

        data1 = {
            "type": type
        }

        data2 = {
            "label": label,
            "icon": "https://storage.googleapis.com/askdata/datasets/icons/icoDataMySQL.png",
            "settings": {
                "datasourceUrl": "jdbc:mysql://{}:{}/{}".format(host, port, schema),
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
        logging.info('--- Create Dataset with id: {}'.format(datasetId))

        return datasetId

    def __create_dataset_df(self, label):

        data1 = {

            "type": "DATAFRAME"

        }

        data2 = {
            "label": label,
            "icon": "https://storage.googleapis.com/askdata/datasets/icons/icoDataPandas.png"
        }

        with requests.Session() as s:
            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
            s.mount('https://', HTTPAdapter(max_retries=retries))

            # create askdata dataset of type dataframe
            authentication_url1 = self._base_url_askdata + '/smartbot/agents/' + self._agentId + '/datasets'
            r1 = s.post(url=authentication_url1, headers=self._headers, json=data1)
            r1.raise_for_status()

            # add name and icon to settings
            datasetId = r1.json()['id']
            settingDataset = r1.json()['settings']

            authentication_url2 = self._base_url_askdata + '/smartdataset/datasets/{}/settings'.format(datasetId)
            r2 = s.put(url=authentication_url2, headers=self._headers, json=data2)
            r2.raise_for_status()

        logging.debug('--- ----------- -----')
        logging.debug('--- Create Dataset with id: {}'.format(str(datasetId)))

        return datasetId, settingDataset
