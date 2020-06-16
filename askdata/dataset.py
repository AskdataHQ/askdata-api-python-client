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
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from askdata.askdata_client import Agent

from datetime import datetime

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

    def get_id_dataset_by_name(self, name_ds: str, exact=False):

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

    # Cancellare dopo aver verificato che load_datset_to_df va ok perchè possiamo riutilizzare __get_dataset_settings_info
    # per avere le stesse informazione di return

    # def __get_dataset_connection(self, datasetid):
    #
    #     s = requests.Session()
    #     s.keep_alive = False
    #     retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    #     s.mount('https://', HTTPAdapter(max_retries=retries))
    #
    #     dataset_url = self._base_url_dataset + '/datasets?agentId=' + self._agentId
    #     response = requests.get(url=dataset_url, headers=self._headers)
    #     response.raise_for_status()
    #     r = response.json()
    #     connection_df = pd.DataFrame([row['connection'] for row in r if row['id'] == datasetid])
    #     id_createdby = [row['createdBy'] for row in r if row['id'] == datasetid][0]
    #     return connection_df.table_id.item(), connection_df.schema.item(), id_createdby

    def load_entities_dataset(self, datasetid, select_custom=True):

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

        #table_id, schema, id_createdby = self.__get_dataset_connection(dataset_id)

        dataset_info = self.__get_dataset_settings_info(dataset_id, all_info=True)

        table_id = dataset_info["settings"]["table_id"]
        schema = dataset_info["settings"]["schema"]
        id_createdby = dataset_info["createdBy"]

        #check if userid/username (agent) is also the owner of the
        if id_createdby not in (self.userid, self.username):
            raise Exception("the user {} haven't privilege for this dataset".format(id_createdby))

        # select entities columnId and CODE of the dataset
        entitie_code, columnsid = self.load_entities_dataset(dataset_id, select_custom=False)

        fields_query = ", ".join([str(n) for n in [f"`{n}`" for n in columnsid]])
        size = 1000
        authentication_url2 = '{}/smartdataset/v2/datasets/{}/query'.format(self._base_url_askdata, dataset_id)

        # Check if this query it's correct with null
        query_count = "SELECT COUNT(`{}`) FROM `{}`.{} WHERE `{}` is not NULL;".format(columnsid[0], schema, table_id,
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

        query = "SELECT {} FROM `{}`.{};".format(fields_query, schema, table_id)
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

    def create_dataset_byconn(self, label: str, settings: dict, type="MYSQL"):

        data1 = {
            "type": type.upper()
        }
        if type.upper() == "MYSQL":
            icon = "https://storage.googleapis.com/askdata/datasets/icons/icoDataMySQL.png"

        data2 = {
            "label": label,
            "icon": icon,
            "settings": {
                "datasourceUrl": "jdbc:mysql://{}:{}/{}".format(settings['host'], settings['port'], settings['schema']),
                "host": settings['host'],
                "port": settings['port'],
                "schema": settings['schema'],
                "username": settings['username'],
                "password": settings['password'],
                "table_id": settings['table_id'],
                "enableValues": True,
                "importValues": False},
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

    def migration_dataset(self, agent_source: 'Agent', dataset_id_source: str):

        # da qui il codice

        datasets_setting = agent_source.__get_dataset_settings_info(dataset_id_source, all_info=True)
        label = datasets_setting['name']
        settings = datasets_setting['settings']
        type_dataset = datasets_setting['type']
        dataset_id_dest = self.create_dataset_byconn(label=label, settings=settings, type=type_dataset)
        dataset_entities_doc_source = agent_source.retrive_dataset_entities(dataset_id=dataset_id_source, dataset_type=type_dataset)

        # measures = datasets_setting['measures']
        # entitytypes = datasets_setting['entityTypes']
        for index, entity in enumerate(dataset_entities_doc_source["data"]):
            self.copy_entity_dataset(entity_code=entity['code'], dataset_id=dataset_id_dest, settigs_entity_source=entity,
                                     dataset_type=type_dataset)
            if entity["importValues"]:
                # verfica se questa entity ha importValues=true e legge i value da copiare

                entity_values_settings_source = agent_source.__get_value_entity(entity["code"])
                self.copy_values_entity_dataset(entity_code=entity['code'], dataset_id=dataset_id_dest,
                                                values_entity_list_source=entity_values_settings_source["data"])
        # for measure in measures:
        #     self.copy_entity_dataset(entity_code=measure['code'], dataset_id=dataset_id_dest,
        #                             settigs_entity= measure, entity_type='measure', dataset_type= type_dataset)

    def __get_dataset_settings_info(self, dataset_id: str, all_info=False) -> dict:

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        if all_info:
            dataset_url = self._base_url_askdata + '/smartdataset/datasets/' + dataset_id
        else:
            dataset_url = self._base_url_askdata + '/smartdataset/datasets/' + dataset_id + '/settings'

        # devo aggiungere anche le info di get self._base_url_askdata + '/smartdataset/datasets/' + dataset_id
        # alcuni field li trovo qui e bastqa mentre sinonimi etc li trovo su retrive_dataset_entities
        response = s.get(url=dataset_url, headers=self._headers)
        response.raise_for_status()
        dataset_document = response.json()

        return dataset_document

    def retrive_dataset_entities(self, dataset_id: str, dataset_type: str) -> dict:

        page = 0
        limit = 1000
        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        dataset_url = self._base_url_askdata + '/smartbot/dataset/type/' + dataset_type + '/id/' + dataset_id \
                          + '/subset/' + dataset_type + '?_page=' + str(page) + '&_limit=' + str(limit)
        response = s.get(url=dataset_url, headers=self._headers)
        response.raise_for_status()
        n_entity = response.json()['payload']['totalElements']
        entities_dataset = response.json()["payload"]
        if int(n_entity) > len(entities_dataset["data"]):
            raise NameError('not all entities are fetching')


        return entities_dataset

    def copy_entity_dataset(self, entity_code: str, dataset_id: str, settigs_entity_source: dict, dataset_type: str):

        if settigs_entity_source["custom"] == True:

            # crea una custom prima della put dell'entity
            data_custom = {"entry": [{"datasetId": dataset_id,
                                        "code": settigs_entity_source["code"],
                                        "enabled": True,
                                        "importValues": False,
                                        "custom": True,
                                        "mandatory": False,
                                        "parameterType": settigs_entity_source["parameterType"]
                                        }]}

            s = requests.Session()
            s.keep_alive = False
            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
            s.mount('https://', HTTPAdapter(max_retries=retries))

            dataset_url = self._base_url_askdata + '/smartbot/dataset/type/' + dataset_type + '/id/' + dataset_id \
                          + '/subset/' + dataset_type + '/entry'

            response = s.post(url=dataset_url, headers=self._headers, json=data_custom)
            response.raise_for_status()

        settigs_entities_dest = self.retrive_dataset_entities(dataset_id=dataset_id, dataset_type=dataset_type)

        for index, settigs_entity_dest in enumerate(settigs_entities_dest['data']):
                if settigs_entity_dest["code"] == entity_code:

                    settigs_entity = {
                    'schemaMetaData': settigs_entity_dest["schemaMetaData"],
                    'parameterType': settigs_entity_dest["parameterType"],
                    'code': settigs_entity_dest["code"],
                    'name': settigs_entity_source.get("name",settigs_entity_dest["code"]),
                    'description': settigs_entity_source.get("description", ""),
                    'synonyms': settigs_entity_source.get("synonyms", list()),
                    'icon': settigs_entity_source.get("icon", ""),
                    'sampleQueries': settigs_entity_source.get("sampleQueries", list()),
                    'importValues': settigs_entity_source.get("importValues", False),
                    'mandatory': settigs_entity_source.get("mandatory", False),
                    'enabled': settigs_entity_source.get("enabled", True),
                    'advancedConfiguration': settigs_entity_source.get("advancedConfiguration", dict()),
                    'custom': settigs_entity_source.get("custom",False),
                    'dynamicParameterValues': settigs_entity_source.get("dynamicParameterValues", list()),
                    'searchable': settigs_entity_source.get("searchable", False),
                    'nameTransformer': settigs_entity_source.get("nameTransformer", None),
                    'synonymTransformers': settigs_entity_source.get("synonymTransformers", None)}


                    # copia tutti i setting della entity con code entity_code nel dataset di destinazione
                    self.__put_entity_dataset(entity_code, dataset_id, settigs_entity,
                                              settigs_entity["parameterType"], dataset_type)

    def copy_values_entity_dataset(self, entity_code: str, dataset_id: str, values_entity_list_source: list):

        values_entity_list_source_dest = self.__get_value_entity(entity_code=entity_code)
        # salva i value copiati nel nuovo dataset di destinazione  to do: oder both values_entity_list_source_dest and values_entity_list_source and improve performce
        for index, value_entity_source in enumerate(values_entity_list_source):

            for index, value_entity_dest in enumerate(values_entity_list_source_dest['data']):
                if value_entity_dest["code"] == value_entity_source["code"]:

                    value_entity = {"_id": value_entity_dest["_id"],
                            "code": value_entity_dest["code"],
                            "datasetSync": value_entity_dest.get("datasetSync", list()),
                            "datasets": value_entity_dest["datasets"],
                            "description": value_entity_source.get("description", ""),
                            "details": value_entity_source.get("details", dict()),
                            "domain": self._domain,
                            "icon": value_entity_source.get("icon", ""),
                            "localizedName": value_entity_source.get("localizedName", dict()),
                            "localizedSynonyms": value_entity_source.get("localizedSynonyms", list()),
                            "name": value_entity_source.get("name", value_entity_dest["code"]),
                            "sampleQueries": value_entity_source.get("sampleQueries", list()),
                            "synonyms": value_entity_source["synonyms"],
                            "type": entity_code}

                    self.__put_value_entity(entity_code=entity_code, dataset_id=dataset_id,
                                    settings_value=value_entity)



    def __put_entity_dataset(self, entity_code: str, dataset_id: str, settigs_entity: dict, entity_type: str, dataset_type: str):

        """
        this method put fields in exist entity (measure/entityType)
        :param entity_code: str
        :param dataset_id: str
        :param settigs_entity: dict
        :param entity_type: str
            "ENTITY_TYPE" or "MEASURE"
        :param dataset_type: str
        :return: None
        """

        # data = {"entry": [{"datasetId": dataset_id,
        #                    "schemaMetaData": {"columnId": settigs_entity["schemaMetaData"]["columnId"],
        #                                       "columnName": settigs_entity["schemaMetaData"]["columnName"],
        #                                       "dataType": settigs_entity["schemaMetaData"]["dataType"],
        #                                       "dataExample": settigs_entity["schemaMetaData"]["dataExample"],
        #                                       "internalDataType": settigs_entity["schemaMetaData"]["internalDataType"],
        #                                       "indexedWith": settigs_entity["schemaMetaData"].get("indexedWith",None),
        #                                       "join": settigs_entity["schemaMetaData"].get("join",None),
        #                                       "details": settigs_entity["schemaMetaData"].get("details", dict())},
        #                    "parameterType": entity_type.upper(),
        #                    "code": settigs_entity["code"],
        #                    "name": settigs_entity.get("name", settigs_entity["code"]),
        #                    "description": settigs_entity.get("description", ""),
        #                    "synonyms": settigs_entity.get("synonyms", list()),
        #                    "icon": settigs_entity.get("icon", ""),
        #                    "sampleQueries": settigs_entity.get("sampleQueries", list()),
        #                    "importValues": settigs_entity.get("importValues", False),
        #                    "mandatory": settigs_entity.get("mandatory", False),
        #                    "enabled": settigs_entity.get("enabled", True),
        #                    #"locked": settigs_entity.get("locked", False),
        #                    "advancedConfiguration": settigs_entity.get("advancedConfiguration", dict()),
        #                    "custom": settigs_entity.get("custom",False),
        #                    "dynamicParameterValues": settigs_entity.get("searchable", list()),
        #                    "searchable": settigs_entity.get("searchable", False),
        #                    "nameTransformer": settigs_entity.get("nameTransformer", None),
        #                    "synonymTransformers": settigs_entity.get("synonymTransformers", None)

                           # questi vengono presi dal datasets get self._base_url_askdata + '/smartdataset/datasets/' + dataset_id
                           # "valid": settigs_entity.get("locale", True),
                           # "locale": settigs_entity.get("locale", ""),
                           # "filter": settigs_entity.get("filter", None),
                           # "customExpression": settigs_entity.get("customExpression", None),
                           # "format": settigs_entity.get("format", ""),
                           # "indexed": settigs_entity.get("indexed", False),
                           # "date": settigs_entity.get("date", False),
                           # "customAggregation": settigs_entity.get("customAggregation", ""),
                           # "ignoreAggregation": settigs_entity.get("ignoreAggregation", False),
                           # "injections": settigs_entity.get("injections", list()),
                           # "defaultInjections": settigs_entity.get("defaultInjections", list()),
                           # "geo": settigs_entity.get("geo", dict())
                          #}]}

        data = {'entry': [{'datasetId': dataset_id,
                    'schemaMetaData': {'columnId': settigs_entity["schemaMetaData"]["columnId"],
                                       'columnName': settigs_entity["schemaMetaData"]["columnName"],
                                       'dataType': settigs_entity["schemaMetaData"]["dataType"],
                                       'dataExample': settigs_entity["schemaMetaData"]["dataExample"],
                                       'internalDataType': settigs_entity["schemaMetaData"]["internalDataType"],
                                       'indexedWith': settigs_entity["schemaMetaData"].get("indexedWith",None),
                                       'join': settigs_entity["schemaMetaData"].get("join",None),
                                       'details': settigs_entity["schemaMetaData"].get("details", dict())},
                    'parameterType': entity_type.upper(),
                    'code': settigs_entity["code"],
                    'name': settigs_entity.get("name", settigs_entity["code"]),
                    'description': settigs_entity.get("description", ""),
                    'synonyms': settigs_entity.get("synonyms", list()),
                    'icon': settigs_entity.get("icon", ""),
                    'sampleQueries': settigs_entity.get("sampleQueries", list()),
                    'importValues': settigs_entity.get("importValues", False),
                    'mandatory': settigs_entity.get("mandatory", False),
                    'enabled': settigs_entity.get("enabled", True),
                    'advancedConfiguration': settigs_entity.get("advancedConfiguration", dict()),
                    'custom': settigs_entity.get("custom",False),
                    'dynamicParameterValues': settigs_entity.get("dynamicParameterValues", list()),
                    'searchable': settigs_entity.get("searchable", False),
                    'nameTransformer': settigs_entity.get("nameTransformer", None),
                    'synonymTransformers': settigs_entity.get("synonymTransformers", None)}]}

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        dataset_url = self._base_url_askdata + '/smartbot/dataset/type/' + dataset_type + '/id/' + dataset_id \
                      + '/subset/' + dataset_type + '/entry/' + entity_code

        response = s.put(url=dataset_url, headers=self._headers, json=data)
        response.raise_for_status()

    def __get_value_entity(self, entity_code: str) -> dict:

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartmanager/data/' + self._domain + '/entity/' + entity_code
        r = s.get(url=authentication_url, headers=self._headers, verify=True)
        r.raise_for_status()

        return r.json()["payload"]

    def __put_value_entity(self, entity_code: str, dataset_id: str, settings_value: dict):

        # settings_value = {"_id":"884EF2BE-5174-4490-9846-0DB7269C538A-CITTA-Genova", "code":"Genova", "name":"Genova",
        #                   "synonyms":["genova","genova_test","genova_test2","genova_test3","genova_test44"]}

        # la macanza o non di field non mandatory

        domain = self._domain
        language = self._language

        # filling of the body request for the entity value
        data = {"_id": settings_value["_id"],
                "code": settings_value["code"],
                "datasetSync": settings_value.get("datasetSync", list()),
                "datasets": settings_value["datasets"],
                "description": settings_value.get("description", ""),
                "details": settings_value.get("details", dict()),
                "domain": domain,
                "icon": settings_value.get("icon", ""),
                "localizedName": settings_value.get("localizedName", dict()),
                "localizedSynonyms": settings_value.get("localizedSynonyms", list()),
                "name": settings_value.get("name", settings_value["code"]),
                "sampleQueries": settings_value.get("sampleQueries", list()),
                "synonyms": settings_value["synonyms"],
                "type": entity_code}

        # [{
        #     "sourceValue": [{"language": language, "value": settings_value["code"]}],
        #     "sourceValueId": settings_value["code"],
        #     "datasetId": dataset_id}]

        # data = {'_id': '0a82e596-b6e9-45d7-ba0e-ec75b974b752-CITTA-Pescara', 'code': 'Pescara', 'datasetSync': [
        #     {'datasetId': '0a82e596-b6e9-45d7-ba0e-ec75b974b752-MYSQL-17ff8411-5077-4add-b3d0-544f841a352d'},
        #     {'datasetId': '0a82e596-b6e9-45d7-ba0e-ec75b974b752-MYSQL-8fb5566c-bc89-40c2-bd9d-a4d1733c61b6'}],
        #  'datasets': [{'sourceValue': [{'language': 'en', 'value': 'Pescara'}], 'sourceValueId': 'Pescara',
        #                'datasetId': '0a82e596-b6e9-45d7-ba0e-ec75b974b752-MYSQL-17ff8411-5077-4add-b3d0-544f841a352d'},
        #               {'sourceValue': [{'language': 'en', 'value': 'Pescara'}], 'sourceValueId': 'Pescara',
        #                'datasetId': '0a82e596-b6e9-45d7-ba0e-ec75b974b752-MYSQL-8fb5566c-bc89-40c2-bd9d-a4d1733c61b6'}],
        #  'description': '', 'details': {}, 'domain': '0a82e596-b6e9-45d7-ba0e-ec75b974b752', 'icon': '',
        #  'localizedName': {}, 'localizedSynonyms': [], 'name': 'Pescara', 'sampleQueries': [],
        #  'synonyms': ['pescara', 'pescara_test'], 'type': 'CITTA'}

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_askdata + '/smartmanager/data/' + self._domain + '/entity/' + entity_code

        # put field into exist value
        r = s.put(url=authentication_url, headers=self._headers, verify=True, json=data)
        r.raise_for_status()

# il code è il columnValueId cioè il colimValue il campo preso dalla tabella ripulito e poi trimmato
# la logica usata per ripulirlo è tramite l'uso di queste regex
# value = value.replaceAll("[\uFEFF-\uFFFF]", "").replaceAll("\\p{C}", "").replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
# poi trimmato

#code = columValueId.dirtyChars().trim()
#colomValueId = columValue

# aggiungere il get del dict che in caso di assenza ritorni un valore di default
# chiedere a fra perchè la get del dataset mi ritorna un sacco di campi con null