import requests
import yaml
import os
import pandas as pd
from pandas import DataFrame
import numpy as np
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
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

# askdata.Askdata
class Insight:

    '''
    Insight Object
    '''

    _agentId = None
    _domain = None

    def __init__(self, env, token):

        self._headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer" + " " + token
        }

        if env == 'dev':
            self._base_url_insight = url_list['BASE_URL_INSIGHT_DEV']
        if env == 'qa':
            self._base_url_insight = url_list['BASE_URL_INSIGHT_QA']
        if env == 'prod':
            self._base_url_insight = url_list['BASE_URL_INSIGHT_PROD']

    def load_rules(self):

        insight_url = self._base_url_insight + '/' + 'rules' + '/' + '?agentId=' + self._agentId + '&page=0&limit=100000'
        response = requests.get(url=insight_url, headers=self._headers)
        response.raise_for_status()
        r = response.json()

        df_rules = pd.DataFrame(r['data'])

        return df_rules

    def execute_rule(self, id_insight):

        insight_url = self._base_url_insight + '/' + 'rules' + '/' + id_insight + '/produceAndSend'
        r = requests.post(url=insight_url, headers=self._headers)
        r.raise_for_status()

        return r

    def execute_rules(self, listid_insight):

        data = listid_insight

        insight_url = self._base_url_insight + '/' + 'insight' + '/produceAndSendAsync'
        r = requests.post(url=insight_url, headers=self._headers, json=data)

        r.raise_for_status()

        return

    def create_rule(self, insight):
        # insight is json with specific mandatory fileds ...
        data = insight

        s = requests.Session()
        s.keep_alive = False
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))

        authentication_url = self._base_url_insight + '/rules'
        r = s.post(url=authentication_url, headers=self._headers, json=data)
        r.raise_for_status()
        return r

    def migration_insight(self, agent_source, insights_source_: DataFrame) -> DataFrame:

        insights_source = insights_source_.drop(columns=['createdAt', 'createdBy', 'id'])
        insights_source.drop(insights_source[(insights_source["name"] == 'Sample rule') & (
                    insights_source["process"] == 'viewAnalytics') & (insights_source["type"] == 'ANALYTICS')].index, inplace=True)

        insights_source = insights_source.copy()
        for row in insights_source.itertuples():

            Ind = row.Index
            for name_elem, elem in zip(row._fields, row):
                # Set a channel list on every document update
                if type(elem) == list:
                    if(len(elem)>0):
                        if type(elem[0]) == dict:
                            replace_elem = []
                            for d in elem:

                                replace_elem_ = {k: v if v is None else v.replace(agent_source._domain, self._domain)
                                if k != "query" else v.replace(agent_source._agentId.lower(), self._agentId.lower())
                                                for (k, v) in d.items()}
                                replace_elem.append(replace_elem_)

                            insights_source.at[Ind, name_elem] = replace_elem
                        else:
                            replace_elem_ = [str(n).replace(agent_source._agentId, self._agentId) for n in elem]
                            if replace_elem_.sort() == elem.sort():
                                replace_elem = [str(n).replace(agent_source._domain, self._domain) for n in replace_elem_]
                            else:
                                replace_elem = replace_elem
                            insights_source.loc[Ind, name_elem] = replace_elem

                if type(elem) == dict:

                    replace_elem = {k: v.replace(agent_source._agentId.lower(), self._agentId.lower())
                    if type(v)==str else v for (k, v) in elem.items()}

                    # replace_elem = {k: v.replace(agent_source._domain.lower(), self._domain.lower())
                    #     if type(v)==str else v for (k, v) in replace_elem_.items()}

                    insights_source.at[Ind, name_elem] = replace_elem

                if type(elem) == str and type(elem) != int and type(elem) != bool:
                    replace_elem_ = elem.replace(agent_source._agentId, self._agentId)
                    if replace_elem_ == elem:
                        replace_elem = replace_elem_.replace(agent_source._domain, self._domain)
                    else:
                        replace_elem = replace_elem_

                    insights_source.loc[Ind,name_elem] = replace_elem
                else:
                    pass

        insight_records = insights_source.to_dict(orient='records')
        for insight_record in insight_records:
            try:
                self.create_rule(insight_record)
                logging.info(
                    'migrate to agenteId {} --> ruleId: {}'.
                        format(self._agentId,
                               str(insight_record["domain"]) + "-" + str(insight_record["type"]) + "-" + str(insight_record["code"])))
                logging.info('------ ------- --------------')
            except:

                logging.info('--------- ruleId already exist ------------')
                logging.info('------ ------- --------------')
                insight_record['code'] = insight_record['code'] + '_' + 'D' + '{}'.format(datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S"))

                self.create_rule(insight_record)
                logging.info('migrate with new ruleId: {}'.format(str(insight_record["domain"]) + "-" + str(insight_record["type"]) + "-" + str(insight_record["code"])))
        return insights_source