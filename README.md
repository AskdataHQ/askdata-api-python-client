# README #

This README list steps necessary to get your application seamlessly integrated with Askdata.

* Askdata python client 
* Version: 0.1
* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)

### How do I get set up? ###

Example of usage:

from askdata.client import Askdata

from askdata.client import Askdata

client = Askdata.authenticate('APP_SECRET_KEY', 'PASSOWRD')
...
client = Askdata.login('username@sss...', '***passowrd**')

agent = client.init_index('agent_id')

''' Priority '''
agent.executeInsightAutomation([{'insight_id': '235e43q234'}])
agent.synchronizeDataset([{'dataset_id': '235e43q234'}])

''' After '''
agent.deleteInsights([{'feed_id': '235e43q234'}])
agent.createInsight([{'feed_id': '235e43q234'}, 'insight': (new Insight ('title': 'Latest ressult', 'components' :[myTable1]  ))])

''' '''
myTable1 = askdata.dataframeToComponentTable(dataset1)



### Who do I talk to? ###

* This repository is mantained by the Askdata data science team datascience@askdata.com