

from askdata_api_python_client.askdata  import Askdata
from askdata_api_python_client.askdata  import Agent


if __name__ == '__main__':

    username = 'g.demaio@askdata.com'
    password = 'g.demaio'


    Askdata = Askdata(username, password)

    # instatiate client
    client = Agent(Askdata)
    # login
    client.Login()
    # get list of Agents
    lista = client.GetAgents()
    # get id agent
    id_agent = client.GetAgent('AB_NYC_2019')
    print (id_agent)




