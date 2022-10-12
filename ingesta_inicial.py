#%% #Cargado de librerias

import yaml
import pickle
from datetime import date
from sodapy import Socrata

#%% Cargado de las credenciales

with open("/home/credentials.yaml", "r") as f:
    config = yaml.safe_load(f)


token = config['api_chicago']['api_token']
user = config['api_chicago']['user']
passwd = config['api_chicago']['password']
#%% Carga de los datos

chicago_dataset = "4ijn-s7e5" # codigo para el dataset de  inspecciones (food inspections)

def get_client():
    client = Socrata("data.cityofchicago.org",
                 token,
                 username=user,
                 password=passwd)
    return client
    
 #lectura de los datos desde la API   
def ingesta_inicial(chicago_dataset, client, limit):
    datasets = client.get(chicago_dataset, limit=limit, offset=0, order='inspection_date')
    return datasets
    
client = get_client()
datasets = ingesta_inicial(chicago_dataset, client, 300000)
    
#%% vereficar longitud de los datos
print(len(datasets))
print(datasets[0]['inspection_date'])
print(datasets[-1]['inspection_date'])

#%% guardar los datos en un disco montado en el servidor
TODAY = date.today()
archivo= "inspecciones-consecutivas-" + str(TODAY) + ".pkl"
file = open('/mnt/exdisk/STORAGE/'+archivo, 'wb')
pickle.dump(datasets, file)
file.close()
