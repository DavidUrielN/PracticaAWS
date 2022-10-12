#%% #Cargado de librerias
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
import yaml
import pickle
# import boto3  #  es para interactuar con AWS S3
import pandas as pd

from datetime import date
from sodapy import Socrata
#%% Cargado de las credenciales (Tanto de la API como de S3)

with open("C:/Users/chale/Documents/Maestria_Ciencia_de_datos_aplicada/Trimestre_IV/Aplicaciones_de_ciencia_de_datos_I/credentials.yaml", "r") as f:
    config = yaml.safe_load(f)
# with open("/home/credentials.yaml", "r") as f:
#     config = yaml.safe_load(f)

#%% #credenciales de la API
token = config['api_chicago']['api_token']
user = config['api_chicago']['user']
passwd = config['api_chicago']['password']
#%%

# code for dataset of food inspections
chicago_dataset = "4ijn-s7e5"

def get_client():
    client = Socrata("data.cityofchicago.org",
                 token,
                 username=user,
                 password=passwd)
    
    return client
    


#%%
# open a file, where you stored the pickled data
file = open('/mnt/exdisk/STORAGE/inspecciones.pkl', 'rb')
print('Cargando dataset previo...')
# dump information to that file
datasets = pickle.load(file)

# close the file
file.close()

#%% ingesta consecutiva
def ingesta_consecutiva(chicago_dataset, client, fecha, limit):
    new_dataset = client.get(chicago_dataset, limit=limit, where="inspection_date>='{}'".format(fecha))
    
    return new_dataset

client = get_client()
print('Descargando nuevo dataset...')

new_dataset = ingesta_consecutiva(chicago_dataset, client, datasets[-1]['inspection_date'], 10000)
new_dataset[0]['inspection_date']
new_dataset[-1]['inspection_date']
print('Concatenando datasets y guardando')

datasets.extend(new_dataset[:])
df=pd.DataFrame(datasets)
df.drop_duplicates('inspection_id',inplace=True)
datasetComplete=df.values.tolist()
TODAY = date.today()
archivo= "inspecciones.pkl"
file = open('/mnt/exdisk/STORAGE/'+archivo, 'wb')
pickle.dump(datasetComplete, file)
file.close()
