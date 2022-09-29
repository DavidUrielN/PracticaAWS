#%% #Cargado de librerias
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import yaml
import pickle
import boto3

from datetime import date
from sodapy import Socrata
#%% #Cargado de las credenciales (Tanto de la API como de S3)
with open("credentials.yaml", "r") as f:
    config = yaml.safe_load(f)
#%%
# print(config)
# print(config['s3'][0])

# session = boto3.Session(aws_access_key_id = config['s3']['aws_access_key_id'],  aws_secret_access_key = config['s3']['aws_secret_access_key'] )
# s3 = session.resource("s3")

# print(session)

# este ya tiene un cambiio
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
    
 #lectura de los datos desde la API   
def ingesta_inicial(chicago_dataset, client, limit):
    datasets = client.get(chicago_dataset, limit=limit, offset=0, order='inspection_date')
    
    return datasets
    
client = get_client()
datasets = ingesta_inicial(chicago_dataset, client, 300000)
    
#%%
print(len(datasets))

print(datasets[0])


#Comienza el proceso para guardar los datos en el bucket de S3
def guardar_ingesta(bucket, bucket_path, dataset):
    session = boto3.Session(
        aws_access_key_id = config['s3']['aws_access_key_id'],
        aws_secret_access_key = config['s3']['aws_secret_access_key'],
        aws_session_token = config['s3']['aws_session_token'])
        
    s3 = session.resource('s3')
    print(session)
    s3.Object(bucket, bucket_path).put(Body=dataset)
    
TODAY = date.today()
pickle_data = pickle.dumps(datasets)

bucket = "bucket-practica-1"
key = "ingesta/inicial/inspecciones-historicas-" + str(TODAY) + ".pkl"

guardar_ingesta(bucket, key, pickle_data)


def ingesta_consecutiva(chicago_dataset, client, fecha, limit):
    new_dataset = client.get(chicago_dataset, limit=limit, where="inspection_date>='{}'".format(fecha))
    
    return new_dataset

client = get_client()
new_dataset = ingesta_consecutiva(chicago_dataset, client, '2020-11-03', 1000)
new_dataset[0]
#print(len(new_dataset))

pickled_new_data = pickle.dumps(new_dataset)

bucket = "bucket-practica-1"
key = "ingesta/consecutiva/inspecciones-consecutivas-" + str(TODAY) + ".pkl"

guardar_ingesta(bucket, key, pickled_new_data)