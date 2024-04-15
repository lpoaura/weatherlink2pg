# pip install pandas sqlalchemy

# Librairies et options :
import pandas as pd
import datetime
import requests
import json
import tqdm

from Codes import SecretsAPI, SecretsBDD

from sqlalchemy import create_engine
from sqlalchemy.types import Integer, BigInteger, JSON


# Informations sur l'API : https://weatherlink.github.io/v2-api/
APIKey = SecretsAPI.get('APIKey')
APISecret = SecretsAPI.get('APISecret')
StationID = SecretsAPI.get('StationID')


# Création des TS pour l'utilisation de l'option temps sur l'API :
# Date d'aujourd'hui à minuit :
Today = datetime.date.today()
TodayMidnight = datetime.datetime.combine(Today, datetime.time.min)

# Date du début de la station :
StartStation = datetime.datetime(2021, 9, 29, 0, 0)

# Convertir la date en timestamp
StartStationTimestamp = int(StartStation.timestamp())
TodayMidnightTimestamp = int(TodayMidnight.timestamp())


# Récupération des données : 
# DataFrame historiques :
dfHistorique = pd.DataFrame()

# Nb de jours à récupérer :
NbJours = int((TodayMidnightTimestamp - StartStationTimestamp) / 86400)

for i in tqdm.tqdm(range(NbJours)):
    StartTime = StartStationTimestamp + i * 86400
    EndTime = StartTime + 86400
    
    # Lien de la request : 
    link = 'https://api.weatherlink.com/v2/historic/{}?api-key={}&start-timestamp={}&end-timestamp={}'.format(StationID, APIKey, StartTime, EndTime)
    headers = {'X-Api-Secret' : APISecret}

    # Requête :
    r = requests.get(link, headers=headers)

    # Si la requête a réussi :
    if r.status_code == 200:
        # Lecture de la request en json :
        data = r.json()

        # Transformation en DF : 
        dfJour = pd.DataFrame(data)
        dfJour = dfJour[['station_id','sensors']]

        # Récupération des valeurs se trouvant dans sensors :
        dfSensors = pd.json_normalize(data['sensors'][0]['data'])
    
        # Récupération des json sur une colonne :
        dfJour = pd.DataFrame({'station_id': data['station_id'], 'infos_json' : data['sensors'][0]['data']})
    
        # Convertir les objets JSON en chaînes de caractères JSON :
        dfJour['infos_json'] = dfJour['infos_json'].apply(json.dumps)

        # Concat des données :
        dfJour = pd.concat([dfJour, dfSensors], axis=1)
        
        # Concaténation des données :
        dfHistorique = pd.concat([dfHistorique, dfJour], ignore_index=True)
    else:
        print("La requête {} a échoué avec le code d'erreur {}".format(link, r.status_code))



# Transfert sur PostgreSQL
# Paramètres de connexion à la base de données PostgreSQL en local :
host = SecretsBDD.get('host')
database = SecretsBDD.get('database')
user = SecretsBDD.get('user')
password = SecretsBDD.get('password')
nomtable = 'historiquemeteo'

# Création de la chaîne de connexion PostgreSQL :
conn_str = f"postgresql://{user}:{password}@{host}/{database}"

# Création de la connexion à la base de données PostgreSQL :
engine = create_engine(conn_str)

# Définir les types de données pour chaque colonne :
dtype = {'station_id': Integer(),
         'ts': BigInteger(),
         'infos_json': JSON}

# Insérer le DataFrame dans la base de données PostgreSQL :
dfHistorique.to_sql(nomtable, engine, if_exists = 'replace', index=False, dtype=dtype)

# Fermeture de la connexion :
engine.dispose()




