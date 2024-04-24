# Librairies et options :
# Divers :
import pandas as pd
import datetime
import requests
import json
import tqdm

# Importer les codes depuis un fichier .env :
from dotenv import load_dotenv
import os
load_dotenv()

# Création et lecture des BDD postgresl :
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, BigInteger, JSON



# Clés API et BDD :
# Informations API : https://weatherlink.github.io/v2-api/
# Clés API :
APIKey = os.getenv("APIKey")
APISecret = os.getenv("APISecret")
stationID = os.getenv("stationID")

# Paramètres de connexion à la base de données PostgreSQL en local :
host = os.getenv("host")
database = os.getenv("database")
user = os.getenv("user")
password = os.getenv("password")
nomTable = os.getenv("nomTable")



# Création des TS pour l'utilisation de l'option temps sur l'API :
# Date d'aujourd'hui à minuit :
today = datetime.date.today()
todayMidnight = datetime.datetime.combine(today, datetime.time.min)

# Date du début de la station :
startStation = datetime.datetime(2021, 9, 29, 0, 0)

# Convertir la date en timestamp
startStationTimestamp = int(startStation.timestamp())
todayMidnightTimestamp = int(todayMidnight.timestamp())



# Récupération des données : 
# DataFrame historiques :
dfHistorique = pd.DataFrame()

# Nb de jours à récupérer :
nbJours = int((todayMidnightTimestamp - startStationTimestamp) / 86400)

for i in tqdm.tqdm(range(nbJours)):
    startTime = startStationTimestamp + i * 86400
    endTime = startTime + 86400
    
    # Lien de la request : 
    link = 'https://api.weatherlink.com/v2/historic/{}?api-key={}&start-timestamp={}&end-timestamp={}'.format(stationID, APIKey, startTime, endTime)
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
# Création de la chaîne de connexion PostgreSQL :
connStr = f"postgresql://{user}:{password}@{host}/{database}"

# Création de la connexion à la base de données PostgreSQL :
engine = create_engine(connStr)

# Définir les types de données pour chaque colonne :
dtype = {'station_id': Integer(),
         'ts': BigInteger(),
         'infos_json': JSON}

# Insérer le DataFrame dans la base de données PostgreSQL :
dfHistorique.to_sql(nomTable, engine, if_exists = 'replace', index=False, dtype=dtype)

# Fermeture de la connexion :
engine.dispose()




